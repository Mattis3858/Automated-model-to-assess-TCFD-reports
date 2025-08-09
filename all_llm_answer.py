# -*- coding: utf-8 -*-
import os
import pandas as pd
from glob import glob
from typing import List, Optional, Tuple, Dict
from dotenv import load_dotenv
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential

from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate

#  PROMPT
from prompt.V1 import PROMPT


INPUT_DIR = "data/handroll_query_result"
INPUT_PATTERN = "*_output_chunks.csv"   
POS_EXAMPLE_SOURCE = "data/2023_query_result/富邦金控_2023_output_chunks_fewshot_with_CoT_v1_few_shot.csv"

GUIDELINES_USE_DEFINITION_AS_LABEL = True  # PROMPT 的 {label} 位置是否顯示 Definition（通常 True）

MODEL_NAME = "gpt-4o-mini"
MAX_WORKERS = 10              
SKIP_IF_OUTPUT_EXISTS = True  

# ===== 欄位名稱常數 =====
COL_CHUNK = "Chunk Text"
COL_LABEL = "Label"
COL_DEF   = "Definition"
COL_PE1   = "Positive Example1"
COL_PE2   = "Positive Example2"
COL_REASON = "reasoning"
COL_YN     = "是否真的有揭露此標準?(Y/N)"
COL_COMPANY= "Company"
COL_RANK   = "Rank"

# ===== 輸出檔名樣式（對齊你的彙整碼 PATTERN） =====
OUTPUT_SUFFIX = "_output_chunks_fewshot_with_CoT_v1_few_shot.csv"

# ===== Pydantic 資料模型 =====
class Result(BaseModel):
    reasoning: Optional[str] = None
    is_disclosed: Optional[str] = None

class ResultList(BaseModel):
    result: List[Result]

# ===== 工具：從富邦檔萃取 Label→(PE1,PE2) 對應 =====
def first_nonempty(series: pd.Series) -> str:
    for x in series:
        s = str(x) if x is not None else ""
        if s.strip():
            return s
    return ""

def load_pos_examples_from_verified(path: str) -> Dict[str, Tuple[str, str]]:
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        df = pd.read_excel(path, dtype=str)
    else:
        # 預設當 CSV
        df = pd.read_csv(path, dtype=str)

    # 正常應包含 Label, Positive Example1, Positive Example2
    needed = {COL_LABEL, COL_PE1, COL_PE2}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(
            f"正例來源檔缺少必要欄位：{missing}。請確認 {path} 來自人工驗證後的富邦輸出檔。"
        )

    df = df[[COL_LABEL, COL_PE1, COL_PE2]].fillna("")
    # 以 Label 群組，取每欄第一個非空值
    agg = (
        df.groupby(COL_LABEL, as_index=True)
          .agg({COL_PE1: first_nonempty, COL_PE2: first_nonempty})
    )
    # 轉為 dict
    pe_map = {}
    for label, row in agg.iterrows():
        pe_map[str(label)] = (str(row[COL_PE1]) or "", str(row[COL_PE2]) or "")
    return pe_map

# ===== 組合提示 =====
def get_prompt(chunk: str, standard_text_for_label: str, pos1: str, pos2: str) -> str:
    return PROMPT.format(
        chunk=chunk,
        label=standard_text_for_label,  # PROMPT 的 {label} 這裡放 Definition（或 Label 代碼，依你的配置）
        positive_example1=pos1,
        positive_example2=pos2,
    )

# ===== 建鏈（只建一次） =====
def build_chain(api_key: str):
    llm = ChatOpenAI(model=MODEL_NAME, api_key=api_key, temperature=0)
    parser = PydanticOutputParser(pydantic_object=ResultList)
    prompt_template = ChatPromptTemplate.from_messages([
        ("system", "你是一位專業的 TCFD 揭露標準判讀專家。請嚴格依 JSON 結構輸出。"),
        ("human", "{input}"),
    ])
    return prompt_template | llm | parser

# ===== 呼叫鏈（退避重試） =====
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=30))
def call_chain(chain, chunk: str, standard_text_for_label: str, pos1: str, pos2: str) -> dict:
    prompt = get_prompt(chunk, standard_text_for_label, pos1, pos2)
    resp = chain.invoke({"input": prompt})
    return resp.model_dump()

# ===== 由檔名推公司，決定輸出路徑 =====
def infer_company_and_output_path(input_path: str) -> Tuple[str, str]:
    base = os.path.basename(input_path)
    company = base.split("_output_chunks")[0]
    if "_output_chunks" in base:
        out_base = base.replace("_output_chunks", OUTPUT_SUFFIX.replace(".csv", ""))
    else:
        name_no_ext = os.path.splitext(base)[0]
        out_base = f"{name_no_ext}_with_CoT_v1_few_shot.csv"
    if not out_base.lower().endswith(".csv"):
        out_base += ".csv"
    out_path = os.path.join(os.path.dirname(input_path), out_base)
    return company, out_path

# ===== 把富邦正例映射灌回各公司 df =====
def attach_positive_examples(df: pd.DataFrame, pe_map: Dict[str, Tuple[str, str]]) -> pd.DataFrame:
    # 確保欄位存在
    if COL_PE1 not in df.columns:
        df[COL_PE1] = ""
    if COL_PE2 not in df.columns:
        df[COL_PE2] = ""
    df[COL_PE1] = df[COL_PE1].fillna("")
    df[COL_PE2] = df[COL_PE2].fillna("")

    # 只把空白的補上（若原本就有，尊重原值）
    if COL_LABEL in df.columns:
        mask1 = df[COL_PE1].eq("")
        mask2 = df[COL_PE2].eq("")
        df.loc[mask1, COL_PE1] = df.loc[mask1, COL_LABEL].map(lambda k: pe_map.get(k, ("",""))[0]).fillna("")
        df.loc[mask2, COL_PE2] = df.loc[mask2, COL_LABEL].map(lambda k: pe_map.get(k, ("",""))[1]).fillna("")
    return df

# ===== 補齊 Company / Rank / reasoning / YN =====
def ensure_util_columns(df: pd.DataFrame, company: str) -> pd.DataFrame:
    if COL_COMPANY not in df.columns:
        df[COL_COMPANY] = company
    else:
        df[COL_COMPANY] = df[COL_COMPANY].fillna(company).replace("", company)

    if COL_REASON not in df.columns:
        df[COL_REASON] = ""
    if COL_YN not in df.columns:
        df[COL_YN] = ""

    # 產 Rank：每個 Label 內依出現順序 1..N（你的查詢本來就是 Top-N 依序 append，這樣就等同於 Rank）
    if COL_RANK not in df.columns:
        if COL_LABEL in df.columns:
            df[COL_RANK] = df.groupby(COL_LABEL).cumcount() + 1
        else:
            df[COL_RANK] = range(1, len(df) + 1)
    return df

# ===== 單檔處理 =====
def process_one_file(path: str, chain, pe_map: Dict[str, Tuple[str, str]]):
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        # 若不是 CSV，嘗試 xlsx（理論上查詢輸出檔是 CSV）
        try:
            df = pd.read_excel(path, dtype=str).fillna("")
        except Exception as e:
            print(f"[ERROR] 讀檔失敗：{os.path.basename(path)} → {e}")
            return

    company, out_path = infer_company_and_output_path(path)
    if SKIP_IF_OUTPUT_EXISTS and os.path.exists(out_path):
        print(f"[SKIP] 已存在輸出：{os.path.basename(out_path)}")
        return

    # 灌正例 + 補欄位
    df = attach_positive_examples(df, pe_map)
    df = ensure_util_columns(df, company)

    # 準備任務：允許正例為空；只要 chunk 與標準文字存在就跑
    tasks = []
    for idx, row in df.iterrows():
        chunk = str(row.get(COL_CHUNK, "") or "")
        # PROMPT 的 {label} 內容：預設用 Definition，若空再用 Label
        label_text_for_prompt = (
            str(row.get(COL_DEF, "") or "").strip()
            if GUIDELINES_USE_DEFINITION_AS_LABEL else ""
        )
        if not label_text_for_prompt:
            label_text_for_prompt = str(row.get(COL_LABEL, "") or "").strip()

        if not (chunk and label_text_for_prompt):
            continue

        pos1 = str(row.get(COL_PE1, "") or "")
        pos2 = str(row.get(COL_PE2, "") or "")
        tasks.append((idx, chunk, label_text_for_prompt, pos1, pos2))

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(call_chain, chain, chunk, label_text_for_prompt, pos1, pos2): idx
            for (idx, chunk, label_text_for_prompt, pos1, pos2) in tasks
        }
        for fut in tqdm(as_completed(futures), total=len(futures), desc=os.path.basename(path)):
            idx = futures[fut]
            try:
                data = fut.result()
                if data and data.get("result"):
                    first = data["result"][0]
                    reasoning = (first.get("reasoning") or "").strip()
                    yn = (first.get("is_disclosed") or "").strip().upper()
                    yn = "Y" if yn == "Y" else "N"
                    results.append((idx, reasoning, yn, None))
                else:
                    results.append((idx, "", "N", "Empty parser result"))
            except Exception as e:
                results.append((idx, "", "N", f"API error: {e}"))

    for idx, reasoning, yn, err in results:
        df.at[idx, COL_REASON] = reasoning if not err else err
        df.at[idx, COL_YN] = yn

    # 輸出（UTF-8-SIG 方便 Excel）
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[SUCCESS] 輸出：{out_path}")

# ===== 主程式 =====
def main():
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("請在 .env 設定 OPENAI_API_KEY")

    # 從「富邦」檔萃取正例映射
    pe_map = load_pos_examples_from_verified(POS_EXAMPLE_SOURCE)
    if not pe_map:
        print("[WARN] 正例映射為空，將在無 few-shot 的情況下判讀。")

    # 建立 LLM 鏈
    chain = build_chain(api_key)

    # 掃描所有原始查詢輸出檔
    paths = sorted(glob(os.path.join(INPUT_DIR, INPUT_PATTERN)))
    if not paths:
        print(f"[ERROR] 在 {INPUT_DIR} 找不到 {INPUT_PATTERN}")
        return

    print(f"[INFO] 共找到 {len(paths)} 個輸入檔")
    for p in paths:
        process_one_file(p, chain, pe_map)

if __name__ == "__main__":
    main()
