import os
import pandas as pd
from glob import glob
from dotenv import load_dotenv
from tqdm.auto import tqdm
from tenacity import retry, stop_after_attempt
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
from pydantic import BaseModel

from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate

from prompt.V1 import PROMPT  
# ===== 可調參數 =====
INPUT_DIR = "data/handroll_query_result"
PATTERN = "*_output_chunks.csv"  
GUIDELINES_PATH = "data/tcfd第四層揭露指引.xlsx"
GUIDELINES_SHEET = "工作表2"
MODEL_NAME = "gpt-4o-mini"
MAX_WORKERS = 8

class Result(BaseModel):
    reasoning: Optional[str] = None
    is_disclosed: Optional[str] = None

class ResultList(BaseModel):
    result: List[Result]

def load_pos_examples(path: str, sheet: str):
    df = pd.read_excel(path, sheet_name=sheet, dtype=str).fillna("")
    if "Label" not in df.columns:
        raise ValueError("指引檔缺少必要欄位：Label")
    has_single = "Positive Example" in df.columns
    pos_map = {}
    for _, row in df.iterrows():
        label = str(row["Label"]).strip()
        if not label:
            continue
        if has_single:
            pos_map[label] = (str(row.get("Positive Example", "") or ""), "")
        else:
            pos_map[label] = (
                str(row.get("Positive Example1", "") or ""),
                str(row.get("Positive Example2", "") or "")
            )
    return pos_map

def get_prompt(chunk: str, label: str, positive_example1: str, positive_example2: str) -> str:
    return PROMPT.format(
        chunk=chunk, label=label,
        positive_example1=positive_example1,
        positive_example2=positive_example2
    )

@retry(stop=stop_after_attempt(3))
def call_llm(llm: ChatOpenAI, chunk: str, label: str, pos1: str, pos2: str):
    parser = PydanticOutputParser(pydantic_object=ResultList)
    prompt_template = ChatPromptTemplate.from_messages([
        ("system", "你是一位專業的 TCFD 揭露標準判讀專家。請根據以下內容判斷是否有揭露該標準。"),
        ("human", "{input}"),
    ])
    chain = prompt_template | llm | parser
    prompt = get_prompt(chunk, label, pos1, pos2)
    response = chain.invoke({"input": prompt})
    return response.model_dump()

def process_one_company(path: str, llm: ChatOpenAI, pos_map: dict):
    df = pd.read_csv(path, dtype=str).fillna("")
    yn_col = "是否真的有揭露此標準?(Y/N)"
    reason_col = "reasoning"
    for col in [yn_col, reason_col]:
        if col not in df.columns:
            df[col] = ""

    tasks = []
    for idx, row in df.iterrows():
        chunk = str(row.get("Chunk Text", "") or "")
        label = str(row.get("Definition", "") or "")
        if not (chunk and label):
            continue
        pos1, pos2 = pos_map.get(label, ("", ""))
        tasks.append((idx, chunk, label, pos1, pos2))

    def _worker(args):
        idx, chunk, label, pos1, pos2 = args
        try:
            result = call_llm(llm, chunk, label, pos1, pos2)
            if result and result.get("result"):
                first = result["result"][0]
                reasoning = (first.get("reasoning") or "").strip()
                yn = (first.get("is_disclosed") or "").strip().upper()
                yn = "Y" if yn == "Y" else "N"
                return idx, reasoning, yn, None
        except Exception as e:
            return idx, "", "N", f"API error: {e}"
        return idx, "", "N", None

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(_worker, t): t[0] for t in tasks}
        for fut in tqdm(as_completed(futures), total=len(futures), desc=os.path.basename(path)):
            results.append(fut.result())

    for idx, reasoning, yn, err in results:
        if err:
            df.at[idx, reason_col] = err
            df.at[idx, yn_col]     = "N"
        else:
            df.at[idx, reason_col] = reasoning
            df.at[idx, yn_col]     = yn

    base = os.path.splitext(path)[0]
    out_csv = f"{base}_with_CoT_v1_few_shot.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"[SUCCESS] 已輸出：{out_csv}")

def main():
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("請在 .env 設定 OPENAI_API_KEY")

    llm = ChatOpenAI(model=MODEL_NAME, api_key=api_key, temperature=0)
    pos_map = load_pos_examples(GUIDELINES_PATH, GUIDELINES_SHEET)

    paths = sorted(glob(os.path.join(INPUT_DIR, PATTERN)))
    if not paths:
        print(f"[ERROR] 在 {INPUT_DIR} 找不到 {PATTERN}")
        return

    print(f"[INFO] 共有 {len(paths)} 個公司檔要處理。")
    for p in paths:
        process_one_company(p, llm, pos_map)

if __name__ == "__main__":
    main()
