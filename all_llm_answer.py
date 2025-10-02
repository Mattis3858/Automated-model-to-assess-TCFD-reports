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
from langchain_google_vertexai import ChatVertexAI
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from prompt.V2 import TCFD_LLM_ANSWER_PROMPT

INPUT_DIR = "data/TCFD_report_improved_300_query_result"
INPUT_PATTERN = "*_output_chunks.csv"
POS_EXAMPLE_SOURCE = "data/2023_query_result/temp/富邦金控_2023_output_chunks_fewshot_with_CoT_v1_few_shot.csv"

GUIDELINES_USE_DEFINITION_AS_LABEL = True

MODEL_NAME = "gpt-4o-mini"
MAX_WORKERS = 10
SKIP_IF_OUTPUT_EXISTS = True

COL_CHUNK = "Chunk Text"
COL_LABEL = "Label"
COL_DEF = "Definition"
COL_PE1 = "Positive Example1"
COL_PE2 = "Positive Example2"
COL_REASON = "reasoning"
COL_YN = "是否真的有揭露此標準?(Y/N)"
COL_CONFIDENCE = "confidence"
COL_COMPANY = "Company"
COL_RANK = "Rank"
OUTPUT_SUBDIR = "TCFD_report_improved_300_llm_answer_second_invocation"
OUTPUT_SUFFIX = "_output_chunks_fewshot_with_CoT_v2_few_shot.csv"


class Result(BaseModel):
    reasoning: Optional[str] = None
    is_disclosed: Optional[str] = None
    confidence: Optional[float] = None


class ResultList(BaseModel):
    result: List[Result]


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
        df = pd.read_csv(path, dtype=str)

    needed = {COL_LABEL, COL_PE1, COL_PE2}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(
            f"正例來源檔缺少必要欄位：{missing}。請確認 {path} 來自人工驗證後的富邦輸出檔。"
        )

    df = df[[COL_LABEL, COL_PE1, COL_PE2]].fillna("")
    agg = df.groupby(COL_LABEL, as_index=True).agg(
        {COL_PE1: first_nonempty, COL_PE2: first_nonempty}
    )
    # 轉為 dict
    pe_map = {}
    for label, row in agg.iterrows():
        pe_map[str(label)] = (str(row[COL_PE1]) or "", str(row[COL_PE2]) or "")
    return pe_map


def get_prompt(chunk: str, standard_text_for_label: str, pos1: str, pos2: str) -> str:
    return TCFD_LLM_ANSWER_PROMPT.format(
        chunk=chunk,
        label=standard_text_for_label,  # PROMPT 的 {label} 這裡放 Definition（或 Label 代碼，依你的配置）
        # positive_example1=pos1,
        # positive_example2=pos2,
    )


def build_chain(api_key: str):
    llm = ChatOpenAI(model=MODEL_NAME, api_key=api_key, temperature=0)
    parser = PydanticOutputParser(pydantic_object=ResultList)
    prompt_template = ChatPromptTemplate.from_messages(
        [
            ("system", "你是一位專業的 TCFD 揭露標準判讀專家。"),
            ("human", "{input}"),
        ]
    )
    return prompt_template | llm | parser


def _return_default(retry_state):
    print(retry_state)
    return {"result": [{"reasoning": "", "is_disclosed": "", "confidence": 0.0}]}


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(min=1, max=30),
    retry_error_callback=_return_default,
)
def call_chain(
    chain, chunk: str, standard_text_for_label: str, pos1: str, pos2: str
) -> dict:
    prompt = get_prompt(chunk, standard_text_for_label, pos1, pos2)
    resp = chain.invoke({"input": prompt})
    result = resp.model_dump()
    # print(result)
    if result.get("result")[0].get("confidence") < 0.8:
        second_chain = second_invocation_chain(api_key=os.getenv("OPENAI_API_KEY"))
        response = second_chain.invoke({"input": prompt})
        result = response.model_dump()
        # print("\nSecond:", result)
    return result


def second_invocation_chain(api_key: str):
    llm = ChatOpenAI(model="gpt-4.1-mini", api_key=api_key, temperature=0)
    # llm = ChatVertexAI(model_name="gemini-2.5-flash", temperature=0)
    parser = PydanticOutputParser(pydantic_object=ResultList)
    prompt_template = ChatPromptTemplate.from_messages(
        [
            ("system", "你是一位專業的 TCFD 揭露標準判讀專家。"),
            ("human", "{input}"),
        ]
    )
    return prompt_template | llm | parser


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

    original_dir = os.path.dirname(input_path)
    output_dir = os.path.join(original_dir, OUTPUT_SUBDIR)
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, out_base)

    return company, out_path


def attach_positive_examples(
    df: pd.DataFrame, pe_map: Dict[str, Tuple[str, str]]
) -> pd.DataFrame:
    if COL_PE1 not in df.columns:
        df[COL_PE1] = ""
    if COL_PE2 not in df.columns:
        df[COL_PE2] = ""
    df[COL_PE1] = df[COL_PE1].fillna("")
    df[COL_PE2] = df[COL_PE2].fillna("")

    if COL_LABEL in df.columns:
        mask1 = df[COL_PE1].eq("")
        mask2 = df[COL_PE2].eq("")
        df.loc[mask1, COL_PE1] = (
            df.loc[mask1, COL_LABEL]
            .map(lambda k: pe_map.get(k, ("", ""))[0])
            .fillna("")
        )
        df.loc[mask2, COL_PE2] = (
            df.loc[mask2, COL_LABEL]
            .map(lambda k: pe_map.get(k, ("", ""))[1])
            .fillna("")
        )
    return df


def ensure_util_columns(df: pd.DataFrame, company: str) -> pd.DataFrame:
    if COL_COMPANY not in df.columns:
        df[COL_COMPANY] = company
    else:
        df[COL_COMPANY] = df[COL_COMPANY].fillna(company).replace("", company)

    if COL_REASON not in df.columns:
        df[COL_REASON] = ""
    if COL_YN not in df.columns:
        df[COL_YN] = ""

    if COL_RANK not in df.columns:
        if COL_LABEL in df.columns:
            df[COL_RANK] = df.groupby(COL_LABEL).cumcount() + 1
        else:
            df[COL_RANK] = range(1, len(df) + 1)
    return df


def process_one_file(path: str, chain, pe_map: Dict[str, Tuple[str, str]]):
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        try:
            df = pd.read_excel(path, dtype=str).fillna("")
        except Exception as e:
            print(f"[ERROR] 讀檔失敗：{os.path.basename(path)} → {e}")
            return

    company, out_path = infer_company_and_output_path(path)
    if SKIP_IF_OUTPUT_EXISTS and os.path.exists(out_path):
        print(f"[SKIP] 已存在輸出：{os.path.basename(out_path)}")
        return

    df = attach_positive_examples(df, pe_map)
    df = ensure_util_columns(df, company)

    tasks = []
    for idx, row in df.iterrows():
        chunk = str(row.get(COL_CHUNK, "") or "")
        label_text_for_prompt = (
            str(row.get(COL_DEF, "") or "").strip()
            if GUIDELINES_USE_DEFINITION_AS_LABEL
            else ""
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
        for fut in tqdm(
            as_completed(futures), total=len(futures), desc=os.path.basename(path)
        ):
            idx = futures[fut]
            try:
                data = fut.result()
                if data and data.get("result"):
                    first = data["result"][0]
                    reasoning = (first.get("reasoning") or "").strip()
                    yn = (first.get("is_disclosed") or "").strip().upper()
                    yn = "Y" if yn == "Y" else "N"
                    confidence = first.get("confidence", 0.0)
                    results.append((idx, reasoning, yn, confidence, None))
                else:
                    results.append((idx, "", "N", 0.0, "Empty parser result"))
            except Exception as e:
                results.append((idx, "", "N", 0.0, f"API error: {e}"))

    for idx, reasoning, yn, confidence, err in results:
        df.at[idx, COL_REASON] = reasoning if not err else err
        df.at[idx, COL_YN] = yn
        try:
            df.at[idx, COL_CONFIDENCE] = float(confidence)
        except Exception:
            df.at[idx, COL_CONFIDENCE] = 0.0

    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[SUCCESS] 輸出：{out_path}")


def main():
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("請在 .env 設定 OPENAI_API_KEY")

    pe_map = load_pos_examples_from_verified(POS_EXAMPLE_SOURCE)
    if not pe_map:
        print("[WARN] 正例映射為空，將在無 few-shot 的情況下判讀。")

    chain = build_chain(api_key)

    paths = sorted(glob(os.path.join(INPUT_DIR, INPUT_PATTERN)))
    if not paths:
        print(f"[ERROR] 在 {INPUT_DIR} 找不到 {INPUT_PATTERN}")
        return

    print(f"[INFO] 共找到 {len(paths)} 個輸入檔")
    for p in paths:
        process_one_file(p, chain, pe_map)


if __name__ == "__main__":
    main()
