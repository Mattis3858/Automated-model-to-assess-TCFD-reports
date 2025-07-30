import os
import pandas as pd
import openai
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv
from tqdm.auto import tqdm
from prompt.V1 import PROMPT
from langchain_core.output_parsers import PydanticOutputParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from langchain.schema import SystemMessage, HumanMessage
from pydantic import BaseModel
from typing import List, Optional
from collections import Counter
from tenacity import retry, stop_after_attempt
from langchain_core.prompts import ChatPromptTemplate

class Result(BaseModel):
    reasoning: Optional[str] = None
    is_disclosed: Optional[str] = None

class ResultList(BaseModel):
    result: List[Result]

def get_prompt(chunk: str, label: str, positive_example1: str, positive_example2: str) -> str:
    return PROMPT.format(chunk=chunk, label=label, positive_example1=positive_example1, positive_example2=positive_example2)

@retry(stop=stop_after_attempt(3))
def get_llm_answer(chunk: str, label: str, positive_example1: str, positive_example2: str):
    try:
        load_dotenv()
        llm = ChatOpenAI(
            model="gpt-4o-mini",
            api_key=os.getenv("OPENAI_API_KEY"),
        )
        parser = PydanticOutputParser(pydantic_object=ResultList)
        prompt_template = ChatPromptTemplate.from_messages([
            ("system", "你是一位專業的 TCFD 揭露標準判讀專家。請根據以下內容判斷是否有揭露該標準。"),
            ("human", "{input}"),
        ])
        chain = prompt_template | llm | parser
        prompt = get_prompt(chunk, label, positive_example1, positive_example2)
        response = chain.invoke({"input": prompt})
        result = response.model_dump()
        return result
    except Exception as e:
        print(f"Error occurred: {e}")
        return None

MAX_WORKERS = 10
def process_row(idx, chunk, label, positive_example1, positive_example2):
    result_dict = get_llm_answer(chunk, label, positive_example1, positive_example2)
    return idx, result_dict

def process_tcfd_file(
    input_xlsx: str,
    output_csv: str = None,
):
    load_dotenv()
    df = pd.read_excel(input_xlsx, dtype=str)
    print("欄位名稱：", df.columns.tolist())
    df['reasoning'] = ""
    yn_col = "是否真的有揭露此標準?(Y/N)"
    if yn_col not in df.columns:
        df[yn_col] = ""

    tasks = []
    for idx, row in df.iterrows():
        chunk = row.get("Chunk Text", "")
        label = row.get("Definition", "")
        positive_example1 = row.get("Positive Example1", "")
        positive_example2 = row.get("Positive Example2", "")
        if chunk and label and positive_example1 and positive_example2:
            tasks.append((idx, chunk, label, positive_example1, positive_example2))
        else:
            # 留空或給預設
            df.at[idx, 'reasoning'] = ""
            df.at[idx, yn_col]    = ""

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # submit all tasks
        future_to_idx = {
            executor.submit(process_row, idx, chunk, label, positive_example1, positive_example2): idx
            for idx, chunk, label, positive_example1, positive_example2 in tasks
        }
        for future in tqdm(as_completed(future_to_idx), total=len(future_to_idx), desc="並行呼叫 LLM"):
            idx = future_to_idx[future]
            try:
                _, result_dict = future.result()
                if result_dict and result_dict.get("result"):
                    first = result_dict["result"][0]
                    df.at[idx, 'reasoning'] = first.get("reasoning", "").strip()
                    df.at[idx, yn_col]    = first.get("is_disclosed", "").strip()
                else:
                    df.at[idx, 'reasoning'] = ""
                    df.at[idx, yn_col]    = ""
            except Exception as e:
                # 出錯給預設
                print(f"[ERROR] Row {idx}: {e}")
                df.at[idx, 'reasoning'] = ""
                df.at[idx, yn_col]    = ""

    # 決定輸出檔名
    if output_csv is None:
        base, _ = os.path.splitext(input_xlsx)
        output_csv = f"{base}_with_CoT_v1_few_shot.csv"

    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"已儲存結果至：{output_csv}")


if __name__ == "__main__":
    INPUT_XLSX = "data/2023_query_result/瑞興銀行_2023_output_chunks_fewshot.xlsx"
    process_tcfd_file(INPUT_XLSX)