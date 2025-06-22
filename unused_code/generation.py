import os
import pandas as pd
import openai
import numpy as np
from tqdm import tqdm
from dotenv import load_dotenv
import tiktoken
import ast
import json
# from langchain_core.output_parsers import JsonOutputParser
import concurrent.futures
import matplotlib.pyplot as plt
from tenacity import retry, wait_random_exponential, stop_after_attempt
import logging

load_dotenv()
openai.api_key = os.environ['OPENAI_API_KEY']

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(), # 輸出到控制台
        # logging.FileHandler("llm_qa.log") # 如果需要輸出到檔案
    ],
)

MODEL_NAME = "gpt-4o-mini"
# tokenizer = tiktoken.encoding_for_model(MODEL_NAME)

excel_path = "data/tcfd第四層揭露指引.xlsx"
df_labels = pd.read_excel(excel_path, sheet_name='工作表2').dropna(subset=['Label', 'Definition'])
label_mapping = dict(zip(df_labels['Label'], df_labels['Definition']))

@retry(wait=wait_random_exponential(min=2, max=10), stop=stop_after_attempt(3))
def query_llm_for_verification(chunk, matched_label_json):
    print(matched_label_json)
    prompt = f"""
你是一位嚴謹的 TCFD 專家，請根據下列「報告書內容」及「揭露標準列表」做分類判斷，每個標準均要獨立評估。

### 報告書內容 ###
請仔細審閱以下報告書片段：
{chunk}

**請評估的揭露標準列表 (包含標籤代碼和其定義，請逐一評估此列表中的每個標準):**
{matched_label_json}

### 回答格式 ###
請以「純 JSON 陣列」回覆，每個物件包含：
- "label": 必須精確對應於列表中的 label
- "reason": 必須提供詳細推理，解釋為何該標準適用或不適用於報告書內容
- "is_disclosed": true 或 false

注意：
1. 不要評估未列出的標準。
2. label 僅能使用列表內精確內容。
3. 嚴禁輸出任何 markdown、開場白、說明或程式區塊。
4. JSON 陣列長度必須和標準列表一樣。

範例（不要包含任何文字說明）：
[
  {{"label": "G-1-1_1", "reason": "...", "is_disclosed": true}},
  {{"label": "G-1-1_2", "reason": "...", "is_disclosed": false}}
]
"""
    try:
        response = openai.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        return response.choices[0].message.content.strip()
    except openai.APIError as e:
        logging.error(f"OpenAI API Error querying LLM: {e}")
        return "Error: OpenAI API Error"
    except Exception as e:
        logging.error(f"Unexpected error querying LLM: {e}")
        return "Error: Unexpected LLM Query Error"

def convert_to_list(x):
    try:
        return ast.literal_eval(x) if isinstance(x, str) else list(x)
    except:
        return []

def transform_matched_categories(val):
    if pd.isnull(val):
        return json.dumps([])
    try:
        label_list = ast.literal_eval(val) if isinstance(val, str) else val
    except Exception:
        label_list = []
    json_list = []
    for label in label_list:
        json_list.append({
            "label": label,
            "label_definition": label_mapping.get(label, "")
        })
    return json.dumps(json_list, ensure_ascii=False)

def process_row(index, row):
    chunk_text = row['Chunk_Text']
    matched_categories_json = row['Matched_Categories']
    logging.info(f"處理第 {index} 筆資料...")
    
    # Directly call the function; no need for json_parser.invoke()
    response = query_llm_for_verification(chunk_text, matched_categories_json)
    
    return index, response

def is_valid_json(text):
    try:
        json.loads(text)
        return True
    except:
        return False

def main():
    similarity_matched_dir = "data/tcfd_report_pdf_chunks_matching_result_第四層/"
    chunk_csv_files = [os.path.join(similarity_matched_dir, f) for f in os.listdir(similarity_matched_dir) if f.endswith('.csv')]
    df_chunks_all = pd.concat([pd.read_csv(file) for file in chunk_csv_files], ignore_index=True)
    # print(df_chunks_all)
    all_labels_lists = df_chunks_all['Matched_Categories_List'].apply(convert_to_list)

    unique_labels = set()
    for labels in all_labels_lists:
        unique_labels.update(labels)

    # print("所有欄位的 Matched_Categories 中共有", len(unique_labels), "個 unique 的 label")
    # print("Unique labels:", unique_labels)

    df_chunks_all['Matched_Categories'] = df_chunks_all['Matched_Categories_List'].apply(transform_matched_categories)

    # print(df_chunks_all.head())
    # json_parser = JsonOutputParser()
    # format_instructions = json_parser.get_format_instructions()
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        futures = [executor.submit(process_row, index, row) for index, row in df_chunks_all.iterrows()]
        for f in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            try:
                idx, response = f.result()
                results[idx] = response
            except Exception as exc:
                logging.error(f"第 {idx} 筆處理錯誤: {exc}")
                results[idx] = "Error"
    df_chunks_all['LLM_Response'] = df_chunks_all.index.map(results.get)
    df_chunks_all['Valid_JSON'] = df_chunks_all['LLM_Response'].apply(is_valid_json)
    df_chunks_all.to_csv("data/llm_question_answering_results/tcfd_report_RAG_response_2023.csv", index=False, encoding='utf-8')
    print(df_chunks_all[['Chunk_Text', 'Matched_Categories', 'LLM_Response', 'Valid_JSON']].head())


if __name__ == "__main__":
    main()
