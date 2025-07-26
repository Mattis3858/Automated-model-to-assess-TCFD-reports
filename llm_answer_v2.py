import os
import pandas as pd
import openai
from dotenv import load_dotenv
from tqdm.auto import tqdm
import re
from concurrent.futures import ThreadPoolExecutor, as_completed


MAX_WORKERS = 10

# 解析模型回覆的函數
def parse_llm_response(response_text: str) -> tuple[str, str]:
    try:
        reason_match = re.search(r"\[REASON\](.*?)\[END REASON\]", response_text, re.DOTALL)
        answer_match = re.search(r"\[FINAL ANSWER\](.*?)\[END FINAL ANSWER\]", response_text, re.DOTALL)

        reason = reason_match.group(1).strip() if reason_match else "模型未提供推理過程。"
        final_answer = answer_match.group(1).strip().upper() if answer_match else "N"
        if final_answer not in ["Y", "N"]:
            final_answer = "N"
        return reason, final_answer
    except Exception as e:
        return f"解析回覆時發生錯誤: {e}", "N"

# 呼叫 LLM 的函數
def get_llm_result(chunk: str, definition: str, model: str):
    prompt = f"""### 角色 ###
你是一位極其精確且嚴謹的 TCFD 審查專家。

### 任務 ###
根據下方提供的「報告書片段」和「TCFD 指引問題」，進行兩步驟判斷。你的判斷必須完全基於所提供的文本證據。

### 輸入資料 ###
**1. TCFD 指引問題 (Definition):**
"{definition}"

**2. 報告書片段 (Chunk):**
"{chunk}"

### 思考與輸出框架 (極度重要) ###
請嚴格遵循以下兩步驟思考，並使用指定的標籤格式輸出你的答案。你的回覆中，除了指定的標籤與內容外，不應包含任何其他說明文字。

[REASON]
(推理分析過程，引用文本證據。)
[END REASON]
[FINAL ANSWER]
(Y 或 N)
[END FINAL ANSWER]
"""

    resp = openai.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "你是一位 TCFD 審查專家，請嚴格遵循指定的結構化輸出格式。"},
            {"role": "user", "content": prompt}
        ],
        temperature=0,
    )
    response_text = resp.choices[0].message.content
    return parse_llm_response(response_text)


def fill_tcfd_flags_v2(
    input_xlsx: str,
    model: str = "gpt-4o-mini",
):
    load_dotenv()
    openai.api_key = os.getenv("OPENAI_API_KEY")

    df = pd.read_excel(input_xlsx)
    yn_col = "是否真的有揭露此標準?(Y/N)"
    reason_col = "Reason"

    if yn_col not in df.columns:
        df[yn_col] = ""
    if reason_col not in df.columns:
        df[reason_col] = ""

    tasks = []
    for idx, row in df.iterrows():
        definition = row["Definition"]
        chunk = str(row["Chunk Text"])
        tasks.append((idx, chunk, definition))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_idx = {
            executor.submit(get_llm_result, chunk, definition, model): idx
            for idx, chunk, definition in tasks
        }

        for future in tqdm(as_completed(future_to_idx), total=len(future_to_idx), desc="並行處理 TCFD"):
            idx = future_to_idx[future]
            try:
                reason, final_answer = future.result()
                df.at[idx, yn_col] = final_answer
                df.at[idx, reason_col] = reason
            except Exception as e:
                df.at[idx, yn_col] = "N"
                df.at[idx, reason_col] = f"呼叫 API 時發生錯誤: {e}"

    base, _ = os.path.splitext(input_xlsx)
    output_xlsx = f"{base}_with_flags_v2.xlsx"
    df.to_excel(output_xlsx, index=False, engine='openpyxl')
    print(f"\n處理完成！結果已儲存至：{output_xlsx}")


if __name__ == "__main__":
    INPUT_XLSX = "data/2023_query_result/富邦金控_2023_output_chunks.xlsx"
    fill_tcfd_flags_v2(INPUT_XLSX)