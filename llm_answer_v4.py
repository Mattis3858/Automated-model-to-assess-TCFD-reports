import os
import pandas as pd
import openai
from dotenv import load_dotenv
from tqdm.auto import tqdm
import re
from concurrent.futures import ThreadPoolExecutor, as_completed


MAX_WORKERS = 10

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


def get_llm_result(chunk: str, definition: str, model: str):
    """
    建構 v4 Prompt (Few-shot Examples + Zero-shot CoT Instruction) 並呼叫 LLM。
    """
   
    prompt = f"""### 角色 ###
你是一位極其精確且嚴謹的 TCFD 審查專家。你的任務是學習以下真實範例的判斷結果，然後對新的任務進行有邏輯的推理並給出答案。

---
### 學習範例 ###

**範例 1:**
- **TCFD 指引問題**: "公司是否描述向董事會和/或董事會下設委員會，定期報告氣候相關風險與機會之流程？"
- **報告書片段**: "隨著氣候變遷影響加劇...董事會扮演的角色 富邦由董事會擔任推動永續發展之最高督導單位...依據責任金融及風險管理目的，分別定期呈報氣候變遷風險資訊至「公司治理及永續委員會」以及「審計委員會」"
- **你的判斷**:
[FINAL ANSWER]
Y
[END FINAL ANSWER]


**範例 2:**
- **TCFD 指引問題**: "公司是否描述有跨部門之氣候相關工作小組統籌執行相關工作？"
- **報告書片段**: "富邦金控暨子公司目前取得永續相關證照共99張...亦安排同仁研習ISO...持續強化氣候變遷風險因應能力。氣候管理人才 富邦金控跟進國際趨勢...邀請顧問團隊...本公司及各子公司之責任金融小組主管及成員定期參與相關會議..."
- **你的判斷**:
[FINAL ANSWER]
N
[END FINAL ANSWER]

---

### 正式任務 ###
現在，請你嚴格遵循【思考與輸出框架】，對以下新的內容進行判斷。

**1. TCFD 指引問題 (Definition):**
"{definition}"

**2. 報告書片段 (Chunk):**
"{chunk}"

### 思考與輸出框架 (極度重要) ###
請嚴格遵循以下兩步驟思考，並使用指定的標籤格式輸出你的答案。你的回覆中，除了指定的標籤與內容外，不應包含任何其他說明文字。

[REASON]
(請在這裡提供你的推理分析過程，引用文本證據，解釋你為何判斷為 Y 或 N。)
[END REASON]
[FINAL ANSWER]
(Y 或 N)
[END FINAL ANSWER]
"""

    resp = openai.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "你是一位 TCFD 審查專家，請學習範例的判斷結果，並在處理新任務時，嚴格遵循指定的思考與輸出框架。"},
            {"role": "user", "content": prompt}
        ],
        temperature=0,
    )
    response_text = resp.choices[0].message.content
    return parse_llm_response(response_text)


def fill_tcfd_flags_v4(
    input_xlsx: str,
    model: str = "gpt-4o-mini",
):
   
    load_dotenv()
    openai.api_key = os.getenv("OPENAI_API_KEY")
    if not openai.api_key:
        raise ValueError("OPENAI_API_KEY 未設定，請檢查您的 .env 檔案或環境變數。")

    df = pd.read_excel(input_xlsx)
    yn_col = "是否真的有揭露此標準?(Y/N)"
    reason_col = "Reason"

    if yn_col not in df.columns:
        df[yn_col] = ""
    if reason_col not in df.columns:
        df[reason_col] = ""

    tasks = []
    for idx, row in df.iterrows():
        if pd.notna(row["Definition"]) and pd.notna(row["Chunk Text"]):
            definition = row["Definition"]
            chunk = str(row["Chunk Text"])
            tasks.append((idx, chunk, definition))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_idx = {
            executor.submit(get_llm_result, chunk, definition, model): idx
            for idx, chunk, definition in tasks
        }

        for future in tqdm(as_completed(future_to_idx), total=len(future_to_idx), desc="並行處理 TCFD (v4 Hybrid)"):
            idx = future_to_idx[future]
            try:
                reason, final_answer = future.result()
                df.at[idx, yn_col] = final_answer
                df.at[idx, reason_col] = reason
            except Exception as e:
                print(f"\n處理索引 {idx} 時發生錯誤: {e}")
                df.at[idx, yn_col] = "Error"
                df.at[idx, reason_col] = f"呼叫 API 時發生錯誤: {e}"

    base, _ = os.path.splitext(input_xlsx)
    output_xlsx = f"{base}_with_flags_v4.xlsx"
    df.to_excel(output_xlsx, index=False, engine='openpyxl')
    print(f"\n處理完成！結果已儲存至：{output_xlsx}")


if __name__ == "__main__":
    # --- 請將這裡的路徑換成您要處理的 Excel 檔案 ---
    # 這邊使用您之前上傳的檔案名稱作為範例
    INPUT_XLSX = "富邦金控2023_output_chunks.xlsx - 富邦金控2023_output_chunks.csv"
    
    if not os.path.exists(INPUT_XLSX):
        print(f"錯誤：找不到輸入檔案 '{INPUT_XLSX}'。請確認路徑是否正確。")
    else:
        fill_tcfd_flags_v4(INPUT_XLSX, model="gpt-4o-mini")