import os
import pandas as pd
import openai
from dotenv import load_dotenv
from tqdm.auto import tqdm

def fill_tcfd_flags(
    input_xlsx: str,
    guidelines_excel: str,
    sheet_name: str = "工作表2",
    model: str = "gpt-4o-mini",
):
    load_dotenv()
    openai.api_key = os.getenv("OPENAI_API_KEY")

    guidelines_df = pd.read_excel(guidelines_excel, sheet_name=sheet_name)

    df = pd.read_excel(
        input_xlsx,
        dtype={"是否真的有揭露此標準?(Y/N)": "object"}
    )

    yn_col = "是否真的有揭露此標準?(Y/N)"
    if yn_col not in df.columns:
        df[yn_col] = ""

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="方案一：使用豐富上下文"):
        definition = row["Definition"]
        chunk      = row["Chunk Text"]

        prompt = f"""
            ### 背景 ###
            你是一位專業的 TCFD 報告書審查專家，專精於解讀企業的 TCFD 報告。你的任務是嚴謹地、客觀地評估企業報告書中的一個文本段落 (Chunk Text)，判斷它是否「實質且具體地」回應了指定的 TCFD 揭露指引 (Definition)。

            ### 任務說明 ###
            請根據下方提供的「TCFD 揭露指引問題」和「報告書文本段落」，進行專業判斷。

            - **TCFD 揭露指引問題 (Definition):**
            「{definition}」

            - **報告書文本段落 (Chunk Text):**
            「{chunk}」

            ### 判斷標準 ###
            1.  **直接相關性**: 文本段落是否直接討論了指引問題中的核心主題？
            2.  **具體性**: 文本段落是否提供了具體的資訊、數據、案例或策略來回答該問題，而不僅僅是模糊、概括性的描述？
            3.  **完整性**: 文本段落的內容是否足以構成對該問題的完整回答？

            ### 你的任務  ###
            綜合以上標準，判斷此「報告書文本段落」是否有效地回答了「TCFD 揭露指引問題」。

            ### 回覆格式 ###
            - 如果文本段落「是」對該問題的有效回答，請只回覆「Y」。
            - 如果文本段落「否」，或只是間接提及、內容空泛，並未有效回答問題，請只回覆「N」。
            """

        try:
            resp = openai.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "你是一位專業的 ESG 永續報告書審查專家，請嚴格依照指示，僅回覆「Y」或「N」。"},
                    {"role": "user",   "content": prompt}
                ],
                temperature=0,
            )
            ans = resp.choices[0].message.content.strip()
            df.at[idx, yn_col] = "Y" if "Y" in ans else "N"
        except Exception as e:
            print(f"[ERROR] Row {idx}: {e}")
            df.at[idx, yn_col] = "N"

    
    base, _      = os.path.splitext(input_xlsx)
    output_xlsx  = f"{base}_with_flags_v1.xlsx"
    df.to_excel(output_xlsx, index=False)
    print(f"已儲存結果至：{output_xlsx}")

if __name__ == "__main__":
    INPUT_XLSX       = "data/2023_query_result/臺灣銀行_2023_output_chunks.xlsx"
    GUIDELINES_XLSX  = "data/tcfd第四層揭露指引.xlsx"
    fill_tcfd_flags(INPUT_XLSX, GUIDELINES_XLSX)