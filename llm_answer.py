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

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="填寫TCFD揭露標準"):
        definition = row["Definition"]
        chunk      = row["Chunk Text"]

        prompt = f"""
            ### 任務 ###
            根據以下內容判斷此報告書是否有回答「{definition}」的敘述：
            {chunk}

            ### 回覆格式 ###
            如果報告書中有做到「{definition}」，只回覆「Y」。
            如果報告書中沒有做到「{definition}」，只回覆「N」。
            """

        try:
            resp = openai.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "你是一位專業的 TCFD 揭露標準判讀專家，請僅回覆「Y」或「N」。"},
                    {"role": "user",   "content": prompt}
                ],
                temperature=0,
            )
            ans = resp.choices[0].message.content.strip()
            df.at[idx, yn_col] = "Y" if "Y" in ans else "N"
        except Exception as e:
            print(f"[ERROR] Row {idx}: {e}")
            df.at[idx, yn_col] = "N"

    # 2. 輸出成新的 Excel
    base, _      = os.path.splitext(input_xlsx)
    output_xlsx  = f"{base}_with_flags.xlsx"
    df.to_excel(output_xlsx, index=False)
    print(f"已儲存結果至：{output_xlsx}")

if __name__ == "__main__":
    INPUT_XLSX       = "data/2023_query_result/富邦金控_2023_output_chunks.xlsx"
    GUIDELINES_XLSX  = "data/tcfd第四層揭露指引.xlsx"
    fill_tcfd_flags(INPUT_XLSX, GUIDELINES_XLSX)