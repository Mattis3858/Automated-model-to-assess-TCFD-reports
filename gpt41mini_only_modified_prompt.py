import os
import glob
import json
from PyPDF2 import PdfReader
import pandas as pd
import tiktoken
import openai
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
PDF_DIR = "data/tcfd_report_pdf_preprocessed"
LABEL_EXCEL = "data/answer/tcfd第四層揭露指引.xlsx"
MAX_CHUNK_TOKENS = 300
OVERLAP_TOKENS = 50
openai.api_key = os.getenv("OPENAI_API_KEY")

MODEL_NAME = "gpt-4.1-mini"
tokenizer = tiktoken.encoding_for_model("gpt-4o")

client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY")
)

def count_tokens(text: str) -> int:
    return len(tokenizer.encode(text))


def split_text(text: str, max_tokens: int, overlap: int) -> list[str]:
    words = text.split()
    chunks = []
    start = 0
    while start < len(words):
        end = start + max_tokens
        chunk = " ".join(words[start:end])
        chunks.append(chunk)
        start = max(end - overlap, end)
    return chunks


def extract_text_from_pdf(pdf_path: str) -> str:
    reader = PdfReader(pdf_path)
    texts = []
    for page in reader.pages:
        txt = page.extract_text()
        if txt:
            texts.append(txt)
    return "\n".join(texts)


def build_prompt(chunk: str, label_json: str, label_list: list[str]) -> str:
    return f"""
        ### 背景資訊 ###
        你是氣候相關財務揭露工作的專家，熟悉 Task Force on Climate-related Financial Disclosures(TCFD) 第四層揭露指引的標準與其詳細定義有深入的理解。你的任務是嚴謹地審閱提供的報告書內容，並判斷其中是否明確揭露了以下特定的 TCFD 標準。

        請根據以下報告書內容進行判斷：
        {chunk}

        請僅針對以下揭露標準及其**明確定義**進行評估。你的判斷必須**嚴格依據報告書內容**以及**每個標準所提供的定義**，不要評估或包含任何其他未列出的標準。
        {label_json}

        ### 回覆格式 ###
        請務必僅回覆純 JSON 格式，不要包含任何 Markdown 語法、程式碼區塊或額外說明文字。你的回覆必須是一個 JSON 陣列，其中每個物件對應一個你所評估的揭露標準。回覆中的 JSON 物件數量必須與你收到的揭露標準數量相同。每個 JSON 物件必須包含以下欄位：
        1. chunk: string，此判斷所依據的原始報告書內容片段。
        2. label: string，對應的揭露標準代碼。
        3. reason: string，請詳細說明你的判斷推理過程。
           - 若 `is_disclosed` 為 true (1)，請具體指出報告書中哪部分內容、如何符合該揭露標準的定義。
           - 若 `is_disclosed` 為 false (0)，請具體說明為何報告書中**未見相關揭露**，例如缺乏特定資訊或不符合定義要求。
        4. is_disclosed: boolean，若報告書中**明確且充分地**揭露了該標準的定義，則回覆 1 (true)；若**未明確揭露**或內容不符合定義，則回覆 0 (false)。

        請你針對以下所有標準逐一進行評估，並在 JSON 陣列中提供對應的結果：
        [{', '.join([f'/"{lb}/"' for lb in label_list])}]
    """


def call_gpt_for_chunk(chunk: str, label_json: str, label_list: list[str]) -> str:
    prompt = build_prompt(chunk, label_json, label_list)
    resp = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return resp.choices[0].message.content


def process_pdf(pdf_path: str, label_json: str, label_list: list[str]):
    pdf_filename = os.path.basename(pdf_path)
    print(f"--- Processing PDF: {pdf_filename} ---")

    text = extract_text_from_pdf(pdf_path)
    chunks = split_text(text, MAX_CHUNK_TOKENS, OVERLAP_TOKENS)
    results = []

    print(f"Total chunks for {pdf_filename}: {len(chunks)}")
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_idx = {executor.submit(call_gpt_for_chunk, ch, label_json, label_list): i 
            for i, ch in enumerate(chunks)}
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            print(f"  Processing chunk {idx+1}/{len(chunks)} of {pdf_filename}...")
            try:
                res = future.result()
                results.append({"chunk_index": idx, "response": res})
            except Exception as e:
                print(f"Error in chunk {idx} of {os.path.basename(pdf_path)}: {e}")
    print(results)
    results = sorted(results, key=lambda x: x["chunk_index"])
    out_file = pdf_path.replace(".pdf", "_gpt_results.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump({"pdf": os.path.basename(pdf_path), "results": results}, f, ensure_ascii=False, indent=2)
    print(f"[Saved] {out_file}")


def main():
    df_lbl = pd.read_excel("data/tcfd第四層揭露指引.xlsx", usecols=["Label", "Definition"])
    label_list = df_lbl["Label"].astype(str).tolist()
    label_definitions = []
    for index, row in df_lbl.iterrows():
        label_definitions.append({"label": str(row["Label"]), "definition": str(row["Definition"])})
    
    label_json = json.dumps(label_definitions, ensure_ascii=False, indent=2)

    print(label_json)
    pdf_files = glob.glob(os.path.join(PDF_DIR, "*.pdf"))
    for pdf in pdf_files:
        process_pdf(pdf, label_json, label_list)

if __name__ == "__main__":
    main()
