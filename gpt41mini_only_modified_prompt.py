import os
import glob
import json
from PyPDF2 import PdfReader
import pandas as pd
import tiktoken
import openai
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
PDF_DIR = "data/tcfd_report_pdf_preprocessed"
LABEL_EXCEL = "data/answer/tcfd第四層揭露指引.xlsx"
MAX_CHUNK_TOKENS = 5000
OVERLAP_TOKENS = 100
openai.api_key = os.getenv("OPENAI_API_KEY")

MODEL_NAME = "gpt-4.1-mini"
tokenizer = tiktoken.encoding_for_model(MODEL_NAME)

def count_tokens(text: str) -> int:
    """Return number of tokens for given text"""
    return len(tokenizer.encode(text))


def split_text(text: str, max_tokens: int, overlap: int) -> list[str]:
    """
    Split text into chunks of at most max_tokens, with specified overlap.
    """
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
    """Extract and concatenate text from all pages of a PDF."""
    reader = PdfReader(pdf_path)
    texts = []
    for page in reader.pages:
        txt = page.extract_text()
        if txt:
            texts.append(txt)
    return "\n".join(texts)


def build_prompt(chunk: str, label_json: str, label_list: list[str]) -> str:
    """Construct the prompt for a given chunk."""
    return f"""
        ### 背景資訊 ###
        你是氣候相關財務揭露標準專家，熟悉 TCFD 第四層揭露指引的標準與定義。你將會收到一段報告書內容，並且需要判斷該報告書內容是否符合某些特定揭露標準。

        請根據以下報告書內容進行判斷：
        {chunk}

        請僅針對以下揭露標準進行評估，不要評估或包含其他任何標準：
        {label_json}

        ### 回覆格式 ###
        請僅回覆純 JSON 格式，不要包含任何 Markdown 語法、程式碼區塊或額外說明文字。每個 JSON 物件必須包含以下欄位：
        1. chunk: string，報告書內容。
        2. label: string，對應的揭露標準代碼。
        3. reason: string，詳細說明判斷的推理過程，解釋為何該揭露標準有或沒有被揭露。
        4. is_disclosed: boolean，若報告書中有揭露該標準則回覆 1；未揭露則回覆 0。

        請僅針對我提供的標準列表中的標準提供評估，不要添加任何其他標準。你的回覆應該包含同樣數量的 JSON 物件，每個對應到我提供的一個標準。

        [{', '.join([f'/"{lb}/"' for lb in label_list])}]
    """


def call_gpt_for_chunk(chunk: str, label_json: str, label_list: list[str]) -> str:
    """Send one chunk to the GPT model and return its response."""
    prompt = build_prompt(chunk, label_json, label_list)
    resp = openai.ChatCompletion.create(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return resp.choices[0].message.content


def process_pdf(pdf_path: str, label_json: str, label_list: list[str]):
    """Extract, chunk, and query GPT in parallel for a single PDF."""
    text = extract_text_from_pdf(pdf_path)
    chunks = split_text(text, MAX_CHUNK_TOKENS, OVERLAP_TOKENS)
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_idx = {executor.submit(call_gpt_for_chunk, ch, label_json, label_list): i 
            for i, ch in enumerate(chunks)}
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                res = future.result()
                results.append({"chunk_index": idx, "response": res})
            except Exception as e:
                print(f"Error in chunk {idx} of {os.path.basename(pdf_path)}: {e}")

    # Sort by chunk index and save
    results = sorted(results, key=lambda x: x["chunk_index"])
    out_file = pdf_path.replace(".pdf", "_gpt_results.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump({"pdf": os.path.basename(pdf_path), "results": results}, f, ensure_ascii=False, indent=2)
    print(f"[Saved] {out_file}")


def main():
    # Load all labels
    df_lbl = pd.read_excel(LABEL_EXCEL, usecols=["Label"])  # adjust column name
    label_list = df_lbl["Label"].astype(str).tolist()
    # Prepare JSON to inject into prompt
    label_json = json.dumps([{"label": lb} for lb in label_list], ensure_ascii=False)
    print(label_json)
    # Process all PDFs in folder
    # pdf_files = glob.glob(os.path.join(PDF_DIR, "*.pdf"))
    # for pdf in pdf_files:
    #     process_pdf(pdf, label_json, label_list)

if __name__ == "__main__":
    main()
