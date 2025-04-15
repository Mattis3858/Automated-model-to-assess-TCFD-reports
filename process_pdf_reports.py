import os
import pandas as pd
from PyPDF2 import PdfReader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
import openai
from dotenv import load_dotenv
import pymupdf

# 讀取 .env 設定
load_dotenv()
openai.api_key = os.environ['OPENAI_API_KEY']

# 路徑設定PDF 檔案路徑
PDF_DIRECTORY = "data/tcfd_report_pdf_preprocessed/"
OUTPUT_DIRECTORY = "data/tcfd_report_pdf_chunks_第四層/"

# 確保輸出目錄存在
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

# 初始化 OpenAIEmbeddings
embedding_model = OpenAIEmbeddings(model="text-embedding-ada-002")

# 設定 chunk size 和 chunk overlap
CHUNK_SIZE = 300
CHUNK_OVERLAP = 50

def load_pdf(file_path):
    """Reads PDF and extracts text."""
    doc = pymupdf.open(file_path)
    text = ""
    for page in doc:
        text += page.get_text() or ""
    return text

def split_pdf_text(text):
    """Splits text from PDF into smaller chunks."""
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        length_function=len,
        add_start_index=True,
    )
    return text_splitter.split_text(text)

def get_chunk_embedding(chunk_text):
    """Gets embedding vector for a text chunk using OpenAIEmbeddings."""
    return embedding_model.embed_query(chunk_text)

def extract_company_and_year(file_name):
    """
    Extracts company name and year from the file name.
    假設檔名格式為 "公司名稱_年份_其他資訊.pdf"，
    則預設取檔名第一段為公司名稱、第二段為年份。
    """
    parts = file_name.split("_")
    company_name = parts[0]  # 假設公司名稱在第一部分
    year = parts[1] if len(parts) > 1 else "unknown"  # 假設年份在第二部分
    return company_name, year

def process_pdf_file(file_path, output_file_path):
    """
    Processes a single PDF file and saves chunk embeddings to a CSV.
    """
    print(f"\nProcessing {file_path}...")
    text = load_pdf(file_path)
    chunks = split_pdf_text(text)

    data = []
    for idx, chunk_text in enumerate(chunks):
        print(f"  Processing chunk {idx + 1}/{len(chunks)}")
        chunk_embedding = get_chunk_embedding(chunk_text)
        
        data.append({
            "Filename": os.path.basename(file_path),
            "Chunk_ID": idx,
            "Chunk_Text": chunk_text,
            "Chunk_Embedding": chunk_embedding
        })

    # 將結果儲存到 DataFrame
    result_df = pd.DataFrame(data)
    
    # 將結果輸出到 CSV
    result_df.to_csv(output_file_path, index=False)
    print(f"Results saved to {output_file_path}.")

def main():
    # 自動偵測資料夾中的所有 PDF 檔案
    pdf_files = [
        os.path.join(PDF_DIRECTORY, f)
        for f in os.listdir(PDF_DIRECTORY)
        if f.endswith('.pdf')
    ]
    
    # 開始處理每個 PDF 檔案
    for file_path in pdf_files:
        # 先根據 PDF 檔名組出預計輸出的 CSV 檔名
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        company_name, year = extract_company_and_year(file_name)
        output_file_name = f"chunk_embeddings_{company_name}_{year}_{CHUNK_SIZE}_{CHUNK_OVERLAP}.csv"
        output_file_path = os.path.join(OUTPUT_DIRECTORY, output_file_name)

        # 如果同名的 CSV 已經存在，就直接跳過
        if os.path.exists(output_file_path):
            print(f"Skipping {file_path}: CSV file already exists ({output_file_path})")
            continue
        
        # 如果 CSV 不存在，才進行後續的處理
        process_pdf_file(file_path, output_file_path)

if __name__ == "__main__":
    main()
