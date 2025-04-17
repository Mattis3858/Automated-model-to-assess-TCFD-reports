import os
import pandas as pd
from PyPDF2 import PdfReader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
import openai
from dotenv import load_dotenv
import pymupdf

load_dotenv()
openai.api_key = os.environ['OPENAI_API_KEY']

PDF_DIRECTORY = "data/tcfd_report_pdf_preprocessed/"
OUTPUT_DIRECTORY = "data/tcfd_report_pdf_chunks_第四層/"

os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

embedding_model = OpenAIEmbeddings(model="text-embedding-ada-002")

CHUNK_SIZE = 500
CHUNK_OVERLAP = 50

def load_pdf(file_path):
    doc = pymupdf.open(file_path)
    text = ""
    for page in doc:
        text += page.get_text() or ""
    return text

def split_pdf_text(text):
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        length_function=len,
        add_start_index=True,
    )
    return text_splitter.split_text(text)

def get_chunk_embedding(chunk_text):
    return embedding_model.embed_query(chunk_text)

def extract_company_and_year(file_name):
    parts = file_name.split("_")
    company_name = parts[0]
    year = parts[1] if len(parts) > 1 else "unknown"
    return company_name, year

def process_pdf_file(file_path, output_file_path):
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

    result_df = pd.DataFrame(data)
    
    result_df.to_csv(output_file_path, index=False)
    print(f"Results saved to {output_file_path}.")

def main():
    pdf_files = [
        os.path.join(PDF_DIRECTORY, f)
        for f in os.listdir(PDF_DIRECTORY)
        if f.endswith('.pdf')
    ]
    
    for file_path in pdf_files:
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        company_name, year = extract_company_and_year(file_name)
        output_file_name = f"chunk_embeddings_{company_name}_{year}_{CHUNK_SIZE}_{CHUNK_OVERLAP}.csv"
        output_file_path = os.path.join(OUTPUT_DIRECTORY, output_file_name)

        if os.path.exists(output_file_path):
            print(f"Skipping {file_path}: CSV file already exists ({output_file_path})")
            continue
        
        process_pdf_file(file_path, output_file_path)

if __name__ == "__main__":
    main()
