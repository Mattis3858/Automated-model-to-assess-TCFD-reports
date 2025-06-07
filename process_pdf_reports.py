import os
import pandas as pd
from PyPDF2 import PdfReader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
import openai
from dotenv import load_dotenv
import pymupdf
import logging
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()
openai.api_key = os.environ['OPENAI_API_KEY']

PDF_DIRECTORY = "data/tcfd_report_pdf_preprocessed/"
OUTPUT_DIRECTORY = "data/tcfd_report_pdf_chunks_第四層/"

os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

embedding_model = OpenAIEmbeddings(model="text-embedding-ada-002")

CHUNK_SIZE = 300
CHUNK_OVERLAP = 50
BATCH_SIZE = 8
MAX_WORKERS = 4

def load_pdf(file_path):
    text = ""
    try:
        doc = pymupdf.open(file_path)
        for page in doc:
            text += page.get_text() or ""
        doc.close()
        logging.info(f"Successfully loaded text from {os.path.basename(file_path)}")
        return text
    except Exception as e:
        logging.error(f"Error loading PDF {file_path}: {e}")
        return None

def split_pdf_text(text):
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        length_function=len,
        add_start_index=True,
    )
    chunks = text_splitter.split_text(text)
    logging.info(f"Text split into {len(chunks)} chunks.")
    return chunks

def get_chunks_embedding_batch(chunk_texts):
    try:
        embeddings = embedding_model.embed_documents(chunk_texts)
        return embeddings
    except openai.APIError as e:
        logging.error(f"OpenAI API Error during embedding batch: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during embedding batch: {e}")
        return None



def extract_company_and_year(file_name):
    parts = file_name.split("_")
    company_name = parts[0] if parts else "unknown_company"
    year = parts[1] if len(parts) > 1 else "unknown_year"
    return company_name, year

def process_pdf_file(file_path, output_file_path):
    logging.info(f"Starting processing for {os.path.basename(file_path)}...")

    text = load_pdf(file_path)
    if text is None:
        logging.warning(f"Skipping {os.path.basename(file_path)} due to PDF loading error.")
        return

    chunks = split_pdf_text(text)
    
    data = []
    for i in tqdm(range(0, len(chunks), BATCH_SIZE), desc=f"Embedding chunks for {os.path.basename(file_path)}"):
        batch_chunks = chunks[i : i + BATCH_SIZE]
        batch_embeddings = get_chunks_embedding_batch(batch_chunks)

        if batch_embeddings is None:
            logging.error(f"Failed to get embeddings for a batch in {os.path.basename(file_path)}. Skipping remaining chunks.")
            break

        for idx, chunk_text in enumerate(batch_chunks):
            global_chunk_id = i + idx
            data.append({
                "Filename": os.path.basename(file_path),
                "Company": extract_company_and_year(os.path.splitext(os.path.basename(file_path))[0])[0],
                "Year": extract_company_and_year(os.path.splitext(os.path.basename(file_path))[0])[1],
                "Chunk_ID": global_chunk_id,
                "Chunk_Text": chunk_text,
                "Chunk_Embedding": batch_embeddings[idx]
            })

    if not data:
        logging.warning(f"No data generated for {os.path.basename(file_path)}. Skipping CSV creation.")
        return

    result_df = pd.DataFrame(data)
    
    try:
        result_df.to_csv(output_file_path, index=False)
        logging.info(f"Results for {os.path.basename(file_path)} saved to {output_file_path}.")
    except Exception as e:
        logging.error(f"Error saving CSV for {os.path.basename(file_path)} to {output_file_path}: {e}")

def main():
    pdf_files_to_process = []
    for f in os.listdir(PDF_DIRECTORY):
        if f.endswith('.pdf'):
            file_path = os.path.join(PDF_DIRECTORY, f)
            file_name_without_ext = os.path.splitext(os.path.basename(file_path))[0]
            company_name, year = extract_company_and_year(file_name_without_ext)
            output_file_name = f"chunk_embeddings_{company_name}_{year}_{CHUNK_SIZE}_{CHUNK_OVERLAP}.csv"
            output_file_path = os.path.join(OUTPUT_DIRECTORY, output_file_name)

            if os.path.exists(output_file_path):
                logging.info(f"Skipping {os.path.basename(file_path)}: CSV file already exists ({os.path.basename(output_file_path)})")
                continue
            
            pdf_files_to_process.append((file_path, output_file_path))

    if not pdf_files_to_process:
        logging.info("No new PDF files to process.")
        return

    logging.info(f"Found {len(pdf_files_to_process)} PDF files to process. Starting parallel execution...")
    

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = {executor.submit(process_pdf_file, file_path, output_file_path): os.path.basename(file_path)
                   for file_path, output_file_path in pdf_files_to_process}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Overall PDF Processing"):
            file_name = futures[future]
            try:
                future.result()
                logging.info(f"Finished processing {file_name}.")
            except Exception as exc:
                logging.error(f'{file_name} generated an exception: {exc}')

    logging.info("All PDF processing tasks completed.")

if __name__ == "__main__":
    main()
