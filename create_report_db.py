import os
import shutil
from dotenv import load_dotenv
import openai
from langchain_community.document_loaders import PyMuPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.embeddings import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma

load_dotenv()
openai.api_key = os.getenv('OPENAI_API_KEY')

BASE_CHROMA_PATH = "chroma_report"
CHUNK_SIZE = 500
CHUNK_OVERLAP = 50

def process_pdf(pdf_path):
    if not os.path.exists(pdf_path):
        print(f"[ERROR] File not found: {pdf_path}")
        return

    pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
    chroma_path = os.path.join(BASE_CHROMA_PATH, pdf_name)
    
    try:
        print(f"[INFO] Processing PDF: {pdf_name} with PyMuPDFLoader")
        loader = PyMuPDFLoader(pdf_path)
        pages = loader.load()
        print(f"[INFO] PDF loaded. Number of pages: {len(pages)}")

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=CHUNK_SIZE,
            chunk_overlap=CHUNK_OVERLAP
        )
        documents = text_splitter.split_documents(pages)
        print(f"[INFO] {pdf_name} 分割後的文本塊數量：{len(documents)}")

        for i, doc in enumerate(documents):

            page_num = doc.metadata.get('page', -1) + 1
            doc.metadata['page'] = page_num
            
            chunk_id = f"{i}"
            

            doc.metadata['chunk_id'] = chunk_id
        

        if documents:
            print("-" * 50)
            print("[INFO] Metadata of the first chunk:")
            print(documents[0].metadata)
            print("-" * 50)

        if os.path.exists(chroma_path):
            print(f"[INFO] Clearing existing ChromaDB at: {chroma_path}")
            shutil.rmtree(chroma_path)

        print(f"[INFO] Creating embeddings and storing in ChromaDB...")
        
        db = Chroma.from_documents(
            documents=documents,
            embedding=OpenAIEmbeddings(),
            persist_directory=chroma_path,  
            **{"collection_metadata": {"hnsw:space": "cosine"}}
        )
        db.persist()
        print(f"[SUCCESS] {pdf_name} 的 ChromaDB 已建立並儲存至 '{chroma_path}'")
        
    except Exception as e:
        print(f"[ERROR] Failed to process {pdf_path}: {e}")

def main():
    pdf_paths = [
        "data/tcfd_report_pdf_preprocessed/tcfd_report_pdf_preprocessed_2023/富邦金控_2023.pdf"
    ]

    for pdf_path in pdf_paths:
        print(f"[INFO] Starting to process: {pdf_path}")
        process_pdf(pdf_path)
        print(f"[INFO] Finished processing: {pdf_path}\n")


if __name__ == "__main__":
    main()