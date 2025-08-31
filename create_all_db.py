import os
import shutil
from dotenv import load_dotenv
from langchain_community.document_loaders import PyMuPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.embeddings import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma
from tqdm.auto import tqdm
from langchain_community.embeddings import HuggingFaceEmbeddings
import torch

# ===== 可調參數 =====
BASE_CHROMA_PATH = "chroma_report_TNFD"
PDF_ROOT = "data/TNFD報告書"
CHUNK_SIZE = 500
CHUNK_OVERLAP = 50
EMBEDDING_SPACE = "cosine"
EMBEDDING_MODEL_NAME = "Qwen/Qwen3-Embedding-0.6B"


def find_all_pdfs(root_dir: str):
    pdfs = []
    for r, _, files in os.walk(root_dir):
        for f in files:
            if f.lower().endswith(".pdf"):
                pdfs.append(os.path.join(r, f))
    return sorted(pdfs)


def process_pdf(pdf_path: str, embeddings):
    pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
    chroma_path = os.path.join(BASE_CHROMA_PATH, pdf_name)

    print(f"[INFO] Processing PDF: {pdf_name}")
    loader = PyMuPDFLoader(pdf_path)
    pages = loader.load()
    print(f"[INFO] PDF loaded. Number of pages: {len(pages)}")

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP
    )
    documents = splitter.split_documents(pages)
    print(f"[INFO] {pdf_name} 分割後的文本塊數量：{len(documents)}")

    for i, doc in enumerate(documents):
        page_num = doc.metadata.get("page", -1) + 1
        doc.metadata["page"] = page_num
        doc.metadata["chunk_id"] = str(i)

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
        embedding=embeddings,
        persist_directory=chroma_path,
        **{"collection_metadata": {"hnsw:space": EMBEDDING_SPACE}},
    )
    db.persist()
    print(f"[SUCCESS] {pdf_name} 的 ChromaDB 已建立：{chroma_path}")


def main():
    load_dotenv()
    os.makedirs(BASE_CHROMA_PATH, exist_ok=True)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"[INFO] Using device: {device}")

    print(f"[INFO] Initializing embedding model: {EMBEDDING_MODEL_NAME}...")
    embeddings = HuggingFaceEmbeddings(
        model_name=EMBEDDING_MODEL_NAME, model_kwargs={"device": device}
    )

    pdf_paths = find_all_pdfs(PDF_ROOT)
    if not pdf_paths:
        print(f"[ERROR] 在 {PDF_ROOT} 找不到任何 PDF。")
        return
    print(f"[INFO] 共找到 {len(pdf_paths)} 份 PDF。")
    for p in tqdm(pdf_paths, desc="建立 ChromaDB"):
        process_pdf(p, embeddings)


if __name__ == "__main__":
    main()
