import os
import shutil
from dotenv import load_dotenv, find_dotenv
from langchain_community.document_loaders import PyMuPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings  # ← 新版
from langchain_community.vectorstores import Chroma
from tqdm.auto import tqdm

# ===== 可調參數 =====
BASE_CHROMA_PATH = "chroma_report"
PDF_ROOT = "data/handroll"
CHUNK_SIZE = 500
CHUNK_OVERLAP = 50
EMBEDDING_SPACE = "cosine"
EMBED_MODEL = "text-embedding-3-small"  # 或 "text-embedding-3-large"


def find_all_pdfs(root_dir: str):
    pdfs = []
    for r, _, files in os.walk(root_dir):
        for f in files:
            if f.lower().endswith(".pdf"):
                pdfs.append(os.path.join(r, f))
    return sorted(pdfs)


def build_embedder():
    """建立 OpenAI 向量嵌入器（顯式帶入 api_key，避免吃到錯誤的 org/project）"""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("找不到 OPENAI_API_KEY，請確認 .env 是否正確載入。")

    # 若你確定不要吃到任何殘留 org/project，可強制 None
    # 也可以改成：organization=os.getenv("OPENAI_ORG") 或 project=os.getenv("OPENAI_PROJECT")
    emb = OpenAIEmbeddings(
        model=EMBED_MODEL,
        api_key=api_key,
        # organization=None,
        # project=None,
    )
    return emb


def process_pdf(pdf_path: str, embedder: OpenAIEmbeddings):
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
    try:
        db = Chroma.from_documents(
            documents=documents,
            embedding=embedder,
            persist_directory=chroma_path,
            **{"collection_metadata": {"hnsw:space": EMBEDDING_SPACE}},
        )
        db.persist()
        print(f"[SUCCESS] {pdf_name} 的 ChromaDB 已建立：{chroma_path}")
    except Exception as e:
        # 這裡補一個更友好的提示，協助判斷是否為額度/專案/組織問題
        print("[ERROR] 建立向量或寫入 Chroma 失敗：", repr(e))
        print("        若訊息含有 insufficient_quota / 429，請確認：")
        print("        1) OPENAI_API_KEY 是否正確、未過期且對應到有額度的帳戶/Project")
        print("        2) 是否有殘留 OPENAI_ORG / OPENAI_PROJECT / OPENAI_API_BASE 造成導流")
        print("        3) Billing 的 Hard limit 是否太低")
        raise


def main():
    # 明確載入當前工作目錄的 .env，避免 OneDrive 多份檔案誤載
    env_path = find_dotenv(usecwd=True)
    load_dotenv(env_path)
    print("[ENV] dotenv:", env_path or "<not found>")
    print("[ENV] OPENAI_API_KEY(prefix):", (os.getenv("OPENAI_API_KEY", "")[:7] + "…") or "<missing>")
    print("[ENV] OPENAI_ORG:", os.getenv("OPENAI_ORG") or "<unset>")
    print("[ENV] OPENAI_PROJECT:", os.getenv("OPENAI_PROJECT") or "<unset>")
    print("[ENV] OPENAI_API_BASE:", os.getenv("OPENAI_API_BASE") or "<unset>")
    print("[CFG] EMBED_MODEL:", EMBED_MODEL)
    print("[CFG] CHUNK_SIZE:", CHUNK_SIZE, "CHUNK_OVERLAP:", CHUNK_OVERLAP)

    os.makedirs(BASE_CHROMA_PATH, exist_ok=True)

    embedder = build_embedder()

    pdf_paths = find_all_pdfs(PDF_ROOT)
    if not pdf_paths:
        print(f"[ERROR] 在 {PDF_ROOT} 找不到任何 PDF。")
        return

    print(f"[INFO] 共找到 {len(pdf_paths)} 份 PDF。")
    for p in tqdm(pdf_paths, desc="建立 ChromaDB"):
        process_pdf(p, embedder)


if __name__ == "__main__":
    main()
