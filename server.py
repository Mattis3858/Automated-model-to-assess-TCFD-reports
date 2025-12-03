import os
import shutil
import torch
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from langchain_community.document_loaders import PyMuPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings
from dotenv import load_dotenv

# ===== 參數設定 (與你的 create_all_db.py 保持一致) =====
BASE_CHROMA_PATH = "chroma/chroma_report_TCFD"
UPLOAD_FOLDER = "data/frontend_test_pdf"
CHUNK_SIZE = 500
CHUNK_OVERLAP = 50
EMBEDDING_SPACE = "cosine"
EMBEDDING_MODEL_NAME = "Qwen/Qwen3-Embedding-0.6B"

# 確保目錄存在
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(BASE_CHROMA_PATH, exist_ok=True)

# 載入環境變數
load_dotenv()

# 初始化 FastAPI
app = FastAPI()

# 設定 CORS (允許前端 React 連線)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 開發階段允許所有來源，上線時可改為 ["http://localhost:3000"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 全域變數存放模型
embeddings_model = None

@app.on_event("startup")
async def startup_event():
    """伺服器啟動時載入模型，避免每次請求都重新載入"""
    global embeddings_model
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"[INFO] Server starting... Loading model on {device}")
    try:
        embeddings_model = HuggingFaceEmbeddings(
            model_name=EMBEDDING_MODEL_NAME, 
            model_kwargs={"device": device}
        )
        print(f"[INFO] Model loaded successfully: {EMBEDDING_MODEL_NAME}")
    except Exception as e:
        print(f"[ERROR] Failed to load model: {e}")

def process_pdf_task(file_path: str):
    """
    背景任務：執行 PDF 切割與 Embedding 
    """
    global embeddings_model
    if not embeddings_model:
        print("[ERROR] Embedding model not loaded.")
        return

    try:
        pdf_name = os.path.splitext(os.path.basename(file_path))[0]
        chroma_path = os.path.join(BASE_CHROMA_PATH, pdf_name)

        print(f"[BG TASK] Processing PDF: {pdf_name}")
        loader = PyMuPDFLoader(file_path)
        pages = loader.load()
        print(f"[BG TASK] PDF loaded. Pages: {len(pages)}")
        
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP
        )
        documents = splitter.split_documents(pages)
        
        # Add metadata
        for i, doc in enumerate(documents):
            page_num = doc.metadata.get("page", -1) + 1
            doc.metadata["page"] = page_num
            doc.metadata["chunk_id"] = str(i)

        # Clear existing DB if any
        if os.path.exists(chroma_path):
            print(f"[BG TASK] Clearing existing ChromaDB at: {chroma_path}")
            shutil.rmtree(chroma_path)

        # Create ChromaDB
        print(f"[BG TASK] Creating embeddings...")
        db = Chroma.from_documents(
            documents=documents,
            embedding=embeddings_model,
            persist_directory=chroma_path,
            collection_metadata={"hnsw:space": EMBEDDING_SPACE},
        )
        db.persist()
        print(f"[BG TASK] SUCCESS: {pdf_name} processed and saved to ChromaDB.")
        
    except Exception as e:
        print(f"[ERROR] Failed to process {file_path}: {e}")

@app.post("/upload")
async def upload_file(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    """
    API 端點：接收檔案 -> 存檔 -> 觸發背景處理
    """
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are allowed")

    file_location = os.path.join(UPLOAD_FOLDER, file.filename)
    
    # 寫入檔案
    try:
        with open(file_location, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        print(f"[INFO] File received and saved to: {file_location}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File save failed: {e}")
    
    # 使用 BackgroundTasks 在背景執行耗時的 embedding 工作
    background_tasks.add_task(process_pdf_task, file_location)

    return {
        "filename": file.filename, 
        "status": "uploaded", 
        "message": "File uploaded successfully. Processing started in background."
    }

if __name__ == "__main__":
    import uvicorn
    # 啟動伺服器，監聽 8000 port
    uvicorn.run(app, host="0.0.0.0", port=8000)