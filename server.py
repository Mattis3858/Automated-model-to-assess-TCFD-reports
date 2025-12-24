import os
import shutil
import torch
import uuid
from typing import Dict
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from langchain_community.document_loaders import PyMuPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings
from dotenv import load_dotenv

BASE_DB_ROOT = "chroma"
UPLOAD_FOLDER = "data/frontend_test_pdf"
CHUNK_SIZE = 500
CHUNK_OVERLAP = 50
EMBEDDING_SPACE = "cosine"
EMBEDDING_MODEL_NAME = "Qwen/Qwen3-Embedding-0.6B"

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

embeddings_model = None
processing_status: Dict[str, dict] = {}

@app.on_event("startup")
async def startup_event():
    global embeddings_model
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"[INFO] Server starting... Loading model on {device}")
    try:
        embeddings_model = HuggingFaceEmbeddings(
            model_name=EMBEDDING_MODEL_NAME, 
            model_kwargs={"device": device}
        )
        print(f"[INFO] Model loaded successfully.")
    except Exception as e:
        print(f"[ERROR] Failed to load model: {e}")

def process_pdf_task(task_id: str, file_path: str, standard: str, force_update: bool):
    global embeddings_model, processing_status
    
    try:
        processing_status[task_id] = {"status": "processing", "message": "Checking file status..."}
        
        pdf_name = os.path.splitext(os.path.basename(file_path))[0]
        standard_folder_name = f"chroma_report_{standard}"
        chroma_path = os.path.join(BASE_DB_ROOT, standard_folder_name, pdf_name)
        
        if os.path.exists(chroma_path) and not force_update:
            print(f"[BG TASK] Skipped: {pdf_name} already exists.")
            processing_status[task_id] = {
                "status": "completed", 
                "message": "File already exists. Skipped processing."
            }
            return

        if not embeddings_model:
            raise Exception("Embedding model not loaded")

        os.makedirs(os.path.join(BASE_DB_ROOT, standard_folder_name), exist_ok=True)

        processing_status[task_id]["message"] = "Loading PDF..."
        loader = PyMuPDFLoader(file_path)
        pages = loader.load()
        
        processing_status[task_id]["message"] = f"Splitting text ({len(pages)} pages)..."
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP
        )
        documents = splitter.split_documents(pages)
        
        for i, doc in enumerate(documents):
            doc.metadata["page"] = doc.metadata.get("page", -1) + 1
            doc.metadata["chunk_id"] = str(i)
            doc.metadata["standard"] = standard
            doc.metadata["source_filename"] = pdf_name

        if os.path.exists(chroma_path):
            shutil.rmtree(chroma_path)

        processing_status[task_id]["message"] = "Generating embeddings..."
        
        Chroma.from_documents(
            documents=documents,
            embedding=embeddings_model,
            persist_directory=chroma_path,
            collection_metadata={"hnsw:space": EMBEDDING_SPACE},
        )
        
        print(f"[BG TASK] SUCCESS: {pdf_name} saved.")
        processing_status[task_id] = {"status": "completed", "message": "Processing completed successfully."}
        
    except Exception as e:
        print(f"[ERROR] Task {task_id} failed: {e}")
        processing_status[task_id] = {"status": "failed", "message": str(e)}

@app.post("/upload")
async def upload_file(
    background_tasks: BackgroundTasks, 
    file: UploadFile = File(...), 
    standard: str = Form(...),
    force_update: bool = Form(False)
):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are allowed")

    task_id = str(uuid.uuid4())
    file_location = os.path.join(UPLOAD_FOLDER, file.filename)
    
    try:
        with open(file_location, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File save failed: {e}")
    
    processing_status[task_id] = {"status": "queued", "message": "File uploaded, waiting in queue..."}
    
    background_tasks.add_task(process_pdf_task, task_id, file_location, standard, force_update)

    return {
        "task_id": task_id,
        "filename": file.filename, 
        "status": "uploaded", 
        "message": "Upload successful."
    }

@app.get("/status/{task_id}")
async def get_status(task_id: str):
    status = processing_status.get(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Task ID not found")
    return status

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)