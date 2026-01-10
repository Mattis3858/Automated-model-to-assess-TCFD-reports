import os
import uuid
import shutil
from typing import Dict
from fastapi import FastAPI, BackgroundTasks, Form, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from models import model_manager
import core_logic
import uvicorn
import logging
import pandas as pd
from fastapi import HTTPException

logger = logging.getLogger(__name__)
load_dotenv()

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

BASE_DB_ROOT = "chroma"
UPLOAD_FOLDER = "data/uploads"
OUTPUT_DIR = "data/results"
STANDARD_DIR = "data/standards"
SUMMARY_DIR = os.path.join(OUTPUT_DIR, "summary")

for d in [UPLOAD_FOLDER, OUTPUT_DIR, STANDARD_DIR]: 
    os.makedirs(d, exist_ok=True)

processing_status: Dict[str, dict] = {}

@app.on_event("startup")
async def startup():
    model_manager.load_models("Qwen/Qwen3-Embedding-0.6B", "BAAI/bge-reranker-v2-m3")

@app.post("/upload-standard")
async def upload_standard(standard_name: str = Form(...), file: UploadFile = File(...)):

    path = os.path.join(STANDARD_DIR, f"{standard_name}.xlsx")
    with open(path, "wb") as b:
        shutil.copyfileobj(file.file, b)
    return {"message": f"Standard '{standard_name}' guidelines saved."}


@app.post("/process-pdf")
async def process_pdf(
    background_tasks: BackgroundTasks, 
    file: UploadFile = File(...), 
    standard: str = Form(...), 
    force_update: bool = Form(False)
):

    excel_path = os.path.join(STANDARD_DIR, f"{standard}.xlsx")
    if not os.path.exists(excel_path):
        raise HTTPException(status_code=400, detail=f"Standard '{standard}' not found. Please upload excel via /upload-standard first.")

    task_id = str(uuid.uuid4())
    pdf_path = os.path.join(UPLOAD_FOLDER, f"{file.filename}")
    
    with open(pdf_path, "wb") as b:
        shutil.copyfileobj(file.file, b)

    processing_status[task_id] = {
        "status": "processing", 
        "step": "initializing",
        "message": "File uploaded, starting pipeline..."
    }

    background_tasks.add_task(
        handle_full_pipeline_bg, 
        task_id, 
        pdf_path, 
        standard, 
        excel_path,
        force_update
    )
    
    return {"task_id": task_id, "message": "Processing started."}


@app.get("/status/{task_id}")
async def get_status(task_id: str):
    return processing_status.get(task_id, {"status": "not_found"})


def handle_full_pipeline_bg(tid, pdf_path, standard, excel_path, force):
    logger.info(f"Starting pipeline for task {tid} with file {pdf_path}")
    try:
        def update_status_message(msg):
            processing_status[tid] = {
                "status": "processing",
                "message": msg
            }
        update_status_message("Step 1/3: Vectorizing...")
        
        result_msg = core_logic.process_pipeline(
            file_path=pdf_path,
            base_db_root=BASE_DB_ROOT,
            output_dir=OUTPUT_DIR,
            standard=standard,
            excel_path=excel_path,
            force_update=force,
            update_callback=update_status_message
        )
        
        processing_status[tid] = {"status": "completed", "message": result_msg}
        
    except Exception as e:
        processing_status[tid] = {"status": "failed", "message": str(e)}

@app.get("/history/summary")
async def get_history_summary():
    summary_file = os.path.join(SUMMARY_DIR, "company_disclosure_summary.csv")
    
    if not os.path.exists(summary_file):
        return []
    
    try:
        df = pd.read_csv(summary_file)
        result = df.to_dict(orient="records")
        return result
    except Exception as e:
        logger.error(f"Error reading summary: {e}")
        return []

@app.get("/history/detail/{company_name}")
async def get_company_detail(company_name: str):
    detail_file = os.path.join(SUMMARY_DIR, "company_label_detail.csv")
    
    if not os.path.exists(detail_file):
        raise HTTPException(status_code=404, detail="Detail file not found")
        
    try:
        df = pd.read_csv(detail_file)
        company_df = df[df["Company"] == company_name]
        
        if company_df.empty:
            return {"message": "No details found for this company", "data": []}
            
        records = company_df.to_dict(orient="records")
        return {"company": company_name, "data": records}
        
    except Exception as e:
        logger.error(f"Error reading detail: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)