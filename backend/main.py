# main.py
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

load_dotenv()

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# 設定路徑
BASE_DB_ROOT = "chroma"
UPLOAD_FOLDER = "data/uploads"
OUTPUT_DIR = "data/results"
STANDARD_DIR = "data/standards" # 存放 Excel 規則的地方

for d in [UPLOAD_FOLDER, OUTPUT_DIR, STANDARD_DIR]: 
    os.makedirs(d, exist_ok=True)

processing_status: Dict[str, dict] = {}

@app.on_event("startup")
async def startup():
    # 這裡可以加個判斷，如果是開發環境才自動下載，生產環境建議預先準備好模型
    model_manager.load_models("Qwen/Qwen3-Embedding-0.6B", "BAAI/bge-reranker-v2-m3")

# --- 新增：上傳 Standard (Excel) 的端點 ---
@app.post("/upload-standard")
async def upload_standard(standard_name: str = Form(...), file: UploadFile = File(...)):
    """
    上傳某個標準對應的 Excel 規則檔 (例如: SASB.xlsx)
    """
    path = os.path.join(STANDARD_DIR, f"{standard_name}.xlsx")
    with open(path, "wb") as b:
        shutil.copyfileobj(file.file, b)
    return {"message": f"Standard '{standard_name}' guidelines saved."}


# --- 修改：PDF 上傳並自動觸發全流程 ---
@app.post("/process-pdf")
async def process_pdf(
    background_tasks: BackgroundTasks, 
    file: UploadFile = File(...), 
    standard: str = Form(...), 
    force_update: bool = Form(False)
):
    """
    上傳 PDF -> 建立向量庫 -> 根據 Standard Excel 進行分析 -> 存檔
    """
    # 檢查該 Standard 的 Excel 是否存在
    excel_path = os.path.join(STANDARD_DIR, f"{standard}.xlsx")
    if not os.path.exists(excel_path):
        raise HTTPException(status_code=400, detail=f"Standard '{standard}' not found. Please upload excel via /upload-standard first.")

    task_id = str(uuid.uuid4())
    pdf_path = os.path.join(UPLOAD_FOLDER, f"{task_id}_{file.filename}")
    
    # 儲存 PDF
    with open(pdf_path, "wb") as b:
        shutil.copyfileobj(file.file, b)

    # 設定初始狀態
    processing_status[task_id] = {
        "status": "processing", 
        "step": "initializing",
        "message": "File uploaded, starting pipeline..."
    }

    # 觸發背景任務 (全流程)
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


# 背景任務處理函數
def handle_full_pipeline_bg(tid, pdf_path, standard, excel_path, force):
    try:
        processing_status[tid] = {
            "status": "processing", 
            "message": "Step 1/3: Vectorizing..."
        }
        
        result_msg = core_logic.process_pipeline(
            file_path=pdf_path,
            base_db_root=BASE_DB_ROOT,
            output_dir=OUTPUT_DIR,
            standard=standard,
            excel_path=excel_path,
            force_update=force
        )
        
        processing_status[tid] = {"status": "completed", "message": result_msg}
        
    except Exception as e:
        processing_status[tid] = {"status": "failed", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)