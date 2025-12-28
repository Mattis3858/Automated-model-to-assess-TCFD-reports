# core_logic.py
import os
import shutil
import pandas as pd
from typing import List, Tuple
from langchain_community.document_loaders import PyMuPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import Chroma
from models import model_manager
# --- 新增 ---
from llm_analyzer import LLMAnalyzer

def process_pipeline(file_path: str, base_db_root: str, output_dir: str, standard: str, excel_path: str, force_update: bool):
    """
    Pipeline: PDF -> Vectorize -> Retrieve & Rerank (CSV) -> LLM Analysis (Final CSV)
    """
    pdf_name = os.path.splitext(os.path.basename(file_path))[0]
    
    # Step 1: Vectorize
    try:
        chroma_msg = process_pdf_to_chroma(file_path, base_db_root, standard, force_update)
    except Exception as e:
        return f"Vectorization Failed: {str(e)}"

    if not os.path.exists(excel_path):
        return f"Analysis Failed: Standard Excel not found at {excel_path}"
        
    try:
        analyze_msg, generated_files = run_rerank_analysis(
            standard, excel_path, base_db_root, output_dir, target_pdf_name=pdf_name
        )
    except Exception as e:
        return f"Rerank Analysis Failed: {str(e)}"

    llm_msg = "LLM Skipped (No files)"
    if generated_files:
        try:
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                return f"{analyze_msg} -> LLM Failed: Missing OPENAI_API_KEY"
            
            analyzer = LLMAnalyzer(api_key=api_key)
            llm_results = []
            
            for csv_path in generated_files:
                base_name = os.path.splitext(csv_path)[0]
                final_output_path = f"{base_name}_LLM.csv"
                
                res = analyzer.process_csv(csv_path, final_output_path)
                llm_results.append(res)
            
            llm_msg = ", ".join(llm_results)
            
        except Exception as e:
            return f"{analyze_msg} -> LLM Failed: {str(e)}"

    return f"Pipeline Completed. {chroma_msg} -> {analyze_msg} -> {llm_msg}"


def process_pdf_to_chroma(file_path: str, base_db_root: str, standard: str, force_update: bool):
    # (保持原樣)
    pdf_name = os.path.splitext(os.path.basename(file_path))[0]
    chroma_path = os.path.join(base_db_root, f"chroma_report_{standard}", pdf_name)
    
    if os.path.exists(chroma_path) and not force_update:
        return f"Vector DB already exists for {pdf_name}"

    loader = PyMuPDFLoader(file_path)
    pages = loader.load()
    splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
    documents = splitter.split_documents(pages)

    for i, doc in enumerate(documents):
        doc.metadata.update(
            {"page": doc.metadata.get("page", 0) + 1, "chunk_id": str(i)}
        )

    if os.path.exists(chroma_path):
        shutil.rmtree(chroma_path)

    Chroma.from_documents(
        documents=documents,
        embedding=model_manager.embeddings,
        persist_directory=chroma_path,
        collection_metadata={"hnsw:space": "cosine"},
    )
    return "Vectorization done"


def run_rerank_analysis(standard_name: str, excel_path: str, base_db_root: str, output_dir: str, target_pdf_name: str = None) -> Tuple[str, List[str]]:
    df_guide = pd.read_excel(excel_path, sheet_name="工作表2")
    if "Definition" not in df_guide.columns and "Label" in df_guide.columns:
         df_guide["Definition"] = df_guide["Label"]

    guidelines = df_guide.to_dict(orient="records")
    standard_folder = os.path.join(base_db_root, f"chroma_report_{standard_name}")
    
    if target_pdf_name:
        company_dirs = [os.path.join(standard_folder, target_pdf_name)]
    else:
        company_dirs = [
            os.path.join(standard_folder, d)
            for d in os.listdir(standard_folder)
            if os.path.isdir(os.path.join(standard_folder, d))
        ]

    results_summary = []
    generated_files = []

    for chroma_dir in company_dirs:
        if not os.path.exists(chroma_dir):
            continue
            
        comp_name = os.path.basename(chroma_dir)
        db = Chroma(persist_directory=chroma_dir, embedding_function=model_manager.embeddings)
        
        output_records = []

        for item in guidelines:
            definition = str(item.get("Definition", "")).strip()
            if not definition: continue

            rough = db.similarity_search_with_score(definition, k=20)
            if not rough:
                continue

            pairs = [[definition, doc.page_content] for doc, _ in rough]
            scores = model_manager.reranker.compute_score(pairs, normalize=True)
            
            reranked = sorted(zip(rough, scores), key=lambda x: x[1], reverse=True)[:5]
            
            for rank, ((doc, dist), sim) in enumerate(reranked, start=1):
                record = {
                    "Company": comp_name,
                    "Label": item.get("Label", ""),
                    "Definition": definition,
                    "Point": item.get("Point", ""),
                    "Page": doc.metadata.get("page"),
                    "RerankScore": float(sim),
                    "Content": doc.page_content,
                }
                output_records.append(record)

        out_path = os.path.join(output_dir, f"{comp_name}_rerank.csv")
        pd.DataFrame(output_records).to_csv(out_path, index=False, encoding="utf-8-sig")
        
        results_summary.append(comp_name)
        generated_files.append(out_path)

    return f"Rerank saved for: {', '.join(results_summary)}", generated_files