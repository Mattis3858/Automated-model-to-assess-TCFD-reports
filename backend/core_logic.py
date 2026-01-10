import os
import shutil
import pandas as pd
from typing import List, Tuple, Dict
from langchain_community.document_loaders import PyMuPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import Chroma
from models import model_manager
from llm_analyzer import LLMAnalyzer
import logging

logger = logging.getLogger(__name__)

class SummaryManager:
    def __init__(self, summary_dir: str):
        self.summary_dir = summary_dir
        self.summary_file = os.path.join(summary_dir, "company_disclosure_summary.csv")
        self.detail_file = os.path.join(summary_dir, "company_label_detail.csv")
        os.makedirs(summary_dir, exist_ok=True)

    def update_summary(self, llm_result_csv: str):

        try:
            df = pd.read_csv(llm_result_csv)
            if df.empty:
                return "Empty CSV, skip summary."

            required_col = "is_disclosed"
            if required_col not in df.columns:
                return f"Missing '{required_col}' column."

            if "Company" in df.columns:
                company = df["Company"].iloc[0]
            else:
                basename = os.path.basename(llm_result_csv)
                company = basename.replace("_rerank_LLM.csv", "").replace("_LLM.csv", "")

            
            label_results = []
            detail_rows = []
            
            for label, group in df.groupby("Label"):

                vals = group[required_col].astype(str).str.upper().str.strip().tolist()
                
                y_count = sum(1 for v in vals if "Y" in v)
                
                final_status = "Y" if y_count >= 1 else "N"
                
                label_results.append(final_status)
                
                detail_rows.append({
                    "Company": company,
                    "Label": label,
                    "Final_YN": final_status,
                    "Y_Count": y_count,
                    "Total_Chunks": len(vals)
                })

            total_labels = len(label_results)
            y_labels_count = sum(1 for res in label_results if res == "Y")
            ratio = y_labels_count / total_labels if total_labels > 0 else 0.0

            new_summary_row = {
                "Company": company,
                "Total_Labels": total_labels,
                "Y_Labels": y_labels_count,
                "N_Labels": total_labels - y_labels_count,
                "Disclosure_Ratio": round(ratio, 4),
                "Last_Updated": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            self._update_csv(self.summary_file, new_summary_row, key_col="Company")
            
            self._update_detail_csv(self.detail_file, detail_rows, company)

            return f"Summary updated for {company} (Ratio: {ratio:.2%})"

        except Exception as e:
            logger.error(f"Error updating summary: {e}")
            return f"Summary update failed: {str(e)}"

    def _update_csv(self, file_path: str, new_row: dict, key_col: str):
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
        else:
            df = pd.DataFrame(columns=new_row.keys())

        if not df.empty and key_col in df.columns:
            mask = df[key_col] == new_row[key_col]
            if mask.any():
                for col, val in new_row.items():
                    df.loc[mask, col] = val
            else:
                # 新增 row
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        else:
             df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

        df.to_csv(file_path, index=False, encoding="utf-8-sig")

    def _update_detail_csv(self, file_path: str, new_rows: List[dict], company: str):
        if not new_rows: return
        
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            df = df[df["Company"] != company]
            df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        else:
            df = pd.DataFrame(new_rows)
            
        df.to_csv(file_path, index=False, encoding="utf-8-sig")


def process_pipeline(file_path: str, base_db_root: str, output_dir: str, standard: str, excel_path: str, force_update: bool, update_callback=None):

    pdf_name = os.path.splitext(os.path.basename(file_path))[0]
    
    try:
        chroma_msg = process_pdf_to_chroma(file_path, base_db_root, standard, force_update)
    except Exception as e:
        logger.error(f"Vectorization Failed: {str(e)}")
        return f"Vectorization Failed: {str(e)}"

    if not os.path.exists(excel_path):
        return f"Analysis Failed: Standard Excel not found at {excel_path}"

    if update_callback:
        logger.info("Updating status to Retrieving and Reranking")
        update_callback("Step 2/3: Retrieving and Reranking...")
        
    try:
        analyze_msg, generated_files = run_rerank_analysis(
            standard, excel_path, base_db_root, output_dir, target_pdf_name=pdf_name
        )
    except Exception as e:
        logger.error(f"Rerank Analysis Failed: {str(e)}")
        return f"Rerank Analysis Failed: {str(e)}"

    if update_callback:
        logger.info("Updating status to LLM Generating Insights")
        update_callback("Step 3/3: LLM Generating Insights...")

    llm_msg = "LLM Skipped (No files)"
    summary_msg = ""
    
    if generated_files:
        try:
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                return f"{analyze_msg} -> LLM Failed: Missing OPENAI_API_KEY"
            
            analyzer = LLMAnalyzer(api_key=api_key)
            llm_results = []
            
            summary_manager = SummaryManager(summary_dir=os.path.join(output_dir, "summary"))

            for csv_path in generated_files:
                base_name = os.path.splitext(csv_path)[0]
                final_output_path = f"{base_name}_LLM.csv"
                
                res = analyzer.process_csv(csv_path, final_output_path)
                llm_results.append(res)
                
                s_msg = summary_manager.update_summary(final_output_path)
                summary_msg += f" [{s_msg}]"
            
            llm_msg = ", ".join(llm_results)
            
        except Exception as e:
            logger.error(f"LLM Failed: {str(e)}")
            return f"{analyze_msg} -> LLM Failed: {str(e)}"

    return f"Pipeline Completed. {chroma_msg} -> {analyze_msg} -> {llm_msg}. {summary_msg}"


def process_pdf_to_chroma(file_path: str, base_db_root: str, standard: str, force_update: bool):
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
        clean_name = comp_name
        if "_" in comp_name:
            parts = comp_name.split('_', 1)
            if len(parts) > 1:
                clean_name = parts[1]
        
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
                    "Company": clean_name,
                    "Label": item.get("Label", ""),
                    "Definition": definition,
                    "Point": item.get("Point", ""),
                    "Page": doc.metadata.get("page"),
                    "RerankScore": float(sim),
                    "Rank": rank,
                    "Content": doc.page_content,
                }
                output_records.append(record)

        out_path = os.path.join(output_dir, f"{clean_name}_rerank.csv")
        pd.DataFrame(output_records).to_csv(out_path, index=False, encoding="utf-8-sig")
        
        results_summary.append(clean_name)
        generated_files.append(out_path)

    return f"Rerank saved for: {', '.join(results_summary)}", generated_files