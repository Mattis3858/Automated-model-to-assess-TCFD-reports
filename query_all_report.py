import os
import pandas as pd
from dotenv import load_dotenv
from tqdm.auto import tqdm
from langchain_community.embeddings import OpenAIEmbeddings
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import Chroma
from FlagEmbedding import FlagReranker
import torch

def load_guidelines(excel_path: str, sheet_name: str = '工作表2'):
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    
    for col in ["Label", "Definition"]:
        if col not in df.columns:
            raise ValueError(f"指引檔缺少必要欄位：{col}")
    return df[["Label", "Definition"]].to_dict(orient='records')

def get_chroma_dirs(base_chroma_path: str):
    chroma_dirs = []
    if os.path.exists(base_chroma_path):
        for item in os.listdir(base_chroma_path):
            full_path = os.path.join(base_chroma_path, item)
            if os.path.isdir(full_path):
                chroma_dirs.append(full_path)
    return sorted(chroma_dirs)

def main():
    load_dotenv()
    GUIDELINES_PATH = 'data/tcfd第四層揭露指引.xlsx'
    BASE_CHROMA_PATH = 'chroma_sustainability_report'
    OUTPUT_DIR = 'data/sustainability_report_query_result'

    CANDIDATE_K = 50
    TOP_N = 5  
    EMBEDDING_MODEL_NAME = "Qwen/Qwen3-Embedding-0.6B"

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"[INFO] Using device: {device}")
    print("CUDA 可用：", torch.cuda.is_available())
    print("可見 GPU 數量：", torch.cuda.device_count())
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    guidelines = load_guidelines(GUIDELINES_PATH)

    reranker = FlagReranker('BAAI/bge-reranker-v2-m3', use_fp16=True, device=device)

    chroma_paths = get_chroma_dirs(BASE_CHROMA_PATH)
    if not chroma_paths:
        print(f"[ERROR] 在 '{BASE_CHROMA_PATH}' 中沒有找到任何 ChromaDB 目錄。請確認建立 DB 已成功。")
        return

    print(f"[INFO] 找到 {len(chroma_paths)} 個 ChromaDB 準備處理。")

    for chroma_dir in chroma_paths:
        company_name = os.path.basename(chroma_dir)
        output_filename = os.path.join(OUTPUT_DIR, f"{company_name}_output_chunks.csv")

        if os.path.exists(output_filename):
            print(f"[INFO] 檔案 '{output_filename}' 已存在，跳過處理 {company_name}。")
            continue

        print(f"\n--- 開始處理 {company_name} 的 ChromaDB ---")

        # embeddings = HuggingFaceEmbeddings(
        #     model_name=EMBEDDING_MODEL_NAME, model_kwargs={"device": device}  # 指定運行裝置
        # )
        embeddings = OpenAIEmbeddings()
        try:
            db = Chroma(persist_directory=chroma_dir, embedding_function=embeddings)
            print(f"[INFO] 成功載入 {company_name} 的 ChromaDB。")
        except Exception as e:
            print(f"[ERROR] 載入 {company_name} 的 ChromaDB 失敗：{e}")
            continue

        output_records = []

        for item in tqdm(guidelines, desc=f"TCFD 指引進度 ({company_name})"):
            label, definition = item['Label'], item['Definition']

            rough = db.similarity_search_with_score(definition, k=CANDIDATE_K)
            if not rough:
                continue

            pairs  = [[definition, doc.page_content] for doc, _ in rough]
            scores = reranker.compute_score(pairs, normalize=True)

            reranked = sorted(zip(rough, scores), key=lambda x: x[1], reverse=True)[:TOP_N]

            for rank, ((doc, dist), sim) in enumerate(reranked, start=1):
                output_records.append({
                    'Company': company_name,
                    'Label': label,
                    'Definition': definition,
                    '報告書頁數': doc.metadata.get('page', 'N/A'),
                    'Chunk ID': doc.metadata.get('chunk_id', 'N/A'),
                    'Chunk Text': doc.page_content.replace('\n', ' '),
                    '是否真的有揭露此標準?(Y/N)': "",   
                    'reasoning': "",
                    'RerankScore': float(sim),
                    'InitScoreOrDist': float(dist),
                    'Rank': rank                
                })

        out_df = pd.DataFrame(output_records)
        out_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        print(f'\nCSV 已輸出：{output_filename}')
        print(f"--- 完成處理 {company_name} 的 ChromaDB ---\n")

if __name__ == '__main__':
    main()
