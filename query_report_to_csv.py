import os
import pandas as pd
from dotenv import load_dotenv
from langchain_community.embeddings import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma
from FlagEmbedding import FlagReranker
from tqdm.auto import tqdm          # ★ 新增：進度條

# ------------ 輔助函式 ------------
def load_guidelines(excel_path: str, sheet_name: str = '工作表2'):
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    return df.to_dict(orient='records')

# ------------ 主流程 ------------
def main():
    load_dotenv()
    import openai
    openai.api_key = os.getenv('OPENAI_API_KEY')

    # ▶︎ 常數設定
    GUIDELINES_PATH = 'data/tcfd第四層揭露指引.xlsx'
    CHROMA_DIR      = os.path.join('chroma_report', '臺灣銀行_2023')

    CANDIDATE_K = 50
    TOP_N       = 10

    guidelines  = load_guidelines(GUIDELINES_PATH)

    # ▶︎ 初始化向量庫與 reranker
    embedding = OpenAIEmbeddings()
    db        = Chroma(persist_directory=CHROMA_DIR,
                       embedding_function=embedding)

    reranker  = FlagReranker('BAAI/bge-reranker-v2-m3', use_fp16=True)

    # ▶︎ 逐條指引查詢 → 粗排 → rerank → 收集結果
    output_records = []

    for item in tqdm(guidelines, desc="TCFD 指引進度"):   # ★ 用 tqdm 包住迴圈
        label, definition = item['Label'], item['Definition']

        # 粗排：取前 CANDIDATE_K 個 (doc, distance)
        rough = db.similarity_search_with_score(definition, k=CANDIDATE_K)

        # 準備 reranker 輸入 (query, passage)
        pairs  = [[definition, doc.page_content] for doc, _ in rough]
        scores = reranker.compute_score(pairs, normalize=True)  # 0~1 分數

        # 依 rerank 分數由高到低取前 TOP_N
        reranked = sorted(zip(rough, scores),
                          key=lambda x: x[1], reverse=True)[:TOP_N]

        for rank, ((doc, dist), sim) in enumerate(reranked, start=1):
            output_records.append({
                'Label'     : label,
                'Definition': definition,
                'Rank'      : rank,
                'CosDist'   : dist,
                'ReScore'   : sim,   # rerank 分數
                'Page'      : doc.metadata.get('page', 'N/A'),
                'Chunk ID'  : doc.metadata.get('chunk_id', 'N/A'),
                'Content'   : doc.page_content.replace('\n', ' ')
            })

    # ▶︎ 輸出 CSV
    out_df = pd.DataFrame(output_records)
    out_df.to_csv('臺灣銀行2023_output_chunks.csv',
                  index=False, encoding='utf-8-sig')
    print('\nCSV 已輸出：臺銀2023_output_chunks.csv')

if __name__ == '__main__':
    main()
