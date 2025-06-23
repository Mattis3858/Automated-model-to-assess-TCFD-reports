import os
import pandas as pd
from dotenv import load_dotenv
from langchain_community.embeddings import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma


def load_guidelines(excel_path: str, sheet_name: str = '工作表2'):
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    return df.to_dict(orient='records')


def main():
    # 載入環境變數
    load_dotenv()
    import openai
    openai.api_key = os.getenv('OPENAI_API_KEY')

    # 常數設定
    GUIDELINES_PATH = 'data/tcfd第四層揭露指引.xlsx'  # Excel 檔案路徑
    CHROMA_DIR = os.path.join('chroma_report', '永豐金控_2023')  # ChromaDB 資料夾
    TOP_K = 3  # 每個揭露指引取最相關的 chunk 數量

    # 載入揭露指引列表
    guidelines = load_guidelines(GUIDELINES_PATH)

    # 初始化 Embedding 與 ChromaDB
    embedding = OpenAIEmbeddings()
    db = Chroma(persist_directory=CHROMA_DIR, embedding_function=embedding)

    # 逐條查詢，並印出最相關的 chunks
    for item in guidelines:
        label = item['Label']
        definition = item['Definition']
        print(f"[INFO] Querying guideline {label}: {definition}")

        # similarity_search_with_score 回傳 (Document, score) 的 tuple 列表
        results = db.similarity_search_with_score(definition, k=TOP_K)

        for idx, (doc, score) in enumerate(results, start=1):
            page = doc.metadata.get('page', 'N/A')
            chunk_id = doc.metadata.get('chunk_id', 'N/A')
            content_snip = doc.page_content.replace("\n", " ")[:200]
            print(f"  Rank {idx}: score={score:.4f}, page={page}, chunk_id={chunk_id}")
            print(f"    Content: {content_snip}...")

        print('-' * 80)


if __name__ == '__main__':
    main()
