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
    GUIDELINES_PATH = 'data/tcfd第四層揭露指引.xlsx'
    CHROMA_DIR = os.path.join('chroma_report', '永豐金控_2023')
    TOP_K = 5

    # 載入揭露指引列表
    guidelines = load_guidelines(GUIDELINES_PATH)

    # 初始化 Embedding 與 ChromaDB
    embedding = OpenAIEmbeddings()
    db = Chroma(persist_directory=CHROMA_DIR, embedding_function=embedding)

    # 準備要輸出的資料
    output_records = []

    # 查詢與整理資料
    for item in guidelines:
        label = item['Label']
        definition = item['Definition']

        results = db.similarity_search_with_score(definition, k=TOP_K)

        for idx, (doc, score) in enumerate(results, start=1):
            record = {
                'Label': label,
                'Definition': definition,
                'Rank': idx,
                'Score': score,
                'Page': doc.metadata.get('page', 'N/A'),
                'Chunk ID': doc.metadata.get('chunk_id', 'N/A'),
                'Content': doc.page_content.replace("\n", " ")
            }
            output_records.append(record)

    # 建立 DataFrame
    output_df = pd.DataFrame(output_records)

    
    output_csv_path = '永豐金控2023_output_chunks.csv'
    output_df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')

    print(f"Results have been saved to {output_csv_path}")


if __name__ == '__main__':
    main()
