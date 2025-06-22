import os
from dotenv import load_dotenv
import openai
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import OpenAIEmbeddings

load_dotenv()
openai.api_key = os.getenv('OPENAI_API_KEY')

BASE_CHROMA_PATH = "chroma_report"
PDF_NAME = "永豐金控_2023"

def main():
    chroma_path = os.path.join(BASE_CHROMA_PATH, PDF_NAME)

    if not os.path.exists(chroma_path):
        print(f"[ERROR] ChromaDB directory not found at: {chroma_path}")
        print("Please make sure you have run the data creation script first and the PDF_NAME is correct.")
        return

    print(f"[INFO] Loading ChromaDB from: {chroma_path}")
    
    db = Chroma(
        persist_directory=chroma_path,
        embedding_function=OpenAIEmbeddings()
    )

    total_docs = db._collection.count()
    print(f"\n[+] Total documents (chunks) in the database: {total_docs}")

    if total_docs == 0:
        print("[WARNING] The database is empty.")
        return

    print("\n[+] Inspecting the first 5 chunks...")
    retrieved_data = db.get(
        limit=20,
        include=["metadatas", "documents"]
    )

    for i in range(len(retrieved_data['ids'])):
        content = retrieved_data['documents'][i]
        metadata = retrieved_data['metadatas'][i]
        print("-" * 30)
        print(f"Chunk {i+1}:")
        print(f"  Content: '{content}'")
        print(f"  Metadata: {metadata}")
        print(f"  --> Page Number: {metadata.get('page')}")



    # print("\n[+] Performing a sample similarity search...")
    # # 您可以修改這個查詢詞來測試不同的內容
    # query = "氣候變遷風險" 
    # print(f"Query: '{query}'")

    # # 相似度搜尋
    # results = db.similarity_search(query, k=3)

    # if not results:
    #     print("[WARNING] Similarity search returned no results.")
    # else:
    #     print("\nTop 3 most relevant results:")
    #     for i, doc in enumerate(results):
    #         print("-" * 30)
    #         print(f"Result {i+1}:")
    #         print(f"  Content: '{doc.page_content.replace(os.linesep, ' ')}'")
    #         print(f"  Source Page: {doc.metadata.get('page')}")

if __name__ == "__main__":
    main()