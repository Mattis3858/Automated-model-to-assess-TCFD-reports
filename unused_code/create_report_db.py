import os
import shutil
from dotenv import load_dotenv
import openai
from langchain_community.document_loaders import PyPDFLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.embeddings import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma

# 載入環境變數以取得 OpenAI API 金鑰
load_dotenv()
openai.api_key = os.getenv('OPENAI_API_KEY')

# 定義設定
BASE_CHROMA_PATH = "chroma_tcfd"  # ChromaDB 儲存路徑
CHUNK_SIZE = 300                  # 文本分割的塊大小
CHUNK_OVERLAP = 100               # 文本塊之間的重疊大小

def process_pdf(pdf_path):
    """
    處理單一 PDF 檔案：
    - 載入 PDF 並分割為文本塊。
    - 生成嵌入向量並存入 ChromaDB。
    """
    if not os.path.exists(pdf_path):
        print(f"[ERROR] File not found: {pdf_path}")
        return

    # 使用報告書檔案名稱作為 ChromaDB 子資料夾名稱
    pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
    chroma_path = os.path.join(BASE_CHROMA_PATH, pdf_name)

    # 載入並分割 PDF
    try:
        print(f"[INFO] Processing PDF: {pdf_name}")
        loader = PyPDFLoader(pdf_path)
        text_splitter = RecursiveCharacterTextSplitter(
            separators="\n",
            chunk_size=CHUNK_SIZE,
            chunk_overlap=CHUNK_OVERLAP
        )
        documents = loader.load_and_split(text_splitter)
        print(f"[INFO] {pdf_name} 分割後的文本塊數量：{len(documents)}")

        # 如果 ChromaDB 資料夾已存在，清除並重新建立
        if os.path.exists(chroma_path):
            print(f"[INFO] Clearing existing ChromaDB at: {chroma_path}")
            shutil.rmtree(chroma_path)

        # 生成嵌入並存儲到 ChromaDB
        db = Chroma.from_documents(
            documents=documents,
            embedding=OpenAIEmbeddings(),
            persist_directory=chroma_path
        )
        db.persist()
        print(f"[SUCCESS] {pdf_name} 的 ChromaDB 已建立並儲存至 '{chroma_path}'")
    except Exception as e:
        print(f"[ERROR] Failed to process {pdf_path}: {e}")

def main():
    """
    主程式：
    - 定義要處理的 PDF 路徑。
    - 執行 process_pdf 函數處理每個檔案。
    """
    # 正確的 PDF 檔案路徑列表
    pdf_paths = [
        r"C:\Users\bugee\OneDrive\桌面\RAG\rag_for_tcfd_reports\data\processed_reports\永豐金控_2022_TCFD_報告書_preprocessed.pdf",
        r"C:\Users\bugee\OneDrive\桌面\RAG\rag_for_tcfd_reports\data\processed_reports\合庫金控_2022_TCFD_報告書_preprocessed.pdf",
        r"C:\Users\bugee\OneDrive\桌面\RAG\rag_for_tcfd_reports\data\processed_reports\富邦金控_2022_TCFD_報告書_preprocessed.pdf",
        r"C:\Users\bugee\OneDrive\桌面\RAG\rag_for_tcfd_reports\data\processed_reports\2893_新光銀行_2021_TCFD_專章完整永續_preprocessed.pdf",
        r"C:\Users\bugee\OneDrive\桌面\RAG\rag_for_tcfd_reports\data\processed_reports\6039_將來銀行_2022_TCFD_報告書_preprocessed.pdf",
        r"C:\Users\bugee\OneDrive\桌面\RAG\rag_for_tcfd_reports\data\processed_reports\6031_連線銀行_2022_TCFD_報告書.pdf"
    ]

    # 處理每一個 PDF 檔案
    for pdf_path in pdf_paths:
        print(f"[INFO] Starting to process: {pdf_path}")
        process_pdf(pdf_path)
        print(f"[INFO] Finished processing: {pdf_path}")


if __name__ == "__main__":
    main()