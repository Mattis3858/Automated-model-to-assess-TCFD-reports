# from langchain_community.document_loaders import DirectoryLoader
# from langchain.text_splitter import RecursiveCharacterTextSplitter
# from langchain.schema import Document
# # from langchain.embeddings import OpenAIEmbeddings
# from langchain_openai import OpenAIEmbeddings
# from langchain_community.vectorstores import Chroma
# import openai 
# from dotenv import load_dotenv
# import os
# import shutil
# import pandas as pd

# load_dotenv()
# openai.api_key = os.environ['OPENAI_API_KEY']

# CHROMA_PATH = "chroma"
# CSV_PATH = "data/filtered_揭露指引.csv"

# BATCH_SIZE = 50

# def main():
#     generate_data_store()


# def generate_data_store():
#     df = pd.read_csv(CSV_PATH, usecols=['Label', 'Definition'], encoding='utf-8')
#     df = df.drop_duplicates(subset='Definition').reset_index(drop=True)

#     documents = []
#     for idx, row in df.iterrows():
#         text = str(row['Definition']).strip()
#         label = str(row['Label']).strip()
#         if text:
#             metadata = {'label': label, 'source_row': idx}
#             documents.append(Document(page_content=text, metadata=metadata))

#     print(f"Loaded {len(documents)} documents from CSV.")
#     from langchain.text_splitter import RecursiveCharacterTextSplitter
#     text_splitter = RecursiveCharacterTextSplitter(
#         chunk_size=8000,
#         chunk_overlap=4000,
#         length_function=len
#     )
#     chunks = text_splitter.split_documents(documents)
#     print(f"Split into {len(chunks)} chunks.")
#     save_to_chroma(chunks)

# def save_to_chroma(chunks):
#     if os.path.exists(CHROMA_PATH):
#         shutil.rmtree(CHROMA_PATH)

#     embedding_fn = OpenAIEmbeddings(model="text-embedding-ada-002")
#     db = Chroma(persist_directory=CHROMA_PATH, embedding_function=embedding_fn)

#     for i in range(0, len(chunks), BATCH_SIZE):
#         batch = chunks[i:i + BATCH_SIZE]
#         db.add_documents(batch)
#         print(f"Added batch {i // BATCH_SIZE + 1} with {len(batch)} chunks.")

#     db.persist()
#     print(f"Saved {len(chunks)} chunks into Chroma at '{CHROMA_PATH}'.")

# if __name__ == "__main__":
#     main()


from langchain_community.document_loaders import DirectoryLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.schema import Document
# from langchain.embeddings import OpenAIEmbeddings
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma

import os
import shutil
import pandas as pd
import openai

# 新增：讓 dotenv 自動往上層找 .env
from dotenv import load_dotenv, find_dotenv

# find_dotenv() 會從當前工作目錄一路往上，找到第一個 .env 為止
env_path = find_dotenv()
if not env_path:
    raise FileNotFoundError("找不到 .env 檔，請確認專案根目錄下有 .env")
load_dotenv(env_path)

# 確認載入
print(f"Loaded .env from: {env_path}")
print(f"OPENAI_API_KEY length: {len(os.environ.get('OPENAI_API_KEY',''))}")

# 設定 OpenAI API Key
openai.api_key = os.environ['OPENAI_API_KEY']

CHROMA_PATH = "chroma"
CSV_PATH = "data/filtered_揭露指引.csv"

BATCH_SIZE = 50

def main():
    generate_data_store()

def generate_data_store():
    df = pd.read_csv(CSV_PATH, usecols=['Label', 'Definition'], encoding='utf-8')
    df = df.drop_duplicates(subset='Definition').reset_index(drop=True)

    documents = []
    for idx, row in df.iterrows():
        text = str(row['Definition']).strip()
        label = str(row['Label']).strip()
        if text:
            metadata = {'label': label, 'source_row': idx}
            documents.append(Document(page_content=text, metadata=metadata))

    print(f"Loaded {len(documents)} documents from CSV.")
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=8000,
        chunk_overlap=4000,
        length_function=len
    )
    chunks = text_splitter.split_documents(documents)
    print(f"Split into {len(chunks)} chunks.")
    save_to_chroma(chunks)

def save_to_chroma(chunks):
    if os.path.exists(CHROMA_PATH):
        shutil.rmtree(CHROMA_PATH)

    embedding_fn = OpenAIEmbeddings(model="text-embedding-ada-002")
    db = Chroma(persist_directory=CHROMA_PATH, embedding_function=embedding_fn)

    for i in range(0, len(chunks), BATCH_SIZE):
        batch = chunks[i:i + BATCH_SIZE]
        db.add_documents(batch)
        print(f"Added batch {i // BATCH_SIZE + 1} with {len(batch)} chunks.")

    db.persist()
    print(f"Saved {len(chunks)} chunks into Chroma at '{CHROMA_PATH}'.")

if __name__ == "__main__":
    main()
