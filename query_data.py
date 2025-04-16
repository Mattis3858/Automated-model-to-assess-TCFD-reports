import os
import pandas as pd
import numpy as np
from langchain_openai import OpenAIEmbeddings
from langchain_chroma import Chroma
from dotenv import load_dotenv
import openai 

CHROMA_PATH = "chroma"
CHUNK_CSV_DIRECTORY = "data/tcfd_report_pdf_chunks_第四層/"
OUTPUT_CSV_DIRECTORY = "data/tcfd_report_pdf_chunks_matching_result_第四層/"


load_dotenv()
openai.api_key = os.environ['OPENAI_API_KEY']

os.makedirs(OUTPUT_CSV_DIRECTORY, exist_ok=True)

embedding_model = OpenAIEmbeddings(model="text-embedding-ada-002")
def load_chunks_from_csv(csv_path):
    return pd.read_csv(csv_path)

def query_chroma_for_similar_chunks(embedding):
    embedding = np.array(eval(embedding)).flatten()
    db = Chroma(persist_directory=CHROMA_PATH, embedding_function=embedding_model)
    
    results = db.similarity_search_by_vector_with_relevance_scores(embedding, k=91)
    filtered_results = [
        {"類別": doc[0].metadata['類別'], "content": doc[0].page_content, "cosine_distance": doc[1]}
        for doc in results if doc[1] < 0.20
    ]
    return filtered_results

def process_chunks_and_save(csv_path):
    df_chunks = load_chunks_from_csv(csv_path)
    output_data = []

    for _, row in df_chunks.iterrows():
        file_name = row['Filename']
        embedding = row['Chunk_Embedding']
        chunk_id = row['Chunk_ID']
        chunk_text = row['Chunk_Text']
        
        results = query_chroma_for_similar_chunks(embedding)
        matching_categories = [doc['類別'] for doc in results]
        unique_categories = list(set(matching_categories))
        cosine_distance = [doc['cosine_distance'] for doc in results]
        cosine_distance = list(set(cosine_distance))

        output_data.append({
            'Filename': file_name,
            'Chunk_ID': chunk_id,
            "Chunk_Text": chunk_text,
            "Embedding": embedding,
            "Matched_Categories": unique_categories,
            "Cosine_Distance": cosine_distance
        })

    base_name = os.path.basename(csv_path).replace("chunk_embeddings_", "").replace(".csv", "")
    output_file_name = f"{base_name}_matched_chunks.csv"
    output_file_path = os.path.join(OUTPUT_CSV_DIRECTORY, output_file_name)
    
    output_df = pd.DataFrame(output_data)
    output_df.to_csv(output_file_path, index=False)
    print(f"Saved matched chunks and categories to {output_file_path}.")

def main():
    chunk_csv_files = [
        os.path.join(CHUNK_CSV_DIRECTORY, f) 
        for f in os.listdir(CHUNK_CSV_DIRECTORY) if f.endswith('.csv')
    ]
    
    for csv_path in chunk_csv_files:
        print(f"\nProcessing {csv_path}...")
        process_chunks_and_save(csv_path)

if __name__ == "__main__":
    main()
