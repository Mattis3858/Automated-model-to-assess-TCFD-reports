import os
import pandas as pd
import numpy as np
from langchain_openai import OpenAIEmbeddings
from langchain_chroma import Chroma
from dotenv import load_dotenv
import openai
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

CHROMA_PATH = "chroma"
CHUNK_CSV_DIRECTORY = "data/tcfd_report_pdf_chunks_第四層/"
OUTPUT_CSV_DIRECTORY = "data/tcfd_report_pdf_chunks_matching_result_第四層/"

RERANKER_MODEL_NAME = 'BAAI/bge-reranker-base'
TOKENIZER_NAME = 'BAAI/bge-reranker-large'


load_dotenv()
openai.api_key = os.environ['OPENAI_API_KEY']
embedding_model = OpenAIEmbeddings(model="text-embedding-ada-002")
db = Chroma(persist_directory=CHROMA_PATH, embedding_function=embedding_model)
os.makedirs(OUTPUT_CSV_DIRECTORY, exist_ok=True)

try:
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")
    print(f"Loading Reranker Tokenizer: {TOKENIZER_NAME}...")
    reranker_tokenizer = AutoTokenizer.from_pretrained(TOKENIZER_NAME)
    print(f"Loading Reranker PyTorch Model: {RERANKER_MODEL_NAME}...")
    reranker_model = AutoModelForSequenceClassification.from_pretrained(RERANKER_MODEL_NAME)
    reranker_model.to(device)
    reranker_model.eval()
    print("Reranker model and tokenizer loaded successfully.")

except Exception as e:
    print(f"Error loading Reranker model or tokenizer: {e}")
    print("Please ensure 'transformers' and 'torch' are installed.")
    print("If using GPU, ensure CUDA is correctly set up for PyTorch.")
    reranker_tokenizer = None
    reranker_model = None

def load_chunks_from_csv(csv_path):
    return pd.read_csv(csv_path)

def query_chroma_for_initial_candidates(embedding_str, k=50):
    try:
        embedding_vector = np.array(eval(embedding_str)).flatten()
        if embedding_vector.ndim == 0:
            print(f"Warning: Could not parse embedding string: {embedding_str[:100]}...")
            return []
        results_with_scores = db.similarity_search_by_vector_with_relevance_scores(embedding_vector, k=k)
        return results_with_scores
    except Exception as e:
        print(f"Error querying ChromaDB: {e}")
        print(f"Problematic embedding string (first 100 chars): {embedding_str[:100]}")
        return []

def rerank_with_bge_pytorch(original_text, candidates):
    if not candidates or reranker_model is None or reranker_tokenizer is None:
        print("Skipping reranking: No candidates or reranker model/tokenizer not loaded.")
        return [{
            "類別": doc.metadata.get('label', 'N/A'),
            "content": doc.page_content,
            "chroma_distance": score,
            "reranker_score": -float('inf')
         } for doc, score in candidates]

    candidate_texts = [doc.page_content for doc, score in candidates]
    pairs = [[original_text, cand_text] for cand_text in candidate_texts]

    try:
        with torch.no_grad():
            inputs = reranker_tokenizer(pairs, padding=True, truncation=True, return_tensors='pt', max_length=512)
            inputs = {k: v.to(device) for k, v in inputs.items()}

            # Get scores from PyTorch model
            outputs = reranker_model(**inputs, return_dict=True)
            scores = outputs.logits.view(-1,).float()


            scores = scores.cpu().numpy()


        reranked_results = []
        for i, (doc, chroma_score) in enumerate(candidates):
            reranked_results.append({
                "類別": doc.metadata.get('label', 'N/A'),
                "content": doc.page_content,
                "chroma_distance": chroma_score,
                "reranker_score": scores[i].item()
            })

        reranked_results.sort(key=lambda x: x['reranker_score'], reverse=True)

        return reranked_results

    except Exception as e:
        print(f"Error during PyTorch reranking: {e}")
        return [{
            "類別": doc.metadata.get('label', 'N/A'),
            "content": doc.page_content,
            "chroma_distance": score,
            "reranker_score": -float('inf')
         } for doc, score in candidates]

def process_chunks_and_save(csv_path):
    df_chunks = load_chunks_from_csv(csv_path)
    output_data = []
    reranker_available = reranker_model is not None and reranker_tokenizer is not None

    for index, row in df_chunks.iterrows():
        print(f"Processing chunk {index+1}/{len(df_chunks)} from {os.path.basename(csv_path)}...")
        file_name = row['Filename']
        embedding_str = row.get('Chunk_Embedding') or row.get('Embedding')
        chunk_id = row['Chunk_ID']
        chunk_text = row['Chunk_Text']

        if not embedding_str or not isinstance(embedding_str, str):
             print(f"Skipping row {index+1}: Missing or invalid embedding string.")
             continue

        initial_candidates = query_chroma_for_initial_candidates(embedding_str, k=50)
        if reranker_available and initial_candidates:
             reranked_results = rerank_with_bge_pytorch(chunk_text, initial_candidates)
        else:
             reranked_results = [{
                "類別": doc.metadata.get('label', 'N/A'),
                "content": doc.page_content,
                "chroma_distance": score,
                "reranker_score": -float('inf')
             } for doc, score in initial_candidates]
             reranked_results.sort(key=lambda x: x['chroma_distance'])

        top_n = 10
        final_results = reranked_results[:top_n]

        matched_categories = [doc['類別'] for doc in final_results]
        unique_categories = list(set(matched_categories))
        reranker_scores = [doc['reranker_score'] for doc in final_results]
        chroma_distances = [doc['chroma_distance'] for doc in final_results]

        output_data.append({
            'Filename': file_name,
            'Chunk_ID': chunk_id,
            "Chunk_Text": chunk_text,
            "Matched_Categories_Reranked": unique_categories,
            "Top_Reranker_Scores": reranker_scores,
            "Top_Chroma_Distances": chroma_distances
        })

    base_name = os.path.basename(csv_path).replace("chunk_embeddings_", "").replace(".csv", "")
    output_file_name = f"{base_name}_matched_chunks_bge_pytorch_reranked.csv"
    output_file_path = os.path.join(OUTPUT_CSV_DIRECTORY, output_file_name)

    output_df = pd.DataFrame(output_data)
    output_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')
    print(f"Saved matched chunks and categories (BGE PyTorch Reranked) to {output_file_path}.")

def main():
    chunk_csv_files = [
        os.path.join(CHUNK_CSV_DIRECTORY, f)
        for f in os.listdir(CHUNK_CSV_DIRECTORY) if f.endswith('.csv')
    ]

    if not chunk_csv_files:
        print(f"No CSV files found in {CHUNK_CSV_DIRECTORY}")
        return

    # Ensure Reranker is loaded before processing files
    if reranker_model is None or reranker_tokenizer is None:
         print("Cannot proceed without Reranker model/tokenizer. Exiting.")
         return

    for csv_path in chunk_csv_files:
        print(f"\nProcessing {csv_path}...")
        process_chunks_and_save(csv_path)

if __name__ == "__main__":
    main()

# -------------------------------------------------以下是未使用 Reranker 的版本--------------------------------------------------
# def load_chunks_from_csv(csv_path):
#     return pd.read_csv(csv_path)

# def query_chroma_for_similar_chunks(embedding):
#     embedding = np.array(eval(embedding)).flatten()
    
    
#     results = db.similarity_search_by_vector_with_relevance_scores(embedding, k=91)
#     filtered_results = [
#         {"類別": doc[0].metadata['label'], "content": doc[0].page_content, "cosine_distance": doc[1]}
#         for doc in results if doc[1] < 0.20
#     ]
#     return filtered_results

# def process_chunks_and_save(csv_path):
#     df_chunks = load_chunks_from_csv(csv_path)
#     output_data = []

#     for _, row in df_chunks.iterrows():
#         file_name = row['Filename']
#         embedding = row['Chunk_Embedding']
#         chunk_id = row['Chunk_ID']
#         chunk_text = row['Chunk_Text']
        
#         results = query_chroma_for_similar_chunks(embedding)
#         matching_categories = [doc['類別'] for doc in results]
#         unique_categories = list(set(matching_categories))
#         cosine_distance = [doc['cosine_distance'] for doc in results]
#         cosine_distance = list(set(cosine_distance))

#         output_data.append({
#             'Filename': file_name,
#             'Chunk_ID': chunk_id,
#             "Chunk_Text": chunk_text,
#             "Embedding": embedding,
#             "Matched_Categories": unique_categories,
#             "Cosine_Distance": cosine_distance
#         })

#     base_name = os.path.basename(csv_path).replace("chunk_embeddings_", "").replace(".csv", "")
#     output_file_name = f"{base_name}_matched_chunks.csv"
#     output_file_path = os.path.join(OUTPUT_CSV_DIRECTORY, output_file_name)
    
#     output_df = pd.DataFrame(output_data)
#     output_df.to_csv(output_file_path, index=False)
#     print(f"Saved matched chunks and categories to {output_file_path}.")

# def main():
#     chunk_csv_files = [
#         os.path.join(CHUNK_CSV_DIRECTORY, f) 
#         for f in os.listdir(CHUNK_CSV_DIRECTORY) if f.endswith('.csv')
#     ]
    
#     for csv_path in chunk_csv_files:
#         print(f"\nProcessing {csv_path}...")
#         process_chunks_and_save(csv_path)

# if __name__ == "__main__":
#     main()
