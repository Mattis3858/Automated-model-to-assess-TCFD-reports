import os
import pandas as pd
import numpy as np
import ast
import logging
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from langchain_openai import OpenAIEmbeddings
from langchain_chroma import Chroma
from dotenv import load_dotenv
import openai


class Config:
    CHROMA_PATH = "chroma"
    CHUNK_CSV_DIRECTORY = "data/tcfd_report_pdf_chunks_第四層/"
    OUTPUT_CSV_DIRECTORY = "data/tcfd_report_pdf_chunks_matching_result_第四層/"

    COSINE_DISTANCE_THRESHOLD = 1.0
    TOP_K_SIMILAR_CHUNKS = 91

    EMBEDDING_MODEL_NAME = "text-embedding-ada-002"
    MAX_WORKERS = 4


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("chroma_query.log"),
        logging.StreamHandler(),
    ],
)

load_dotenv()
openai.api_key = os.environ.get("OPENAI_API_KEY")

if not openai.api_key:
    logging.error(
        "OPENAI_API_KEY is not set. Please check your .env file or environment variables."
    )
    exit(1)

try:
    embedding_model = OpenAIEmbeddings(model=Config.EMBEDDING_MODEL_NAME)
    logging.info(f"Initialized embedding model: {Config.EMBEDDING_MODEL_NAME}")
except Exception as e:
    logging.error(f"Failed to initialize OpenAIEmbeddings: {e}")
    exit(1)

try:
    os.makedirs(Config.OUTPUT_CSV_DIRECTORY, exist_ok=True)
    db = Chroma(
        persist_directory=Config.CHROMA_PATH, embedding_function=embedding_model
    )
    logging.info(f"Initialized Chroma database at {Config.CHROMA_PATH}")
except Exception as e:
    logging.error(f"Failed to initialize Chroma database: {e}")
    exit(1)


def load_chunks_from_csv(csv_path: str) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(csv_path)
        df["Chunk_Embedding_Vector"] = df["Chunk_Embedding"].apply(
            lambda x: np.array(ast.literal_eval(x)).flatten()
        )
        logging.info(
            f"Successfully loaded {len(df)} chunks from {os.path.basename(csv_path)}"
        )
        return df
    except FileNotFoundError:
        logging.error(f"CSV file not found: {csv_path}")
        return None
    except pd.errors.EmptyDataError:
        logging.warning(f"CSV file is empty: {csv_path}")
        return None
    except Exception as e:
        logging.error(f"Error loading or processing CSV {csv_path}: {e}")
        return None


def query_chroma_for_similar_chunks(embedding_vector: np.ndarray) -> list[dict]:
    try:
        results = db.similarity_search_by_vector_with_relevance_scores(
            embedding_vector, k=Config.TOP_K_SIMILAR_CHUNKS
        )

        filtered_results = [
            {
                "category": doc[0].metadata.get("label", "unknown"),
                "content": doc[0].page_content,
                "cosine_distance": doc[1],
            }
            for doc in results
            if doc[1] < Config.COSINE_DISTANCE_THRESHOLD
        ]
        return filtered_results
    except Exception as e:
        logging.error(f"Error querying Chroma database: {e}")
        return []


def process_chunks_and_save(csv_path: str) -> None:
    logging.info(f"Starting processing for {os.path.basename(csv_path)}...")
    df_chunks = load_chunks_from_csv(csv_path)

    if df_chunks is None or df_chunks.empty:
        logging.warning(
            f"Skipping {os.path.basename(csv_path)} due to no data or loading error."
        )
        return

    output_data = []

    for _, row in tqdm(
        df_chunks.iterrows(),
        total=len(df_chunks),
        desc=f"Querying Chroma for {os.path.basename(csv_path)}",
    ):
        file_name = row["Filename"]
        chunk_id = row["Chunk_ID"]
        chunk_text = row["Chunk_Text"]
        embedding_vector = row["Chunk_Embedding_Vector"]

        results = query_chroma_for_similar_chunks(embedding_vector)

        matching_categories = []
        matching_distances = []
        for doc in results:
            matching_categories.append(doc["category"])
            matching_distances.append(doc["cosine_distance"])

        output_data.append(
            {
                "Filename": file_name,
                "Chunk_ID": chunk_id,
                "Chunk_Text": chunk_text,
                "Original_Embedding_Str": row["Chunk_Embedding"],
                "Matched_Categories_List": matching_categories,
                "Matched_Distances_List": matching_distances,
            }
        )

    if not output_data:
        logging.warning(
            f"No matching data found for {os.path.basename(csv_path)}. Skipping CSV creation."
        )
        return

    output_df = pd.DataFrame(output_data)

    base_name = (
        os.path.basename(csv_path).replace("chunk_embeddings_", "").replace(".csv", "")
    )
    output_file_name = f"{base_name}_matched_chunks.csv"
    output_file_path = os.path.join(Config.OUTPUT_CSV_DIRECTORY, output_file_name)

    try:
        output_df.to_csv(output_file_path, index=False)
        logging.info(
            f"Results for {os.path.basename(csv_path)} saved to {output_file_path}."
        )
    except Exception as e:
        logging.error(
            f"Error saving output CSV for {os.path.basename(csv_path)} to {output_file_path}: {e}"
        )


def main():
    os.makedirs(Config.OUTPUT_CSV_DIRECTORY, exist_ok=True)

    chunk_csv_files = []
    for f in os.listdir(Config.CHUNK_CSV_DIRECTORY):
        if f.endswith(".csv"):
            csv_path = os.path.join(Config.CHUNK_CSV_DIRECTORY, f)
            # 可以選擇性地檢查輸出檔案是否已存在，跳過已處理的檔案
            # output_file_name = f"{os.path.splitext(os.path.basename(csv_path))[0].replace('chunk_embeddings_', '')}_matched_chunks.csv"
            # output_file_path = os.path.join(Config.OUTPUT_CSV_DIRECTORY, output_file_name)
            # if os.path.exists(output_file_path):
            #     logging.info(f"Skipping {os.path.basename(csv_path)}: output CSV already exists.")
            #     continue
            chunk_csv_files.append(csv_path)

    if not chunk_csv_files:
        logging.info("No chunk CSV files found to process.")
        return

    logging.info(
        f"Found {len(chunk_csv_files)} chunk CSV files to process. Starting parallel execution..."
    )

    with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_chunks_and_save, csv_path): csv_path
            for csv_path in chunk_csv_files
        }

        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Overall CSV Processing"
        ):
            csv_path_being_processed = futures[future]
            try:
                future.result()
                logging.info(
                    f"Finished processing {os.path.basename(csv_path_being_processed)}."
                )
            except Exception as exc:
                logging.error(
                    f"Error processing {os.path.basename(csv_path_being_processed)}: {exc}"
                )

    logging.info("All chunk CSV processing tasks completed.")


if __name__ == "__main__":
    main()
