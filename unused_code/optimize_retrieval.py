import os
import time
import psutil
import pandas as pd
from dotenv import load_dotenv
from langchain_community.embeddings import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma

# Load environment variables
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")

# Paths to files
EXCEL_PATH = r"C:\Users\bugee\OneDrive\桌面\RAG\rag_for_tcfd_reports\data\tcfd第四層接露指引 - 加上第一層指標及關鍵字.xlsx"
RANK_PATH = r"C:\Users\bugee\OneDrive\桌面\RAG\rag_for_tcfd_reports\data\answer\rank.xlsx"

BASE_CHROMA_PATH = "chroma_tcfd"
OUTPUT_PATH = "rag_for_tcfd_reports/data/label_result"
ACCURACY_RESULT_PATH_TEMPLATE = "rag_for_tcfd_reports/data/accuracy_result/{}"  # 動態加入 report_name
ALL_RESULTS_PATH_TEMPLATE = r"C:\Users\bugee\OneDrive\桌面\query_result\all_query_results_{}.csv"

# Ensure base directories exist
os.makedirs(OUTPUT_PATH, exist_ok=True)

def map_label_to_q(label: str) -> str:
    last_part = label.split('_')[-1]
    if last_part.startswith("#MT"):
        replaced = last_part.replace("#MT", "MT", 1)
        return f"Q{replaced}"
    elif last_part.startswith("#S"):
        replaced = last_part.replace("#S", "S", 1)
        return f"Q{replaced}"
    elif last_part.isdigit():
        return f"Q{last_part}"
    else:
        return label

def load_documents():
    df = pd.read_excel(EXCEL_PATH)
    documents = []
    for _, row in df.iterrows():
        text = row['第四層(TCFD) 揭露指引']
        label = row['類別']
        documents.append((text, label))
    return documents

def get_all_results(report_name, k=50):  # 改為 k=50
    chroma_path = os.path.join(BASE_CHROMA_PATH, report_name)
    documents = load_documents()
    embedding_function = OpenAIEmbeddings()
    db = Chroma(persist_directory=chroma_path, embedding_function=embedding_function)
    ALL_RESULTS_PATH = ALL_RESULTS_PATH_TEMPLATE.format(report_name)

    all_results = []
    total_docs = len(documents)
    print(f"[INFO] Start retrieving all results for {total_docs} documents (k={k})")

    start_time = time.time()
    log_interval = 50  # 每50個文件印出一次狀態

    for i, (text, original_label) in enumerate(documents, start=1):
        mapped_label = map_label_to_q(original_label)
        query = f'揭露指標：{original_label}, 定義如下：{text}'
        results = db.similarity_search_with_relevance_scores(query, k=k)

        # 每50筆文件印出中間狀態
        if i % log_interval == 0:
            elapsed_time = time.time() - start_time
            cpu_usage = psutil.cpu_percent()
            mem_info = psutil.virtual_memory()
            memory_usage = mem_info.used / (1024**3)  # 轉換成GB
            print(f"[INFO] Processed {i}/{total_docs} documents...")
            print(f"[INFO] Elapsed Time: {elapsed_time:.2f}s, CPU Usage: {cpu_usage}%, Memory Usage: {memory_usage:.2f}GB")

        for doc, score in results:
            all_results.append({
                "original_label": original_label,
                "mapped_label": mapped_label,
                "score": score,
                "content": doc.page_content
            })

    df_all = pd.DataFrame(all_results)
    df_all.to_csv(ALL_RESULTS_PATH, index=False, encoding="utf-8-sig")
    print(f"[INFO] All query results saved to: {ALL_RESULTS_PATH}")
    return df_all

def calculate_accuracy(predicted, rank_file, institution, year, output_path, threshold, report_name):
    rank_df = pd.read_excel(rank_file)
    ground_truth_df = rank_df[(rank_df['Financial_Institutions'] == institution) &
                              (rank_df['Year'] == year)]
    ground_truth = ground_truth_df.loc[:, "Q1":"Q82"].iloc[0].to_dict()

    comparison = []
    ground_truth_list = []
    predicted_list = []

    for key, value in ground_truth.items():
        ground_truth_value = int(value) if not pd.isna(value) else 0
        predicted_value = 1 if key in predicted else 0

        ground_truth_list.append(ground_truth_value)
        predicted_list.append(predicted_value)
        match = int(ground_truth_value == predicted_value)
        comparison.append({
            "Label": key,
            "Ground Truth": ground_truth_value,
            "Predicted": predicted_value,
            "Match": match
        })

    print("\n[INFO] Ground Truth Labels:")
    print(ground_truth_list)
    print("\n[INFO] Predicted Labels:")
    print(predicted_list)

    # 計算準確率
    accuracy = sum([1 for gt, pred in zip(ground_truth_list, predicted_list) if gt == pred]) / len(ground_truth_list)

    # 在 result_df 裡面加入一列顯示 Accuracy
    result_df = pd.DataFrame(comparison)
    summary_df = pd.DataFrame([{
        "Label": f"Accuracy at {threshold}",
        "Ground Truth": "",
        "Predicted": "",
        "Match": f"{accuracy:.2%}"
    }])
    result_df = pd.concat([result_df, summary_df], ignore_index=True)

    # 報告書特定的 accuracy 路徑
    report_accuracy_path = os.path.join(output_path, report_name)
    os.makedirs(report_accuracy_path, exist_ok=True)

    accuracy_file = os.path.join(report_accuracy_path, f"accuracy_result_{threshold}.csv")
    result_df.to_csv(accuracy_file, index=False, encoding="utf-8-sig")
    print(f"[INFO] Accuracy results saved to: {accuracy_file}")
    print(f"[INFO] Accuracy: {accuracy:.2%}")

    return accuracy

def optimize_threshold(df_all, institution, year, report_name):
    thresholds = [x / 100 for x in range(75, 96, 2)]  # 0.75, 0.77, 0.79, ... 0.95
    best_threshold = 0.75
    best_accuracy = 0

    ACCURACY_RESULT_PATH = ACCURACY_RESULT_PATH_TEMPLATE.format(report_name)
    os.makedirs(ACCURACY_RESULT_PATH, exist_ok=True)

    for threshold in thresholds:
        print(f"\n[INFO] Testing threshold: {threshold}")
        filtered = df_all[df_all["score"] >= threshold]

        predicted = {}
        for label in filtered["mapped_label"].unique():
            predicted[label] = 1

        accuracy = calculate_accuracy(predicted, RANK_PATH, institution, year, ACCURACY_RESULT_PATH, threshold, report_name)
        print(f"[INFO] Accuracy at {threshold}: {accuracy:.2%}")

        if accuracy > best_accuracy:
            best_threshold = threshold
            best_accuracy = accuracy

    print(f"\n[INFO] Best threshold: {best_threshold}, Accuracy: {best_accuracy:.2%}")
    return best_threshold

def main():
    report_name = "富邦金控_2022_TCFD_報告書_preprocessed"
    institution = "富邦金"
    year = 2022

    ALL_RESULTS_PATH = ALL_RESULTS_PATH_TEMPLATE.format(report_name)
    if not os.path.exists(ALL_RESULTS_PATH):
        df_all = get_all_results(report_name, k=50)  # 這裡設定k=50
    else:
        print(f"[INFO] Loading precomputed results from {ALL_RESULTS_PATH}")
        df_all = pd.read_csv(ALL_RESULTS_PATH)

    best_threshold = optimize_threshold(df_all, institution, year, report_name)
    print(f"[INFO] Optimal threshold for {report_name}: {best_threshold}")

if __name__ == "__main__":
    main()
