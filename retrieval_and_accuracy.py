import os
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
ACCURACY_RESULT_PATH = "rag_for_tcfd_reports/data/accuracy_result"

# Ensure output directories exist
os.makedirs(OUTPUT_PATH, exist_ok=True)
os.makedirs(ACCURACY_RESULT_PATH, exist_ok=True)

########################################################################
# 自動將 Excel「類別」映射成 Qx / QMTx / QSx 等的函式
########################################################################
def map_label_to_q(label: str) -> str:
    """
    根據 label 最後一段自動轉成對應的 Qxxx 規則：
    1. 若最後一段以 '#MT' 開頭，則 '#MT1' => 'QMT1'
    2. 若最後一段以 '#S' 開頭，則 '#S2' => 'QS2'
    3. 若最後一段是純數字 => '4' => 'Q4'
    4. 否則維持原樣 (或可自行加其他規則)
    """
    last_part = label.split('_')[-1]  # 例："#MT1", "#S2", "4", "82"

    if last_part.startswith("#MT"):
        replaced = last_part.replace("#MT", "MT", 1)  # "#MT1" -> "MT1"
        return f"Q{replaced}"                        # -> "QMT1"

    elif last_part.startswith("#S"):
        replaced = last_part.replace("#S", "S", 1)   # "#S2" -> "S2"
        return f"Q{replaced}"                        # -> "QS2"

    elif last_part.isdigit():
        return f"Q{last_part}"                       # 純數字 -> Q4, Q82

    else:
        # 如果有其他特殊邏輯，可在此補充
        return label

########################################################################
# Load documents from Excel
########################################################################
def load_documents():
    df = pd.read_excel(EXCEL_PATH)
    documents = []
    for _, row in df.iterrows():
        text = row['第四層(TCFD) 揭露指引']
        label = row['類別']
        documents.append((text, label))
    return documents

########################################################################
# Query text with threshold
########################################################################
def query_text(query, chroma_path, threshold):
    embedding_function = OpenAIEmbeddings()
    db = Chroma(persist_directory=chroma_path, embedding_function=embedding_function)
    results = db.similarity_search_with_relevance_scores(query, k=100)
    filtered_results = [result for result in results if result[1] >= threshold]
    return filtered_results

########################################################################
# Process report and retrieve results
########################################################################
def process_report(report_name, threshold):
    chroma_path = os.path.join(BASE_CHROMA_PATH, report_name)
    documents = load_documents()
    results = {}
    output_data = []

    total_docs = len(documents)
    print(f"[INFO] Start processing {total_docs} documents with threshold={threshold}")

    for i, (text, original_label) in enumerate(documents, start=1):
        # 將 Excel 讀到的原始 label 映射成對應的 Qxxx
        mapped_label = map_label_to_q(original_label)

        query = f'揭露指標：{original_label}, 定義如下：{text}'
        retrieved = query_text(query, chroma_path, threshold)
        
        # 顯示進度條或進度訊息
        if i % 10 == 0:  # 每隔10筆顯示一次進度
            print(f"[INFO] Processed {i}/{total_docs} documents...")

        if retrieved:
            # 如果有檢索到 => 視為該 mapped_label 預測為 1
            results[mapped_label] = 1
            for doc, score in retrieved:
                output_data.append({
                    "原始指標名稱": original_label,
                    "映射後指標名稱": mapped_label,
                    "相似度": score,
                    "片段內容": doc.page_content
                })

    label_output_file = os.path.join(OUTPUT_PATH, f"label_result_{report_name}_threshold_{threshold}.csv")
    output_df = pd.DataFrame(output_data)
    output_df.to_csv(label_output_file, index=False, encoding="utf-8-sig")
    print(f"[INFO] Label result saved to: {label_output_file}")

    return results

########################################################################
# Calculate accuracy
########################################################################
def calculate_accuracy(predicted, rank_file, institution, year, output_path):
    rank_df = pd.read_excel(rank_file)
    
    # 過濾指定的 institution 和 year
    ground_truth_df = rank_df[(rank_df['Financial_Institutions'] == institution) &
                              (rank_df['Year'] == year)]
    
    # 選取 Ground Truth 欄位 Q1 到 Q82
    ground_truth = ground_truth_df.loc[:, "Q1":"Q82"].iloc[0].to_dict()

    comparison = []
    ground_truth_list = []
    predicted_list = []

    for key, value in ground_truth.items():
        ground_truth_value = int(value) if not pd.isna(value) else 0
        # 只要 key 在 predicted 裏就認定為 1
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

    # Debug: 印出被預測為 0 的 label
    for i, (gt_key, gt_val) in enumerate(ground_truth.items()):
        if predicted_list[i] == 0:
            print(f"[DEBUG] Label = {gt_key} 被預測為 0")

    # 計算準確率
    accuracy = sum([1 for gt, pred in zip(ground_truth_list, predicted_list) if gt == pred]) / len(ground_truth_list)

    result_df = pd.DataFrame(comparison)
    accuracy_file = os.path.join(output_path, "accuracy_result.csv")
    result_df.to_csv(accuracy_file, index=False, encoding="utf-8-sig")
    print(f"[INFO] Accuracy results saved to: {accuracy_file}")
    print(f"[INFO] Accuracy: {accuracy:.2%}")

    return accuracy

########################################################################
# Optimize threshold
########################################################################
def optimize_threshold(report_name, institution, year):
    thresholds = [x / 100 for x in range(70, 91, 2)]  # 0.70, 0.72, ..., 0.90
    best_threshold = 0.7
    best_accuracy = 0

    for threshold in thresholds:
        print(f"\nTesting threshold: {threshold}")
        predicted = process_report(report_name, threshold)  
        accuracy = calculate_accuracy(predicted, RANK_PATH, institution, year, ACCURACY_RESULT_PATH)
        print(f"Accuracy at {threshold}: {accuracy:.2%}")
        if accuracy > best_accuracy:
            best_threshold = threshold
            best_accuracy = accuracy

    print(f"\n[INFO] Best threshold: {best_threshold}, Accuracy: {best_accuracy:.2%}")
    return best_threshold

########################################################################
# Main process
########################################################################
def main():
    report_name = "新光金控_2022_TCFD_報告書_preprocessed"
    institution = "新光金"
    year = 2022
    best_threshold = optimize_threshold(report_name, institution, year)
    print(f"[INFO] Optimal threshold for {report_name}: {best_threshold}")

if __name__ == "__main__":
    main()
