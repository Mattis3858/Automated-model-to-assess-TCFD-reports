import os
import pandas as pd

# 設定文件路徑
LABEL_RESULT_DIR = "C:\\Users\\bugee\\OneDrive\\桌面\\RAG\\rag_for_tcfd_reports\\data\\label_result"
MERGE_DATA_PATH = "C:\\Users\\bugee\\OneDrive\\桌面\\RAG\\rag_for_tcfd_reports\\data\\merge_data.xlsx"
OUTPUT_DIR = "C:\\Users\\bugee\\OneDrive\\桌面\\RAG\\rag_for_tcfd_reports\\data\\accuracy_result"

# 確保輸出目錄存在
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 定義報告書名稱和對應文件名
report_files = [
    
    ("2881_富邦金控_2022_TCFD_報告書.pdf", "label_result_富邦金控_2022_TCFD_報告書_preprocessed.csv")
]

# 定義固定的 47 個標籤
fixed_labels = [
    "G-1-1", "G-1-2", "G-1-3", "G-2-1", "G-2-2", "G-2-3", "G-2-4",
    "MT-1-1", "MT-1-2", "MT-1-3", "MT-1-4", "MT-1-5", "#MT-1-6", "#MT-1-7", "#MT-1-8",
    "MT-2-1", "MT-2-2", "MT-2-3", "#MT-2-4", "MT-3-1", "MT-3-2", "MT-3-3", "MT-3-4",
    "R-1-1", "R-1-2", "R-1-3", "#R-1-4", "#R-1-5", "R-2-1", "R-2-2", "R-2-3",
    "R-3-1", "R-3-2", "S-1-1", "S-1-2", "S-1-3", "S-1-4", "#S-1-5", "S-2-1", 
    "S-2-2", "S-2-3", "S-2-4", "S-2-5", "S-2-6", "S-3-1", "S-3-2", "S-3-3"
]

def load_data(label_result_path, merge_data_df, target_filename):
    # 從 target_filename 中提取「銀行 + 年份」
    extracted_filename = target_filename.split('_')[1] + target_filename.split('_')[2][:4]  # 如 "彰化銀行2022"
    
    # 讀取 label_result 和 merge_data 文件
    label_result_df = pd.read_csv(label_result_path)
    target_row = merge_data_df[merge_data_df['filename'] == extracted_filename].iloc[0]
    
    # 建立真實標籤列表
    true_labels = target_row[fixed_labels].tolist()  # 取出對應 filename 的 row 並取得固定標籤的值
    
    # 建立模型預測的標籤列表，將每個標籤設為 1 或 0
    predicted_labels = [1 if label in label_result_df['指標名稱'].values else 0 for label in fixed_labels]
    
    # 印出 true_labels 和 predicted_labels 查看內容
    print(f"報告書: {target_filename}")
    print("真實標籤:", true_labels)
    print("預測標籤:", predicted_labels)
    
    return true_labels, predicted_labels

def evaluate_predictions(true_labels, predicted_labels):
    correct_labels = []
    incorrect_labels = []

    # 計算正確和錯誤的標籤
    for i, (true, pred) in enumerate(zip(true_labels, predicted_labels)):
        label_name = fixed_labels[i]
        if true == pred:  # 預測正確的情況
            correct_labels.append(label_name)
        else:  # 預測錯誤的情況
            incorrect_labels.append(label_name)
    
    # 計算準確率
    accuracy = len(correct_labels) / len(fixed_labels) if fixed_labels else 0

    return {
        "accuracy": accuracy,
        "correct_labels": correct_labels,
        "incorrect_labels": incorrect_labels
    }

def main():
    # 載入人工標籤資料
    merge_data_df = pd.read_excel(MERGE_DATA_PATH)
    total_accuracy = 0
    num_reports = len(report_files)

    for report_name, label_result_file in report_files:
        # 加載每份報告書的結果文件
        label_result_path = os.path.join(LABEL_RESULT_DIR, label_result_file)
        true_labels, predicted_labels = load_data(label_result_path, merge_data_df, report_name)
        
        # 計算準確率和分類結果
        result_summary = evaluate_predictions(true_labels, predicted_labels)
        
        # 累加準確率以便計算平均值
        total_accuracy += result_summary['accuracy']
        
        # 打印每份報告的結果
        print(f"報告書: {report_name}")
        print(f"準確率: {result_summary['accuracy'] * 100:.2f}%")
        print("正確分類的標籤:", result_summary['correct_labels'])
        print("錯誤分類的標籤:", result_summary['incorrect_labels'])
        
        # 將結果存到 CSV
        output_csv_path = os.path.join(OUTPUT_DIR, f"accuracy_result_{report_name}.csv")
        pd.DataFrame([result_summary]).to_csv(output_csv_path, index=False, encoding="utf-8-sig")
        print(f"結果已儲存到：{output_csv_path}\n")

    # 計算並打印平均準確率
    average_accuracy = total_accuracy / num_reports if num_reports > 0 else 0
    print(f"平均準確率: {average_accuracy * 100:.2f}%")

if __name__ == "__main__":
    main()
