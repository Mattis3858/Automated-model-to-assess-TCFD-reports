import os
import time
import psutil
import pandas as pd
from dotenv import load_dotenv
from langchain_community.embeddings import OpenAIEmbeddings
from langchain_chroma import Chroma  # 更新導入方式
import matplotlib.pyplot as plt  # 繪圖庫

# 全局字型設置（可選，避免繪圖時出現中文字符缺失）
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']  # 設置支持 CJK 的字型
plt.rcParams['axes.unicode_minus'] = False  # 避免負號顯示問題

# 載入環境變數
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")

# 檔案路徑
EXCEL_PATH = r"C:\Users\bugee\OneDrive\桌面\RAG\rag_for_tcfd_reports\data\tcfd第四層接露指引 - 加上第一層指標及關鍵字.xlsx"
RANK_PATH = r"C:\Users\bugee\OneDrive\桌面\RAG\rag_for_tcfd_reports\data\answer\rank.xlsx"

BASE_CHROMA_PATH = "chroma_tcfd"
OUTPUT_PATH = "rag_for_tcfd_reports/data/label_result"
ACCURACY_RESULT_BASE = "rag_for_tcfd_reports/data/accuracy_result"
ALL_RESULTS_PATH_TEMPLATE = r"C:\Users\bugee\OneDrive\桌面\query_result\all_query_results_{}.csv"

# 確保基礎目錄存在
os.makedirs(OUTPUT_PATH, exist_ok=True)
os.makedirs(ACCURACY_RESULT_BASE, exist_ok=True)

# 目標欄位
TARGET_COLUMNS = [
    'Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7', 'Q8', 'Q9', 'Q10', 
    'Q11', 'Q12', 'Q13', 'Q14', 'Q15', 'Q16', 'Q17', 'Q18', 'Q19', 
    'Q20', 'Q21', 'Q22', 'QS1', 'QS2', 'Q23', 'Q24', 'Q25', 'Q26', 
    'Q27', 'Q28', 'Q29', 'Q30', 'Q31', 'Q32', 'Q33', 'Q34', 'Q35', 
    'Q36', 'Q37', 'Q38', 'Q39', 'Q40', 'Q41', 'Q42', 'Q43', 'Q44', 
    'Q45', 'Q46', 'Q47', 'Q48', 'Q49', 'Q50', 'QR1', 'QR2', 'Q51', 
    'Q52', 'Q53', 'Q54', 'Q55', 'Q56', 'Q57', 'Q58', 'Q59', 'Q60', 
    'Q61', 'Q62', 'Q63', 'Q64', 'Q65', 'Q66', 'QMT1', 'QMT2', 
    'QMT3', 'QMT4', 'Q67', 'Q68', 'Q69', 'Q70', 'Q71', 'Q72', 
    'Q73', 'QMT5', 'Q74', 'Q75', 'Q76', 'Q77', 'Q78', 'Q79', 
    'Q80', 'Q81', 'Q82'
]

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
    for idx, row in df.iterrows():
        text = row.get('第四層(TCFD) 揭露指引')
        label = row.get('類別')
        if pd.isna(text) or pd.isna(label):
            print(f"[WARN] 檔案中存在缺失的文本或標籤。行號: {idx}")
            continue
        documents.append((text, label))
    print(f"[INFO] 共載入 {len(documents)} 個文件。")
    return documents

def get_all_results(report_name, k=50):
    chroma_path = os.path.join(BASE_CHROMA_PATH, report_name)
    documents = load_documents()
    if not documents:
        print(f"[ERROR] {report_name} 的文件列表為空。")
        return pd.DataFrame()
    embedding_function = OpenAIEmbeddings()
    db = Chroma(persist_directory=chroma_path, embedding_function=embedding_function)
    ALL_RESULTS_PATH = ALL_RESULTS_PATH_TEMPLATE.format(report_name)

    if os.path.exists(ALL_RESULTS_PATH):
        try:
            df_all = pd.read_csv(ALL_RESULTS_PATH)
            if df_all.empty:
                print(f"[WARN] {ALL_RESULTS_PATH} 是空的，將重新生成結果。")
            else:
                print(f"[INFO] 從 {ALL_RESULTS_PATH} 載入預計算結果")
                return df_all
        except pd.errors.EmptyDataError:
            print(f"[WARN] {ALL_RESULTS_PATH} 是空的或格式錯誤，將重新生成結果。")

    # 如果文件不存在或是空的，則重新生成
    all_results = []
    total_docs = len(documents)
    print(f"[INFO] 開始檢索 {report_name} 的所有結果：{total_docs} 個文件 (k={k})")

    start_time = time.time()
    log_interval = 100  # 每處理 100 個文件打印一次進度

    for i, (text, original_label) in enumerate(documents, start=1):
        mapped_label = map_label_to_q(original_label)
        query = f'揭露指標：{original_label}, 定義如下：{text}'
        try:
            results = db.similarity_search_with_relevance_scores(query, k=k)
        except Exception as e:
            print(f"[ERROR] {report_name}: 查詢過程中發生錯誤：{e}")
            continue

        # 每 log_interval 個文件打印一次進度
        if i % log_interval == 0:
            elapsed_time = time.time() - start_time
            cpu_usage = psutil.cpu_percent()
            mem_info = psutil.virtual_memory()
            memory_usage = mem_info.used / (1024**3)  # 轉換為 GB
            print(f"[INFO] {report_name}: 已處理 {i}/{total_docs} 個文件...")
            print(f"[INFO] 已用時間: {elapsed_time:.2f}s, CPU 使用率: {cpu_usage}%, 記憶體使用量: {memory_usage:.2f}GB")

        if not results:
            print(f"[WARN] {report_name}: 文件 {i} 沒有找到相關結果。")
            continue
        else:
            print(f"[INFO] {report_name}: 文件 {i} 返回 {len(results)} 個相關結果。")

        for doc, score in results:
            all_results.append({
                "original_label": original_label,
                "mapped_label": mapped_label,
                "score": score,
                "content": doc.page_content
            })

    if not all_results:
        print(f"[WARN] {report_name} 沒有生成任何結果。")
        return pd.DataFrame()  # 返回空的 DataFrame

    df_all = pd.DataFrame(all_results)
    df_all.to_csv(ALL_RESULTS_PATH, index=False, encoding="utf-8-sig")
    print(f"[INFO] {report_name}: 所有查詢結果已保存至: {ALL_RESULTS_PATH}")
    return df_all

def calculate_accuracy_for_report(df_all, threshold, rank_file, institution, year, report_name):
    # 根據指定的閾值篩選文件並計算準確率
    filtered = df_all[df_all["score"] >= threshold]
    predicted_labels = set(filtered["mapped_label"].unique())

    # 載入真實標籤
    rank_df = pd.read_excel(rank_file)
    ground_truth_df = rank_df[(rank_df['Financial_Institutions'] == institution) &
                              (rank_df['Year'] == year)]
    
    if ground_truth_df.empty:
        print(f"[WARN] 找不到 {institution} 在 {year} 年的真實標籤。跳過。")
        return None, None, None, None  # 增加一個返回值
    
    ground_truth = ground_truth_df.loc[:, TARGET_COLUMNS].iloc[0].to_dict()

    # 初始化二進制格式
    predict_binary = []
    ground_truth_binary = []
    mismatches = []  # 用於收集不一致的標籤資訊

    for q in TARGET_COLUMNS:
        ground_truth_value = int(ground_truth[q]) if not pd.isna(ground_truth[q]) else 0
        predicted_value = 1 if q in predicted_labels else 0
        ground_truth_binary.append(ground_truth_value)
        predict_binary.append(predicted_value)
        
        if ground_truth_value != predicted_value:
            mismatches.append(f"{q}: predicted {predicted_value}, actual {ground_truth_value}")

    # 計算準確率
    accuracy = sum(gt == pred for gt, pred in zip(ground_truth_binary, predict_binary)) / len(ground_truth_binary)

    return accuracy, predict_binary, ground_truth_binary, mismatches

def main():
    # 定義六份報告的相關資訊 (報告名稱, 機構, 年份)
    report_info_list = [
        ("瑞興銀行_2022_TCFD_報告書", "瑞興銀行", 2022),
        ("新光金控_2022_TCFD_報告書_preprocessed", "新光金", 2022),
        ("永豐銀行_2022_TCFD_報告書_preprocessed", "永豐銀行", 2022),
        ("富邦金控_2022_TCFD_報告書_preprocessed", "富邦金", 2022),
        ("開發金控_2021_TCFD_報告書_preprocesed", "開發金", 2021),
        ("華泰銀行_2022_TCFD_報告書_preprocessed", "華泰銀行", 2022)
    ]

    # 載入所有報告的資料
    all_reports_data = []
    all_mismatches = []  # 用於收集所有報告的錯誤標籤
    for report_name, institution, year in report_info_list:
        print(f"\n[INFO] 處理報告: {report_name}")
        df_all = get_all_results(report_name, k=50)
        if df_all.empty:
            print(f"[ERROR] {report_name} 沒有生成任何結果，請檢查數據或查詢邏輯。")
        all_reports_data.append({
            "report_name": report_name,
            "institution": institution,
            "year": year,
            "df_all": df_all
        })

    # 定義以 0.0001 為增量的閾值，從 0.75 開始
    thresholds = [x / 10000 for x in range(7500, 10001, 1)]  # 0.7500, 0.7501, ..., 1.0000
    best_threshold = None
    best_average_accuracy = -1.0
    thresholds_list = []  # 用於繪圖
    average_accuracies_list = []  # 用於繪圖

    print("\n[INFO] 開始閾值優化...")

    total_thresholds = len(thresholds)
    for idx, threshold in enumerate(thresholds, start=1):
        accuracies = []
        for report_data in all_reports_data:
            report_name = report_data["report_name"]
            institution = report_data["institution"]
            year = report_data["year"]
            df_all = report_data["df_all"]

            if df_all.empty:
                continue

            acc, _, _, mismatches = calculate_accuracy_for_report(df_all, threshold, RANK_PATH, institution, year, report_name)
            if acc is not None:
                accuracies.append(acc)
                if mismatches:
                    all_mismatches.extend(mismatches)

        if not accuracies:
            continue

        avg_acc = sum(accuracies) / len(accuracies)
        thresholds_list.append(threshold)
        average_accuracies_list.append(avg_acc)

        if avg_acc > best_average_accuracy:
            best_average_accuracy = avg_acc
            best_threshold = threshold

        # 每處理 100 個閾值打印一次進度
        if idx % 100 == 0 or idx == total_thresholds:
            progress = (idx / total_thresholds) * 100
            print(f"[INFO] 閾值優化進度: {idx}/{total_thresholds} ({progress:.2f}%) 完成")
            print(f"[INFO] 當前閾值: {threshold:.4f}, 當前平均準確率: {avg_acc:.2%}")

    if best_threshold is None:
        print("[ERROR] 沒有找到有效的閾值。結束程式。")
        return

    print(f"\n[INFO] 所有六份報告的最佳閾值: {best_threshold:.4f}, 平均準確率: {best_average_accuracy:.2%}")

    # 保存閾值與平均準確率到 CSV
    df_threshold = pd.DataFrame({
        'threshold': thresholds_list,
        'average_accuracy': average_accuracies_list
    })
    threshold_accuracy_path = os.path.join(ACCURACY_RESULT_BASE, "thresholds_accuracy.csv")
    df_threshold.to_csv(threshold_accuracy_path, index=False, encoding='utf-8-sig')
    print(f"[INFO] 閾值與平均準確率已保存至: {threshold_accuracy_path}")

    # 繪製平均準確率與閾值的趨勢圖
    plt.figure(figsize=(10, 6))
    plt.plot(thresholds_list, [acc * 100 for acc in average_accuracies_list], label='平均準確率', color='blue')
    plt.xlabel('閾值')
    plt.ylabel('平均準確率 (%)')
    plt.title('平均準確率與閾值的趨勢圖')
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plot_path = os.path.join(ACCURACY_RESULT_BASE, "accuracy_vs_threshold.png")
    plt.savefig(plot_path)
    plt.close()
    print(f"[INFO] 趨勢圖已保存至: {plot_path}")

    # 使用最佳閾值計算並保存每份報告的準確率及預測標籤和真實標籤
    summary_results = []
    # 重置 all_mismatches 以收集最佳閾值下的錯誤標籤
    all_mismatches = []
    for report_data in all_reports_data:
        report_name = report_data["report_name"]
        institution = report_data["institution"]
        year = report_data["year"]
        df_all = report_data["df_all"]

        if df_all.empty:
            print(f"[WARN] {report_name} 的數據為空，跳過此報告的準確率計算。")
            continue

        acc, predict_binary, ground_truth_binary, mismatches = calculate_accuracy_for_report(
            df_all, best_threshold, RANK_PATH, institution, year, report_name
        )
        if acc is not None:
            summary_results.append({
                "report_name": report_name,
                "institution": institution,
                "year": year,
                "best_threshold": best_threshold,
                "accuracy_at_best_threshold": acc,
                "PREDICT": ','.join(map(str, predict_binary)),
                "GROUND_TRUTH": ','.join(map(str, ground_truth_binary)),
                "mismatches": '; '.join(mismatches)
            })
            print(f"[INFO] {report_name}: 在最佳閾值 {best_threshold:.4f} 下的準確率 = {acc:.2%}")
            if mismatches:
                all_mismatches.extend(mismatches)
        else:
            print(f"[WARN] {report_name}: 無法計算準確率，跳過總結。")

    if summary_results:
        summary_df = pd.DataFrame(summary_results)
        
        # 生成錯誤標籤的統計
        mismatch_counts = {}
        for mismatch in all_mismatches:
            label = mismatch.split(':')[0].strip()  # 提取標籤名稱，如 'Q10'
            if label in mismatch_counts:
                mismatch_counts[label] += 1
            else:
                mismatch_counts[label] = 1
        
        # 將統計結果轉換為字符串，並按錯誤次數排序
        sorted_mismatches = sorted(mismatch_counts.items(), key=lambda item: item[1], reverse=True)
        mismatch_summary = '; '.join([f"{label} 錯誤 {count} 次" for label, count in sorted_mismatches])
        
        # 創建一個新的 DataFrame 用於錯誤標籤統計
        mismatch_df = pd.DataFrame([{
            "report_name": "Most Mismatches",
            "institution": "",
            "year": "",
            "best_threshold": "",
            "accuracy_at_best_threshold": "",
            "PREDICT": "",
            "GROUND_TRUTH": "",
            "mismatches": mismatch_summary
        }])
        
        # 將統計結果追加到 summary_df
        summary_df = pd.concat([summary_df, mismatch_df], ignore_index=True)
        
        summary_path = os.path.join(ACCURACY_RESULT_BASE, "summary_of_all_reports_best_threshold.csv")
        summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
        print(f"[INFO] 含最佳閾值及預測標籤和真實標籤的總結已保存至: {summary_path}")
    else:
        print("[WARN] 沒有生成任何總結結果。")

if __name__ == "__main__":
    main()
