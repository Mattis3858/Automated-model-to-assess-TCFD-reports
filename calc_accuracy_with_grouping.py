import pandas as pd
import os

def compute_accuracy_new(xlsx_path, pred_csv_path):
    """
    使用新的分組邏輯計算準確率。

    Args:
        xlsx_path (str): 真實答案的 Excel 檔案路徑。
        pred_csv_path (str): 預測結果的 CSV 檔案路徑。
    """
    try:
        truth_df = pd.read_excel(xlsx_path)
        pred_df = pd.read_csv(pred_csv_path)
    except FileNotFoundError as e:
        print(f"檔案讀取錯誤: {e}")
        return

    # 定義欄位名稱
    key_cols = ['Label', '報告書頁數', 'Chunk ID']
    truth_col = '是否真的有揭露此標準?(Y/N)'
    pred_col = '是否真的有揭露此標準?(Y/N)'

    # 確保所有必要的欄位都存在
    for col in key_cols + [truth_col]:
        if col not in truth_df.columns:
            print(f"錯誤: 真實答案檔案 '{xlsx_path}' 中缺少欄位 '{col}'。")
            return
    for col in key_cols + [pred_col]:
        if col not in pred_df.columns:
            print(f"錯誤: 預測檔案 '{pred_csv_path}' 中缺少欄位 '{col}'。")
            return

    # 排序以確保資料對齊
    truth_df = truth_df.sort_values(key_cols).reset_index(drop=True)
    pred_df = pred_df.sort_values(key_cols).reset_index(drop=True)
    
    # 標準化答案欄位 (Y/N)
    truth_df['y_true'] = truth_df[truth_col].astype(str).str.upper().str.strip()
    pred_df['y_pred'] = pred_df[pred_col].astype(str).str.upper().str.strip()

    # 合併兩個 DataFrame 以便於按 Label 處理
    merged_df = pd.merge(
        truth_df[key_cols + ['y_true']],
        pred_df[key_cols + ['y_pred']],
        on=key_cols,
        how='outer' # 使用 outer join 以處理可能存在的標籤或 chunk 缺失
    )

    # 初始化計數器
    yy, yn, ny, nn = 0, 0, 0, 0
    mismatched_labels = []

    # 按 'Label' 分組進行判斷
    for label, group in merged_df.groupby('Label'):
        # 只取每個 Label 的前 5 個 chunks
        top5_chunks = group.head(10)

        # 提取真實值和預測值，並將 NaN (可能由 outer join 產生) 替換為 'N'
        true_vals = top5_chunks['y_true'].fillna('N').tolist()
        pred_vals = top5_chunks['y_pred'].fillna('N').tolist()
        
        # 判斷 Label 的 Ground Truth 是否包含 'Y'
        has_true_y = 'Y' in true_vals
        
        if has_true_y:
            # --- Case 1: Ground Truth 至少有一個 'Y' ---
            
            # YY: 只要有一個 chunk 的 T/P 皆為 'Y'
            is_yy = any(t == 'Y' and p == 'Y' for t, p in zip(true_vals, pred_vals))
            
            if is_yy:
                yy += 1
            # YN: Ground Truth 有 'Y'，但 Prediction 全部是 'N'
            elif 'Y' not in pred_vals:
                yn += 1
                mismatched_labels.append({
                    'Label': label, 'Type': 'YN (漏報)',
                    'True': true_vals, 'Predicted': pred_vals
                })
            # 其他情況: T 和 P 都有 'Y'，但位置不匹配，視為錯誤
            else:
                yn += 1 # 歸類為 YN (因為沒有正確預測出 Y 的位置)
                mismatched_labels.append({
                    'Label': label, 'Type': 'YN (位置錯誤)',
                    'True': true_vals, 'Predicted': pred_vals
                })
        else:
            # --- Case 2: Ground Truth 全部是 'N' ---
            
            # NY: Ground Truth 全為 'N'，但 Prediction 有 'Y'
            if 'Y' in pred_vals:
                ny += 1
                mismatched_labels.append({
                    'Label': label, 'Type': 'NY (誤報)',
                    'True': true_vals, 'Predicted': pred_vals
                })
            # NN: Ground Truth 和 Prediction 全為 'N'
            else:
                nn += 1

    # 計算總數和準確率
    total_labels = yy + nn + yn + ny
    if total_labels == 0:
        accuracy = 0.0
    else:
        accuracy = (yy + nn) / total_labels

    # 輸出結果
    print(f"檔案 '{os.path.basename(xlsx_path)}' 的準確率報告:")
    print("-" * 40)
    print(f"YY (正確揭露): {yy}")
    print(f"NN (正確未揭露): {nn}")
    print(f"YN (錯誤 - 漏報/位置錯誤): {yn}")
    print(f"NY (錯誤 - 誤報): {ny}")
    print("-" * 40)
    print(f"總標籤數: {total_labels}")
    print(f"準確率: {accuracy:.2%} ({(yy + nn)} / {total_labels})")

    # 列出錯誤的案例
    if mismatched_labels:
        print("\n錯誤案例分析:")
        mismatch_df = pd.DataFrame(mismatched_labels)
        print(mismatch_df.to_string(index=False))
    
    print("\n" + "="*50 + "\n")


if __name__ == '__main__':
    try:
        # 範例 1
        xlsx_path_1 = 'data/2023_query_answer/臺灣銀行2023_output_chunks.xlsx'
        pred_csv_path_1 = 'data/2023_query_result/臺灣銀行_2023_output_chunks_with_CoT_v1.csv'
        compute_accuracy_new(xlsx_path_1, pred_csv_path_1)

        xlsx_path_1 = 'data/2023_query_answer/臺灣銀行2023_output_chunks.xlsx'
        pred_csv_path_1 = 'data/2023_query_result/臺灣銀行_2023_output_chunks_fewshot_with_CoT_v1_few_shot.csv'
        compute_accuracy_new(xlsx_path_1, pred_csv_path_1)

        # 範例 2
        xlsx_path_2 = 'data/2023_query_answer/瑞興銀行2023_output_chunks.xlsx'
        pred_csv_path_2 = 'data/2023_query_result/瑞興銀行_2023_output_chunks_with_CoT_v1.csv'
        compute_accuracy_new(xlsx_path_2, pred_csv_path_2)
        xlsx_path_2 = 'data/2023_query_answer/瑞興銀行2023_output_chunks.xlsx'
        pred_csv_path_2 = 'data/2023_query_result/瑞興銀行_2023_output_chunks_fewshot_with_CoT_v1_few_shot.csv'
        compute_accuracy_new(xlsx_path_2, pred_csv_path_2)
        
        # # 範例 3
        xlsx_path_3 = 'data/2023_query_answer/富邦金控2023_output_chunks.xlsx'
        pred_csv_path_3 = 'data/2023_query_result/富邦金控_2023_output_chunks_with_CoT_v1.csv'
        compute_accuracy_new(xlsx_path_3, pred_csv_path_3)
        xlsx_path_3 = 'data/2023_query_answer/富邦金控2023_output_chunks.xlsx'
        pred_csv_path_3 = 'data/2023_query_result/富邦金控_2023_output_chunks_fewshot_with_CoT_v1_few_shot.csv'
        compute_accuracy_new(xlsx_path_3, pred_csv_path_3)

    except NameError:
        print("請在 'if __name__ == '__main__':' 區塊中設定您的檔案路徑。")
    except Exception as e:
        print(f"執行時發生未預期的錯誤: {e}")