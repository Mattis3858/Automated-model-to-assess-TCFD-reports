import pandas as pd

def compute_accuracy(xlsx_path, pred_xlsx_path, output_path):
    # 讀取檔案
    truth_df = pd.read_excel(xlsx_path)
    pred_df = pd.read_excel(pred_xlsx_path)

    # 依 Label、頁數、Chunk ID 排序，確保對齊
    key_cols = ['Label', '報告書頁數', 'Chunk ID']
    truth_df = truth_df.sort_values(key_cols).reset_index(drop=True)
    pred_df = pred_df.sort_values(key_cols).reset_index(drop=True)

    # 欄位名稱
    truth_col = '是否真的有揭露此標準?(Y/N)'
    pred_col = '是否真的有揭露此標準?(Y/N)'

    # 擷取並標準化文字（大寫、去除空白）
    y_true = truth_df[truth_col].astype(str).str.upper().str.strip()
    y_pred = pred_df[pred_col].astype(str).str.upper().str.strip()

    # 計算匹配數與準確率
    matches = y_true == y_pred
    correct = matches.sum()
    total = len(matches)
    accuracy = correct / total

    print(f"Accuracy: {accuracy:.2%} ({correct} / {total})")

    # 將錯誤的案例儲存到 Excel
    mismatches = pd.DataFrame({
        'Label': truth_df.loc[~matches, 'Label'],
        'Page': truth_df.loc[~matches, '報告書頁數'],
        'Chunk ID': truth_df.loc[~matches, 'Chunk ID'],
        'True': y_true.loc[~matches],
        'Predicted': y_pred.loc[~matches],
    })

    # 儲存到 Excel
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        summary_df = pd.DataFrame({
            'Accuracy': [accuracy],
            'Correct': [correct],
            'Total': [total]
        })
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        mismatches.to_excel(writer, sheet_name='Mismatches', index=False)

    print(f"Results saved to: {output_path}")


if __name__ == '__main__':
    datasets = [
        ('data/2023_query_answer/臺灣銀行2023_output_chunks.xlsx', 'data/2023_query_result/臺灣銀行_2023_output_chunks_with_flags_v3.xlsx', '臺灣銀行_accuracy_results.xlsx'),
        ('data/2023_query_answer/瑞興銀行2023_output_chunks.xlsx', 'data/2023_query_result/瑞興銀行_2023_output_chunks_with_flags_v3.xlsx', '瑞興銀行_accuracy_results.xlsx'),
        ('data/2023_query_answer/富邦金控2023_output_chunks.xlsx', 'data/2023_query_result/富邦金控_2023_output_chunks_with_flags_v3.xlsx', '富邦金控_accuracy_results.xlsx'),
    ]

    for truth_path, pred_path, output_path in datasets:
        compute_accuracy(truth_path, pred_path, output_path)
