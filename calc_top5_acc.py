import os
import pandas as pd

def compute_accuracy(truth_path: str, pred_path: str, top_k: int = 5) -> None:
    """比較兩份 Excel 的 Y/N 標註，僅取每個 Label 的前 top_k 筆（依 Rank）。
    同時計算 Accuracy、Precision、Recall、F1，並列出各 Label 的 Accuracy。"""
    # 讀檔
    truth_df = pd.read_excel(truth_path)
    pred_df  = pd.read_excel(pred_path)

    # 只保留 Rank ≤ top_k 的資料
    truth_df = truth_df[truth_df["Rank"] <= top_k]
    pred_df  = pred_df[pred_df["Rank"] <= top_k]

    # 依 (Label, Page, Chunk ID) 排序，以確保資料對齊
    key_cols = ["Label", "報告書頁數", "Chunk ID"]
    truth_df = truth_df.sort_values(key_cols).reset_index(drop=True)
    pred_df  = pred_df.sort_values(key_cols).reset_index(drop=True)

    # 取 Y/N 欄位並標準化
    col = "是否真的有揭露此標準?(Y/N)"
    y_true = truth_df[col].astype(str).str.upper().str.strip()
    y_pred = pred_df[col].astype(str).str.upper().str.strip()

    # === 整體指標 ===
    matches  = y_true == y_pred
    correct  = matches.sum()
    total    = len(matches)
    accuracy = correct / total if total else 0

    tp = ((y_true == "Y") & (y_pred == "Y")).sum()
    fp = ((y_true == "N") & (y_pred == "Y")).sum()
    fn = ((y_true == "Y") & (y_pred == "N")).sum()

    precision = tp / (tp + fp) if (tp + fp) else 0
    recall    = tp / (tp + fn) if (tp + fn) else 0
    f1        = 2 * precision * recall / (precision + recall) if (precision + recall) else 0

    print(f"Top {top_k} chunks per label — Total records: {total}")
    print(f"Accuracy : {accuracy:.2%}  ({correct}/{total})")
    print(f"Precision: {precision:.2%}")
    print(f"Recall   : {recall:.2%}")
    print(f"F1 Score : {f1:.2%}")

    # === 各 Label 的前 top_k 準確率 ===
    per_label_acc = (truth_df.assign(match=matches)
                            .groupby("Label")["match"]
                            .mean()
                            .sort_index())
    print("\nPer-label accuracy (前五筆):")
    print(per_label_acc.apply(lambda x: f"{x:.2%}"))

    # === 錯配清單 ===
    if correct < total:
        print("\nMismatches:")
        mismatches = pd.DataFrame({
            "Label":     truth_df.loc[~matches, "Label"],
            "Rank":      truth_df.loc[~matches, "Rank"],
            "Page":      truth_df.loc[~matches, "報告書頁數"],
            "Chunk ID":  truth_df.loc[~matches, "Chunk ID"],
            "True":      y_true.loc[~matches],
            "Predicted": y_pred.loc[~matches],
        })
        print(mismatches.to_string(index=False))

if __name__ == "__main__":
    file_pairs = [
        ("data/2023_query_answer/臺灣銀行2023_output_chunks.xlsx",
         "data/2023_query_result/臺灣銀行_2023_output_chunks_with_flags.xlsx"),
        ("data/2023_query_answer/瑞興銀行2023_output_chunks.xlsx",
         "data/2023_query_result/瑞興銀行_2023_output_chunks_with_flags.xlsx"),
        ("data/2023_query_answer/富邦金控2023_output_chunks.xlsx",
         "data/2023_query_result/富邦金控_2023_output_chunks_with_flags.xlsx"),
    ]

    for truth_fp, pred_fp in file_pairs:
        print(f"\n=== {os.path.basename(truth_fp)} vs {os.path.basename(pred_fp)} ===")
        compute_accuracy(truth_fp, pred_fp, top_k=5)
