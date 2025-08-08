import os
import pandas as pd
from glob import glob

# ===== 可調參數 =====
INPUT_DIR = "data/handroll_query_result"
SUMMARY_DIR = "data/handroll_summary"
PATTERN = "*_output_chunks_fewshot_with_CoT_v1_few_shot.csv"  
TOP_K_FOR_DECISION = 5
Y_THRESHOLD_IN_TOPK = 1  

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def decide_label(df_company_label: pd.DataFrame) -> dict:
    take = df_company_label.sort_values("Rank").head(TOP_K_FOR_DECISION)
    vals = take["是否真的有揭露此標準?(Y/N)"].astype(str).str.upper().str.strip().tolist()
    y_count = sum(v == "Y" for v in vals)
    n_count = len(vals) - y_count
    final = "Y" if y_count >= Y_THRESHOLD_IN_TOPK else "N"
    return {"Final_YN": final, "Y_in_topK": y_count, "N_in_topK": n_count, "Considered": len(vals)}

def main():
    ensure_dir(SUMMARY_DIR)
    paths = sorted(glob(os.path.join(INPUT_DIR, PATTERN)))
    if not paths:
        print(f"[ERROR] 在 {INPUT_DIR} 找不到 {PATTERN}")
        return

    rows_detail, rows_ratio = [], []

    for p in paths:
        df = pd.read_csv(p)
        required = {"Company", "Label", "Rank", "是否真的有揭露此標準?(Y/N)"}
        if not required.issubset(df.columns):
            print(f"[WARN] {os.path.basename(p)} 缺少必要欄位，跳過。需要：{required}")
            continue

        company = df["Company"].iloc[0] if len(df) else os.path.basename(p).split("_output_chunks")[0]

        label_results = []
        for label, g in df.groupby("Label"):
            d = decide_label(g)
            rows_detail.append({
                "Company": company,
                "Label": label,
                **d,
                "Rule_Y_Threshold": Y_THRESHOLD_IN_TOPK,
                "TopK_For_Decision": TOP_K_FOR_DECISION,
            })
            label_results.append(d["Final_YN"])

        if label_results:
            total = len(label_results)
            y_cnt = sum(v == "Y" for v in label_results)
            ratio = y_cnt / total if total else 0.0
            rows_ratio.append({
                "Company": company,
                "Total_Labels": total,
                "Y_Labels": y_cnt,
                "N_Labels": total - y_cnt,
                "Disclosure_Ratio": round(ratio, 4),
                "Rule_Y_Threshold": Y_THRESHOLD_IN_TOPK,
                "TopK_For_Decision": TOP_K_FOR_DECISION,
            })

    df_detail = pd.DataFrame(rows_detail).sort_values(["Company", "Label"]).reset_index(drop=True)
    df_ratio  = pd.DataFrame(rows_ratio).sort_values(["Company"]).reset_index(drop=True)

    out_a = os.path.join(SUMMARY_DIR, "company_label_summary.csv")
    out_b = os.path.join(SUMMARY_DIR, "company_disclosure_ratio.csv")
    df_detail.to_csv(out_a, index=False, encoding="utf-8-sig")
    df_ratio.to_csv(out_b, index=False, encoding="utf-8-sig")
    print(f"[SUCCESS] 輸出：{out_a}")
    print(f"[SUCCESS] 輸出：{out_b}")

if __name__ == "__main__":
    main()
