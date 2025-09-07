# -*- coding: utf-8 -*-
"""
把 LLM 與人工判讀結果對齊到同一格：
- 以「公司代碼 + 年份 + label」對齊
- LLM 檔：Label 在「列」，報告檔名在「欄」
- 只保留人工有答案的格子；同一格顯示為 "人工/模型"（模型缺值顯示 "-"）
- 產出：
    1) comparison_long.csv  （整潔長表）
    2) comparison_wide.xlsx （寬表，欄位為 label，各格=人工/模型）
"""

import os
import re
import pandas as pd
from typing import Optional

# ======== 檔案路徑 ========
LLM_CSV_PATH   = r"C:\Users\bugee\Downloads\company_label_summary_wide.csv"
HUMAN_XLSX_PATH = r"C:\Users\bugee\Downloads\銀行業_各組判讀結果.xlsx"

# ======== 人工檔前三欄（若名稱不同會自動退回用前 3 欄） ========
HUMAN_ID_COLS = ["代碼", "公司名稱", "年份"]  # 你的 Excel 實際是 Symbol/Financial Institution/Year，會自動偵測

# ---------- 共用 ----------

def _read_csv_any_encoding(path: str) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp950", "big5"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path, dtype=str)

def normalize_yn(x: Optional[str]) -> str:
    """標準化為 'Y' / 'N'，其他或缺失回空字串。"""
    if x is None:
        return ""
    s = str(x).strip().upper()
    if s in {"Y", "YES", "TRUE", "T", "1", "是"}:
        return "Y"
    if s in {"N", "NO", "FALSE", "F", "0", "否"}:
        return "N"
    return ""

# ---------- LLM（CSV, Label 在列） ----------

def extract_label_key_from_label(label: str) -> Optional[str]:
    """
    從 'G-1-3_50' / 'R-1-5_#R2' 取出最後一個底線後的代碼：'50'、'#R2'
    """
    m = re.search(r"_([^_]+)$", str(label))
    if not m:
        return None
    key = m.group(1).strip()
    return key if re.fullmatch(r"#?[A-Za-z0-9]+", key) else None

def extract_year_from_report(report_col: str) -> str:
    """
    從 '2801_彰化銀行_2020_TCFD_報告書' 抓年份（避免把代碼 2801 當年份）
    """
    parts = str(report_col).split("_")
    if len(parts) >= 3:
        for p in parts[1:]:
            if re.fullmatch(r"\d{4}", p):
                return p
    m = re.findall(r"(\d{4})", str(report_col))
    return m[-1] if m else ""

def load_llm_long(path: str) -> pd.DataFrame:
    df = _read_csv_any_encoding(path).fillna("")
    if "Label" not in df.columns:
        raise ValueError("LLM CSV 缺少 'Label' 欄。請確認檔案。")

    # 寬轉長：Label + 報告欄 → 長表
    long_df = df.melt(id_vars=["Label"], var_name="report", value_name="model_raw")
    long_df["model_yn"]  = long_df["model_raw"].map(normalize_yn)
    long_df["label_key"] = long_df["Label"].map(extract_label_key_from_label)

    # 解析 code/company/year
    long_df["code"]    = long_df["report"].str.extract(r"^(\d+)_")
    long_df["company"] = long_df["report"].str.split("_").str[1]
    long_df["year"]    = long_df["report"].map(extract_year_from_report)

    # 僅保留有 label_key 的列
    long_df = long_df[long_df["label_key"].notna()].copy()

    return long_df[["code", "company", "year", "label_key", "model_yn"]]

# ---------- 人工（Excel） ----------

def load_human_long(path: str) -> pd.DataFrame:
    human_df = pd.read_excel(path, dtype=str, engine="openpyxl").fillna("")
    cols = list(human_df.columns)

    # 取識別欄：若預設找不到就用前 3 欄（你的檔實際是 Symbol / Financial Institution / Year）
    if HUMAN_ID_COLS and all(c in cols for c in HUMAN_ID_COLS):
        code_col, name_col, year_col = HUMAN_ID_COLS
    else:
        code_col, name_col, year_col = cols[:3]

    # 剩下全部視為 label 欄，過濾掉全空欄（移除 Unnamed、備註等）
    label_cols = [c for c in cols if c not in [code_col, name_col, year_col]]
    label_cols = [c for c in label_cols if human_df[c].replace("", pd.NA).notna().any()]

    # 去除欄名空白
    human_df = human_df.rename(columns={c: str(c).strip() for c in label_cols})
    label_cols = [str(c).strip() for c in label_cols]

    # 長表
    long_df = human_df.melt(
        id_vars=[code_col, name_col, year_col],
        value_vars=label_cols,
        var_name="label_key",
        value_name="human_raw",
    )

    long_df = long_df.rename(columns={code_col: "code", name_col: "company", year_col: "year"})
    long_df["code"] = long_df["code"].astype(str).str.strip()
    long_df["year"] = long_df["year"].astype(str).str.extract(r"(\d{4})", expand=False).fillna("")
    long_df["human_yn"] = long_df["human_raw"].map(normalize_yn)

    # 僅保留人工有填（Y/N）
    long_df = long_df[long_df["human_yn"].isin(["Y", "N"])].copy()

    return long_df[["code", "company", "year", "label_key", "human_yn"]]

# ---------- 輸出 ----------

def build_outputs(llm_long: pd.DataFrame, human_long: pd.DataFrame, out_dir: str = ".") -> None:
    # 以「人工」為主 left join（只有模型有、人工沒有 → 不輸出）
    merged = human_long.merge(
        llm_long[["code", "year", "label_key", "model_yn"]],
        on=["code", "year", "label_key"],
        how="left",
    )

    # 合併顯示格：人工/模型；模型缺值以 '-'
    merged["model_yn"] = merged["model_yn"].fillna("-")
    combined_col = "combined_cell(人工/模型)"  # ← 欄名直接註明左右意義
    merged[combined_col] = merged["human_yn"] + "/" + merged["model_yn"]

    # 是否一致
    merged["is_match"] = (merged["human_yn"] == merged["model_yn"]).astype(int)

    # ===== 整潔長表（CSV） =====
    long_csv_path = os.path.join(out_dir, "comparison_long.csv")
    merged[
        ["code", "company", "year", "label_key", "human_yn", "model_yn", combined_col, "is_match"]
    ].sort_values(["code", "year", "label_key"]).to_csv(long_csv_path, index=False, encoding="utf-8-sig")

    # ===== 寬表（DataFrame） =====
    label_order = (
        human_long[["label_key"]]
        .drop_duplicates()
        .loc[:, "label_key"]
        .tolist()
    )
    wide = (
        merged.pivot_table(
            index=["code", "company", "year"],
            columns="label_key",
            values=combined_col,
            aggfunc="first",
        )
        .reindex(columns=label_order)
        .reset_index()
    )

    # 寬表欄名加上 (人/模) 註記（保留前三個識別欄不加）
    wide = wide.rename(columns={
        c: (f"{c}(人/模)" if c not in ["code", "company", "year"] else c)
        for c in wide.columns
    })

    # ===== 寬表：Excel + CSV =====
    wide_xlsx_path = os.path.join(out_dir, "comparison_wide.xlsx")
    with pd.ExcelWriter(wide_xlsx_path, engine="openpyxl") as writer:
        wide.to_excel(writer, index=False, sheet_name="combined")
        acc = (
            merged.groupby(["code", "company", "year"], as_index=False)["is_match"]
            .mean(numeric_only=True)
            .rename(columns={"is_match": "match_rate"})
        )
        acc["match_rate"] = (acc["match_rate"] * 100).round(2)
        acc.to_excel(writer, index=False, sheet_name="match_rate_summary")

    # 另外多存一份寬表 CSV（欄名已含 (人/模) 註記）
    wide_csv_path = os.path.join(out_dir, "comparison_wide.csv")
    wide.to_csv(wide_csv_path, index=False, encoding="utf-8-sig")

    print("✅ 已輸出：")
    print(f"- {long_csv_path}")
    print(f"- {wide_xlsx_path}")
    print(f"- {wide_csv_path}")

def main():
    print("讀取 LLM 輸出（Label 在列）...")
    llm_long = load_llm_long(LLM_CSV_PATH)

    print("讀取 人工判讀（Excel）...")
    human_long = load_human_long(HUMAN_XLSX_PATH)

    print(f"LLM 筆數（長表）：{len(llm_long):,}")
    print(f"人工筆數（長表）：{len(human_long):,}")

    out_dir = os.path.dirname(HUMAN_XLSX_PATH) or "."
    build_outputs(llm_long, human_long, out_dir=out_dir)

if __name__ == "__main__":
    main()
