import pandas as pd
import numpy as np
import re
from pathlib import Path

def compute_from_detailed():
    # Fallback: recompute matches quickly if detailed file is missing
    company_path = Path("data/TCFD_report_165_summary/company_label_summary.csv")
    bank_path = Path("data/銀行業_各組判讀結果.xlsx")
    def read_csv_robust(path):
        for enc in ["utf-8-sig", "utf-8", "cp950", "big5", "latin1"]:
            try:
                return pd.read_csv(path, encoding=enc)
            except Exception:
                continue
        return pd.read_csv(path)
    df_company = read_csv_robust(company_path)
    df_bank = pd.read_excel(bank_path, sheet_name=0)
    bank_code_col = "Symbol" if "Symbol" in df_bank.columns else "公司代碼" if "公司代碼" in df_bank.columns else None
    bank_year_col = "Year" if "Year" in df_bank.columns else "年份" if "年份" in df_bank.columns else None
    def extract_code_year(s: str):
        s = str(s)
        code, year = None, None
        m = re.match(r"^\s*(\d{3,6})[_\-]?", s)
        code = m.group(1) if m else (re.search(r"(\d{3,6})", s).group(1) if re.search(r"(\d{3,6})", s) else None)
        parts = s.split("_")
        if len(parts) >= 3 and re.search(r"(19|20)\d{2}", parts[2]):
            year = re.search(r"(19|20)\d{2}", parts[2]).group(0)
        if year is None and re.search(r"(19|20)\d{2}", s):
            year = re.search(r"(19|20)\d{2}", s).group(0)
        return code, year
    df_company["__code__"], df_company["__year__"] = zip(*df_company["Company"].map(extract_code_year))
    def extract_label_key(label: str):
        label = str(label).strip()
        m = re.search(r"#\w+", label.upper())
        if m:
            return m.group(0)
        if "_" in label:
            return label.split("_")[-1].strip()
        m2 = re.search(r"[A-Za-z0-9\-]+", label)
        return (m2.group(0) if m2 else label).upper()
    def normalize_yn(v):
        s = str(v).strip().upper()
        if s in {"Y","YES","TRUE","T","1"}: return "Y"
        if s in {"N","NO","FALSE","F","0"}: return "N"
        return s
    df_company["__label_key__"] = df_company["Label"].map(extract_label_key)
    df_company["__final_yn__"] = df_company["Final_YN"].map(normalize_yn)
    df_bank["__code__"] = df_bank[bank_code_col].astype(str).str.strip()
    df_bank["__year__"] = df_bank[bank_year_col].astype(str).str.extract(r"((?:19|20)\d{2})", expand=False)
    def std_key(s): return str(s).upper().strip().replace(" ", "").replace("－", "-").replace("—", "-")
    exclude_cols = {"__code__", "__year__", bank_code_col, bank_year_col}
    bank_label_cols = [c for c in df_bank.columns if c not in exclude_cols]
    bank_label_map = {std_key(c): c for c in bank_label_cols}
    rows = []
    for _, r in df_company.iterrows():
        code, year = r["__code__"], r["__year__"]
        label_key, final_yn = r["__label_key__"], r["__final_yn__"]
        company_str, label_raw = r["Company"], r["Label"]
        if pd.isna(code) or pd.isna(year):
            rows.append({"Company": company_str,"code": code,"year": year,"Label": label_raw,"label_key": label_key,
                         "Final_YN": final_yn,"bank_value": None,"correct": np.nan,"status":"missing_code_or_year"})
            continue
        hit = df_bank[(df_bank["__code__"] == str(code)) & (df_bank["__year__"] == str(year))]
        if hit.empty:
            rows.append({"Company": company_str,"code": code,"year": year,"Label": label_raw,"label_key": label_key,
                         "Final_YN": final_yn,"bank_value": None,"correct": np.nan,"status":"bank_row_not_found"})
            continue
        label_std = std_key(label_key)
        bank_col = bank_label_map.get(label_std)
        if bank_col is None:
            for k in {label_std.replace("_",""), label_std.replace("-",""), label_std.replace("#",""), "#"+label_std.replace("#","")}:
                if k in bank_label_map:
                    bank_col = bank_label_map[k]; break
            if bank_col is None and label_key in df_bank.columns:
                bank_col = label_key
        if bank_col is None:
            rows.append({"Company": company_str,"code": code,"year": year,"Label": label_raw,"label_key": label_key,
                         "Final_YN": final_yn,"bank_value": None,"correct": np.nan,"status":"bank_label_col_not_found"})
            continue
        bank_value = "Y" if str(hit.iloc[0][bank_col]).strip().upper() in {"Y","YES","TRUE","T","1"} else \
                     ("N" if str(hit.iloc[0][bank_col]).strip().upper() in {"N","NO","FALSE","F","0"} else str(hit.iloc[0][bank_col]).strip().upper())
        correct = (final_yn == bank_value) if (final_yn in {"Y","N"} and bank_value in {"Y","N"}) else np.nan
        rows.append({"Company": company_str,"code": code,"year": year,"Label": label_raw,"label_key": label_key,
                     "Final_YN": final_yn,"bank_value": bank_value,"correct": correct,"status":"ok"})
    df = pd.DataFrame(rows)
    return df

df_detail = compute_from_detailed()

# Filter valid rows and compute accuracy by (code, year)
valid = df_detail[df_detail["status"] == "ok"].dropna(subset=["correct"]).copy()
valid["correct_num"] = valid["correct"].astype(int)

acc_by_code_year = (
    valid.groupby(["code", "year"])
    .agg(total=("correct_num", "size"), correct=("correct_num", "sum"))
    .assign(accuracy=lambda x: np.round(x["correct"].astype(float) / x["total"].astype(float), 4))
    .reset_index()
    .sort_values(["code", "year"])
)

overall_accuracy = float(np.round(valid["correct_num"].sum() / len(valid), 4)) if not valid.empty else np.nan

# Save and display
out_path = Path("data/TCFD_report_165_summary/accuracy_by_company_year.csv")
acc_by_code_year.to_csv(out_path, index=False, encoding="utf-8-sig")

print(f"Rows used: {len(valid)} / {len(df_detail)} | Overall accuracy (unchanged): {overall_accuracy}")