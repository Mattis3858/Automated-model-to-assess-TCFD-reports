# -*- coding: utf-8 -*-
import pandas as pd
from pathlib import Path

# 路徑
INPUT_CSV  = Path(r"C:\Users\user\Documents\Project_SE\1LLM_run\ESGreport_SE_level_output_combined_FROM_LIST_1015.csv")
OUTPUT_CSV = Path(r"C:\Users\user\Documents\Project_SE\1LLM_run\LLM_10151245.csv")

# 欄位與利害關係人順序
STAKEHOLDERS_ORDER = ["員工", "股東", "供應商", "客戶", "政府", "社會"]

# 讀檔
df = pd.read_csv(INPUT_CSV, dtype=str, encoding="utf-8-sig")

# 若重複（同 Year/Firmcode/Stakeholder 多列），保留第一筆
df = df.drop_duplicates(subset=["Year", "Firmcode", "Stakeholder"], keep="first")

# 轉置（寬表）
wide = df.pivot(index=["Year", "Firmcode"], columns="Stakeholder", values="levelscore")

# 依指定順序排列欄位；缺的補上
for col in STAKEHOLDERS_ORDER:
    if col not in wide.columns:
        wide[col] = "NA"
wide = wide[STAKEHOLDERS_ORDER]

# 輸出
wide = wide.reset_index()
wide.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

print(f"Done. Saved to: {OUTPUT_CSV}")
