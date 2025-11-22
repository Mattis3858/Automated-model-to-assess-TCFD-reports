# -*- coding: utf-8 -*-
"""
掃描兩個資料夾，產生清單：| Type | YEAR | code |
- Type 取自資料夾名稱結尾：ESGreport 或 stewardship
- YEAR 與 code 取自資料夾名稱前兩段：YYYY_CODE_...
- 僅掃描一層子資料夾（不往下遞迴）
"""

from pathlib import Path
import re
import pandas as pd

# === 路徑設定（請確認） ===
STEW_BASE = Path(r"C:\Users\user\Documents\Project_SE\Stewardship_report\Report_chunks")
ESG_BASE  = Path(r"C:\Users\user\Documents\Project_SE\ESG_report\Report\Report_chunks")

# 輸出檔名（可改）
OUTPUT_XLSX = Path(r"C:\Users\user\Documents\Project_SE\1LLM_run\Report_pairs_index_1015.xlsx")
OUTPUT_CSV  = Path(r"C:\Users\user\Documents\Project_SE\1LLM_run\Report_pairs_index_1015.csv")

# === 資料夾命名規則：YYYY_CODE_(ESGreport|stewardship) ===
# code 可能有 4～5 位數（例：2801、28944）
FOLDER_RE = re.compile(r"^(?P<year>\d{4})_(?P<code>\d{4,5})_(?P<typ>ESGreport|stewardship)$", re.IGNORECASE)

def scan_once(base: Path):
    rows = []
    if not base.exists():
        print(f"[WARN] 根目錄不存在：{base}")
        return rows

    for p in base.iterdir():
        if not p.is_dir():
            continue
        m = FOLDER_RE.match(p.name)
        if m:
            year = m.group("year")
            code = m.group("code")
            typ  = m.group("typ")
            # 正規化 Type 大小寫（照你需求顯示）
            typ_norm = "ESGreport" if typ.lower() == "esgreport" else "stewardship"
            rows.append({"Type": typ_norm, "YEAR": year, "code": code})
        else:
            # 非符合命名規則的資料夾，提示但不報錯
            print(f"[SKIP] 不符合命名規則：{p.name}")
    return rows

def main():
    all_rows = []
    all_rows.extend(scan_once(STEW_BASE))
    all_rows.extend(scan_once(ESG_BASE))

    if not all_rows:
        print("[INFO] 沒找到任何符合的資料夾。")
        return

    df = pd.DataFrame(all_rows, columns=["Type", "YEAR", "code"])

    # 去重（同一路徑可能重複或你多處備份時可避免重複）
    df = df.drop_duplicates()

    # 排序（先按 YEAR、再 code、最後 Type）
    df = df.sort_values(by=["YEAR", "code", "Type"], ascending=[True, True, True]).reset_index(drop=True)

    # 存檔 Excel + CSV（Excel 2021 可直接開）
    OUTPUT_XLSX.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(OUTPUT_XLSX, index=False)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print(f"[DONE] 共 {len(df)} 筆，已輸出：")
    print(f" - {OUTPUT_XLSX}")
    print(f" - {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
