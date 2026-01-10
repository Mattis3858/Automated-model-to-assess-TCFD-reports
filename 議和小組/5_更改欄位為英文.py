import pandas as pd

# 檔案路徑
in_path  = r"C:\Users\user\Documents\Project_SE\1LLM_run\LLM_10151245.csv"
out_path = r"C:\Users\user\Documents\Project_SE\1LLM_run\LLM_R_10151245.csv"

# 嘗試以常見編碼讀檔（先用 UTF-8-SIG，有 BOM 也能處理；不行再退回 cp950/Big5）
for enc in ("utf-8-sig", "cp950"):
    try:
        df = pd.read_csv(in_path, encoding=enc)
        break
    except UnicodeDecodeError:
        df = None

if df is None:
    raise UnicodeDecodeError("讀檔失敗", b"", 0, 0, "請確認檔案編碼（建議存成 UTF-8）")

# 去除欄位名前後空白（避免隱性空白）
df.columns = df.columns.str.strip()

# 欄位重新命名對應
rename_map = {
    "Firmcode": "code",
    "員工": "employee_level_llm_cb",
    "股東": "shareholder_level_llm_cb",
    "供應商": "supplier_level_llm_cb",
    "客戶": "consumer_level_llm_cb",
    "政府": "government_level_llm_cb",
    "社會": "society_level_llm_cb",
}

# 進行欄位更名（存在才更名，不存在就略過以避免報錯）
df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

#（可選）若想確認欄位順序，可指定：
desired_order = [
    "Year", "code",
    "employee_level_llm_cb", "shareholder_level_llm_cb", "supplier_level_llm_cb",
    "consumer_level_llm_cb", "government_level_llm_cb", "society_level_llm_cb",
]
# 保留既有欄位的同時依序排列（不存在的會自動忽略）
cols = [c for c in desired_order if c in df.columns] + [c for c in df.columns if c not in desired_order]
df = df[cols]

# 輸出（用 UTF-8-SIG，Excel 開啟較友善）
df.to_csv(out_path, index=False, encoding="utf-8-sig")
print("已完成輸出：", out_path)
