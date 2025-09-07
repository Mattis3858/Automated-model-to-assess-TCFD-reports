# -*- coding: utf-8 -*-
"""
依 disagreements.csv 逐列查找對應檔案，依 question 比對 Label 底線後的數字，
支援檔名中間任意字樣，但結尾一定是 _output_chunks_fewshot_with_CoT_v1_few_shot.csv。
若同一筆匹配到多個檔案，將全部合併輸出並標註來源檔案。
"""

import os
import pandas as pd
from pathlib import Path
from glob import glob

# === 可調整參數 ===
INPUT_CSV = "disagreements.csv"
BASE_DIR = Path("data/NAS_165家報告書_LLM_fewshot_result")
SUFFIX = "_output_chunks_fewshot_with_CoT_v1_few_shot.csv"

# 讀入 disagreements
df = pd.read_csv(INPUT_CSV, dtype={"code": str, "company": str, "year": str, "question": str})
df["year"] = df["year"].astype(str)

def split_answer(s: str):
    s = (s or "").strip()
    if "/" in s:
        a, b = s.split("/", 1)
        return a.strip(), b.strip()
    return s, ""

df["human_answer"], df["model_answer"] = zip(*df["answer (human/model)"].map(split_answer))

def find_candidate_files(code: str, company: str, year: str) -> list[Path]:
    """
    搜尋所有以 {code}_{company}_{year}_ 開頭、以 SUFFIX 結尾的檔案。
    中間字樣不限制（報告書/專章/其他皆可）。
    """
    prefix = f"{code}_{company}_{year}_"
    pattern = str(BASE_DIR / f"{prefix}*{SUFFIX}")
    return [Path(p) for p in glob(pattern)]

all_matches = []
summary_rows = []

for _, row in df.iterrows():
    code = row["code"]
    company = row["company"]
    year = row["year"]
    question_raw = row["question"]
    question = (question_raw or "").strip()
    human_ans = row.get("human_answer", "")
    model_ans = row.get("model_answer", "")

    cand_files = find_candidate_files(code, company, year)

    if not cand_files:
        summary_rows.append({
            "code": code, "company": company, "year": year, "question": question,
            "human_answer": human_ans, "model_answer": model_ans,
            "file_paths": "", "num_files_found": 0, "matched_rows": 0,
            "read_ok": False, "note": "找不到任何對應檔案（含任意中間字樣）"
        })
        continue

    total_matched = 0
    read_any = False

    for fpath in cand_files:
        # 多種常見編碼嘗試
        df_file = None
        for enc in ["utf-8-sig", "utf-8", "cp950", "big5"]:
            try:
                df_file = pd.read_csv(fpath, encoding=enc)
                read_any = True
                break
            except Exception:
                continue
        if df_file is None:
            summary_rows.append({
                "code": code, "company": company, "year": year, "question": question,
                "human_answer": human_ans, "model_answer": model_ans,
                "file_paths": str(fpath), "num_files_found": len(cand_files),
                "matched_rows": 0, "read_ok": False, "note": "讀檔失敗（可能為編碼或格式）"
            })
            continue

        # 找 Label 欄位
        label_col = None
        for c in df_file.columns:
            if c.strip().lower() == "label":
                label_col = c
                break
        if label_col is None:
            summary_rows.append({
                "code": code, "company": company, "year": year, "question": question,
                "human_answer": human_ans, "model_answer": model_ans,
                "file_paths": str(fpath), "num_files_found": len(cand_files),
                "matched_rows": 0, "read_ok": True, "note": "找不到 Label 欄位"
            })
            continue

        # 以 "_{question}" 結尾篩選
        suffix = f"_{question}"
        mask = df_file[label_col].astype(str).str.endswith(suffix, na=False)
        df_match = df_file[mask].copy()

        if not df_match.empty:
            total_matched += len(df_match)
            df_match.insert(0, "source_file_path", str(fpath))
            df_match.insert(0, "model_answer", model_ans)
            df_match.insert(0, "human_answer", human_ans)
            df_match.insert(0, "question", question)
            df_match.insert(0, "year", year)
            df_match.insert(0, "company", company)
            df_match.insert(0, "code", code)
            all_matches.append(df_match)

    summary_rows.append({
        "code": code, "company": company, "year": year, "question": question,
        "human_answer": human_ans, "model_answer": model_ans,
        "file_paths": " | ".join(map(str, cand_files)),
        "num_files_found": len(cand_files),
        "matched_rows": total_matched,
        "read_ok": read_any,
        "note": "" if read_any else "所有檔案皆讀取失敗"
    })

# === 輸出 ===
pd.DataFrame(summary_rows).to_csv("summary_per_row.csv", index=False, encoding="utf-8-sig")
if all_matches:
    pd.concat(all_matches, ignore_index=True).to_csv("matched_rows_long.csv", index=False, encoding="utf-8-sig")
else:
    pd.DataFrame().to_csv("matched_rows_long.csv", index=False, encoding="utf-8-sig")

print("完成：已輸出 summary_per_row.csv 與 matched_rows_long.csv")
