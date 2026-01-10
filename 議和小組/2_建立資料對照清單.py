# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd

INPUT_CSV  = Path("議和小組/filed_list.csv")
OUT_DIR    = Path("議和小組")
OUT_SUMMARY_CSV = OUT_DIR / "filed_list_pairs.csv"
OUT_LINES_TXT   = OUT_DIR / "filed_list_pairs_lines.txt"

def main():
    df = pd.read_csv(INPUT_CSV, dtype=str, encoding="utf-8-sig")
    for c in ["Type", "YEAR", "code", "Fiem", "NEW"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
        else:
            raise ValueError(f"缺少必要欄位：{c}")

    # 正規化 Type
    df["Type"] = df["Type"].str.lower().map(
        {"esgreport": "ESGreport", "stewardship": "stewardship"}
    ).fillna(df["Type"])

    df = df[df["Type"].isin(["ESGreport", "stewardship"])]
    groups = df.groupby(["YEAR", "NEW"], dropna=False)

    summary_rows = []
    line_rows = []

    for (year, new), g in groups:
        esg_codes  = sorted(g.loc[g["Type"] == "ESGreport", "code"].dropna().unique().tolist())
        stew_codes = sorted(g.loc[g["Type"] == "stewardship", "code"].dropna().unique().tolist())

        summary_rows.append({
            "YEAR": year,
            "NEW": new,
            "ESG_codes": ";".join(esg_codes) if esg_codes else "",
            "STEW_codes": ";".join(stew_codes) if stew_codes else "",
            "pair_count": (len(esg_codes) * len(stew_codes)) if esg_codes and stew_codes else 0
        })

        if esg_codes and stew_codes:
            for e in esg_codes:
                for s in stew_codes:
                    # ✅ 無空格、無尾註
                    line_rows.append(f"{year}_{e}_ESGreport=={year}_{s}_stewardship")
        elif esg_codes:
            for e in esg_codes:
                line_rows.append(f"{year}_{e}_ESGreport")
        elif stew_codes:
            for s in stew_codes:
                line_rows.append(f"{year}_{s}_stewardship")

    summary_df = pd.DataFrame(summary_rows)
    summary_df = summary_df.sort_values(by=["YEAR", "NEW", "pair_count"], ascending=[True, True, False]).reset_index(drop=True)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(OUT_SUMMARY_CSV, index=False, encoding="utf-8-sig")

    with OUT_LINES_TXT.open("w", encoding="utf-8-sig", newline="\n") as f:
        for line in sorted(line_rows):
            f.write(line + "\n")

    print(f"[DONE] 彙總表：{OUT_SUMMARY_CSV}")
    print(f"[DONE] 對照清單：{OUT_LINES_TXT}")

if __name__ == "__main__":
    main()
