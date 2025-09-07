import pandas as pd

df = pd.read_csv("data/模型與人工TCFD報告書比對結果.csv")

discrepancies = []

for index, row in df.iterrows():
    for col_name in df.columns[3:]:
        if row[col_name] == "N/Y" or row[col_name] == "Y/N":
            discrepancies.append(
                {
                    "code": row["code"],
                    "company": row["company"],
                    "year": row["year"],
                    "question": col_name.replace('(人/模)', ''),
                    "answer (human/model)": row[col_name],
                }
            )

discrepancies_df = pd.DataFrame(discrepancies)

discrepancies_df.to_csv("disagreements.csv", index=False, encoding="utf-8-sig")

print("已將模型與人工答案不一致的部分整理至 `disagreements.csv` 檔案。")
