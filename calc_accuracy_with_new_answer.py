import pandas as pd
import re

# 讀取資料
df1 = pd.read_csv("data/handroll_summary/company_label_summary_final.csv")
df2 = pd.read_excel("data/銀行業_各組判讀結果.xlsx", sheet_name="Sheet1")

# 從 Company 抽取銀行代號 (前綴數字) 和年份 (例如 2020)
df1["Symbol"] = df1["Company"].str.extract(r"^(\d+)")
df1["Year"] = df1["Company"].str.extract(r"_(\d{4})_")

# 從 Label 拆出對應的欄位 (底線後面的數字或字母)
df1["ColName"] = df1["Label"].str.extract(r"_(\d+|#\w+)$")

# 建立比對結果
results = []
for _, row in df1.iterrows():
    symbol = row["Symbol"]
    year = row["Year"]
    col = row["ColName"]
    target = row["Final_YN"]

    # 在 df2 中找到對應公司 Symbol + Year
    match = df2[
        (df2["Symbol"].astype(str) == str(symbol))
        & (df2["Year"].astype(str) == str(year))
    ]
    if match.empty or col not in match.columns:
        continue

    # 拿第二份資料的值
    val = match.iloc[0][col]

    # 紀錄比對
    results.append(
        {
            "Symbol": symbol,
            "Year": year,
            "Company": row["Company"],
            "Label": row["Label"],
            "Expected": target,
            "Actual": val,
            "Correct": target == val,
        }
    )

# 轉成 DataFrame
compare_df = pd.DataFrame(results)

# 每家公司+年份的準確率
company_year_acc = (
    compare_df.groupby(["Symbol", "Year"])["Correct"].mean().reset_index()
)
company_year_acc.rename(columns={"Correct": "Accuracy"}, inplace=True)

# 總體準確率
overall_acc = compare_df["Correct"].mean()

# 輸出結果
print("每家公司+年份準確率：")
print(company_year_acc)
print("\n總體準確率：", overall_acc)
