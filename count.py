import pandas as pd

# 讀取 csv
df = pd.read_csv("matched_rows_long.csv")

# 計算 unique 數量
unique_count = df.drop_duplicates(subset=["code", "company", "year"]).shape[0]

print("在 code-company-year 組合下的 unique 數量:", unique_count)

# 如果你想看每個組合的數量，可以這樣：
grouped_counts = df.groupby(["code", "company", "year"]).size().reset_index(name="count")
# print(grouped_counts)
