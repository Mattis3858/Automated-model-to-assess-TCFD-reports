import pandas as pd

# 為了讓程式碼更清晰，我將函數名稱從 csv 改為 excel
def transform_excel_to_wide_format(input_path, output_path):
    """
    讀取一個長格式的 Excel 檔案，並將其轉換為寬格式的 CSV。

    Args:
        input_path (str): 輸入的 Excel 檔案路徑。
        output_path (str): 轉換後要儲存的 CSV 檔案路徑。
    """
    try:
        # 讀取 Excel 檔案
        df = pd.read_excel(input_path)

        # 使用 pivot_table 進行資料轉換
        print("正在進行資料轉換...")
        df_pivoted = df.pivot_table(index='Label',
                                    columns='Company',
                                    values='Final_YN',
                                    aggfunc='first')

        # 將轉換後的結果儲存為新的 CSV 檔案
        df_pivoted.to_csv(output_path, encoding='utf-8-sig')

        print(f"檔案已成功轉換並儲存為 '{output_path}'")
        print("轉換後表格的預覽：")
        print(df_pivoted.head())

    except FileNotFoundError:
        print(f"錯誤：找不到檔案 '{input_path}'。請確認檔案路徑是否正確。")
    except KeyError as e:
        # 將錯誤訊息中的 CSV 改為 Excel，使其更準確
        print(f"錯誤：Excel 檔案中缺少必要的欄位：{e}。請確認 'Company', 'Label', 'Final_YN' 是否存在。")
    except Exception as e:
        print(f"處理過程中發生未預期的錯誤：{e}")

# --- 程式執行區 ---
if __name__ == '__main__':
    # --- 主要修改處 ---
    # 在檔案路徑的引號前加上 r，告訴 Python 这是一个「原始字串」
    # 這樣反斜線 \ 就不會被當作跳脫字元處理
    input_file_path = r"C:\Users\bugee\Downloads\company_label_summary.xlsx"
    
    # 輸出的路徑也同樣處理
    output_file_path = r"C:\Users\bugee\Downloads\company_label_summary_wide.csv"

    # 執行轉換函式
    transform_excel_to_wide_format(input_file_path, output_file_path)