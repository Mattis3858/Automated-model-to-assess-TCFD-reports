import pandas as pd

def filter_excel_file(input_excel_path, output_excel_path):
    """
    讀取一個 Excel 檔案，篩選出 'Human Answer' 和 'Model Answer' 欄位不相同的橫列，
    然後將結果儲存為一個新的 Excel 檔案。

    Args:
        input_excel_path (str): 輸入的 Excel 檔案路徑 (.xlsx)。
        output_excel_path (str): 輸出的 Excel 檔案路徑 (.xlsx)。
    """
    try:
        # 讀取 Excel 檔案
        # 如果您的資料在第一個工作表 (Sheet)，直接讀取即可
        # 如果在特定的工作表，可以使用 sheet_name='Sheet名' 參數
        df = pd.read_excel(input_excel_path)

        # 確保欄位是字串格式再進行比較
        df['Human Answer'] = df['Human Answer'].astype(str)
        df['Model Answer'] = df['Model Answer'].astype(str)

        # 篩選出 'Human Answer' 和 'Model Answer' 欄位不相同的橫列
        # .str.strip() 是為了移除答案前後可能存在的空白字元
        filtered_df = df[df['Human Answer'].str.strip() != df['Model Answer'].str.strip()]

        # 將篩選後的結果儲存為新的 Excel 檔案
        # index=False 表示在輸出的 Excel 中不包含 pandas 的索引列
        filtered_df.to_excel(output_excel_path, index=False)

        print(f"處理完成！篩選後的資料已儲存至：{output_excel_path}")

    except FileNotFoundError:
        print(f"錯誤：找不到檔案 '{input_excel_path}'。請確認檔案路徑和名稱是否正確。")
    except KeyError as e:
        print(f"錯誤：輸入的檔案中找不到指定的欄位：{e}。請確認 'Human Answer' 和 'Model Answer' 欄位是否存在。")
    except Exception as e:
        print(f"發生未預期的錯誤：{e}")

# --- 使用說明 ---
# 1. 請將您原始的 "模型與人工判讀相異驗證_6家.xlsx" 檔案和這個 Python 程式放在同一個資料夾中。
# 2. 如果檔案不在同一個資料夾，請將 input_file 的路徑修改為您電腦中的完整路徑。
# 3. 您可以自訂輸出的檔案名稱。

# 設定輸入的 Excel 檔案路徑
input_file = "data/mike/模型與人工判讀相異驗證_6家.xlsx" 

# 設定輸出檔案的名稱
output_file = "data/mike/模型與人工判讀相異驗證.xlsx"

# 執行函式
filter_excel_file(input_file, output_file)