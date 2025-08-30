import pandas as pd

def transform_csv_to_wide_format(input_path, output_path):
    """
    讀取一個長格式的 CSV 檔案，並將其轉換為寬格式。

    Args:
        input_path (str): 輸入的 CSV 檔案路徑。
        output_path (str): 轉換後要儲存的 CSV 檔案路徑。
    """
    try:
        # 讀取 CSV 檔案
        df = pd.read_csv(input_path)

        # 使用 pivot_table 進行資料轉換
        # - index='Label': 將 'Label' 欄位的值變成新表格的列
        # - columns='Company': 將 'Company' 欄位的值變成新表格的欄
        # - values='Final_YN': 將 'Final_YN' 欄位的值填入表格中
        # - aggfunc='first': 如果有重複的公司/標籤組合，只取第一個值
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
        print(f"錯誤：CSV 檔案中缺少必要的欄位：{e}。請確認 'Company', 'Label', 'Final_YN' 是否存在。")
    except Exception as e:
        print(f"處理過程中發生未預期的錯誤：{e}")

# --- 程式執行區 ---
if __name__ == '__main__':
    # 設定您在 VSCode 中的檔案路徑
    # 提醒：請根據您實際的資料夾結構調整此路徑
    input_file_path = 'data/永續報告書_summary/company_label_summary.csv'
    
    # 設定輸出的檔案名稱
    output_file_path = 'data/永續報告書_summary/company_label_summary_wide.csv'

    # 執行轉換函式
    transform_csv_to_wide_format(input_file_path, output_file_path)