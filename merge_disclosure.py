import pandas as pd
import re

def process_report_file(file_path, report_type):
    """
    讀取單一報告書的 CSV 檔案，並進行清理與格式化。

    Args:
        file_path (str): CSV 檔案的路徑。
        report_type (str): 報告書類型 (例如: TCFD, TNFD, 永續報告書).

    Returns:
        pandas.DataFrame: 清理過的 DataFrame，包含公司代碼、公司簡稱、年份和揭露比例。
    """
    try:
        # 讀取 CSV 檔案
        df = pd.read_csv(file_path)

        # --- 欄位提取與清理 ---
        # 提取公司代碼 (通常是第一個底線前方的數字)
        df['Company_Code'] = df['Company'].str.split('_').str[0]

        # 提取公司簡稱 (通常是第二個部分)
        df['Company_Name'] = df['Company'].str.split('_').str[1]
        
        # 使用正規表達式提取四位數的年份，這比用底線分割更穩定
        df['Year'] = df['Company'].str.extract(r'(\d{4})')
        
        # 確保 Disclosure_Ratio 是數值型態
        df['Disclosure_Ratio'] = pd.to_numeric(df['Disclosure_Ratio'], errors='coerce')

        # 重新命名揭露比例欄位，以報告類型區分
        df.rename(columns={'Disclosure_Ratio': f'{report_type}_Ratio'}, inplace=True)
        
        # 選取我們需要的欄位
        processed_df = df[['Company_Code', 'Company_Name', 'Year', f'{report_type}_Ratio']].copy()

        # 移除可能因為提取失敗產生的空值列
        processed_df.dropna(subset=['Company_Code', 'Year'], inplace=True)

        return processed_df

    except FileNotFoundError:
        print(f"錯誤：找不到檔案 {file_path}")
        return None
    except Exception as e:
        print(f"處理檔案 {file_path} 時發生錯誤: {e}")
        return None

# --- 檔案路徑設定 ---
# 請確保這三個 CSV 檔和你的 Python 程式放在同一個資料夾裡
tcfd_file = 'data/report_disclosure_rate/company_disclosure_ratio.csv'
tnfd_file = 'data/report_disclosure_rate/TNFD_報告書揭露比例.csv'
sustainability_file = 'data/report_disclosure_rate/永續報告書(50家)_揭露比例.csv'

# --- 處理三份報告書 ---
tcfd_df = process_report_file(tcfd_file, 'TCFD')
tnfd_df = process_report_file(tnfd_file, 'TNFD')
sustainability_df = process_report_file(sustainability_file, '永續報告書')

# --- 合併資料 ---
if tcfd_df is not None and tnfd_df is not None and sustainability_df is not None:
    # 建立一個包含所有公司代碼和名稱的對照表，避免合併後名稱遺失
    company_map = pd.concat([
        tcfd_df[['Company_Code', 'Company_Name']],
        tnfd_df[['Company_Code', 'Company_Name']],
        sustainability_df[['Company_Code', 'Company_Name']]
    ]).drop_duplicates(subset=['Company_Code'])

    # 第一次合併：TCFD & TNFD
    # 使用 'outer' 合併，確保任何一份報告有的公司和年份都會被保留
    merged_df = pd.merge(
        tcfd_df.drop(columns=['Company_Name']),
        tnfd_df.drop(columns=['Company_Name']),
        on=['Company_Code', 'Year'],
        how='outer'
    )
    
    # 第二次合併：加入永續報告書
    final_df = pd.merge(
        merged_df,
        sustainability_df.drop(columns=['Company_Name']),
        on=['Company_Code', 'Year'],
        how='outer'
    )
    
    # 將公司名稱加回合併後的表格
    final_df = pd.merge(final_df, company_map, on='Company_Code', how='left')

    # --- 計算平均揭露比例 ---
    avg_tcfd = final_df['TCFD_Ratio'].mean()
    avg_tnfd = final_df['TNFD_Ratio'].mean()
    avg_sustainability = final_df['永續報告書_Ratio'].mean()
    
    # 建立一個新的 DataFrame 來存放平均值
    average_row = pd.DataFrame([{
        'Company_Name': '平均揭露比例',
        'TCFD_Ratio': avg_tcfd,
        'TNFD_Ratio': avg_tnfd,
        '永續報告書_Ratio': avg_sustainability
    }])
    
    # 將平均值列加到最終表格的底部
    final_df_with_avg = pd.concat([final_df, average_row], ignore_index=True)

    # --- 格式化與排序 ---
    # 重新排列欄位順序，讓報告更清晰
    column_order = ['Company_Code', 'Company_Name', 'Year', 'TCFD_Ratio', 'TNFD_Ratio', '永續報告書_Ratio']
    final_df_with_avg = final_df_with_avg[column_order]
    
    # 依公司代碼和年份排序
    # 我們將平均值列暫時排除排序，完成後再加回去
    sorted_df = final_df_with_avg.iloc[:-1].sort_values(by=['Company_Code', 'Year'])
    final_sorted_df = pd.concat([sorted_df, final_df_with_avg.iloc[-1:]], ignore_index=True)
    
    # 將揭露比例轉換為百分比格式，並處理空值
    ratio_cols = ['TCFD_Ratio', 'TNFD_Ratio', '永續報告書_Ratio']
    for col in ratio_cols:
        # 使用 .loc 避免 SettingWithCopyWarning
        final_sorted_df.loc[:, col] = final_sorted_df[col].apply(
            lambda x: f'{x*100:.2f}%' if pd.notna(x) else ''
        )

    # --- 輸出檔案 ---
    output_file = 'data/report_disclosure_rate/綜合報告書揭露比例_合併版.csv'
    # 使用 'utf-8-sig' 編碼，確保 Excel 打開時中文不會亂碼
    final_sorted_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"成功！合併後的檔案已儲存為: {output_file}")

else:
    print("因為有檔案讀取失敗，無法進行合併。")
