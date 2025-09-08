import pandas as pd
import re
import os

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
        
        # --- BUG 修正 ---
        # 使用更精確的正規表達式 (20\d{2}) 來提取年份，避免抓到4位數的公司代碼
        df['Year'] = df['Company'].str.extract(r'(20\d{2})')
        
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

# --- 檔案路徑設定 (已更新為您提供的路徑) ---
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

    # --- 計算摘要統計資料 ---
    # 平均值
    avg_tcfd = final_df['TCFD_Ratio'].mean()
    avg_tnfd = final_df['TNFD_Ratio'].mean()
    avg_sustainability = final_df['永續報告書_Ratio'].mean()
    
    # 中位數
    median_tcfd = final_df['TCFD_Ratio'].median()
    median_tnfd = final_df['TNFD_Ratio'].median()
    median_sustainability = final_df['永續報告書_Ratio'].median()

    # 報告數量
    count_tcfd = final_df['TCFD_Ratio'].count()
    count_tnfd = final_df['TNFD_Ratio'].count()
    count_sustainability = final_df['永續報告書_Ratio'].count()

    # 建立摘要統計的 DataFrame
    summary_df = pd.DataFrame([
        {'Company_Name': '平均揭露比例', 'TCFD_Ratio': avg_tcfd, 'TNFD_Ratio': avg_tnfd, '永續報告書_Ratio': avg_sustainability},
        {'Company_Name': '揭露比例中位數', 'TCFD_Ratio': median_tcfd, 'TNFD_Ratio': median_tnfd, '永續報告書_Ratio': median_sustainability},
        {'Company_Name': '報告書數量', 'TCFD_Ratio': count_tcfd, 'TNFD_Ratio': count_tnfd, '永續報告書_Ratio': count_sustainability}
    ])

    # --- 格式化與排序 ---
    # 重新排列欄位順序，讓報告更清晰
    column_order = ['Company_Code', 'Company_Name', 'Year', 'TCFD_Ratio', 'TNFD_Ratio', '永續報告書_Ratio']
    
    # 排序主要資料
    main_data_df = final_df.sort_values(by=['Company_Code', 'Year'])
    main_data_df = main_data_df[column_order]

    # 格式化主要資料的揭露比例為百分比
    ratio_cols = ['TCFD_Ratio', 'TNFD_Ratio', '永續報告書_Ratio']
    for col in ratio_cols:
        main_data_df.loc[:, col] = main_data_df[col].apply(
            lambda x: f'{x*100:.2f}%' if pd.notna(x) else ''
        )

    # 格式化摘要統計資料
    # 對平均值和中位數列應用百分比格式
    for col in ratio_cols:
        summary_df.loc[0:1, col] = summary_df.loc[0:1, col].apply(
            lambda x: f'{x*100:.2f}%' if pd.notna(x) else ''
        )
    # 將數量列轉為整數
    summary_df.loc[2, ratio_cols] = summary_df.loc[2, ratio_cols].astype(int)

    # 合併主要資料與摘要統計資料
    final_combined_df = pd.concat([main_data_df, summary_df], ignore_index=True)

    # --- 輸出檔案 (已更新為您提供的路徑) ---
    output_file = 'data/report_disclosure_rate/綜合報告書揭露比例_含統計.csv'
    
    # 檢查輸出目錄是否存在，如果不存在就建立它
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 使用 'utf-8-sig' 編碼，確保 Excel 打開時中文不會亂碼
    final_combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"成功！包含摘要統計的檔案已儲存為: {output_file}")

else:
    print("因為有檔案讀取失敗，無法進行合併。")

