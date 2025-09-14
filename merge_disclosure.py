import pandas as pd
import os

def process_report_file(file_path, report_type):
    """
    讀取單一報告書的 CSV 檔案，並進行最簡化處理。
    只保留原始的 'Company' 全名和 'Disclosure_Ratio'。

    Args:
        file_path (str): CSV 檔案的路徑。
        report_type (str): 報告書類型 (例如: TCFD, TNFD, 永續報告書).

    Returns:
        pandas.DataFrame: 清理過的 DataFrame。
    """
    try:
        df = pd.read_csv(file_path)
        
        # 重新命名欄位，為串接做準備
        df.rename(columns={
            'Company': '報告書名稱',
            'Disclosure_Ratio': f'{report_type}_Ratio'
        }, inplace=True)
        
        # 確保揭露比例是數值型態
        df[f'{report_type}_Ratio'] = pd.to_numeric(df[f'{report_type}_Ratio'], errors='coerce')

        # 只選取我們需要的兩欄
        processed_df = df[['報告書名稱', f'{report_type}_Ratio']].copy()
        
        return processed_df

    except FileNotFoundError:
        print(f"錯誤：找不到檔案 {file_path}")
        return None
    except Exception as e:
        print(f"處理檔案 {file_path} 時發生錯誤: {e}")
        return None

# --- 檔案路徑設定 ---
tcfd_file = 'data/report_disclosure_rate/company_disclosure_ratio.csv'
tnfd_file = 'data/report_disclosure_rate/TNFD_報告書揭露比例.csv'
sustainability_file = 'data/report_disclosure_rate/永續報告書(50家)_揭露比例.csv'

# --- 處理三份報告書 ---
tcfd_df = process_report_file(tcfd_file, 'TCFD')
tnfd_df = process_report_file(tnfd_file, 'TNFD')
sustainability_df = process_report_file(sustainability_file, '永續報告書')

# --- 核心修正：使用 concat 而不是 merge ---
if tcfd_df is not None and tnfd_df is not None and sustainability_df is not None:
    
    # 將三份 DataFrame 垂直串接起來
    # ignore_index=True 會重新建立索引
    combined_df = pd.concat([tcfd_df, tnfd_df, sustainability_df], ignore_index=True)

    # --- 計算摘要統計資料 ---
    ratio_cols = ['TCFD_Ratio', 'TNFD_Ratio', '永續報告書_Ratio']
    
    # 在格式化前，先用原始數值計算統計資料
    avg_ratios = combined_df[ratio_cols].mean()
    median_ratios = combined_df[ratio_cols].median()
    std_ratios = combined_df[ratio_cols].std()
    max_ratios = combined_df[ratio_cols].max()
    min_ratios = combined_df[ratio_cols].min()
    count_ratios = combined_df[ratio_cols].count()

    summary_df = pd.DataFrame([
        {
            '報告書名稱': '平均揭露比例', 
            'TCFD_Ratio': avg_ratios['TCFD_Ratio'], 
            'TNFD_Ratio': avg_ratios['TNFD_Ratio'], 
            '永續報告書_Ratio': avg_ratios['永續報告書_Ratio']
        },
        {
            '報告書名稱': '揭露比例中位數', 
            'TCFD_Ratio': median_ratios['TCFD_Ratio'], 
            'TNFD_Ratio': median_ratios['TNFD_Ratio'], 
            '永續報告書_Ratio': median_ratios['永續報告書_Ratio']
        },
        {
            '報告書名稱': '標準差', 
            'TCFD_Ratio': std_ratios['TCFD_Ratio'], 
            'TNFD_Ratio': std_ratios['TNFD_Ratio'], 
            '永續報告書_Ratio': std_ratios['永續報告書_Ratio']
        },
        {
            '報告書名稱': '最大值', 
            'TCFD_Ratio': max_ratios['TCFD_Ratio'], 
            'TNFD_Ratio': max_ratios['TNFD_Ratio'], 
            '永續報告書_Ratio': max_ratios['永續報告書_Ratio']
        },
        {
            '報告書名稱': '最小值', 
            'TCFD_Ratio': min_ratios['TCFD_Ratio'], 
            'TNFD_Ratio': min_ratios['TNFD_Ratio'], 
            '永續報告書_Ratio': min_ratios['永續報告書_Ratio']
        },
        {
            '報告書名稱': '報告書數量', 
            'TCFD_Ratio': count_ratios['TCFD_Ratio'], 
            'TNFD_Ratio': count_ratios['TNFD_Ratio'], 
            '永續報告書_Ratio': count_ratios['永續報告書_Ratio']
        }
    ])

    # --- 格式化資料 ---
    # 1. 格式化主要資料的揭露比例為百分比
    for col in ratio_cols:
        combined_df[col] = combined_df[col].apply(
            lambda x: f'{x*100:.2f}%' if pd.notna(x) else ''
        )

    # 2. 格式化摘要統計資料
    # 對需要百分比的統計項目進行格式化 (前5列)
    for col in ratio_cols:
        summary_df.loc[0:4, col] = summary_df.loc[0:4, col].apply(
            lambda x: f'{x*100:.2f}%' if pd.notna(x) else ''
        )
    # 將數量列轉為整數 (第6列，索引為5)
    summary_df.loc[5, ratio_cols] = summary_df.loc[5, ratio_cols].astype(int)

    # --- 合併主要資料與摘要統計 ---
    final_df = pd.concat([combined_df, summary_df], ignore_index=True)

    # --- 輸出檔案 ---
    output_file = 'data/report_disclosure_rate/綜合報告書揭露比例_最終版.csv'
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"成功！最終版檔案已儲存為: {output_file}")

else:
    print("因為有檔案讀取失敗，無法進行處理。")

