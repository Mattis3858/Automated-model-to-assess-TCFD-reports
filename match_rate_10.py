import pandas as pd
import os

def filter_companies_and_export():
    """
    讀取Excel檔案，篩選指定的10家公司資料，並輸出新的Excel檔
    """
    
    # 設定檔案路徑
    input_path = 'data/report_disclosure_rate/模型與人工判讀相異驗證.xlsx'
    output_path = 'data/report_disclosure_rate/篩選後_6家公司報告書.xlsx'
    
    # 定義要保留的10家公司
    target_companies = [
        
        '5841_中信銀行_2022_TCFD_報告書',
        '2812_台中銀行_2023_TCFD_報告書',
        '2836_高雄銀行_2022_TCFD_報告書',
        '5870_花旗銀行_2023_TCFD_報告書',
        '2801_彰化銀行_2021_TCFD_報告書',
        '2890_永豐金控_2021_TCFD_專章'
        # '2897_王道銀行_2021_TCFD_專章',
        # '6031_連線銀行_2022_TCFD_報告書',
        # '5835_國泰銀行_2022_TCFD_報告書',
        # '2845_遠東商銀_2021_TCFD_專章'
    ]
    
    try:
        # 讀取Excel檔案
        print(f"正在讀取檔案: {input_path}")
        df = pd.read_excel(input_path)
        print(f"原始資料筆數: {len(df)}")
        
        # 檢查Company欄位是否存在
        if 'Company' not in df.columns:
            print("錯誤：找不到 'Company' 欄位")
            print(f"可用的欄位: {df.columns.tolist()}")
            return
        
        # 篩選指定公司的資料
        print("\n開始篩選指定的10家公司...")
        filtered_df = df[df['Company'].isin(target_companies)]
        
        # 顯示篩選結果統計
        print(f"\n篩選後資料筆數: {len(filtered_df)}")
        print("\n各公司的資料筆數:")
        company_counts = filtered_df['Company'].value_counts()
        for company in target_companies:
            if company in company_counts.index:
                print(f"  {company}: {company_counts[company]} 筆")
            else:
                print(f"  {company}: 0 筆 (未找到資料)")
        
        # 確保輸出目錄存在
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"\n已建立輸出目錄: {output_dir}")
        
        # 輸出到新的Excel檔案
        filtered_df.to_excel(output_path, index=False, engine='openpyxl')
        print(f"\n成功輸出檔案: {output_path}")
        
        # 顯示欄位資訊
        print(f"\n輸出檔案包含 {len(filtered_df.columns)} 個欄位:")
        print(f"欄位名稱: {filtered_df.columns.tolist()[:10]}...")  # 顯示前10個欄位
        
        return filtered_df
        
    except FileNotFoundError:
        print(f"錯誤：找不到檔案 {input_path}")
        print("請確認檔案路徑是否正確")
    except Exception as e:
        print(f"發生錯誤: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # 執行篩選並輸出
    result = filter_companies_and_export()
    
    if result is not None:
        print("\n" + "="*50)
        print("處理完成！")
        print(f"篩選後共有 {len(result)} 筆資料")
        print("檔案已儲存至: data/report_disclosure_rate/篩選後_10家公司報告書.xlsx")