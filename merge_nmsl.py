import pandas as pd
import re
import os

def parse_company_string(company_string, report_type):
    """
    解析 'Company' 欄位的字串，根據不同的報告類型提取資訊。
    此版本修正了所有已知的邊界情況，包括無代碼的報告。

    Args:
        company_string (str): 來自CSV檔案的 'Company' 欄位值。
        report_type (str): 報告的類型 ('TCFD', 'TNFD', '永續報告書', '年報')。

    Returns:
        dict: 包含 code, company_name, year, tcfd_subtype 的字典。
    """
    result = {'code': None, 'company_name': None, 'year': None, 'tcfd_subtype': ''}
    
    # 針對 TCFD 和永續報告書，優先處理有代碼的標準格式
    if report_type in ['TCFD', '永續報告書', 'TNFD', '年報']:
        suffix = report_type
        if report_type == 'TCFD':
            suffix = 'TCFD_報告書' # TCFD的後綴比較特殊
        
        # 模式：(代碼)_(公司名稱)_(年份)_(後綴)
        match_with_code = re.match(rf'([\w-]+)_(.*)_(\d{{4}})_{suffix}(?:_(.*))?$', company_string)
        if match_with_code:
            groups = match_with_code.groups()
            result['code'], result['company_name'], result['year'] = groups[:3]
            if report_type == 'TCFD' and len(groups) > 3 and groups[3] is not None:
                result['tcfd_subtype'] = groups[3].strip()
            return result

    # 如果標準格式不匹配，則處理 TCFD 和永續報告書的「無代碼」特例
    if report_type == 'TCFD':
        # *** FIX 1: 新增 TCFD 的無代碼解析規則 ***
        # 模式：(公司名稱)_(年份)_TCFD_報告書
        match_no_code = re.match(r'(.*)_(\d{4})_TCFD_報告書(?:_(.*))?$', company_string)
        if match_no_code:
            groups = match_no_code.groups()
            result['company_name'], result['year'] = groups[:2]
            result['code'] = result['company_name'] # 用公司名稱當作唯一標識
            if len(groups) > 2 and groups[2] is not None:
                 result['tcfd_subtype'] = groups[2].strip()
            return result
            
    elif report_type == '永續報告書':
        # *** FIX 2: 強化永續報告書的無代碼解析規則 ***
        # 模式：(公司名稱)_(年份)_永續報告書
        match_no_code = re.match(r'(.*)_(\d{4})_永續報告書$', company_string)
        if match_no_code:
            result['company_name'], result['year'] = match_no_code.groups()
            result['code'] = result['company_name'] # 用公司名稱當作唯一標識
            return result

    print(f"警告：無法解析字串 '{company_string}'，類型：{report_type}")
    return None

# --- 主程式開始 ---

data_folder = 'data/report_disclosure_rate'
os.makedirs(data_folder, exist_ok=True)

file_mapping = {
    'TCFD': os.path.join(data_folder, 'TCFD_company_disclosure_ratio.csv'),
    'TNFD': os.path.join(data_folder, 'TNFD_company_disclosure_ratio.csv'),
    '永續報告書': os.path.join(data_folder, '永續報告書_company_disclosure_ratio.csv'),
    '年報': os.path.join(data_folder, '年報_company_disclosure_ratio.csv')
}

all_reports_data = []
all_files_exist = all(os.path.exists(path) for path in file_mapping.values())

if all_files_exist:
    # 1. 解析所有檔案資料
    for report_type, file_path in file_mapping.items():
        print(f"正在處理 {report_type}...")
        df = pd.read_csv(file_path)
        for index, row in df.iterrows():
            parsed_info = parse_company_string(row['Company'], report_type)
            if parsed_info:
                record = {
                    'code': parsed_info['code'],
                    'company_name': parsed_info['company_name'],
                    'year': parsed_info['year'],
                    'report_type': report_type,
                    'disclosure_ratio': row['Disclosure_Ratio'],
                }
                if report_type == 'TCFD':
                    record['tcfd_subtype'] = parsed_info['tcfd_subtype']
                all_reports_data.append(record)

    master_df = pd.DataFrame(all_reports_data)
    
    # NEW: 顯示從來源檔案解析出的確切報告數量，用於驗證
    print("-" * 50)
    print("從來源檔案成功解析的報告數量：")
    initial_counts = master_df['report_type'].value_counts()
    print(initial_counts.to_string())
    print("-" * 50)

    # 2. 建立統一的公司名稱對照表
    name_map_df = master_df.loc[master_df['company_name'].notna(), ['code', 'company_name']].copy()
    name_map_df['name_len'] = name_map_df['company_name'].str.len()
    name_map_df = name_map_df.sort_values('name_len', ascending=False).drop_duplicates('code')[['code', 'company_name']]

    # 3. 將資料拆分為 "標準報告" 和 "TCFD變體"
    is_variant = (master_df['report_type'] == 'TCFD') & (master_df['tcfd_subtype'].str.len() > 0)
    df_tcfd_variants = master_df[is_variant].copy()
    df_standard_reports = master_df[~is_variant].copy()

    # 4. 處理標準報告：將 TCFD標準版, TNFD, 年報, 永續報告書 合併到同一列
    df_main_pivot = df_standard_reports.pivot_table(
        index=['code', 'year'],
        columns='report_type',
        values='disclosure_ratio'
    ).reset_index()

    # 5. 處理 TCFD 變體報告：只保留自己的數據，用於後續附加
    df_tcfd_variants = df_tcfd_variants[['code', 'year', 'tcfd_subtype', 'disclosure_ratio']]
    df_tcfd_variants.rename(columns={'disclosure_ratio': 'TCFD'}, inplace=True)

    # 6. 合併主要表格和 TCFD 變體表格
    final_df = pd.concat([df_main_pivot, df_tcfd_variants], ignore_index=True)

    # 7. 附加統一的公司名稱並整理
    final_df = pd.merge(final_df, name_map_df, on='code', how='left')
    final_df['tcfd_subtype'] = final_df['tcfd_subtype'].fillna('')
    report_cols = ['TCFD', 'TNFD', '永續報告書', '年報']
    for col in report_cols:
        if col not in final_df.columns:
            final_df[col] = pd.NA
    
    final_cols = ['code', 'company_name', 'year', 'tcfd_subtype'] + report_cols
    final_df = final_df[final_cols]
    final_df = final_df.sort_values(by=['code', 'year', 'tcfd_subtype']).reset_index(drop=True)
    
    # 8. 計算統計摘要並儲存
    summary_stats = final_df[report_cols].describe()
    
    output_filename = os.path.join(data_folder, 'merged_disclosure_ratios_final.csv')
    final_df.to_csv(output_filename, index=False, encoding='utf-8-sig')

    with open(output_filename, 'a', encoding='utf-8-sig', newline='') as f:
        f.write('\n\n--- 各類報告揭露率統計摘要 ---\n')
        summary_stats.to_csv(f, encoding='utf-8-sig')

    print(f"處理完成！所有報告已按您的要求合併。")
    print(f"結果與統計摘要已儲存為 {output_filename}")

else:
    print("錯誤：部分檔案缺失，程式已停止運行。")
