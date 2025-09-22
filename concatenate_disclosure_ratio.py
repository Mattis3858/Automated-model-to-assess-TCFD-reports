import pandas as pd
import os

file_list = [
    'data/TCFD_summary/tcfd_company_disclosure_ratio.csv',
    'data/TNFD_summary/TNFD_報告書揭露比例.csv',
    'data/sustainability_report_summary/永續報告書_company_disclosure_ratio.csv',
    'data/年報_summary/年報_company_disclosure_ratio.csv'
]

all_files_exist = True
for f in file_list:
    if not os.path.exists(f):
        print(f"File not found: {f}")
        all_files_exist = False

if not all_files_exist:
    print("One or more files are not accessible. Please try uploading the files again.")
    print(f"Files in current directory: {os.listdir('.')}")

else:
    df_tcfd = pd.read_csv('data/TCFD_summary/tcfd_company_disclosure_ratio.csv')
    df_tnfd = pd.read_csv('data/TNFD_summary/TNFD_報告書揭露比例.csv')
    df_sustainability = pd.read_csv('data/sustainability_report_summary/永續報告書_company_disclosure_ratio.csv')
    df_annual = pd.read_csv('data/年報_summary/年報_company_disclosure_ratio.csv')

    df_tcfd['report_type'] = 'TCFD'
    df_tnfd['report_type'] = 'TNFD'
    df_sustainability['report_type'] = '永續報告書'
    df_annual['report_type'] = '年報'

    all_data = pd.concat([df_tcfd, df_tnfd, df_sustainability, df_annual], ignore_index=True)

    company_parts = all_data['Company'].str.split('_', n=2, expand=True)
    all_data['code'] = company_parts[0]
    all_data['company_name'] = company_parts[1]
    all_data['year'] = company_parts[2].str.extract(r'(\d{4})') # Extract the 4-digit year

    company_name_map = all_data.groupby('code')['company_name'].first().reset_index()
    company_name_map.rename(columns={'company_name': 'company'}, inplace=True)

    pivot_df = all_data.pivot_table(index=['code', 'year'],
                                    columns='report_type',
                                    values='Disclosure_Ratio').reset_index()

    merged_df = pd.merge(pivot_df, company_name_map, on='code', how='left')

    desired_columns = ['code', 'company', 'year', 'TCFD', 'TNFD', '永續報告書', '年報']
    for col in desired_columns:
        if col not in merged_df.columns:
            merged_df[col] = None

    final_df = merged_df[desired_columns]

    final_df.to_csv('merged_disclosure_ratios.csv', index=False)

    print("合併後的檔案已儲存為 merged_disclosure_ratios.csv")
    print(final_df.head())