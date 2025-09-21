import os
import re

# --- 請在這裡修改為您要處理的資料夾路徑 ---
# 提示：使用 r"..." 可以避免 Windows 路徑的反斜線問題
folder_path = r"C:\Users\bugee\Downloads\4-1. 金控銀行業永續報告書\4-1. 金控銀行業永續報告書"
# ------------------------------------------

def rename_sustainability_reports(path):
    """
    這個函式會掃描指定路徑下的所有檔案，
    並將檔名格式為 "..._永續報告書_年份.pdf" 的檔案，
    重新命名為 "..._年份_永續報告書.pdf"。
    """
    # 檢查路徑是否存在
    if not os.path.isdir(path):
        print(f"錯誤：找不到資料夾 '{path}'。請確認路徑是否正確。")
        return

    print(f"正在掃描資料夾： {path}\n")

    # 建立一個正規表示式來匹配 '..._永續報告書_年份.pdf' 格式
    # (.*?)      -> 第1組: 匹配檔名前面的任何字符 (公司名稱等)
    # (_永續報告書_) -> 匹配 "_永續報告書_"
    # (\d{4})    -> 第2組: 匹配四位數字的年份
    # (\.pdf)    -> 第3組: 匹配 ".pdf" 副檔名
    pattern = re.compile(r"(.*?)_永續報告書_(\d{4})(\.pdf)", re.IGNORECASE)
    
    renamed_count = 0
    # 遍歷資料夾中的所有項目
    for filename in os.listdir(path):
        # 使用正規表示式匹配檔名
        match = pattern.match(filename)

        # 如果檔名符合我們要找的格式
        if match:
            # 從匹配結果中提取各個部分
            prefix = match.group(1)
            year = match.group(2)
            extension = match.group(3)

            # 組成新的檔名，格式為 "..._年份_永續報告書.pdf"
            new_filename = f"{prefix}_{year}_永續報告書{extension}"

            # 取得完整的舊檔案路徑和新檔案路徑
            old_filepath = os.path.join(path, filename)
            new_filepath = os.path.join(path, new_filename)

            try:
                # 執行改名操作
                os.rename(old_filepath, new_filepath)
                print(f"已更改： '{filename}'  ->  '{new_filename}'")
                renamed_count += 1
            except Exception as e:
                print(f"更改 '{filename}' 時發生錯誤: {e}")

    if renamed_count == 0:
        print("掃描完畢，沒有找到符合 '_永續報告書_年份' 格式的檔案需要重新命名。")
    else:
        print(f"\n處理完成！總共重新命名了 {renamed_count} 個檔案。")


# --- 執行主程式 ---
if __name__ == "__main__":
    rename_sustainability_reports(folder_path)
