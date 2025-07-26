import os
import pandas as pd
import openai
from dotenv import load_dotenv
from tqdm.auto import tqdm
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 並發設定 ---
# 控制同時運行的最大執行緒數量。
MAX_WORKERS = 10

# 解析模型回覆的函數
def parse_llm_response(response_text: str) -> tuple[str, str]:
    """
    從 LLM 的結構化回覆中解析出 Reason 和 Final Answer。
    """
    try:
        reason_match = re.search(r"\[REASON\](.*?)\[END REASON\]", response_text, re.DOTALL)
        answer_match = re.search(r"\[FINAL ANSWER\](.*?)\[END FINAL ANSWER\]", response_text, re.DOTALL)

        reason = reason_match.group(1).strip() if reason_match else "模型未提供推理過程。"
        final_answer = answer_match.group(1).strip().upper() if answer_match else "N"
        if final_answer not in ["Y", "N"]:
            final_answer = "N"
        return reason, final_answer
    except Exception as e:
        return f"解析回覆時發生錯誤: {e}", "N"

# 呼叫 LLM 的函數
def get_llm_result(chunk: str, definition: str, model: str):
    """
    建構 Few-shot Prompt 並呼叫 LLM。
    """
    # --- V3 版本的 Few-shot CoT Prompt (使用真實數據範例) ---
    prompt = f"""### 角色 ###
你是一位極其精確且嚴謹的 TCFD 審查專家。你的任務是學習以下真實範例，並根據範例的判斷標準來審核新的文本。

---
### 學習範例 ###

**範例 1 (好的揭露):**
- **TCFD 指引問題**: "公司是否描述向董事會和/或董事會下設委員會，定期報告氣候相關風險與機會之流程？"
- **報告書片段**: "隨著氣候變遷影響加劇，世界逐漸達成淨零減碳共識，並透過訂定碳費或碳稅法案等「碳有價」制度或總量管制方式來 控管碳排放量。 臺灣政府亦於2021年宣示2050年淨零轉型目標，制定相關法規以及臺灣2050淨零排放路徑藍圖，規劃開始向排碳大戶 徵收碳費。 面對席捲而來的國際淨零浪潮，辨識及應對氣候變遷相關風險與機會對企業永續經營至關重要。而完善的公司治理則是 企業永續經營的基礎，金融監督管理委員會亦發布多項ESG規範，以「落實公司治理及營造健全ESG生態體系」為核心 願景，要求企業強化董事會組織運作和職責，並推動企業將低碳轉型及氣候韌性納入企業永續經營的重點項目，系統性 審視可能的風險與機會，並研擬對應氣候風險策略，凸顯氣候治理架構於氣候變遷議題上扮演至關重要的角色。 董事會扮演的角色 富邦由董事會擔任推動永續發展之最高督導單位，建構企業永續文化，並監督永續相關計畫的推動與執行。依據責任金 融及風險管理目的，分別定期呈報氣候變遷風險資訊至「公司治理及永續委員會」以及「審計委員會」"
- **你的判斷**:
[FINAL ANSWER]
Y
[END FINAL ANSWER]


**範例 2 (不好的揭露):**
- **TCFD 指引問題**: "公司是否描述有跨部門之氣候相關工作小組統籌執行相關工作？"
- **報告書片段**: "富邦金控暨子公司目前取得永續相關證照共99張，包含全球風險管理協會（GARP ）之SCR持續性與氣候風險管 理認證、美國投資管理研究協會CFA ESG Investing Certificate，以及台灣永續能源研究基金會的企業永續管理 師與永續金融管理師，亦安排同仁研習ISO 14064-1以及ISO 14097相關訓練，持續強化氣候變遷風險因應能力。 氣候管理人才 富邦金控跟進國際趨勢，於TCFD精進專案中持續加強因應氣候風險，邀請顧問團隊依循本國銀行業與保險業財 務碳排放（範疇三）實務手冊計算財務碳排放量、進階評估海外投資性不動產實體風險及精進氣候變遷情境分 析，本公司及各子公司之責任金融小組主管及成員定期參與相關會議，提升相關知識及技術傳承，持續加強自身 執行氣候風險評估與分析能力，並運用於調整相關減碳策略。 富邦期許發揮金融影響力，鼓勵投融資對象一同低碳轉型，今年正式啟動議合專案，與顧問團隊共同研擬制定議 合準則與投票準則，並規劃進一步落實議合行動，包含提供ESG相關建議或辦理工作坊，攜手投融資對象邁向淨 零減碳"
- **你的判斷**:
[FINAL ANSWER]
N
[END FINAL ANSWER]

**範例 3 (好的揭露):**
- **TCFD 指引問題**: "公司應描述在「短期」的時間長度下，可能會產生重大財務影響的氣候相關風險與機會？"
- **報告書片段**: "氣候風險與機會 富邦參考金融穩定委員會（FSB ）之TCFD建議，將氣候風險劃分為兩大 類：1 ）與低碳經濟相關的轉型風險，和2 ）與氣候變遷影響相關的實體 風險。除了風險外，為減緩或適應氣候變遷而做出的努力亦會創造機 會，例如提高資源使用效率和節約成本、採用低碳能源、開發新產品和 服務、進入新市場及提高供應鏈的韌性等。 依據「富邦金融控股股份有限公司暨子公司氣候變遷管理準則」，每年 實施氣候變遷風險與機會評估辨識，2023年共擬定14個轉型與實體風險 項目，各子公司依其業務特性與發展與影響期間1，就潛在脆弱度、潛在 衝擊度及發生可能性三大維度進行辨識2，金控就子公司辨識出之前三名 氣候風險重大性結果，加權計算3後以風險矩陣方式呈現如右：  富邦金控依據辨識結果之重大性篩選出前三大4轉型風險與實體風險如下表： 風險類別 風險構面 氣候風險項目 影響業務範圍 影響面向 對應傳統風險5 重大性排序 實體風險 長期 降雨（水）模式變化和氣候模式的極端變化─乾旱 自身營運／投融資 因氣候模式改變導致缺水及糧食危機，自身營運可能產生額外費用，投融資標的亦 可能面臨營運成本提高及財務損失。"
- **你的判斷**:
[FINAL ANSWER]
Y
[END FINAL ANSWER]


**範例 4 (不好的揭露):**
- **TCFD 指引問題**: "公司是否描述評估氣候相關風險的流程包括認定氣候相關風險相對於其他風險的重要性是重要的？"
- **報告書片段**: "氣候風險與機會 富邦參考金融穩定委員會（FSB ）之TCFD建議，將氣候風險劃分為兩大 類：1 ）與低碳經濟相關的轉型風險，和2 ）與氣候變遷影響相關的實體 風險。除了風險外，為減緩或適應氣候變遷而做出的努力亦會創造機 會，例如提高資源使用效率和節約成本、採用低碳能源、開發新產品和 服務、進入新市場及提高供應鏈的韌性等。 依據「富邦金融控股股份有限公司暨子公司氣候變遷管理準則」，每年 實施氣候變遷風險與機會評估辨識，2023年共擬定14個轉型與實體風險 項目，各子公司依其業務特性與發展與影響期間1，就潛在脆弱度、潛在 衝擊度及發生可能性三大維度進行辨識2，金控就子公司辨識出之前三名 氣候風險重大性結果，加權計算3後以風險矩陣方式呈現如右：  富邦金控依據辨識結果之重大性篩選出前三大4轉型風險與實體風險如下表： 風險類別 風險構面 氣候風險項目 影響業務範圍 影響面向 對應傳統風險5 重大性排序 實體風險 長期 降雨（水）模式變化和氣候模式的極端變化─乾旱 自身營運／投融資 因氣候模式改變導致缺水及糧食危機，自身營運可能產生額外費用，投融資標的亦 可能面臨營運成本提高及財務損失。"
- **你的判斷**:
[FINAL ANSWER]
N
[END FINAL ANSWER]

---

### 正式任務 ###
現在請根據以下內容進行判斷：

**1. TCFD 指引問題 (Definition):**
"{definition}"

**2. 報告書片段 (Chunk):**
"{chunk}"

### 你的判斷 ###
(請遵循範例的格式，提供你的 Reasoning 和 Final Answer)
"""

    resp = openai.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "你是一位 TCFD 審查專家，請學習範例，並嚴格遵循指定的結構化輸出格式。"},
            {"role": "user", "content": prompt}
        ],
        temperature=0,
    )
    response_text = resp.choices[0].message.content
    return parse_llm_response(response_text)


def fill_tcfd_flags_v3(
    input_xlsx: str,
    model: str = "gpt-4o-mini",
):
    load_dotenv()
    openai.api_key = os.getenv("OPENAI_API_KEY")

    df = pd.read_excel(input_xlsx)
    yn_col = "是否真的有揭露此標準?(Y/N)"
    reason_col = "Reason"

    if yn_col not in df.columns:
        df[yn_col] = ""
    if reason_col not in df.columns:
        df[reason_col] = ""

    tasks = []
    for idx, row in df.iterrows():
        definition = row["Definition"]
        chunk = str(row["Chunk Text"])
        tasks.append((idx, chunk, definition))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_idx = {
            executor.submit(get_llm_result, chunk, definition, model): idx
            for idx, chunk, definition in tasks
        }

        for future in tqdm(as_completed(future_to_idx), total=len(future_to_idx), desc="並行處理 TCFD (v3 Few-shot)"):
            idx = future_to_idx[future]
            try:
                reason, final_answer = future.result()
                df.at[idx, yn_col] = final_answer
                df.at[idx, reason_col] = reason
            except Exception as e:
                df.at[idx, yn_col] = "N"
                df.at[idx, reason_col] = f"呼叫 API 時發生錯誤: {e}"

    base, _ = os.path.splitext(input_xlsx)
    output_xlsx = f"{base}_with_flags_v3.xlsx"
    df.to_excel(output_xlsx, index=False, engine='openpyxl')
    print(f"\n處理完成！結果已儲存至：{output_xlsx}")


if __name__ == "__main__":
    INPUT_XLSX = "data/2023_query_result/瑞興銀行_2023_output_chunks.xlsx"
    fill_tcfd_flags_v3(INPUT_XLSX)
