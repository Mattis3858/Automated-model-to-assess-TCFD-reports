from ollama import chat, Client
import json
import os
from dotenv import load_dotenv

load_dotenv()

PROMPT = """
【角色設定】
你是一位嚴謹的氣候相關財務揭露 (TCFD) 專家。你的任務是負責審閱報告書內容，並判斷報告書內容是否有符合提供的 TCFD 揭露標準。

【背景資訊】
我們正在進行自動化評估企業的 TCFD 報告書，以確保其符合最新的揭露標準。
你將協助我們審閱報告書中的文本塊，並根據每個文本塊的內容，判斷其是否符合特定的 TCFD 揭露標準。

【核心任務】
1. 閱讀並理解提供的報告書文本塊。
2. 根據提供的 TCFD 揭露標準，判斷文本塊是否符合該標準。
3. 提供詳細的推理過程，說明你的判斷依據。
4. 以指定的 JSON 格式輸出結果。

【注意事項】
1. 請只根據【任務資訊提供】中的內容進行判斷，請勿進行任何過度推論。
2. 請勿在回覆中包含與任務無關的資訊。

【任務資訊提供】
- TCFD 揭露標準：
「公司是否揭露對「資本的取得」之財務規劃的影響？」

- 報告書文本塊：
「2021 氣候相關財務揭露建議 TCFD  報告書 圖目錄 表目錄 圖 1、企業社會責任組織架構  圖 2、風險管理組織架構 圖 3、RCP 升溫情境 圖 4、STEPS 情境 圖 5、彰銀氣候相關風險矩陣 圖 6、彰銀新興風險管理機制 圖 7、彰銀2017~2020 年溫室氣體排放量 表 1、氣候相關風險影響與對應之時間範圍 表 2、彰銀前五大氣候相關風險排序  表 3、前五大氣候風險財務影響說明  表 4、氣候相關機會與對應之影響 表 5、氣候變遷對營運策略之影響 表 6、氣候變遷對財務規劃之影響 表 7、彰銀採用之氣候情境 表 8、氣候變遷風險與傳統風險連結 表 9、前五大氣候風險之管理流程作為 表 10、氣候相關指標、目標、執行績效與管理措施 表 11、彰銀持續推動能資源改善措施 表 12、氣候相關指標、目標與執行績效 表 13、彰銀辨識之氣候相關信用風險 表 14、彰銀高碳排產業分類 7 8 9 9 12 24 26 10 12 13 15 17 19 20 22 23 25 27 28 29 31」

【思考步驟（每步一句）】
1. 萃取「TCFD 揭露標準」的關鍵要素。
2. 逐一比對「報告書文本塊」是否提供可直接支持該要素的明確證據。
3. 列出支持每個要素的文本證據，或說明缺少哪些要素。
4. 假如缺少了關鍵要素，或文本證據不充分，則判斷為不符合該標準，並在 reasoning 中說明缺少哪些關鍵要素，或哪些文本證據不充分。
5. 根據上述的思考過程，在 reasoning 撰寫詳細的推理過程。如果所有要素皆有充分證據 is_disclosed="Y"；否則 is_disclosed="N"。
6. 依下列刻度給 `confidence`（0–1，保留兩位小數）：
   - 全要素且證據明確：0.80–1.00
   - 部分要素或證據弱：0.50–0.79
   - 幾乎無證據：0.00–0.49

【輸出格式】
請以 JSON 格式輸出，每題一筆資料，包含以下必填欄位：
1.  reasoning: string。上述【思考步驟】完整思考過程。
2.  is_disclosed: string。明確且充分地揭露了該標準的定義，則回覆 'Y'；若未明確揭露或內容不符合定義，則回覆 'N'
3.  confidence: float。請提供一個 0 到 1 之間的數字，表示你對此判斷的信心程度，1 表示非常有信心，0 表示完全沒有信心

【輸出範例】
範例一:
{"result":[
    {
        "reasoning": "...",
        "is_disclosed": "Y",
        "confidence": 0.95
    }
]}
範例二:
{"result":[
    {
        "reasoning": "...",
        "is_disclosed": "N",
        "confidence": 0.43
    }
]}
"""
client = Client(
    host="https://ollama.com",
    headers={'Authorization': os.getenv("OLLAMA_API_KEY")}
)

messages = [
  {
    'role': 'user',
    'content': PROMPT,
  },
]

response = client.chat('gpt-oss:20b', messages=messages, stream=False)
json_from_model = response.message.content
data = json.loads(json_from_model)
result = json.dumps(data, indent=4, ensure_ascii=False)
print(result)
