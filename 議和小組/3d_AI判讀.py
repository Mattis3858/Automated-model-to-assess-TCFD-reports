# -*- coding: utf-8 -*-
"""
Stakeholder Engagement (SE) Level Scoring via gpt-4.1-mini — FIRST 5 ONLY
流程：逐 chunk 評分 → ESG/Stew 各自平均/中位數 → 0.7/0.3 加權
限制：只處理清單中前 5 家；不做關鍵字過濾
特性：公司為單位即時落盤（可隨時 Ctrl+C，中斷不丟已完成公司）
輸出：
  1) ESGreport_SE_level_output_chunk_scores_first5.csv（逐 chunk 六類分數＋理由）
  2) ESGreport_SE_level_output_averaged_weighted_first5.csv
     （每家/每年六類 ESG 平均/中位數、Stew 平均/中位數、加權後最終）
"""

import re
import csv
import json
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
from statistics import mean, median

import pandas as pd
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm.auto import tqdm
from openai import OpenAI

# ======== 基本路徑 ========
STEW_CHUNK_BASE_DIR = Path(r"data/議和小組盡職治理報告書/Report_chunks")
ESG_CHUNK_BASE_DIR  = Path(r"data/議和小組永續報告書/Report/Report_chunks")

DEFINITION_CSV      = Path(r"議和小組/SES_engagement_level.csv")
ENV_PATH            = Path(r".env")
PAIRS_LIST_PATH     = Path(r"議和小組/filed_list_pairs_lines.txt")
# 逐 chunk 稽核輸出（前 5 家）
OUTPUT_CHUNK_CSV    = Path(r"議和小組/output/ESGreport_SE_level_output_chunk_scores_FULL.csv")
OUTPUT_FINAL_CSV    = Path(r"議和小組/output/ESGreport_SE_level_output_averaged_weighted_FULL.csv")

# ======== 參數 ========
MODEL_NAME = "gpt-4.1-mini"
ENCODING   = "utf-8"

# 權重
W_ESG  = 0.70
W_STEW = 0.30

# 固定六類利害關係人（英文鍵、中文名）
STAKEHOLDERS = [
    ("employees",    "員工"),
    ("shareholders", "股東"),
    ("suppliers",    "供應商"),
    ("customers",    "客戶"),
    ("government",   "政府"),
    ("society",      "社會"),
]

# 行格式：允許 4 或 5 位公司碼
RE_ESG_ONLY = re.compile(r"^(?P<eyear>\d{4})_(?P<efirm>\d{4,5})_ESGreport$", re.IGNORECASE)
RE_BOTH     = re.compile(
    r"^(?P<eyear>\d{4})_(?P<efirm>\d{4,5})_ESGreport==(?P<syear>\d{4})_(?P<sfirma>\d{4,5})_stewardship$",
    re.IGNORECASE
)

# ======== 小工具 ========
def append_rows(path: Path, header: list, rows: list):
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=",", lineterminator="\n")
        if not file_exists:
            w.writerow(header)
        if rows:
            w.writerows(rows)

def esg_dir_path(year: str, firm: str) -> Path:
    return ESG_CHUNK_BASE_DIR / f"{year}_{firm}_ESGreport"

def stew_dir_path(year: Optional[str], firm: Optional[str]) -> Optional[Path]:
    if year and firm:
        return STEW_CHUNK_BASE_DIR / f"{year}_{firm}_stewardship"
    return None

def list_chunks(report_dir: Optional[Path]) -> List[Path]:
    if not report_dir or not report_dir.exists():
        return []
    return sorted(report_dir.glob("chunk_*.txt"), key=lambda p: p.name)

def read_text(p: Path) -> str:
    try:
        return p.read_text(encoding=ENCODING, errors="ignore").strip()
    except Exception:
        return p.read_text(encoding="utf-8", errors="ignore").strip()

# ======== 定義表 ========
def load_definitions(def_path: Path) -> List[Dict[str, Any]]:
    if not def_path.exists():
        raise FileNotFoundError(f"找不到定義檔：{def_path}")
    df = pd.read_csv(def_path, dtype=str)
    df = df.fillna("")
    df = df.map(lambda x: x.strip())  # 取代 applymap 避免 FutureWarning
    needed = {"Score", "Level", "Definition"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"定義檔缺少欄位：{missing}（需包含 {needed}）")
    return df.to_dict(orient="records")

def bake_definition_text(def_rows: List[Dict[str, Any]]) -> str:
    lines = []
    for r in def_rows:
        score = r.get("Score", "").strip()
        level = r.get("Level", "").strip()
        definition = r.get("Definition", "").strip()
        if not score:
            continue
        piece = f"Score {score}"
        if level:
            piece += f"（Level: {level}）"
        if definition:
            piece += f"：{definition}"
        lines.append(piece)
    return "\n".join(lines)

# ======== OpenAI ========
def load_client() -> OpenAI:
    load_dotenv(dotenv_path=str(ENV_PATH))
    return OpenAI()

SYSTEM_PROMPT_ESG = (
    "你是一位企業揭露與利害關係人議合的專家。\n"
    "任務：根據提供的『永續報告節選』，依對照表評估六類利害關係人的議合層級。\n"
    "只依本文與對照表判斷，不做外推。"
)
SYSTEM_PROMPT_STEW = (
    "你是一位企業揭露與利害關係人議合的專家。\n"
    "任務：根據提供的『盡職治理/盡職治理報告節選』，依對照表評估六類利害關係人的議合層級。\n"
    "只依本文與對照表判斷，不做外推。"
)

USER_PROMPT_TEMPLATE_COMMON = """請依下列「議合層級對照表」對六類利害關係人（固定順序：員工、股東、供應商、客戶、政府、社會）進行評估。
回傳 1–10 的整數（若完全未提及或無法判定則 NA），並給出**簡短中文理由（不超過 80 字）**。

【判斷原則】
- 不能只因列出「溝通管道／名單」就給高分；需看到「互動強度」「共識形成／共同行動」或「治理/策略/營運納入與追蹤」等證據。
- 同時出現多層級跡象時，選擇**文本可明確支持的最高層級**。
- 僅依本段節選判斷，不引用其他來源。

【「社會」利害關係人定義補充】
- 一般大眾與社區：一般民眾、社區、社區居民、社區鄰里、社區成員、社區社群組織等。
- 非營利組織與公益團體：NPO/NGO、非營利組織、非政府組織、社福團體、公益團體、社區非營利組織或公益團體、捐款人、受助人、愛心志工、志工、藝術家等。
- 產業與專業機構：工會、公會、公協會、工協會、同業公會、產業公協會、金融同業、外部顧問、外部專家、與環境保護攸關之企業或環保攸關組織等。
- 學術、教育與青年：學術單位、顧問單位、學校、學生（含受獎學生）、青年等。
- 永續評比與媒體：永續評比機構、信用評等機構、信評機構、國內外永續組織與評比機構、媒體等。
- 其他／混合：社區／學校／非營利組織、社區／非營利組織／非政府組織／學者專家、社區居民／公益團體、信評團體／公協會等。
若文本中出現上述對象，即視為「社會」利害關係人相關內容。

【只回傳 JSON；鍵名固定】employees, shareholders, suppliers, customers, government, society
每鍵值格式：{{"score": "1-10 或 NA", "reason": "中文理由"}}

【議合層級對照表】
{definition_text}

【本文節選】
{text}

請輸出**唯一**一段 JSON，例如：
{{
  "employees":    {{"score": "NA", "reason": "…"}},
  "shareholders": {{"score": "3",  "reason": "…"}},
  "suppliers":    {{"score": "5",  "reason": "…"}},
  "customers":    {{"score": "NA", "reason": "…"}},
  "government":   {{"score": "7",  "reason": "…"}},
  "society":      {{"score": "2",  "reason": "…"}}
}}
"""

@retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=30))
def call_gpt_json(client: OpenAI, model: str, system_msg: str, user_msg: str) -> Dict[str, Any]:
    resp = client.chat.completions.create(
        model=model,
        temperature=0,
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user",   "content": user_msg},
        ],
    )
    content = resp.choices[0].message.content or ""
    m = re.search(r"\{.*\}", content, flags=re.DOTALL)
    json_str = m.group(0) if m else content
    return json.loads(json_str)

def normalize_score_to_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    s = str(val).strip().upper().replace("，", ",")
    if s == "NA":
        return None
    s = re.sub(r"[^\d]", "", s)
    if not s:
        return None
    try:
        n = int(s)
        if 1 <= n <= 10:
            return float(n)
        return None
    except Exception:
        return None

def ensure_json_keys(d: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for key, _zh in STAKEHOLDERS:
        node = d.get(key, {}) if isinstance(d, dict) else {}
        score_raw = node.get("score")
        reason = node.get("reason", "") or ""
        out[key] = {"score": score_raw, "reason": str(reason).strip()}
    return out

# ======== 清單解析（只取前 5 家） ========
def parse_pairs_list_first5(list_path: Path) -> List[Tuple[str, str, Optional[str], Optional[str]]]:
    if not list_path.exists():
        raise FileNotFoundError(f"找不到清單檔：{list_path}")

    pairs: List[Tuple[str, str, Optional[str], Optional[str]]] = []
    with list_path.open("r", encoding="utf-8") as f:
        for ln, raw in enumerate(f, start=1):
            # 清除 BOM / 全形底線
            line = raw.replace("\ufeff", "").replace("＿", "_").strip()
            if not line or line.startswith("#"):
                continue

            m2 = RE_BOTH.match(line)
            if m2:
                ey, ef, sy, sf = (
                    m2.group("eyear"), m2.group("efirm"),
                    m2.group("syear"), m2.group("sfirma"),
                )
                pairs.append((ey, ef, sy, sf))
            else:
                m1 = RE_ESG_ONLY.match(line)
                if m1:
                    ey, ef = m1.group("eyear"), m1.group("efirm")
                    pairs.append((ey, ef, None, None))
                else:
                    print(f"[WARN] 第 {ln} 行格式不符，略過：{line}")

            # if len(pairs) >= 5:  # 只取前 5 家
            #     break

    return pairs

# ======== 聚合與加權 ========
def safe_mean(nums: List[Optional[float]]) -> Optional[float]:
    vals = [x for x in nums if x is not None]
    return mean(vals) if vals else None

def safe_median(nums: List[Optional[float]]) -> Optional[float]:
    vals = [x for x in nums if x is not None]
    return median(vals) if vals else None

def weighted_combine(esg_avg: Optional[float], stew_avg: Optional[float]) -> Optional[float]:
    has_esg  = esg_avg  is not None
    has_stew = stew_avg is not None
    if has_esg and has_stew:
        return W_ESG * esg_avg + W_STEW * stew_avg
    if has_esg:
        return esg_avg
    if has_stew:
        return stew_avg
    return None

def fmt2(x: Optional[float]) -> str:
    return f"{x:.2f}" if x is not None else "NA"

# ======== 主流程（即時落盤、可安全中斷） ========
def main():
    # 1) 定義表
    def_rows = load_definitions(DEFINITION_CSV)
    definition_text = bake_definition_text(def_rows)

    # 2) OpenAI
    client = load_client()

    # 3) 讀清單（前 5 家）
    entries = parse_pairs_list_first5(PAIRS_LIST_PATH)
    if not entries:
        print("[ERROR] 清單前 5 家不可解析。")
        return
    print(f"[INFO] 將處理前 5 家（實際取得 {len(entries)} 家）。")

    # 4) 即時落盤：先清空舊檔（避免上次測試殘留）
    for p in [OUTPUT_CHUNK_CSV, OUTPUT_FINAL_CSV]:
        if p.exists():
            p.unlink()

    try:
        for (eyear, efirm, syear, sfirma) in tqdm(entries, desc="Processing first 5 firms", unit="firm"):
            # 每家公司各自的暫存列
            local_chunk_rows: List[Tuple[str, str, str, str, str, str, str]] = []
            local_final_rows: List[Tuple[str, str, str, str, str, str, str, str]] = []

            e_dir = esg_dir_path(eyear, efirm)
            s_dir = stew_dir_path(syear, sfirma)

            if not e_dir.exists() and not (s_dir and s_dir.exists()):
                tqdm.write(f"[WARN] ESG 與 Stewardship 皆不存在：{eyear}_{efirm}，略過。")
                continue

            # ===== ESG: 逐 chunk =====
            esg_scores_acc: Dict[str, List[Optional[float]]] = {k: [] for k, _ in STAKEHOLDERS}
            esg_chunks = list_chunks(e_dir) if e_dir.exists() else []

            for p in tqdm(esg_chunks, leave=False, desc=f"ESG chunks {eyear}_{efirm}"):
                text = read_text(p)
                if not text:
                    continue
                user_msg = USER_PROMPT_TEMPLATE_COMMON.format(
                    definition_text=definition_text,
                    text=text
                )
                try:
                    j = call_gpt_json(client, MODEL_NAME, SYSTEM_PROMPT_ESG, user_msg)
                except Exception as e:
                    tqdm.write(f"[ERROR] GPT(ESG) 失敗：{eyear}_{efirm}::{p.name} → {e}")
                    continue
                parsed = ensure_json_keys(j)
                for key, zh in STAKEHOLDERS:
                    sc_raw = parsed[key]["score"]
                    rsn    = parsed[key]["reason"]
                    sc     = normalize_score_to_float(sc_raw)
                    esg_scores_acc[key].append(sc)
                    local_chunk_rows.append((eyear, efirm, "ESG", p.name, zh, (str(int(sc)) if sc is not None else "NA"), rsn))

            # ===== STEW: 逐 chunk =====
            stew_scores_acc: Dict[str, List[Optional[float]]] = {k: [] for k, _ in STAKEHOLDERS}
            stew_chunks = list_chunks(s_dir) if (s_dir and s_dir.exists()) else []

            for p in tqdm(stew_chunks, leave=False, desc=f"STEW chunks {eyear}_{efirm}"):
                text = read_text(p)
                if not text:
                    continue
                user_msg = USER_PROMPT_TEMPLATE_COMMON.format(
                    definition_text=definition_text,
                    text=text
                )
                try:
                    j = call_gpt_json(client, MODEL_NAME, SYSTEM_PROMPT_STEW, user_msg)
                except Exception as e:
                    tqdm.write(f"[ERROR] GPT(STEW) 失敗：{eyear}_{efirm}::{p.name} → {e}")
                    continue
                parsed = ensure_json_keys(j)
                for key, zh in STAKEHOLDERS:
                    sc_raw = parsed[key]["score"]
                    rsn    = parsed[key]["reason"]
                    sc     = normalize_score_to_float(sc_raw)
                    stew_scores_acc[key].append(sc)
                    local_chunk_rows.append((eyear, efirm, "STEW", p.name, zh, (str(int(sc)) if sc is not None else "NA"), rsn))

            # ===== 平均 / 中位數 與加權 =====
            for key, zh in STAKEHOLDERS:
                esg_avg  = safe_mean(esg_scores_acc[key])
                esg_med  = safe_median(esg_scores_acc[key])
                stew_avg = safe_mean(stew_scores_acc[key])
                stew_med = safe_median(stew_scores_acc[key])
                final    = weighted_combine(esg_avg, stew_avg)
                local_final_rows.append(
                    (
                        eyear,
                        efirm,
                        zh,
                        fmt2(esg_avg),
                        fmt2(esg_med),
                        fmt2(stew_avg),
                        fmt2(stew_med),
                        fmt2(final),
                    )
                )

            # ===== 立刻寫檔（公司為單位） =====
            append_rows(
                OUTPUT_CHUNK_CSV,
                ["Year","Firmcode","Source","ChunkName","Stakeholder","Score","Reason"],
                local_chunk_rows
            )
            append_rows(
                OUTPUT_FINAL_CSV,
                ["Year","Firmcode","Stakeholder","ESG_avg","ESG_med","Stew_avg","Stew_med","Final_weighted"],
                local_final_rows
            )
            tqdm.write(f"[SAVED] {eyear}_{efirm} 已寫入 first5 結果。")

    except KeyboardInterrupt:
        tqdm.write("\n[STOPPED] 手動中斷，已保留先前公司完成的結果（first5）。")

    print(f"Done.\nChunk-level (first5): {OUTPUT_CHUNK_CSV}\nAveraged & weighted (first5): {OUTPUT_FINAL_CSV}")

if __name__ == "__main__":
    main()
