# llm_analyzer.py
import os
import pandas as pd
from typing import List, Optional, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pydantic import BaseModel, Field
from tenacity import retry, stop_after_attempt, wait_exponential

from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from prompt.TCFD_LLM_ANSWER_PROMPT import TCFD_LLM_ANSWER_PROMPT

COL_CHUNK = "Content" 
COL_LABEL = "Label"
COL_DEF = "Definition"
COL_POINT = "Point"
COL_REASON = "reasoning"
COL_YN = "is_disclosed"
COL_CONFIDENCE = "confidence"

class Result(BaseModel):
    reasoning: Optional[str] = Field(description="判斷理由")
    is_disclosed: Optional[str] = Field(description="Y 表示有揭露, N 表示無")
    confidence: Optional[float] = Field(description="信心分數 0-1")

class ResultList(BaseModel):
    result: List[Result]

class LLMAnalyzer:
    def __init__(self, api_key: str, model_name: str = "gpt-4o-mini", workers: int = 10):
        self.api_key = api_key
        self.model_name = model_name
        self.workers = workers
        self.chain = self._build_chain()

    def _build_chain(self):
        llm = ChatOpenAI(model=self.model_name, api_key=self.api_key, temperature=0)
        parser = PydanticOutputParser(pydantic_object=ResultList)
        prompt_template = ChatPromptTemplate.from_messages([
            ("system", "你是一位專業的 TCFD 揭露標準判讀專家。"),
            ("human", "{input}"),
        ])
        return prompt_template | llm | parser

    def _get_prompt(self, chunk: str, label: str, point: str = "") -> str:
        return TCFD_LLM_ANSWER_PROMPT.format(
            chunk=chunk,
            label=label,
            point=point
        )

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def _call_chain_safe(self, chunk: str, label: str, point: str) -> dict:
        prompt_text = self._get_prompt(chunk, label, point)
        try:
            resp = self.chain.invoke({"input": prompt_text})
            return resp.model_dump()
        except Exception as e:
            print(f"[LLM Error] {e}")
            return {"result": [{"reasoning": f"Error: {str(e)}", "is_disclosed": "N", "confidence": 0.0}]}

    def process_csv(self, input_path: str, output_path: str):

        if not os.path.exists(input_path):
            return f"File not found: {input_path}"

        try:
            df = pd.read_csv(input_path).fillna("")
        except Exception:
            df = pd.read_excel(input_path).fillna("")

        if COL_CHUNK not in df.columns:
            return f"Column '{COL_CHUNK}' missing in {input_path}"

        for col in [COL_REASON, COL_YN, COL_CONFIDENCE]:
            if col not in df.columns:
                df[col] = ""

        tasks = []
        for idx, row in df.iterrows():
            chunk = str(row.get(COL_CHUNK, "")).strip()
            label = str(row.get(COL_LABEL, "")).strip() 
            point = str(row.get(COL_POINT, "")).strip()
            
            if chunk and label:
                tasks.append((idx, chunk, label, point))

        print(f"[LLM] Starting analysis for {len(tasks)} rows in {os.path.basename(input_path)}...")

        results_map = {}
        
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            future_to_idx = {
                executor.submit(self._call_chain_safe, chunk, label, point): idx 
                for idx, chunk, label, point in tasks
            }
            
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    data = future.result()
                    res = data["result"][0]
                    results_map[idx] = res
                except Exception as e:
                    results_map[idx] = {"reasoning": str(e), "is_disclosed": "N", "confidence": 0.0}

        for idx, res in results_map.items():
            df.at[idx, COL_REASON] = res.get("reasoning", "")
            yn = str(res.get("is_disclosed", "N")).strip().upper()
            df.at[idx, COL_YN] = "Y" if "Y" in yn else "N"
            df.at[idx, COL_CONFIDENCE] = res.get("confidence", 0.0)

        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        return f"Saved LLM result to {os.path.basename(output_path)}"