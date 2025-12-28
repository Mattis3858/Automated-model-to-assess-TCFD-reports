import torch
from langchain_community.embeddings import HuggingFaceEmbeddings
from FlagEmbedding import FlagReranker

class ModelManager:
    def __init__(self):
        self.embeddings = None
        self.reranker = None
        self.device = "cuda" if torch.cuda.is_available() else "cpu"

    def load_models(self, emb_name: str, rerank_name: str):
        print(f"[INFO] Loading models on {self.device}...")
        self.embeddings = HuggingFaceEmbeddings(
            model_name=emb_name, 
            model_kwargs={"device": self.device}
        )
        self.reranker = FlagReranker(
            rerank_name, 
            use_fp16=(self.device == "cuda"), 
            device=self.device
        )
        print("[INFO] Models loaded successfully.")

# 初始化單例
model_manager = ModelManager()