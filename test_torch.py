import torch

# 檢查 CUDA 是否可用
print(f"CUDA is available: {torch.cuda.is_available()}")

# 顯示 PyTorch 正在使用的 CUDA 版本 (這應與您新安裝的版本相符)
print(f"PyTorch CUDA version: {torch.version.cuda}")

# 創建一個 tensor 並放到 GPU 上
if torch.cuda.is_available():
    tensor = torch.rand(3, 3).cuda()
    print("Tensor is on GPU.")

torch.cuda.empty_cache()