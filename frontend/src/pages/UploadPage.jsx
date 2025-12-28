import React, { useState, useRef } from 'react';
import { 
  FileText, CheckCircle, X, ChevronDown, ChevronUp, AlertCircle, 
  Play, Loader2, RotateCcw, Settings, UploadCloud 
} from 'lucide-react';

const UploadPage = () => {
  const [dragActive, setDragActive] = useState(false);
  const [file, setFile] = useState(null);
  const [appState, setAppState] = useState('idle'); // idle, staged, uploading, processing, success, error
  const [progressMessage, setProgressMessage] = useState('');
  
  // Standard 相關狀態
  const [selectedStandard, setSelectedStandard] = useState('TCFD');
  const [isDropdownOpen, setIsDropdownOpen] = useState(false);
  const [forceUpdate, setForceUpdate] = useState(false);
  
  const inputRef = useRef(null);
  const standardInputRef = useRef(null); // 用於上傳 Excel
  
  const standards = ['TCFD', 'TNFD', 'S1', 'SASB'];

  // --- 拖曳與檔案選擇邏輯 (保持不變) ---
  const handleDrag = (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (appState !== 'idle' && appState !== 'staged') return;
    if (e.type === 'dragenter' || e.type === 'dragover') {
      setDragActive(true);
    } else if (e.type === 'dragleave') {
      setDragActive(false);
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    if (appState !== 'idle' && appState !== 'staged') return;
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      handleFileSelect(e.dataTransfer.files[0]);
    }
  };

  const handleChange = (e) => {
    e.preventDefault();
    if (e.target.files && e.target.files[0]) {
      handleFileSelect(e.target.files[0]);
    }
  };

  const handleFileSelect = (uploadedFile) => {
    if (uploadedFile.type !== 'application/pdf') {
      alert('Only PDF files are allowed');
      return;
    }
    setFile(uploadedFile);
    setAppState('staged');
    setProgressMessage('');
  };

  const clearFile = (e) => {
    e?.stopPropagation();
    setFile(null);
    setAppState('idle');
    setForceUpdate(false);
    if (inputRef.current) inputRef.current.value = '';
  };

  const resetAll = () => {
    clearFile();
    setProgressMessage('');
  };

  // --- 新增：上傳 Standard Excel 的邏輯 ---
  const handleStandardUpload = async (e) => {
    const excelFile = e.target.files[0];
    if (!excelFile) return;

    const formData = new FormData();
    formData.append('standard_name', selectedStandard);
    formData.append('file', excelFile);

    try {
      // 這裡呼叫你新寫的 /upload-standard 端點
      const res = await fetch('http://localhost:8000/upload-standard', {
        method: 'POST',
        body: formData,
      });
      if (!res.ok) throw new Error('Failed to upload standard');
      alert(`Standard ${selectedStandard} rules uploaded successfully!`);
    } catch (err) {
      alert(err.message);
    } finally {
        if(standardInputRef.current) standardInputRef.current.value = ''; // 清空 input
    }
  };

  // --- 修改核心：開始處理流程 ---
  const startProcessing = async () => {
    if (!file) return;
    setAppState('uploading');
    setProgressMessage('Uploading PDF & Starting Pipeline...');

    const formData = new FormData();
    formData.append('file', file);
    formData.append('standard', selectedStandard);
    formData.append('force_update', forceUpdate);

    try {
      // 修改 1: 端點改為 /process-pdf
      const response = await fetch('http://localhost:8000/process-pdf', {
        method: 'POST',
        body: formData,
      });

      // 修改 2: 更細緻的錯誤處理 (捕捉 400 Standard Not Found)
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Upload failed');
      }

      const data = await response.json();
      setAppState('processing');
      // 拿到 task_id 後開始輪詢
      pollStatus(data.task_id);
      
    } catch (error) {
      setAppState('error');
      // 顯示後端回傳的具體錯誤訊息 (例如: Standard not found)
      setProgressMessage(error.message);
    }
  };

  // --- 輪詢狀態 (保持邏輯，微調文字) ---
  const pollStatus = (taskId) => {
    const intervalId = setInterval(async () => {
      try {
        const res = await fetch(`http://localhost:8000/status/${taskId}`);
        if (!res.ok) throw new Error('Status check failed');
        const statusData = await res.json();
        
        if (statusData.status === 'completed') {
          clearInterval(intervalId);
          setAppState('success');
          setProgressMessage(statusData.message); // 顯示後端回傳的 "Pipeline Completed..."
        } else if (statusData.status === 'failed') {
          clearInterval(intervalId);
          setAppState('error');
          setProgressMessage(`Processing Error: ${statusData.message}`);
        } else {
          // 顯示後端的即時狀態訊息
          setProgressMessage(statusData.message || 'AI is analyzing...');
        }
      } catch (err) {
        clearInterval(intervalId);
        setAppState('error');
        setProgressMessage('Lost connection to server.');
      }
    }, 1500); // 稍微放慢輪詢速度到 1.5秒
  };

  return (
    <div className="pt-32 pb-12 flex items-center justify-center px-4">
      <div className={`relative w-full max-w-2xl transition-all duration-300 ${dragActive ? 'scale-105' : 'scale-100'}`}>
        
        {/* Main Upload Card */}
        <div 
          className={`w-full rounded-3xl border-4 border-dashed flex flex-col items-center justify-center relative overflow-hidden transition-all duration-300 min-h-[400px]
            ${dragActive ? 'border-blue-400 bg-[#5a5a5a]' : 'border-[#333333] bg-[#3a3a3a]'}
            ${appState === 'success' ? 'border-green-500' : ''}
            ${appState === 'error' ? 'border-red-500' : ''}
          `}
          onDragEnter={handleDrag} onDragLeave={handleDrag} onDragOver={handleDrag} onDrop={handleDrop}
        >
          <input ref={inputRef} type="file" className="hidden" onChange={handleChange} accept=".pdf" disabled={appState !== 'idle' && appState !== 'staged'} />

          {/* State: Idle */}
          {appState === 'idle' && (
            <div className="text-center cursor-pointer w-full h-full py-20" onClick={() => inputRef.current.click()}>
              <p className="text-4xl font-light text-white mb-2 drop-shadow-md">Drop PDF Report</p>
              <p className="text-gray-400 text-sm">Automated Vectorize & Rerank</p>
            </div>
          )}

          {/* State: Staged (File Selected) */}
          {appState === 'staged' && file && (
            <div className="w-full px-8 py-10 flex flex-col items-center">
              <div className="flex items-center justify-between w-full bg-[#2a2a2a] p-4 rounded-xl shadow-lg mb-6 border border-gray-600">
                <div className="flex items-center space-x-4">
                  <div className="bg-red-500/20 p-2 rounded-lg text-red-400"><FileText size={32} /></div>
                  <div className="text-left">
                    <p className="font-medium truncate max-w-[200px] text-white">{file.name}</p>
                    <p className="text-xs text-gray-400">{(file.size / 1024 / 1024).toFixed(2)} MB</p>
                  </div>
                </div>
                <button onClick={clearFile} className="p-2 hover:bg-gray-600 rounded-full transition-colors">
                  <X className="w-5 h-5 text-gray-400 hover:text-white" />
                </button>
              </div>

              {/* Force Update Checkbox */}
              <div className="flex items-center space-x-2 mb-8 bg-[#333] px-4 py-2 rounded-lg border border-gray-600">
                <input type="checkbox" id="forceUpdate" checked={forceUpdate} onChange={(e) => setForceUpdate(e.target.checked)} className="w-4 h-4 text-blue-600 accent-blue-600" />
                <label htmlFor="forceUpdate" className="text-gray-300 text-sm cursor-pointer select-none">Force Re-vectorize</label>
              </div>

              <button onClick={startProcessing} className="flex items-center space-x-2 bg-blue-600 hover:bg-blue-500 text-white px-8 py-3 rounded-full font-medium transition-all transform hover:scale-105 shadow-lg shadow-blue-900/50">
                <Play size={20} fill="currentColor" />
                <span>Start Analysis</span>
              </button>
            </div>
          )}

          {/* State: Uploading / Processing */}
          {(appState === 'uploading' || appState === 'processing') && (
            <div className="text-center py-20 px-8 w-full">
              <div className="relative w-20 h-20 mx-auto mb-6">
                <div className="absolute inset-0 border-4 border-gray-600 rounded-full"></div>
                <div className="absolute inset-0 border-4 border-blue-400 border-t-transparent rounded-full animate-spin"></div>
                <Loader2 className="absolute inset-0 m-auto text-blue-400 animate-pulse" size={32} />
              </div>
              <h3 className="text-xl font-medium text-white mb-2">
                {appState === 'uploading' ? 'Uploading & Initializing...' : 'AI Pipeline Running...'}
              </h3>
              <p className="text-blue-300 text-sm animate-pulse max-w-md mx-auto truncate">
                {progressMessage}
              </p>
            </div>
          )}

          {/* State: Success */}
          {appState === 'success' && (
            <div className="text-center py-20 px-8">
              <CheckCircle className="w-20 h-20 text-green-500 mx-auto mb-6" />
              <h3 className="text-2xl font-bold text-white mb-2">Analysis Complete!</h3>
              <p className="text-green-400 mb-8 text-sm">{progressMessage}</p>
              <button onClick={resetAll} className="flex items-center justify-center space-x-2 bg-[#444] hover:bg-[#555] text-white px-6 py-3 rounded-xl mx-auto border border-gray-500 transition-colors">
                <RotateCcw size={18} /><span>Process New File</span>
              </button>
            </div>
          )}

          {/* State: Error */}
          {appState === 'error' && (
            <div className="text-center py-20 px-8">
              <AlertCircle className="w-16 h-16 text-red-500 mx-auto mb-4" />
              <h3 className="text-xl font-bold text-red-400 mb-2">Pipeline Failed</h3>
              <p className="text-gray-300 mb-6 max-w-md mx-auto">{progressMessage}</p>
              <button onClick={resetAll} className="text-white underline hover:text-blue-400">Try Again</button>
            </div>
          )}
        </div>

        {/* --- Standard Selector & Config --- */}
        {(appState === 'idle' || appState === 'staged') && (
          <div className="absolute -bottom-16 right-0 z-20 flex items-center space-x-2">
            
            {/* 上傳 Excel 用的隱藏 Input */}
            <input 
              ref={standardInputRef} 
              type="file" 
              className="hidden" 
              accept=".xlsx,.xls" 
              onChange={handleStandardUpload} 
            />

            {/* Config 按鈕 (上傳 Excel) */}
            <div className="group relative">
                <button 
                  onClick={() => standardInputRef.current.click()}
                  className="bg-[#444] hover:bg-[#555] p-2 rounded-lg border border-gray-600 text-gray-300 hover:text-white transition-colors"
                  title={`Upload Excel rules for ${selectedStandard}`}
                >
                  <UploadCloud size={20} />
                </button>
                <div className="absolute bottom-full mb-2 right-0 w-max bg-black text-xs text-white px-2 py-1 rounded opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none">
                   Upload {selectedStandard} Excel
                </div>
            </div>

            {/* Dropdown */}
            <div className="relative">
              <button 
                onClick={(e) => { e.stopPropagation(); setIsDropdownOpen(!isDropdownOpen); }} 
                className="flex items-center space-x-2 bg-[#666666] hover:bg-[#777777] text-white px-4 py-2 rounded-lg border border-gray-500 min-w-[140px] justify-between shadow-lg"
              >
                <span className="font-medium">{selectedStandard}</span>
                {isDropdownOpen ? <ChevronUp size={18} /> : <ChevronDown size={18} />}
              </button>
              
              {isDropdownOpen && (
                <div className="absolute bottom-full mb-2 right-0 w-full bg-[#333333] border border-gray-600 rounded-lg shadow-xl overflow-hidden">
                  {standards.map((std) => (
                    <button 
                      key={std} 
                      onClick={() => { setSelectedStandard(std); setIsDropdownOpen(false); }} 
                      className={`w-full text-left px-4 py-3 text-sm hover:bg-[#444] transition-colors ${selectedStandard === std ? 'text-blue-300 font-bold bg-[#3a3a3a]' : 'text-gray-200'}`}
                    >
                      {std}
                    </button>
                  ))}
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default UploadPage;