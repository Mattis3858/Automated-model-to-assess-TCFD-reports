import React, { useState, useRef } from 'react';
import { Upload, FileText, CheckCircle, X, ChevronDown, ChevronUp, AlertCircle } from 'lucide-react';

const App = () => {
  const [dragActive, setDragActive] = useState(false);
  const [file, setFile] = useState(null);
  const [uploadStatus, setUploadStatus] = useState('idle'); // idle, uploading, success, error
  const [errorMessage, setErrorMessage] = useState('');
  const [selectedStandard, setSelectedStandard] = useState('TCFD');
  const [isDropdownOpen, setIsDropdownOpen] = useState(false);
  
  const inputRef = useRef(null);
  
  const standards = ['TCFD', 'TNFD', 'S1'];

  // Handle drag events
  const handleDrag = (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === 'dragenter' || e.type === 'dragover') {
      setDragActive(true);
    } else if (e.type === 'dragleave') {
      setDragActive(false);
    }
  };

  // Handle drop event
  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      handleFile(e.dataTransfer.files[0]);
    }
  };

  // Handle file input change
  const handleChange = (e) => {
    e.preventDefault();
    if (e.target.files && e.target.files[0]) {
      handleFile(e.target.files[0]);
    }
  };

  const handleFile = async (uploadedFile) => {
    if (uploadedFile.type !== 'application/pdf') {
      alert('請上傳 PDF 格式的檔案');
      return;
    }
    setFile(uploadedFile);
    setUploadStatus('uploading');
    setErrorMessage('');

    // Create FormData to send file
    const formData = new FormData();
    formData.append('file', uploadedFile);

    try {
      // 呼叫後端 API
      const response = await fetch('http://localhost:8000/upload', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        throw new Error(`Upload failed: ${response.statusText}`);
      }

      const data = await response.json();
      console.log('Success:', data);
      
      // 為了展示效果，至少轉圈圈 1.5 秒再顯示成功
      setTimeout(() => {
        setUploadStatus('success');
      }, 1500);

    } catch (error) {
      console.error('Error:', error);
      setErrorMessage('上傳失敗，請確認後端伺服器 (server.py) 是否已啟動');
      setUploadStatus('error');
    }
  };

  const onButtonClick = () => {
    inputRef.current.click();
  };

  const resetUpload = (e) => {
    e.stopPropagation();
    setFile(null);
    setUploadStatus('idle');
    setErrorMessage('');
  };

  return (
    <div className="min-h-screen bg-[#4B4B4B] flex items-center justify-center font-sans text-white p-4">
      {/* Main Drop Area Container */}
      <div 
        className={`relative w-full max-w-2xl h-96 transition-all duration-300 ease-in-out
          ${dragActive ? 'scale-105' : 'scale-100'}
        `}
      >
        {/* The Dashed Box */}
        <div 
          className={`
            w-full h-full rounded-3xl border-4 border-dashed flex flex-col items-center justify-center cursor-pointer relative
            transition-colors duration-300
            ${dragActive ? 'border-blue-400 bg-[#5a5a5a]' : 'border-[#333333] bg-[#3a3a3a] hover:bg-[#404040]'}
            ${uploadStatus === 'success' ? 'border-green-500' : ''}
            ${uploadStatus === 'error' ? 'border-red-500' : ''}
          `}
          onDragEnter={handleDrag}
          onDragLeave={handleDrag}
          onDragOver={handleDrag}
          onDrop={handleDrop}
          onClick={onButtonClick}
        >
          <input 
            ref={inputRef}
            type="file"
            className="hidden" 
            onChange={handleChange}
            accept=".pdf"
          />

          {/* Content inside the box */}
          {file ? (
            <div className="text-center animate-fade-in w-full px-8">
              {uploadStatus === 'uploading' && (
                <div className="mb-4">
                  <div className="w-16 h-16 border-4 border-blue-400 border-t-transparent rounded-full animate-spin mx-auto"></div>
                  <p className="mt-4 text-blue-300">正在處理並建立向量資料庫...</p>
                </div>
              )}
              {uploadStatus === 'success' && (
                <div className="animate-in zoom-in duration-300">
                    <CheckCircle className="w-16 h-16 text-green-500 mx-auto mb-4" />
                </div>
              )}
              {uploadStatus === 'error' && (
                <div className="animate-in shake duration-300">
                    <AlertCircle className="w-16 h-16 text-red-500 mx-auto mb-4" />
                </div>
              )}
              
              <div className="flex items-center justify-center space-x-2 bg-[#2a2a2a] px-6 py-3 rounded-xl shadow-lg mx-auto max-w-md">
                <FileText className="w-6 h-6 text-gray-300 flex-shrink-0" />
                <span className="text-lg font-medium truncate flex-1 text-left">{file.name}</span>
                <button 
                  onClick={resetUpload}
                  className="p-1 hover:bg-gray-600 rounded-full transition-colors ml-2"
                >
                  <X className="w-5 h-5 text-gray-400" />
                </button>
              </div>
              
              {uploadStatus === 'success' && (
                <p className="mt-4 text-green-400 font-medium">
                  上傳成功！檔案已存入 ChromaDB，使用 {selectedStandard} 標準
                </p>
              )}
              {uploadStatus === 'error' && (
                <p className="mt-4 text-red-400 font-medium">
                  {errorMessage || "發生錯誤，請重試"}
                </p>
              )}
            </div>
          ) : (
            <div className="text-center pointer-events-none">
              <p className="text-4xl font-light text-white mb-2 drop-shadow-md">Drop File</p>
              <p className="text-gray-400 text-sm">或是點擊上傳 PDF 報告書</p>
            </div>
          )}
        </div>

        {/* Dropdown Menu */}
        <div className="absolute -bottom-16 right-0 z-20">
            <div className="relative">
                <button 
                    onClick={(e) => {
                        e.stopPropagation();
                        setIsDropdownOpen(!isDropdownOpen);
                    }}
                    className="flex items-center space-x-2 bg-[#666666] hover:bg-[#777777] text-white px-4 py-2 rounded-lg shadow-lg transition-colors min-w-[140px] justify-between border border-gray-500"
                >
                    <span className="font-medium tracking-wide">
                        {selectedStandard}
                    </span>
                    {isDropdownOpen ? <ChevronUp size={18} /> : <ChevronDown size={18} />}
                </button>
                
                <div className="absolute -top-6 right-0 text-xs text-gray-300 font-light mb-1 mr-1">
                    揭露標準 ▼
                </div>

                {isDropdownOpen && (
                    <div className="absolute bottom-full mb-2 right-0 w-full bg-[#333333] border border-gray-600 rounded-lg shadow-xl overflow-hidden animate-in fade-in zoom-in-95 duration-200">
                        {standards.map((std) => (
                            <button
                                key={std}
                                onClick={() => {
                                    setSelectedStandard(std);
                                    setIsDropdownOpen(false);
                                }}
                                className={`w-full text-left px-4 py-3 text-sm hover:bg-[#444444] transition-colors
                                    ${selectedStandard === std ? 'text-blue-300 font-bold bg-[#3a3a3a]' : 'text-gray-200'}
                                `}
                            >
                                {std}
                            </button>
                        ))}
                    </div>
                )}
            </div>
        </div>
      </div>
   
    </div>
  );
};

export default App;