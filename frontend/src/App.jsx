import React, { useState, useRef } from 'react';
import { Upload, FileText, CheckCircle, X, ChevronDown, ChevronUp, AlertCircle, Play, Loader2, RotateCcw } from 'lucide-react';

const App = () => {
  const [dragActive, setDragActive] = useState(false);
  const [file, setFile] = useState(null);
  
  const [appState, setAppState] = useState('idle'); 
  const [progressMessage, setProgressMessage] = useState('');
  const [selectedStandard, setSelectedStandard] = useState('TCFD');
  const [isDropdownOpen, setIsDropdownOpen] = useState(false);
  const [forceUpdate, setForceUpdate] = useState(false);
  
  const inputRef = useRef(null);
  const standards = ['TCFD', 'TNFD', 'S1'];

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
    e.stopPropagation();
    setFile(null);
    setAppState('idle');
    setForceUpdate(false);
    if (inputRef.current) inputRef.current.value = '';
  };

  const resetAll = () => {
    setFile(null);
    setAppState('idle');
    setProgressMessage('');
    setForceUpdate(false);
    if (inputRef.current) inputRef.current.value = '';
  };

  const startProcessing = async () => {
    if (!file) return;

    setAppState('uploading');
    setProgressMessage('Uploading file...');

    const formData = new FormData();
    formData.append('file', file);
    formData.append('standard', selectedStandard);
    formData.append('force_update', forceUpdate);

    try {
      const response = await fetch('http://localhost:8000/upload', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) throw new Error('Upload failed');
      
      const data = await response.json();
      const taskId = data.task_id;
      
      setAppState('processing');
      pollStatus(taskId);

    } catch (error) {
      console.error(error);
      setAppState('error');
      setProgressMessage('Connection failed. Please check the backend server.');
    }
  };

  const pollStatus = (taskId) => {
    const intervalId = setInterval(async () => {
      try {
        const res = await fetch(`http://localhost:8000/status/${taskId}`);
        if (!res.ok) throw new Error('Status check failed');
        
        const statusData = await res.json();
        
        if (statusData.status === 'completed') {
          clearInterval(intervalId);
          setAppState('success');
          setProgressMessage(statusData.message || 'Processing complete!');
        } else if (statusData.status === 'failed') {
          clearInterval(intervalId);
          setAppState('error');
          setProgressMessage(`Error: ${statusData.message}`);
        } else {
          setProgressMessage(statusData.message || 'Processing...');
        }
      } catch (err) {
        clearInterval(intervalId);
        setAppState('error');
        setProgressMessage('Failed to retrieve status.');
      }
    }, 1000);
  };

  return (
    <div className="min-h-screen bg-[#4B4B4B] flex items-center justify-center font-sans text-white p-4">
      <div 
        className={`relative w-full max-w-2xl transition-all duration-300 ease-in-out
          ${dragActive ? 'scale-105' : 'scale-100'}
        `}
      >
        <div 
          className={`
            w-full rounded-3xl border-4 border-dashed flex flex-col items-center justify-center relative overflow-hidden
            transition-all duration-300 min-h-[400px]
            ${dragActive ? 'border-blue-400 bg-[#5a5a5a]' : 'border-[#333333] bg-[#3a3a3a]'}
            ${appState === 'success' ? 'border-green-500' : ''}
            ${appState === 'error' ? 'border-red-500' : ''}
          `}
          onDragEnter={handleDrag}
          onDragLeave={handleDrag}
          onDragOver={handleDrag}
          onDrop={handleDrop}
        >
          <input 
            ref={inputRef}
            type="file"
            className="hidden" 
            onChange={handleChange}
            accept=".pdf"
            disabled={appState !== 'idle' && appState !== 'staged'}
          />

          {appState === 'idle' && (
            <div 
                className="text-center cursor-pointer w-full h-full py-20"
                onClick={() => inputRef.current.click()}
            >
              <p className="text-4xl font-light text-white mb-2 drop-shadow-md">Drop File</p>
              <p className="text-gray-400 text-sm">or click to upload PDF report</p>
            </div>
          )}

          {appState === 'staged' && file && (
            <div className="w-full px-8 py-10 flex flex-col items-center animate-fade-in">
              <div className="flex items-center justify-between w-full bg-[#2a2a2a] p-4 rounded-xl shadow-lg mb-6 border border-gray-600">
                <div className="flex items-center space-x-4 overflow-hidden">
                    <div className="bg-red-500/20 p-2 rounded-lg">
                        <FileText className="w-8 h-8 text-red-400" />
                    </div>
                    <div className="text-left">
                        <p className="font-medium truncate max-w-[200px] md:max-w-[300px]">{file.name}</p>
                        <p className="text-xs text-gray-400">{(file.size / 1024 / 1024).toFixed(2)} MB</p>
                    </div>
                </div>
                <button onClick={clearFile} className="p-2 hover:bg-gray-600 rounded-full transition-colors">
                  <X className="w-5 h-5 text-gray-400 hover:text-white" />
                </button>
              </div>

              <div className="flex items-center space-x-2 mb-8 bg-[#333] px-4 py-2 rounded-lg border border-gray-600">
                <input 
                    type="checkbox" 
                    id="forceUpdate"
                    checked={forceUpdate}
                    onChange={(e) => setForceUpdate(e.target.checked)}
                    className="w-4 h-4 text-blue-600 rounded focus:ring-blue-500 bg-gray-700 border-gray-600 cursor-pointer"
                />
                <label htmlFor="forceUpdate" className="text-gray-300 text-sm cursor-pointer select-none">
                    Force Re-process (Overwrite existing)
                </label>
              </div>

              <div className="flex space-x-4">
                  <button 
                    onClick={startProcessing}
                    className="flex items-center space-x-2 bg-blue-600 hover:bg-blue-500 text-white px-8 py-3 rounded-full font-medium transition-all transform hover:scale-105 shadow-lg shadow-blue-500/20"
                  >
                    <Play size={20} fill="currentColor" />
                    <span>Start Processing</span>
                  </button>
              </div>
              <p className="mt-6 text-gray-400 text-sm">Standard: <span className="text-blue-300 font-bold">{selectedStandard}</span></p>
            </div>
          )}

          {(appState === 'uploading' || appState === 'processing') && (
            <div className="text-center py-20 px-8 animate-fade-in">
                <div className="relative w-20 h-20 mx-auto mb-6">
                    <div className="absolute inset-0 border-4 border-gray-600 rounded-full"></div>
                    <div className="absolute inset-0 border-4 border-blue-400 border-t-transparent rounded-full animate-spin"></div>
                    <Loader2 className="absolute inset-0 m-auto text-blue-400 animate-pulse" size={32} />
                </div>
                <h3 className="text-xl font-medium text-white mb-2">
                    {appState === 'uploading' ? 'Uploading...' : 'AI is processing...'}
                </h3>
                <p className="text-blue-300 text-sm animate-pulse">{progressMessage}</p>
            </div>
          )}

          {appState === 'success' && (
            <div className="text-center py-20 px-8 animate-in zoom-in duration-300">
                <div className="w-20 h-20 bg-green-500/20 rounded-full flex items-center justify-center mx-auto mb-6">
                    <CheckCircle className="w-12 h-12 text-green-500" />
                </div>
                <h3 className="text-2xl font-bold text-white mb-2">Success!</h3>
                <p className="text-green-400 mb-2">
                    {progressMessage}
                </p>
                <p className="text-gray-400 text-sm mb-8">
                    Standard: {selectedStandard}
                </p>
                
                <button 
                    onClick={resetAll}
                    className="flex items-center justify-center space-x-2 bg-[#444] hover:bg-[#555] text-white px-6 py-3 rounded-xl transition-colors mx-auto border border-gray-500"
                >
                    <RotateCcw size={18} />
                    <span>Process Another File</span>
                </button>
            </div>
          )}

          {appState === 'error' && (
            <div className="text-center py-20 px-8 animate-in shake duration-300">
                <AlertCircle className="w-16 h-16 text-red-500 mx-auto mb-4" />
                <h3 className="text-xl font-bold text-red-400 mb-2">Error</h3>
                <p className="text-gray-300 mb-6 max-w-md mx-auto">{progressMessage}</p>
                <button 
                    onClick={resetAll}
                    className="text-white underline hover:text-gray-300"
                >
                    Try Again
                </button>
            </div>
          )}
        </div>

        {(appState === 'idle' || appState === 'staged') && (
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
                        Standard ▼
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
        )}
      </div>
    </div>
  );
};

export default App;