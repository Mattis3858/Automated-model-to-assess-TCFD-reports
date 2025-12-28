import React from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import Header from './components/Header';
import UploadPage from './pages/UploadPage';
import HistoryPage from './pages/HistoryPage';

const App = () => {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-[#4B4B4B] text-white font-sans">
        <Header />
        
        <main>
          <Routes>
            <Route path="/" element={<UploadPage />} />
            <Route path="/history" element={<HistoryPage />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
};

export default App;