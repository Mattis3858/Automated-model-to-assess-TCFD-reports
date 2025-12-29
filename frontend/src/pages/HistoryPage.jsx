import React from 'react';
import { FileText, ExternalLink, Calendar, Search } from 'lucide-react';

const HistoryPage = () => {
  const historyData = [
    { id: '1', name: '2801_彰化銀行_2021_TCFD_報告書', date: '2025-12-25', standard: 'TCFD', status: 'Completed' },
    { id: '2', name: '2801_彰化銀行_2020_TCFD_報告書', date: '2025-12-25', standard: 'TCFD', status: 'Completed' },
    { id: '3', name: '2801_彰化銀行_2022_TCFD_報告書', date: '2025-12-28', standard: 'TCFD', status: 'Completed' },
  ];

  return (
    <div className="pt-24 pb-12 max-w-4xl mx-auto px-6">
      <div className="flex items-center justify-between mb-8">
        <div>
          <h2 className="text-3xl font-bold text-white mb-2">歷史分析報告</h2>
          <p className="text-gray-400">管理並檢視過去所有已處理完成的 ESG 分析文件</p>
        </div>
        <div className="bg-[#3a3a3a] p-2 rounded-full border border-gray-600">
          <Search className="text-gray-400 w-5 h-5" />
        </div>
      </div>

      <div className="space-y-4">
        {historyData.map((item) => (
          <div 
            key={item.id} 
            className="group bg-[#3a3a3a] border border-gray-600 rounded-2xl p-5 flex items-center justify-between hover:bg-[#444] hover:border-blue-500/50 transition-all cursor-pointer shadow-lg"
          >
            <div className="flex items-center space-x-5">
              <div className="bg-red-500/10 p-4 rounded-xl text-red-400 group-hover:scale-110 transition-transform">
                <FileText size={28} />
              </div>
              <div>
                <h4 className="font-semibold text-lg text-white group-hover:text-blue-300 transition-colors">
                  {item.name}
                </h4>
                <div className="flex items-center space-x-4 mt-1 text-sm text-gray-400">
                  <span className="flex items-center gap-1.5">
                    <Calendar size={14} /> {item.date}
                  </span>
                  <span className="bg-[#4b4b4b] px-2.5 py-0.5 rounded text-xs text-blue-300 font-bold uppercase tracking-wider">
                    {item.standard}
                  </span>
                </div>
              </div>
            </div>
            
            <div className="flex items-center space-x-4">
              <div className="text-right hidden sm:block">
                <p className="text-[10px] text-gray-500 uppercase font-bold tracking-widest">Status</p>
                <p className="text-sm font-medium text-green-400">{item.status}</p>
              </div>
              <div className="p-3 bg-gray-700/50 rounded-full text-gray-400 group-hover:text-white group-hover:bg-blue-600 transition-all">
                <ExternalLink size={20} />
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default HistoryPage;