import React from "react";
import { useState, useEffect } from "react";
import {
  FileText,
  ExternalLink,
  Calendar,
  Search,
  X,
  CheckCircle,
  XCircle,
  PieChart,
} from "lucide-react";

const DetailModal = ({ isOpen, onClose, companyName }) => {
  const [details, setDetails] = useState([]);
  const [loading, setLoading] = useState(false);
  const [stats, setStats] = useState({ y: 0, n: 0, total: 0 });

  useEffect(() => {
    if (isOpen && companyName) {
      setLoading(true);
      fetch(`http://localhost:8000/history/detail/${companyName}`)
        .then((res) => res.json())
        .then((data) => {
          const records = data.data || [];
          setDetails(records);
          const y = records.filter((r) => r.Final_YN === "Y").length;
          setStats({
            y,
            n: records.length - y,
            total: records.length,
          });
        })
        .catch((err) => console.error("Failed to load details", err))
        .finally(() => setLoading(false));
    }
  }, [isOpen, companyName]);

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm">
      <div className="bg-[#1e1e1e] w-full max-w-5xl h-[85vh] rounded-2xl border border-gray-700 shadow-2xl flex flex-col overflow-hidden">
        <div className="px-6 py-4 border-b border-gray-700 flex justify-between items-center bg-[#252525]">
          <div>
            <h3 className="text-xl font-bold text-white flex items-center gap-2">
              <FileText className="text-blue-400" size={20} />
              {companyName}
            </h3>
            <p className="text-gray-400 text-sm mt-1">
              總計: {stats.total} 題 |
              <span className="text-green-400 ml-1">已揭露: {stats.y}</span> |
              <span className="text-gray-500 ml-1">未揭露: {stats.n}</span>
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-700 rounded-full text-gray-400 hover:text-white transition-colors"
          >
            <X size={24} />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto p-6 bg-[#1a1a1a]">
          {loading ? (
            <div className="h-full flex items-center justify-center text-gray-400">
              載入中...
            </div>
          ) : (
            <>
              <div className="mb-6">
                <div className="flex justify-between items-end mb-3">
                  <h4 className="text-sm font-bold text-gray-400 uppercase tracking-wider">
                    揭露狀態總覽 (依題號)
                  </h4>
                  <span className="text-xs text-gray-500">
                    * 滑鼠懸停可查看詳細定義
                  </span>
                </div>
                <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-2.5">
                  {details.map((item, idx) => {
                    const isDisclosed = item.Final_YN === "Y";
                    return (
                      <div
                        key={idx}
                        className={`
            group relative px-3 py-2 rounded-md border flex items-center justify-between
            transition-all cursor-help shadow-sm
            ${
              isDisclosed
                ? "bg-green-500/10 border-green-500/40 hover:bg-green-500/20"
                : "bg-gray-800/50 border-gray-700 hover:bg-gray-700/80"
            }
          `}
                      >

                        <span
                          className={`text-xs font-mono font-semibold truncate mr-2 ${
                            isDisclosed ? "text-green-400" : "text-gray-400"
                          }`}
                        >
                          {item.Label}
                        </span>
                        <div
                          className={`w-2 h-2 rounded-full flex-shrink-0 ${
                            isDisclosed
                              ? "bg-green-500 shadow-[0_0_5px_rgba(34,197,94,0.6)]"
                              : "bg-gray-600"
                          }`}
                        />

                        <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-72 p-3 bg-gray-900 text-white text-xs rounded-lg border border-gray-600 shadow-xl opacity-0 group-hover:opacity-100 pointer-events-none z-20 transition-opacity">
                          <div className="font-bold mb-1 text-blue-300 text-sm border-b border-gray-700 pb-1">
                            {item.Label}
                          </div>
                          <div className="mt-1 text-gray-300">
                            狀態:{" "}
                            <span
                              className={
                                isDisclosed
                                  ? "text-green-400 font-bold"
                                  : "text-red-400 font-bold"
                              }
                            >
                              {isDisclosed ? "✅ 已揭露" : "❌ 未揭露"}
                            </span>
                          </div>

                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>

              <div className="mt-8">
                <h4 className="text-sm font-bold text-gray-400 uppercase tracking-wider mb-3">
                  詳細列表
                </h4>
                <div className="space-y-2">
                  {details.map((item, idx) => (
                    <div
                      key={idx}
                      className="flex items-start p-3 rounded-lg bg-[#252525] border border-gray-800 hover:border-gray-600 transition-colors"
                    >
                      <div className="mt-0.5 mr-3 flex-shrink-0">
                        {item.Final_YN === "Y" ? (
                          <CheckCircle size={18} className="text-green-500" />
                        ) : (
                          <XCircle size={18} className="text-gray-600" />
                        )}
                      </div>
                      <div className="flex-1">
                        <p
                          className={`text-sm ${
                            item.Final_YN === "Y"
                              ? "text-gray-200"
                              : "text-gray-500"
                          }`}
                        >
                          {item.Label}
                        </p>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
};

const HistoryPage = () => {
  const [historyData, setHistoryData] = useState([]);
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedCompany, setSelectedCompany] = useState(null);

  useEffect(() => {
    fetch("http://localhost:8000/history/summary")
      .then((res) => res.json())
      .then((data) => {
        setHistoryData(data);
      })
      .catch((err) => console.error("Failed to fetch history:", err));
  }, []);

  const filteredData = historyData.filter((item) =>
    item.Company?.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div className="pt-24 pb-12 max-w-5xl mx-auto px-6 min-h-screen">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center justify-between mb-10 gap-4">
        <div>
          <h2 className="text-3xl font-bold text-white mb-2">歷史分析報告</h2>
          <p className="text-gray-400">檢視已分析的企業 ESG 揭露狀況與達成率</p>
        </div>
        <div className="relative">
          <input
            type="text"
            placeholder="搜尋公司..."
            className="bg-[#2a2a2a] border border-gray-600 text-white text-sm rounded-full pl-10 pr-4 py-2.5 focus:outline-none focus:border-blue-500 w-64 transition-all"
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
          <Search className="text-gray-400 w-4 h-4 absolute left-3.5 top-3" />
        </div>
      </div>

      <div className="space-y-4">
        {filteredData.length === 0 ? (
          <div className="text-center text-gray-500 py-10">尚無分析資料</div>
        ) : (
          filteredData.map((item, index) => {
            const ratioPercent = Math.round(item.Disclosure_Ratio * 100);

            return (
              <div
                key={index}
                onClick={() => setSelectedCompany(item.Company)} 
                className="group bg-[#2a2a2a] border border-gray-700 hover:border-blue-500/50 rounded-2xl p-5 flex flex-col sm:flex-row items-center justify-between cursor-pointer transition-all shadow-lg hover:shadow-blue-900/10 hover:-translate-y-1"
              >
                <div className="flex items-center space-x-5 w-full sm:w-auto mb-4 sm:mb-0">
                  <div className="bg-gradient-to-br from-blue-900/30 to-purple-900/30 p-4 rounded-xl border border-white/5 text-blue-400 group-hover:scale-105 transition-transform">
                    <FileText size={28} />
                  </div>
                  <div>
                    <h4 className="font-bold text-lg text-white group-hover:text-blue-300 transition-colors truncate max-w-[250px] sm:max-w-md">
                      {item.Company}
                    </h4>
                    <div className="flex items-center gap-3 mt-1.5">
                      <span className="flex items-center text-xs text-gray-400">
                        <Calendar size={12} className="mr-1" />
                        {item.Last_Updated?.split(" ")[0] || "Unknown Date"}
                      </span>
                      <span className="px-2 py-0.5 rounded text-[10px] bg-gray-700 text-gray-300 font-medium">
                        TCFD
                      </span>
                    </div>
                  </div>
                </div>

                <div className="flex items-center space-x-6 w-full sm:w-auto justify-between sm:justify-end border-t sm:border-t-0 border-gray-700 pt-4 sm:pt-0">
                  <div className="flex flex-col items-end mr-2">
                    <div className="flex items-baseline gap-1">
                      <span
                        className={`text-2xl font-bold ${
                          ratioPercent >= 80
                            ? "text-green-400"
                            : ratioPercent >= 50
                            ? "text-yellow-400"
                            : "text-red-400"
                        }`}
                      >
                        {ratioPercent}%
                      </span>
                      <span className="text-xs text-gray-500 font-medium uppercase">
                        揭露率
                      </span>
                    </div>
                    <div className="w-32 h-1.5 bg-gray-700 rounded-full mt-1 overflow-hidden">
                      <div
                        className={`h-full rounded-full ${
                          ratioPercent >= 80
                            ? "bg-green-500"
                            : ratioPercent >= 50
                            ? "bg-yellow-500"
                            : "bg-red-500"
                        }`}
                        style={{ width: `${ratioPercent}%` }}
                      />
                    </div>
                    <div className="text-[10px] text-gray-500 mt-1">
                      {item.Y_Labels} / {item.Total_Labels} 項目已完成
                    </div>
                  </div>

                  <div className="p-2.5 bg-gray-700/30 rounded-full text-gray-400 group-hover:bg-blue-600 group-hover:text-white transition-all">
                    <ExternalLink size={18} />
                  </div>
                </div>
              </div>
            );
          })
        )}
      </div>

      <DetailModal
        isOpen={!!selectedCompany}
        onClose={() => setSelectedCompany(null)}
        companyName={selectedCompany}
      />
    </div>
  );
};

export default HistoryPage;
