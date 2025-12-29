import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import { Upload, History, LayoutDashboard } from 'lucide-react';

const Header = () => {
  const location = useLocation();
  
  const navItems = [
    { path: '/', label: '分析報告', icon: <Upload size={18} /> },
    { path: '/history', label: '歷史紀錄', icon: <History size={18} /> },
  ];

  return (
    <header className="fixed top-0 left-0 right-0 bg-[#3a3a3a]/80 backdrop-blur-md border-b border-gray-600 z-50">
      <div className="max-w-6xl mx-auto px-6 h-16 flex items-center justify-between">
        <div className="flex items-center space-x-2">
          <div className="bg-blue-600 p-1.5 rounded-lg text-white">
            <LayoutDashboard size={20} />
          </div>
          <span className="text-xl font-bold tracking-tight text-white">Reports AI Analyzer</span>
        </div>
        
        <nav className="flex space-x-1">
          {navItems.map((item) => (
            <Link 
              key={item.path}
              to={item.path} 
              className={`px-4 py-2 rounded-lg flex items-center space-x-2 transition-colors ${
                location.pathname === item.path 
                  ? 'bg-blue-600 text-white shadow-lg' 
                  : 'text-gray-400 hover:bg-gray-700 hover:text-white'
              }`}
            >
              {item.icon}
              <span className="font-medium">{item.label}</span>
            </Link>
          ))}
        </nav>
      </div>
    </header>
  );
};

export default Header;