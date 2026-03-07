// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

interface TabBarProps {
  activeTab: string;
  tabs: { id: string; label: string }[];
  onTabChange: (id: string) => void;
}

export function TabBar({ activeTab, tabs, onTabChange }: TabBarProps) {
  return (
    <div className="rdb-tabs">
      {tabs.map((tab) => (
        <button
          key={tab.id}
          className={`rdb-tabs__tab${activeTab === tab.id ? ' rdb-tabs__tab--active' : ''}`}
          onClick={() => onTabChange(tab.id)}
        >
          {tab.label}
        </button>
      ))}
    </div>
  );
}
