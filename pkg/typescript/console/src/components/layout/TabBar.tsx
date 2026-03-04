// SPDX-License-Identifier: AGPL-3.0-or-later
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
