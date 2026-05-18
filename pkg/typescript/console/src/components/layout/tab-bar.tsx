// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

interface TabBarProps {
  active_tab: string;
  tabs: { id: string; label: string }[];
  on_tab_change: (id: string) => void;
}

export function TabBar({ active_tab, tabs, on_tab_change }: TabBarProps) {
  return (
    <div className="rdb-tabs">
      {tabs.map((tab) => (
        <button
          key={tab.id}
          className={`rdb-tabs__tab${active_tab === tab.id ? ' rdb-tabs__tab--active' : ''}`}
          onClick={() => on_tab_change(tab.id)}
        >
          {tab.label}
        </button>
      ))}
    </div>
  );
}
