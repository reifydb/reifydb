// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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
