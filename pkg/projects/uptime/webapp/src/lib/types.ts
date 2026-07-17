// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export type MonitorKind = "http" | "tcp" | "ping" | "dns";
export type MonitorStatus = "up" | "down" | "unknown";

export interface Monitor {
  id: string;
  name: string;
  kind: MonitorKind;
  target: string;
  interval_ms: number;
  timeout_ms: number;
  http_method: string | null;
  expected_status: number | null;
  keyword: string | null;
  expected_ip: string | null;
  failure_threshold: number;
  enabled: boolean;
  status: MonitorStatus;
  created_at: string;
  last_checked_at: string | null;
  consecutive_failures: number;
}

export interface MonitorInput {
  name: string;
  kind: MonitorKind;
  target: string;
  interval_ms: number;
  timeout_ms: number;
  http_method?: string;
  expected_status?: number;
  keyword?: string;
  expected_ip?: string;
  failure_threshold: number;
  enabled: boolean;
}

export interface Result {
  checked_at: string;
  success: boolean;
  response_time_ms: number | null;
  status_code: number | null;
  error: string | null;
}

export interface StatusPage {
  id: string;
  slug: string;
  title: string;
  created_at: string;
  monitor_ids: string[];
}

export interface StatusPageInput {
  slug: string;
  title: string;
  monitor_ids: string[];
}

export interface DailyUptime {
  day: string;
  total: number;
  up: number;
}

export interface MonitorDaily {
  monitor_id: string;
  daily: DailyUptime[];
}

export interface PublicStatusMonitor {
  name: string;
  status: MonitorStatus;
  uptime_24h: number | null;
  last_checked_at: string | null;
  daily: DailyUptime[];
}

export interface PublicStatus {
  title: string;
  slug: string;
  monitors: PublicStatusMonitor[];
}

export interface Me {
  id: string;
  email: string;
}
