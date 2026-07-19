// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

interface ConfigOverrides {
  UPTIME_API_URL?: string;
  REIFYDB_AUTH_URL?: string;
  REIFYDB_WS_URL?: string;
}

function overrides(): ConfigOverrides {
  if (typeof window === 'undefined') return {};
  return window as ConfigOverrides;
}

export const UPTIME_CONFIG = {
  apiBase(): string {
    return overrides().UPTIME_API_URL ?? '/api';
  },
  authUrl(): string {
    return overrides().REIFYDB_AUTH_URL ?? '/db';
  },
  wsUrl(): string {
    const override = overrides().REIFYDB_WS_URL;
    if (override) return override;
    const proto = window.location.protocol === 'https:' ? 'wss' : 'ws';
    return `${proto}://${window.location.hostname}:8091`;
  },
  storageNamespace: 'reifydb.uptime',
  sessionTtlSeconds: 86400,
};
