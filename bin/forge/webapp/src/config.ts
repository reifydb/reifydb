export const FORGE_CONFIG = {
  DEFAULT_WS_URL: 'ws://127.0.0.1:8091',

  CONNECTION: {
    TIMEOUT_MS: 10000,
    RECONNECT_INTERVAL_MS: 5000,
  },

  getWebSocketUrl(): string {
    if (typeof window !== 'undefined') {
      const envUrl = (window as unknown as Record<string, unknown>).FORGE_WS_URL;
      if (typeof envUrl === 'string') return envUrl;

      if (window.location.hostname !== 'localhost' && window.location.hostname !== '127.0.0.1') {
        return `wss://${window.location.hostname}/ws`;
      }
    }
    return this.DEFAULT_WS_URL;
  },
}
