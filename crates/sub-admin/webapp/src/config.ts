export const REIFYDB_CONFIG = {
  // Default WebSocket URL for local development
  DEFAULT_WS_URL: 'ws://127.0.0.1:8090',
  
  // Connection settings
  CONNECTION: {
    TIMEOUT_MS: 10000,
    RECONNECT_INTERVAL_MS: 5000,
    MAX_RECONNECT_ATTEMPTS: 3,
  },
  
  // Query settings
  QUERY: {
    MAX_HISTORY_SIZE: 100,
    DEFAULT_EXECUTION_TIMEOUT: 30000,
  },
  
  // Environment-specific URLs (can be overridden by environment variables)
  getWebSocketUrl(): string {
    // Check for environment-specific URL
    if (typeof window !== 'undefined') {
      // Browser environment
      const envUrl = (window as any).REIFYDB_WS_URL;
      if (envUrl) return envUrl;
      
      // Check if we're in production
      if (window.location.hostname !== 'localhost' && window.location.hostname !== '127.0.0.1') {
        // Use wss:// for production
        return `wss://${window.location.hostname}/ws`;
      }
    }
    
    return this.DEFAULT_WS_URL;
  },
};