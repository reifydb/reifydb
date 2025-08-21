export interface ColumnInfo {
  name: string;
  dataType: string;
  nullable: boolean;
}

export interface QueryResult {
  columns: ColumnInfo[];
  rows: any[][];
  executionTimeMs: number;
  rowsAffected?: number;
}

export interface TableInfo {
  name: string;
  columns: ColumnInfo[];
  indexes: string[];
  rowCount?: number;
}

export interface QueryHistoryItem {
  id: string;
  query: string;
  timestamp: number;
  executionTimeMs: number;
  success: boolean;
  error?: string;
}

export interface PlaygroundRequest {
  type: 'execute' | 'getSchema' | 'getHistory' | 'loadExample' | 'reset';
  id: string;
  query?: string;
  exampleId?: string;
}

export interface PlaygroundResponse {
  type: 'queryResult' | 'error' | 'schema' | 'history' | 'exampleLoaded' | 'resetComplete';
  id: string;
  result?: QueryResult;
  error?: string;
  schema?: TableInfo[];
  history?: QueryHistoryItem[];
  example?: {
    title: string;
    description: string;
    queries: string[];
  };
}