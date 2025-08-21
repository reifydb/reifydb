import { useState, useEffect, useCallback } from 'react';
import { QueryResult, TableInfo, QueryHistoryItem } from '../types';

// Mock data for demonstration - will be replaced with WebSocket connection
const MOCK_SCHEMA: TableInfo[] = [
  {
    name: 'users',
    columns: [
      { name: 'id', dataType: 'INTEGER', nullable: false },
      { name: 'name', dataType: 'TEXT', nullable: false },
      { name: 'email', dataType: 'TEXT', nullable: true },
      { name: 'created_at', dataType: 'TIMESTAMP', nullable: true },
    ],
    indexes: ['PRIMARY KEY (id)', 'UNIQUE (email)'],
    rowCount: 3,
  },
  {
    name: 'posts',
    columns: [
      { name: 'id', dataType: 'INTEGER', nullable: false },
      { name: 'user_id', dataType: 'INTEGER', nullable: true },
      { name: 'title', dataType: 'TEXT', nullable: false },
      { name: 'content', dataType: 'TEXT', nullable: true },
      { name: 'published', dataType: 'BOOLEAN', nullable: true },
      { name: 'created_at', dataType: 'TIMESTAMP', nullable: true },
    ],
    indexes: ['PRIMARY KEY (id)', 'FOREIGN KEY (user_id) REFERENCES users(id)'],
    rowCount: 4,
  },
];

const MOCK_USERS_DATA = [
  [1, 'Alice Johnson', 'alice@example.com', '2024-01-15 10:30:00'],
  [2, 'Bob Smith', 'bob@example.com', '2024-01-16 14:20:00'],
  [3, 'Charlie Brown', 'charlie@example.com', '2024-01-17 09:15:00'],
];

const MOCK_POSTS_DATA = [
  [
    1,
    1,
    'Getting Started with ReifyDB',
    'ReifyDB is a modern database...',
    true,
    '2024-01-20 11:00:00',
  ],
  [2, 1, 'Advanced Query Optimization', 'In this post we explore...', true, '2024-01-22 15:30:00'],
  [3, 2, 'Building Real-time Apps', 'Learn how to build...', true, '2024-01-23 10:45:00'],
  [4, 3, 'Draft Post', 'This is a work in progress...', false, '2024-01-24 16:20:00'],
];

export function usePlaygroundConnection() {
  const [connected] = useState(true);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [schema, setSchema] = useState<TableInfo[]>(MOCK_SCHEMA);
  const [history, setHistory] = useState<QueryHistoryItem[]>([]);

  // Mock execute query for demonstration
  const executeQuery = useCallback(async (query: string) => {
    const startTime = Date.now();
    setError(null);

    try {
      // Simulate query execution
      await new Promise((resolve) => setTimeout(resolve, 300));

      const queryLower = query.toLowerCase().trim();

      // Mock SELECT queries
      if (queryLower.startsWith('select')) {
        let mockResult: QueryResult;

        if (queryLower.includes('from users')) {
          mockResult = {
            columns: MOCK_SCHEMA[0].columns,
            rows: MOCK_USERS_DATA,
            executionTimeMs: Date.now() - startTime,
          };
        } else if (queryLower.includes('from posts')) {
          mockResult = {
            columns: MOCK_SCHEMA[1].columns,
            rows: MOCK_POSTS_DATA,
            executionTimeMs: Date.now() - startTime,
          };
        } else if (queryLower.includes('join')) {
          mockResult = {
            columns: [
              { name: 'name', dataType: 'TEXT', nullable: false },
              { name: 'title', dataType: 'TEXT', nullable: false },
            ],
            rows: [
              ['Alice Johnson', 'Getting Started with ReifyDB'],
              ['Alice Johnson', 'Advanced Query Optimization'],
              ['Bob Smith', 'Building Real-time Apps'],
            ],
            executionTimeMs: Date.now() - startTime,
          };
        } else {
          mockResult = {
            columns: [],
            rows: [],
            executionTimeMs: Date.now() - startTime,
          };
        }

        setResult(mockResult);

        // Add to history
        const historyItem: QueryHistoryItem = {
          id: Date.now().toString(),
          query,
          timestamp: Date.now(),
          executionTimeMs: mockResult.executionTimeMs,
          success: true,
        };
        setHistory((prev) => [...prev, historyItem]);
      }
      // Mock INSERT/UPDATE/DELETE
      else if (
        queryLower.startsWith('insert') ||
        queryLower.startsWith('update') ||
        queryLower.startsWith('delete')
      ) {
        const mockResult: QueryResult = {
          columns: [],
          rows: [],
          executionTimeMs: Date.now() - startTime,
          rowsAffected: 1,
        };

        setResult(mockResult);

        // Add to history
        const historyItem: QueryHistoryItem = {
          id: Date.now().toString(),
          query,
          timestamp: Date.now(),
          executionTimeMs: mockResult.executionTimeMs,
          success: true,
        };
        setHistory((prev) => [...prev, historyItem]);
      }
      // Mock CREATE/DROP/ALTER
      else if (
        queryLower.startsWith('create') ||
        queryLower.startsWith('drop') ||
        queryLower.startsWith('alter')
      ) {
        setError('DDL operations are not allowed in the playground');

        // Add to history
        const historyItem: QueryHistoryItem = {
          id: Date.now().toString(),
          query,
          timestamp: Date.now(),
          executionTimeMs: Date.now() - startTime,
          success: false,
          error: 'DDL operations are not allowed in the playground',
        };
        setHistory((prev) => [...prev, historyItem]);
      } else {
        setError('Unsupported query type');

        // Add to history
        const historyItem: QueryHistoryItem = {
          id: Date.now().toString(),
          query,
          timestamp: Date.now(),
          executionTimeMs: Date.now() - startTime,
          success: false,
          error: 'Unsupported query type',
        };
        setHistory((prev) => [...prev, historyItem]);
      }
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Unknown error';
      setError(errorMessage);

      // Add to history
      const historyItem: QueryHistoryItem = {
        id: Date.now().toString(),
        query,
        timestamp: Date.now(),
        executionTimeMs: Date.now() - startTime,
        success: false,
        error: errorMessage,
      };
      setHistory((prev) => [...prev, historyItem]);
    }
  }, []);

  const resetDatabase = useCallback(() => {
    setResult(null);
    setError(null);
    setHistory([]);
    setSchema(MOCK_SCHEMA);
  }, []);

  const loadExample = useCallback((exampleQuery: string) => {
    // This would be handled by the parent component
    return exampleQuery;
  }, []);

  // WebSocket connection setup (for future implementation)
  useEffect(() => {
    // TODO: Implement WebSocket connection when backend is ready
    // const ws = new WebSocket('ws://localhost:8080/playground');
    // wsRef.current = ws;
    //
    // ws.onopen = () => setConnected(true);
    // ws.onclose = () => setConnected(false);
    // ws.onerror = () => setConnected(false);
    // ws.onmessage = (event) => {
    //   const response = JSON.parse(event.data);
    //   // Handle response
    // };
    //
    // return () => {
    //   ws.close();
    // };
  }, []);

  return {
    connected,
    result,
    error,
    schema,
    history,
    executeQuery,
    resetDatabase,
    loadExample,
  };
}
