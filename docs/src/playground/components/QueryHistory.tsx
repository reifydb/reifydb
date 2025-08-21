import React from 'react';
import { QueryHistoryItem } from '../types';
import styles from './QueryHistory.module.css';

interface QueryHistoryProps {
  history: QueryHistoryItem[];
  onQuerySelect?: (query: string) => void;
}

export default function QueryHistory({ history, onQuerySelect }: QueryHistoryProps) {
  const formatTime = (timestamp: number) => {
    const date = new Date(timestamp);
    return date.toLocaleTimeString();
  };

  const truncateQuery = (query: string, maxLength: number = 100) => {
    const cleaned = query.replace(/\s+/g, ' ').trim();
    if (cleaned.length <= maxLength) return cleaned;
    return cleaned.substring(0, maxLength) + '...';
  };

  if (history.length === 0) {
    return (
      <div className={styles.emptyState}>
        <p>No query history yet</p>
        <p className={styles.hint}>Executed queries will appear here</p>
      </div>
    );
  }

  return (
    <div className={styles.historyContainer}>
      {history
        .slice()
        .reverse()
        .map((item) => (
          <div
            key={item.id}
            className={`${styles.historyItem} ${!item.success ? styles.error : ''}`}
            onClick={() => onQuerySelect?.(item.query)}
          >
            <div className={styles.queryHeader}>
              <span className={styles.timestamp}>{formatTime(item.timestamp)}</span>
              <span className={styles.executionTime}>{item.executionTimeMs}ms</span>
              {!item.success && <span className={styles.errorBadge}>Error</span>}
            </div>
            <div className={styles.queryText}>{truncateQuery(item.query)}</div>
            {item.error && <div className={styles.errorMessage}>{item.error}</div>}
          </div>
        ))}
    </div>
  );
}
