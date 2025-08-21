import React from 'react';
import { QueryResult } from '../types';
import styles from './ResultViewer.module.css';

interface ResultViewerProps {
  result: QueryResult | null;
  error: string | null;
  isLoading: boolean;
}

export default function ResultViewer({ result, error, isLoading }: ResultViewerProps) {
  if (isLoading) {
    return (
      <div className={styles.centerContent}>
        <div className={styles.loader}>
          <div className={styles.spinner}></div>
          <p>Executing query...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className={styles.errorContainer}>
        <div className={styles.errorHeader}>
          <span className={styles.errorIcon}>⚠️</span>
          <span>Query Error</span>
        </div>
        <pre className={styles.errorMessage}>{error}</pre>
      </div>
    );
  }

  if (!result) {
    return (
      <div className={styles.centerContent}>
        <div className={styles.emptyState}>
          <p>No results to display</p>
          <p className={styles.hint}>Execute a query to see results here</p>
        </div>
      </div>
    );
  }

  if (result.rows.length === 0) {
    return (
      <div className={styles.centerContent}>
        <div className={styles.emptyState}>
          <p>Query executed successfully</p>
          <p className={styles.hint}>No rows returned</p>
          {result.rowsAffected !== undefined && (
            <p className={styles.stats}>{result.rowsAffected} rows affected</p>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className={styles.resultContainer}>
      <div className={styles.resultHeader}>
        <span className={styles.rowCount}>
          {result.rows.length} row{result.rows.length !== 1 ? 's' : ''}
        </span>
        <span className={styles.executionTime}>
          {result.executionTimeMs}ms
        </span>
      </div>
      
      <div className={styles.tableWrapper}>
        <table className={styles.resultTable}>
          <thead>
            <tr>
              {result.columns.map((column, index) => (
                <th key={index}>
                  <div className={styles.columnHeader}>
                    <span className={styles.columnName}>{column.name}</span>
                    <span className={styles.columnType}>{column.dataType}</span>
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {result.rows.map((row, rowIndex) => (
              <tr key={rowIndex}>
                {row.map((cell, cellIndex) => (
                  <td key={cellIndex}>
                    {cell === null ? (
                      <span className={styles.nullValue}>NULL</span>
                    ) : (
                      <span>{String(cell)}</span>
                    )}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}