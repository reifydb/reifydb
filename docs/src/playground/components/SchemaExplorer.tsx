import React, { useState } from 'react';
import { TableInfo } from '../types';
import styles from './SchemaExplorer.module.css';

interface SchemaExplorerProps {
  schema: TableInfo[];
  onTableClick?: (tableName: string) => void;
}

export default function SchemaExplorer({ schema, onTableClick }: SchemaExplorerProps) {
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());

  const toggleTable = (tableName: string) => {
    const newExpanded = new Set(expandedTables);
    if (newExpanded.has(tableName)) {
      newExpanded.delete(tableName);
    } else {
      newExpanded.add(tableName);
    }
    setExpandedTables(newExpanded);
  };

  return (
    <div className={styles.schemaExplorer}>
      <div className={styles.header}>
        <h3>Database Schema</h3>
      </div>

      <div className={styles.tableList}>
        {schema.length === 0 ? (
          <div className={styles.emptyState}>No tables found</div>
        ) : (
          schema.map((table) => (
            <div key={table.name} className={styles.table}>
              <div className={styles.tableHeader} onClick={() => toggleTable(table.name)}>
                <span className={styles.expandIcon}>
                  {expandedTables.has(table.name) ? '▼' : '▶'}
                </span>
                <span
                  className={styles.tableName}
                  onClick={(e) => {
                    e.stopPropagation();
                    onTableClick?.(table.name);
                  }}
                >
                  {table.name}
                </span>
                {table.rowCount !== undefined && (
                  <span className={styles.rowCount}>{table.rowCount} rows</span>
                )}
              </div>

              {expandedTables.has(table.name) && (
                <div className={styles.columnList}>
                  {table.columns.map((column, index) => (
                    <div key={index} className={styles.column}>
                      <span className={styles.columnName}>{column.name}</span>
                      <span className={styles.columnType}>
                        {column.dataType}
                        {!column.nullable && ' NOT NULL'}
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          ))
        )}
      </div>
    </div>
  );
}
