import React, { useState } from 'react';
import styles from './Toolbar.module.css';

interface ToolbarProps {
  connected: boolean;
  isExecuting: boolean;
  onExecute: () => void;
  onReset: () => void;
  onLoadExample: (exampleId: string) => void;
}

const EXAMPLES = [
  { id: 'basic-select', name: 'Basic SELECT', query: 'SELECT * FROM users LIMIT 10;' },
  {
    id: 'join',
    name: 'JOIN Example',
    query: 'SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id;',
  },
  {
    id: 'aggregate',
    name: 'Aggregation',
    query: 'SELECT user_id, COUNT(*) as post_count FROM posts GROUP BY user_id;',
  },
  {
    id: 'insert',
    name: 'INSERT Data',
    query: "INSERT INTO users (name, email) VALUES ('New User', 'new@example.com');",
  },
  {
    id: 'update',
    name: 'UPDATE Data',
    query: "UPDATE posts SET published = true WHERE title LIKE '%ReifyDB%';",
  },
];

export default function Toolbar({
  connected,
  isExecuting,
  onExecute,
  onReset,
  onLoadExample,
}: ToolbarProps) {
  const [showExamples, setShowExamples] = useState(false);

  return (
    <div className={styles.toolbar}>
      <div className={styles.toolbarLeft}>
        <button
          className={`${styles.button} ${styles.primaryButton}`}
          onClick={onExecute}
          disabled={!connected || isExecuting}
        >
          {isExecuting ? 'Executing...' : 'Run Query'}
        </button>

        <div className={styles.dropdown}>
          <button className={styles.button} onClick={() => setShowExamples(!showExamples)}>
            Examples ▼
          </button>
          {showExamples && (
            <div className={styles.dropdownMenu}>
              {EXAMPLES.map((example) => (
                <button
                  key={example.id}
                  className={styles.dropdownItem}
                  onClick={() => {
                    onLoadExample(example.query);
                    setShowExamples(false);
                  }}
                >
                  {example.name}
                </button>
              ))}
            </div>
          )}
        </div>

        <button className={styles.button} onClick={onReset} disabled={!connected}>
          Reset Database
        </button>
      </div>

      <div className={styles.toolbarRight}>
        <div className={styles.connectionStatus}>
          <span
            className={`${styles.statusDot} ${connected ? styles.connected : styles.disconnected}`}
          />
          <span>{connected ? 'Connected' : 'Disconnected'}</span>
        </div>
      </div>
    </div>
  );
}
