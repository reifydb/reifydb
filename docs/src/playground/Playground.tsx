import React, { useState, useCallback, useRef, useEffect } from 'react';
import { Panel, PanelGroup, PanelResizeHandle } from 'react-resizable-panels';
import CodeEditor from './components/CodeEditor';
import ResultViewer from './components/ResultViewer';
import SchemaExplorer from './components/SchemaExplorer';
import QueryHistory from './components/QueryHistory';
import Toolbar from './components/Toolbar';
import { usePlaygroundConnection } from './hooks/usePlaygroundConnection';
import { QueryResult, TableInfo } from './types';
import styles from './Playground.module.css';

const DEFAULT_QUERY = `-- Welcome to ReifyDB Playground!
-- Try running this query:
SELECT * FROM users LIMIT 10;`;

export default function Playground() {
  const [query, setQuery] = useState(DEFAULT_QUERY);
  const [isExecuting, setIsExecuting] = useState(false);
  const [activeTab, setActiveTab] = useState<'result' | 'history'>('result');
  const [editorTheme, setEditorTheme] = useState('vs'); // Default to light theme
  const editorRef = useRef<any>(null);

  const {
    connected,
    result,
    error,
    schema,
    history,
    executeQuery,
    resetDatabase,
    loadExample,
  } = usePlaygroundConnection();

  const handleExecute = useCallback(async () => {
    if (!query.trim() || isExecuting) return;
    
    setIsExecuting(true);
    setActiveTab('result');
    
    try {
      await executeQuery(query);
    } finally {
      setIsExecuting(false);
    }
  }, [query, isExecuting, executeQuery]);

  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      handleExecute();
    }
  }, [handleExecute]);

  React.useEffect(() => {
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [handleKeyDown]);

  // Detect theme changes
  useEffect(() => {
    const detectTheme = () => {
      // Docusaurus adds data-theme attribute to the html element
      const htmlTheme = document.documentElement.getAttribute('data-theme');
      const isDark = htmlTheme === 'dark';
      
      // Update editor theme
      setEditorTheme(isDark ? 'vs-dark' : 'vs');
    };

    // Initial detection
    detectTheme();

    // Watch for theme changes
    const observer = new MutationObserver(detectTheme);
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['data-theme']
    });

    return () => observer.disconnect();
  }, []);

  const handleExampleLoad = useCallback((exampleQuery: string) => {
    setQuery(exampleQuery);
    if (editorRef.current) {
      editorRef.current.setValue(exampleQuery);
    }
  }, []);

  return (
    <div className={styles.playground}>
      <Toolbar
        connected={connected}
        isExecuting={isExecuting}
        onExecute={handleExecute}
        onReset={resetDatabase}
        onLoadExample={loadExample}
      />
      
      <div className={styles.content}>
        <PanelGroup direction="horizontal">
          <Panel defaultSize={20} minSize={15} maxSize={30}>
            <SchemaExplorer
              schema={schema}
              onTableClick={(tableName) => {
                const insertQuery = `SELECT * FROM ${tableName} LIMIT 10;`;
                setQuery(insertQuery);
              }}
            />
          </Panel>
          
          <PanelResizeHandle className={styles.resizeHandle} />
          
          <Panel defaultSize={80}>
            <PanelGroup direction="vertical">
              <Panel defaultSize={40} minSize={20}>
                <CodeEditor
                  ref={editorRef}
                  value={query}
                  onChange={setQuery}
                  onExecute={handleExecute}
                  language="sql"
                  theme={editorTheme}
                />
              </Panel>
              
              <PanelResizeHandle className={styles.resizeHandle} />
              
              <Panel defaultSize={60} minSize={20}>
                <div className={styles.resultPanel}>
                  <div className={styles.tabs}>
                    <button
                      className={`${styles.tab} ${activeTab === 'result' ? styles.active : ''}`}
                      onClick={() => setActiveTab('result')}
                    >
                      Result
                    </button>
                    <button
                      className={`${styles.tab} ${activeTab === 'history' ? styles.active : ''}`}
                      onClick={() => setActiveTab('history')}
                    >
                      History
                    </button>
                  </div>
                  
                  <div className={styles.tabContent}>
                    {activeTab === 'result' ? (
                      <ResultViewer
                        result={result}
                        error={error}
                        isLoading={isExecuting}
                      />
                    ) : (
                      <QueryHistory
                        history={history}
                        onQuerySelect={handleExampleLoad}
                      />
                    )}
                  </div>
                </div>
              </Panel>
            </PanelGroup>
          </Panel>
        </PanelGroup>
      </div>
    </div>
  );
}