// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useState, useCallback, useEffect, useMemo, useRef } from 'react';
import Editor, { type OnMount } from '@monaco-editor/react';
import type { editor } from 'monaco-editor';
import { registerRqlLanguage } from '../monaco/register';
import { SnippetResults } from './SnippetResults';
import { SplitPane } from './layout/SplitPane';
import type { Executor, ExecutionResult } from '../types';
import type { RdbTheme } from './Console';

export interface SnippetProps {
  executor: Executor;
  initial_code: string;
  title?: string;
  description?: string;
  className?: string;
  theme?: RdbTheme;
  monaco_theme?: string | editor.IStandaloneThemeData;
}

interface QueryResult {
  data: Record<string, unknown>[];
  error?: string;
}

export function Snippet({
  executor,
  initial_code,
  title = 'reifydb playground',
  description,
  className,
  theme = 'light',
  monaco_theme,
}: SnippetProps) {
  const [code, setCode] = useState(initial_code);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [is_executing, setIsExecuting] = useState(false);
  const [copied, setCopied] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const handleRunRef = useRef<() => void>(() => {});

  const resolvedMonacoThemeName = useMemo(() => {
    if (!monaco_theme) return undefined;
    if (typeof monaco_theme === 'string') return monaco_theme;
    return 'rdb-custom';
  }, [monaco_theme]);

  const resolvedMonacoThemeData = useMemo(() => {
    if (!monaco_theme || typeof monaco_theme === 'string') return undefined;
    return monaco_theme;
  }, [monaco_theme]);

  const resolvedTheme = resolvedMonacoThemeName ?? (theme === 'light' ? 'premium-light' : 'premium-dark');

  const lineCount = code.split('\n').length;
  const editorHeight = Math.max(lineCount * 20 + 16, 80);

  const toggleFullscreen = useCallback(() => {
    if (!containerRef.current) return;
    if (!document.fullscreenElement) {
      containerRef.current.requestFullscreen();
    } else {
      document.exitFullscreen();
    }
  }, []);

  useEffect(() => {
    const onFsChange = () => setIsFullscreen(!!document.fullscreenElement);
    document.addEventListener('fullscreenchange', onFsChange);
    return () => document.removeEventListener('fullscreenchange', onFsChange);
  }, []);

  const handleRun = useCallback(async () => {
    if (is_executing) return;
    setResult(null);
    setIsExecuting(true);
    await new Promise(r => setTimeout(r, 0));

    try {
      const res: ExecutionResult = await executor.execute(code);
      if (res.success) {
        setResult({ data: res.data ?? [] });
      } else {
        setResult({ data: [], error: res.error });
      }
    } catch (err) {
      setResult({ data: [], error: err instanceof Error ? err.message : String(err) });
    } finally {
      setIsExecuting(false);
    }
  }, [code, executor, is_executing]);

  handleRunRef.current = handleRun;

  const handleReset = useCallback(() => {
    setCode(initial_code);
    setResult(null);
  }, [initial_code]);

  const handleCopy = useCallback(async () => {
    await navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [code]);

  const handleEditorDidMount: OnMount = (editor, monaco) => {
    editorRef.current = editor;
    registerRqlLanguage(monaco);

    editor.addAction({
      id: 'run-query',
      label: 'Run Query',
      keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
      run: () => {
        handleRunRef.current();
      },
    });
  };

  const handleBeforeMount = (monaco: typeof import('monaco-editor')) => {
    registerRqlLanguage(monaco);
    if (resolvedMonacoThemeName && resolvedMonacoThemeData) {
      monaco.editor.defineTheme(resolvedMonacoThemeName, resolvedMonacoThemeData);
    }
  };

  const columns = result?.data && result.data.length > 0 ? Object.keys(result.data[0]) : [];
  const maxKeyLength = columns.length > 0 ? Math.max(...columns.map(c => c.length)) : 0;

  const content = (
    <div ref={containerRef} className={`rdb-snippet${isFullscreen ? ' rdb-snippet--fullscreen' : ''}${theme === 'light' ? ' rdb-theme-light' : ''}${className ? ` ${className}` : ''}`}>
      {/* Header */}
      <div className="rdb-snippet__header">
        <div className="rdb-snippet__title">
          <span className="rdb-snippet__title-marker">$</span> {title}
        </div>
        <div className="rdb-snippet__actions">
          <button
            onClick={toggleFullscreen}
            className="rdb-snippet__action-btn"
            title={isFullscreen ? 'Exit fullscreen' : 'Fullscreen'}
          >
            {isFullscreen ? (
              <>
                <svg className="rdb-snippet__action-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <polyline points="4 14 10 14 10 20" />
                  <polyline points="20 10 14 10 14 4" />
                  <line x1="14" y1="10" x2="21" y2="3" />
                  <line x1="3" y1="21" x2="10" y2="14" />
                </svg>
                Exit
              </>
            ) : (
              <>
                <svg className="rdb-snippet__action-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <polyline points="15 3 21 3 21 9" />
                  <polyline points="9 21 3 21 3 15" />
                  <line x1="21" y1="3" x2="14" y2="10" />
                  <line x1="3" y1="21" x2="10" y2="14" />
                </svg>
                Expand
              </>
            )}
          </button>
          <button
            onClick={handleCopy}
            className="rdb-snippet__action-btn"
            title="Copy code"
          >
            {copied ? (
              <>
                <svg className="rdb-snippet__action-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <polyline points="20 6 9 17 4 12" />
                </svg>
                Copied
              </>
            ) : (
              <>
                <svg className="rdb-snippet__action-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
                  <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
                </svg>
                Copy
              </>
            )}
          </button>
          <button
            onClick={handleReset}
            className="rdb-snippet__action-btn"
            title="Reset code"
          >
            <svg className="rdb-snippet__action-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <polyline points="1 4 1 10 7 10" />
              <path d="M3.51 15a9 9 0 1 0 2.13-9.36L1 10" />
            </svg>
            Reset
          </button>
        </div>
      </div>

      {/* Description */}
      {description && (
        <div className="rdb-snippet__description">
          <p className="rdb-snippet__description-text">
            <span className="rdb-snippet__description-marker">// </span>{description}
          </p>
        </div>
      )}

      {isFullscreen ? (
        <SplitPane
          initialSplit={50}
          top={
            <div className="rdb-snippet__editor--fullscreen">
              <Editor
                height="100%"
                language="rql"
                theme={resolvedTheme}
                value={code}
                onChange={(value) => setCode(value || '')}
                beforeMount={handleBeforeMount}
                onMount={handleEditorDidMount}
                options={{
                  minimap: { enabled: false },
                  lineNumbers: 'on',
                  glyphMargin: false,
                  folding: false,
                  lineDecorationsWidth: 16,
                  lineNumbersMinChars: 3,
                  scrollBeyondLastLine: false,
                  scrollbar: { vertical: 'auto', horizontal: 'auto' },
                  overviewRulerLanes: 0,
                  hideCursorInOverviewRuler: true,
                  overviewRulerBorder: false,
                  renderLineHighlight: 'none',
                  fontFamily: "'Inconsolata', monospace",
                  fontSize: 13,
                  padding: { top: 8, bottom: 8 },
                  wordWrap: 'on',
                  automaticLayout: true,
                }}
              />
            </div>
          }
          bottom={
            <div className="rdb-snippet__fullscreen-bottom">
              {/* Toolbar */}
              <div className="rdb-snippet__toolbar">
                <span className="rdb-snippet__hint">
                  {is_executing ? '$ running...' : '$ ctrl+enter to run'}
                </span>
                <button
                  onClick={handleRun}
                  disabled={is_executing}
                  className={`rdb-snippet__run-btn${is_executing ? ' rdb-snippet__run-btn--loading' : ''}`}
                >
                  {is_executing ? 'Running...' : 'Run'}
                </button>
              </div>

              {/* Results */}
              {result && (
                <div className="rdb-snippet__results rdb-snippet__results--fullscreen">
                  <div className="rdb-snippet__results-header">
                    <span>{result.error ? '--- error ---' : '--- output ---'}</span>
                    {result.data && !result.error && (
                      <span>({result.data.length} row{result.data.length !== 1 ? 's' : ''})</span>
                    )}
                  </div>

                  {result.error && (
                    <div className="rdb-snippet__error">
                      <pre className="rdb-snippet__error-text">ERR: {result.error}</pre>
                    </div>
                  )}

                  {result.data && result.data.length > 0 && !result.error && (
                    <SnippetResults data={result.data} columns={columns} maxKeyLength={maxKeyLength} />
                  )}

                  {result.data && result.data.length === 0 && !result.error && (
                    <div className="rdb-snippet__empty">$ 0 rows returned.</div>
                  )}
                </div>
              )}
            </div>
          }
        />
      ) : (
        <>
          {/* Editor + Results Overlay */}
          <div className="rdb-snippet__editor-wrap" style={{ height: editorHeight }}>
            <Editor
              height="100%"
              language="rql"
              theme={resolvedTheme}
              value={code}
              onChange={(value) => setCode(value || '')}
              beforeMount={handleBeforeMount}
              onMount={handleEditorDidMount}
              options={{
                minimap: { enabled: false },
                lineNumbers: 'on',
                glyphMargin: false,
                folding: false,
                lineDecorationsWidth: 16,
                lineNumbersMinChars: 3,
                scrollBeyondLastLine: false,
                scrollbar: { vertical: 'hidden', horizontal: 'hidden' },
                overviewRulerLanes: 0,
                hideCursorInOverviewRuler: true,
                overviewRulerBorder: false,
                renderLineHighlight: 'none',
                fontFamily: "'Inconsolata', monospace",
                fontSize: 13,
                padding: { top: 8, bottom: 8 },
                wordWrap: 'on',
                automaticLayout: true,
              }}
            />

            {/* Results overlay */}
            {result && (
              <div className="rdb-snippet__results-overlay">
                <div className="rdb-snippet__results-header">
                  <span>{result.error ? '--- error ---' : '--- output ---'}</span>
                  <div className="rdb-snippet__results-header-right">
                    {result.data && !result.error && (
                      <span>({result.data.length} row{result.data.length !== 1 ? 's' : ''})</span>
                    )}
                    <button
                      onClick={() => setResult(null)}
                      className="rdb-snippet__action-btn"
                      title="Close results"
                    >
                      <svg className="rdb-snippet__action-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <line x1="18" y1="6" x2="6" y2="18" />
                        <line x1="6" y1="6" x2="18" y2="18" />
                      </svg>
                    </button>
                  </div>
                </div>

                <div className="rdb-snippet__results-body">
                  {result.error && (
                    <div className="rdb-snippet__error">
                      <pre className="rdb-snippet__error-text">ERR: {result.error}</pre>
                    </div>
                  )}

                  {result.data && result.data.length > 0 && !result.error && (
                    <SnippetResults data={result.data} columns={columns} maxKeyLength={maxKeyLength} />
                  )}

                  {result.data && result.data.length === 0 && !result.error && (
                    <div className="rdb-snippet__empty">$ 0 rows returned.</div>
                  )}
                </div>
              </div>
            )}
          </div>

          {/* Toolbar */}
          <div className="rdb-snippet__toolbar">
            <span className="rdb-snippet__hint">
              {is_executing ? '$ running...' : '$ ctrl+enter to run'}
            </span>
            <button
              onClick={handleRun}
              disabled={is_executing}
              className={`rdb-snippet__run-btn${is_executing ? ' rdb-snippet__run-btn--loading' : ''}`}
            >
              {is_executing ? 'Running...' : 'Run'}
            </button>
          </div>
        </>
      )}
    </div>
  );

  return content;
}
