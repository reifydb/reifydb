// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { useState, useCallback, useEffect, useMemo, useRef } from 'react';
import Editor, { type OnMount } from '@monaco-editor/react';
import type { editor } from 'monaco-editor';
import { registerRqlLanguage } from '../monaco/register';
import { SnippetResults } from './snippet-results';
import { SplitPane } from './layout/split-pane';
import type { Executor, ExecutionResult } from '../types';
import type { RdbTheme } from './console';

export interface SnippetProps {
  executor: Executor;
  initialCode: string;
  title?: string;
  description?: string;
  className?: string;
  theme?: RdbTheme;
  monacoTheme?: string | editor.IStandaloneThemeData;
  readonly?: boolean;
}

interface QueryResult {
  data: Record<string, unknown>[];
  error?: string;
}

export function Snippet({
  executor,
  initialCode,
  title = 'reifydb playground',
  description,
  className,
  theme = 'light',
  monacoTheme,
  readonly = false,
}: SnippetProps) {
  const [code, setCode] = useState(initialCode);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [isExecuting, setIsExecuting] = useState(false);
  const [isInitializing, setIsInitializing] = useState(false);
  const [copied, setCopied] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const handleRunRef = useRef<() => void>(() => {});

  const resolvedMonacoThemeName = useMemo(() => {
    if (!monacoTheme) return undefined;
    if (typeof monacoTheme === 'string') return monacoTheme;
    return 'rdb-custom';
  }, [monacoTheme]);

  const resolvedMonacoThemeData = useMemo(() => {
    if (!monacoTheme || typeof monacoTheme === 'string') return undefined;
    return monacoTheme;
  }, [monacoTheme]);

  const resolvedTheme = resolvedMonacoThemeName ?? (theme === 'light' ? 'premium-light' : 'premium-dark');

  const [editorHeight, setEditorHeight] = useState(() => Math.max(initialCode.split('\n').length * 20 + 16, 80));

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
    if (isExecuting) return;
    setResult(null);
    setIsInitializing(executor.isReady ? !executor.isReady() : false);
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
      setIsInitializing(false);
    }
  }, [code, executor, isExecuting]);

  handleRunRef.current = handleRun;

  const handleReset = useCallback(() => {
    setCode(initialCode);
    setResult(null);
  }, [initialCode]);

  const handleCopy = useCallback(async () => {
    await navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [code]);

  const handleEditorDidMount: OnMount = (editor, monaco) => {
    editorRef.current = editor;
    registerRqlLanguage(monaco);

    setEditorHeight(editor.getContentHeight());
    editor.onDidContentSizeChange(() => setEditorHeight(editor.getContentHeight()));

    if (!readonly) {
      editor.addAction({
        id: 'run-query',
        label: 'Run Query',
        keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
        run: () => {
          handleRunRef.current();
        },
      });
    }
  };

  const handleBeforeMount = (monaco: typeof import('monaco-editor')) => {
    registerRqlLanguage(monaco);
    if (resolvedMonacoThemeName && resolvedMonacoThemeData) {
      monaco.editor.defineTheme(resolvedMonacoThemeName, resolvedMonacoThemeData);
    }
  };

  const columns = result?.data && result.data.length > 0 ? Object.keys(result.data[0]) : [];

  const runHint = isInitializing ? '$ downloading engine...' : isExecuting ? '$ running...' : '$ ctrl+enter to run';
  const runLabel = isInitializing ? 'Downloading...' : isExecuting ? 'Running...' : 'Run';
  const runButton = (
    <button
      onClick={handleRun}
      disabled={isExecuting}
      className={`rdb-snippet__run-btn${isExecuting ? ' rdb-snippet__run-btn--loading' : ''}`}
    >
      {isExecuting ? (
        <span className="rdb-snippet__spinner" />
      ) : (
        <svg className="rdb-snippet__run-icon" viewBox="0 0 24 24" fill="currentColor">
          <polygon points="6 3 20 12 6 21" />
        </svg>
      )}
      {runLabel}
    </button>
  );

  const content = (
    <div ref={containerRef} className={`rdb-snippet${isFullscreen ? ' rdb-snippet--fullscreen' : ''}${theme === 'light' ? ' rdb-theme-light' : ''}${className ? ` ${className}` : ''}`}>
      <div className="rdb-snippet__header">
        <div className="rdb-snippet__title">
          <span className="rdb-snippet__title-marker">$</span> {title}
        </div>
        <div className="rdb-snippet__actions">
          {!readonly && (
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
          )}
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
          {!readonly && (
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
          )}
        </div>
      </div>

      {description && (
        <div className="rdb-snippet__description">
          <p className="rdb-snippet__description-text">
            <span className="rdb-snippet__description-marker">// </span>{description}
          </p>
        </div>
      )}

      {readonly ? (
        <div className="rdb-snippet__editor-wrap" style={{ height: editorHeight }}>
          <Editor
            height="100%"
            language="rql"
            theme={resolvedTheme}
            value={code}
            beforeMount={handleBeforeMount}
            onMount={handleEditorDidMount}
            options={{
              readOnly: true,
              minimap: { enabled: false },
              lineNumbers: 'on',
              glyphMargin: false,
              folding: false,
              lineDecorationsWidth: 16,
              lineNumbersMinChars: 3,
              scrollBeyondLastLine: false,
              scrollbar: { vertical: 'hidden', horizontal: 'hidden', alwaysConsumeMouseWheel: false },
              overviewRulerLanes: 0,
              hideCursorInOverviewRuler: true,
              overviewRulerBorder: false,
              renderLineHighlight: 'none',
              fontFamily: "'JetBrains Mono Variable', monospace",
              fontSize: 13,
              padding: { top: 8, bottom: 8 },
              wordWrap: 'on',
              automaticLayout: true,
            }}
          />
        </div>
      ) : isFullscreen ? (
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
                  fontFamily: "'JetBrains Mono Variable', monospace",
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
              <div className="rdb-snippet__toolbar">
                <span className="rdb-snippet__hint">{runHint}</span>
                {runButton}
              </div>

              {result && (
                <div className="rdb-snippet__results rdb-snippet__results--fullscreen">
                  {result.error && (
                    <div className="rdb-snippet__error">
                      <pre className="rdb-snippet__error-text">ERR: {result.error}</pre>
                    </div>
                  )}

                  {result.data && result.data.length > 0 && !result.error && (
                    <SnippetResults data={result.data} columns={columns} />
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
                scrollbar: { vertical: 'hidden', horizontal: 'hidden', alwaysConsumeMouseWheel: false },
                overviewRulerLanes: 0,
                hideCursorInOverviewRuler: true,
                overviewRulerBorder: false,
                renderLineHighlight: 'none',
                fontFamily: "'JetBrains Mono Variable', monospace",
                fontSize: 13,
                padding: { top: 8, bottom: 8 },
                wordWrap: 'on',
                automaticLayout: true,
              }}
            />
          </div>

          <div className="rdb-snippet__toolbar">
            <span className="rdb-snippet__hint">{runHint}</span>
            {runButton}
          </div>

          {result && (
            <div className="rdb-snippet__results-panel">
              <div className="rdb-snippet__results-body">
                {result.error && (
                  <div className="rdb-snippet__error">
                    <pre className="rdb-snippet__error-text">ERR: {result.error}</pre>
                  </div>
                )}

                {result.data && result.data.length > 0 && !result.error && (
                  <SnippetResults data={result.data} columns={columns} />
                )}

                {result.data && result.data.length === 0 && !result.error && (
                  <div className="rdb-snippet__empty">$ 0 rows returned.</div>
                )}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );

  return content;
}
