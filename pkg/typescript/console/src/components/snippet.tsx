// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useState, useCallback, useEffect, useMemo, useRef } from 'react';
import Editor, { type OnMount } from '@monaco-editor/react';
import type { editor } from 'monaco-editor';
import { register_rql_language } from '../monaco/register';
import { SnippetResults } from './snippet-results';
import { SplitPane } from './layout/split-pane';
import type { Executor, ExecutionResult } from '../types';
import type { RdbTheme } from './console';

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
  const [is_executing, set_is_executing] = useState(false);
  const [copied, setCopied] = useState(false);
  const [is_fullscreen, set_is_fullscreen] = useState(false);
  const editor_ref = useRef<editor.IStandaloneCodeEditor | null>(null);
  const container_ref = useRef<HTMLDivElement | null>(null);
  const handle_run_ref = useRef<() => void>(() => {});

  const resolved_monaco_theme_name = useMemo(() => {
    if (!monaco_theme) return undefined;
    if (typeof monaco_theme === 'string') return monaco_theme;
    return 'rdb-custom';
  }, [monaco_theme]);

  const resolved_monaco_theme_data = useMemo(() => {
    if (!monaco_theme || typeof monaco_theme === 'string') return undefined;
    return monaco_theme;
  }, [monaco_theme]);

  const resolved_theme = resolved_monaco_theme_name ?? (theme === 'light' ? 'premium-light' : 'premium-dark');

  const line_count = code.split('\n').length;
  const editor_height = Math.max(line_count * 20 + 16, 80);

  const toggle_fullscreen = useCallback(() => {
    if (!container_ref.current) return;
    if (!document.fullscreenElement) {
      container_ref.current.requestFullscreen();
    } else {
      document.exitFullscreen();
    }
  }, []);

  useEffect(() => {
    const on_fs_change = () => set_is_fullscreen(!!document.fullscreenElement);
    document.addEventListener('fullscreenchange', on_fs_change);
    return () => document.removeEventListener('fullscreenchange', on_fs_change);
  }, []);

  const handle_run = useCallback(async () => {
    if (is_executing) return;
    setResult(null);
    set_is_executing(true);
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
      set_is_executing(false);
    }
  }, [code, executor, is_executing]);

  handle_run_ref.current = handle_run;

  const handle_reset = useCallback(() => {
    setCode(initial_code);
    setResult(null);
  }, [initial_code]);

  const handle_copy = useCallback(async () => {
    await navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [code]);

  const handle_editor_did_mount: OnMount = (editor, monaco) => {
    editor_ref.current = editor;
    register_rql_language(monaco);

    editor.addAction({
      id: 'run-query',
      label: 'Run Query',
      keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
      run: () => {
        handle_run_ref.current();
      },
    });
  };

  const handle_before_mount = (monaco: typeof import('monaco-editor')) => {
    register_rql_language(monaco);
    if (resolved_monaco_theme_name && resolved_monaco_theme_data) {
      monaco.editor.defineTheme(resolved_monaco_theme_name, resolved_monaco_theme_data);
    }
  };

  const columns = result?.data && result.data.length > 0 ? Object.keys(result.data[0]) : [];
  const max_key_length = columns.length > 0 ? Math.max(...columns.map(c => c.length)) : 0;

  const content = (
    <div ref={container_ref} className={`rdb-snippet${is_fullscreen ? ' rdb-snippet--fullscreen' : ''}${theme === 'light' ? ' rdb-theme-light' : ''}${className ? ` ${className}` : ''}`}>
      {/* Header */}
      <div className="rdb-snippet__header">
        <div className="rdb-snippet__title">
          <span className="rdb-snippet__title-marker">$</span> {title}
        </div>
        <div className="rdb-snippet__actions">
          <button
            onClick={toggle_fullscreen}
            className="rdb-snippet__action-btn"
            title={is_fullscreen ? 'Exit fullscreen' : 'Fullscreen'}
          >
            {is_fullscreen ? (
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
            onClick={handle_copy}
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
            onClick={handle_reset}
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

      {is_fullscreen ? (
        <SplitPane
          initial_split={50}
          top={
            <div className="rdb-snippet__editor--fullscreen">
              <Editor
                height="100%"
                language="rql"
                theme={resolved_theme}
                value={code}
                onChange={(value) => setCode(value || '')}
                beforeMount={handle_before_mount}
                onMount={handle_editor_did_mount}
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
                  onClick={handle_run}
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
                    <SnippetResults data={result.data} columns={columns} max_key_length={max_key_length} />
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
          <div className="rdb-snippet__editor-wrap" style={{ height: editor_height }}>
            <Editor
              height="100%"
              language="rql"
              theme={resolved_theme}
              value={code}
              onChange={(value) => setCode(value || '')}
              beforeMount={handle_before_mount}
              onMount={handle_editor_did_mount}
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
                    <SnippetResults data={result.data} columns={columns} max_key_length={max_key_length} />
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
              onClick={handle_run}
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
