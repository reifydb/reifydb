// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import { useState, useCallback, useEffect, useRef } from 'react';
import Editor, { type OnMount } from '@monaco-editor/react';
import type { editor } from 'monaco-editor';
import { registerRqlLanguage } from '../monaco/register';
import { SnippetResults } from './SnippetResults';
import type { Executor, ExecutionResult } from '../types';
import type { RdbTheme } from './Console';

export interface SnippetProps {
  executor: Executor;
  initialCode: string;
  title?: string;
  description?: string;
  className?: string;
  theme?: RdbTheme;
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
}: SnippetProps) {
  const [code, setCode] = useState(initialCode);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [isExecuting, setIsExecuting] = useState(false);
  const [copied, setCopied] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);
  const handleRunRef = useRef<() => void>(() => {});

  const lineCount = code.split('\n').length;
  const editorHeight = Math.max(lineCount * 20 + 16, 80);

  useEffect(() => {
    if (!isFullscreen) return;

    const handleEsc = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setIsFullscreen(false);
    };
    document.addEventListener('keydown', handleEsc);
    document.body.style.overflow = 'hidden';

    return () => {
      document.removeEventListener('keydown', handleEsc);
      document.body.style.overflow = '';
    };
  }, [isFullscreen]);

  const handleRun = useCallback(async () => {
    if (isExecuting) return;
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
  };

  const columns = result?.data && result.data.length > 0 ? Object.keys(result.data[0]) : [];
  const maxKeyLength = columns.length > 0 ? Math.max(...columns.map(c => c.length)) : 0;

  const content = (
    <div className={`rdb-snippet${isFullscreen ? ' rdb-snippet--fullscreen' : ''}${theme === 'light' ? ' rdb-theme-light' : ''}${className ? ` ${className}` : ''}`}>
      {/* Header */}
      <div className="rdb-snippet__header">
        <div className="rdb-snippet__title">
          <span className="rdb-snippet__title-marker">$</span> {title}
        </div>
        <div className="rdb-snippet__actions">
          <button
            onClick={() => setIsFullscreen(!isFullscreen)}
            className="rdb-snippet__action-btn"
            title={isFullscreen ? 'Exit fullscreen' : 'Fullscreen'}
          >
            {isFullscreen ? '[×]' : '[[]]'}
          </button>
          <button
            onClick={handleCopy}
            className="rdb-snippet__action-btn"
            title="Copy code"
          >
            {copied ? '[ok]' : '[cp]'}
          </button>
          <button
            onClick={handleReset}
            className="rdb-snippet__action-btn"
            title="Reset code"
          >
            [&#8634;]
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

      {/* Editor */}
      <div
        className={isFullscreen ? 'rdb-snippet__editor--fullscreen' : 'rdb-snippet__editor'}
        style={isFullscreen ? undefined : { height: editorHeight }}
      >
        <Editor
          height="100%"
          language="rql"
          theme={theme === 'light' ? 'premium-light' : 'premium-dark'}
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
            scrollbar: {
              vertical: isFullscreen ? 'auto' : 'hidden',
              horizontal: isFullscreen ? 'auto' : 'hidden',
            },
            overviewRulerLanes: 0,
            hideCursorInOverviewRuler: true,
            overviewRulerBorder: false,
            renderLineHighlight: 'none',
            fontFamily: "'IBM Plex Mono', monospace",
            fontSize: 13,
            padding: { top: 8, bottom: 8 },
            wordWrap: 'on',
            automaticLayout: true,
          }}
        />
      </div>

      {/* Toolbar */}
      <div className="rdb-snippet__toolbar">
        <span className="rdb-snippet__hint">
          {isExecuting ? '$ running...' : '$ ctrl+enter to run'}
        </span>
        <button
          onClick={handleRun}
          disabled={isExecuting}
          className={`rdb-snippet__run-btn${isExecuting ? ' rdb-snippet__run-btn--loading' : ''}`}
        >
          {isExecuting ? '[running...]' : '[run]'}
        </button>
      </div>

      {/* Results */}
      {result && (
        <div className={`rdb-snippet__results${isFullscreen ? ' rdb-snippet__results--fullscreen' : ''}`}>
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
  );

  if (isFullscreen) {
    return (
      <div className="rdb-snippet__overlay">
        <div className="rdb-snippet__overlay-inner">
          {content}
        </div>
      </div>
    );
  }

  return content;
}
