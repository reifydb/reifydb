// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { useRef } from 'react';
import Editor, { type OnMount } from '@monaco-editor/react';
import type { editor } from 'monaco-editor';
import { register_rql_language } from '../../monaco/register';

interface QueryEditorProps {
  code: string;
  on_change: (code: string) => void;
  on_run: () => void;
  theme?: 'light' | 'dark';
  monaco_theme_name?: string;
  monaco_theme_data?: editor.IStandaloneThemeData;
}

export function QueryEditor({ code, on_change, on_run, theme = 'light', monaco_theme_name, monaco_theme_data }: QueryEditorProps) {
  const editor_ref = useRef<editor.IStandaloneCodeEditor | null>(null);
  const on_run_ref = useRef(on_run);
  on_run_ref.current = on_run;

  const resolved_theme = monaco_theme_name ?? (theme === 'light' ? 'premium-light' : 'premium-dark');

  const handle_mount: OnMount = (editor, monaco) => {
    editor_ref.current = editor;
    register_rql_language(monaco);

    editor.addAction({
      id: 'run-query',
      label: 'Run Query',
      keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
      run: () => {
        on_run_ref.current();
      },
    });
  };

  const handle_before_mount = (monaco: typeof import('monaco-editor')) => {
    register_rql_language(monaco);
    if (monaco_theme_name && monaco_theme_data) {
      monaco.editor.defineTheme(monaco_theme_name, monaco_theme_data);
    }
  };

  return (
    <Editor
      height="100%"
      language="rql"
      theme={resolved_theme}
      value={code}
      onChange={(value) => on_change(value || '')}
      beforeMount={handle_before_mount}
      onMount={handle_mount}
      options={{
        minimap: { enabled: false },
        lineNumbers: 'on',
        glyphMargin: false,
        folding: true,
        lineDecorationsWidth: 16,
        lineNumbersMinChars: 3,
        scrollBeyondLastLine: false,
        scrollbar: { vertical: 'auto', horizontal: 'auto' },
        overviewRulerLanes: 0,
        hideCursorInOverviewRuler: true,
        overviewRulerBorder: false,
        renderLineHighlight: 'line',
        fontFamily: "'Inconsolata', monospace",
        fontSize: 13,
        padding: { top: 8, bottom: 8 },
        wordWrap: 'on',
        automaticLayout: true,
      }}
    />
  );
}
