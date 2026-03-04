// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import { useRef } from 'react';
import Editor, { type OnMount } from '@monaco-editor/react';
import type { editor } from 'monaco-editor';
import { registerRqlLanguage } from '../../monaco/register';

interface QueryEditorProps {
  code: string;
  onChange: (code: string) => void;
  onRun: () => void;
}

export function QueryEditor({ code, onChange, onRun }: QueryEditorProps) {
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);
  const onRunRef = useRef(onRun);
  onRunRef.current = onRun;

  const handleMount: OnMount = (editor, monaco) => {
    editorRef.current = editor;
    registerRqlLanguage(monaco);

    editor.addAction({
      id: 'run-query',
      label: 'Run Query',
      keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
      run: () => {
        onRunRef.current();
      },
    });
  };

  const handleBeforeMount = (monaco: typeof import('monaco-editor')) => {
    registerRqlLanguage(monaco);
  };

  return (
    <Editor
      height="100%"
      language="rql"
      theme="premium-dark"
      value={code}
      onChange={(value) => onChange(value || '')}
      beforeMount={handleBeforeMount}
      onMount={handleMount}
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
        fontFamily: "'IBM Plex Mono', monospace",
        fontSize: 13,
        padding: { top: 8, bottom: 8 },
        wordWrap: 'on',
        automaticLayout: true,
      }}
    />
  );
}
