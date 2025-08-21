import React, { forwardRef, useImperativeHandle, useRef, useEffect } from 'react';
import Editor, { Monaco } from '@monaco-editor/react';
import { editor } from 'monaco-editor';
import styles from './CodeEditor.module.css';

interface CodeEditorProps {
  value: string;
  onChange: (value: string) => void;
  onExecute?: () => void;
  language?: string;
  theme?: string;
  readOnly?: boolean;
}

const CodeEditor = forwardRef<any, CodeEditorProps>(({
  value,
  onChange,
  onExecute,
  language = 'sql',
  theme = 'vs',
  readOnly = false,
}, ref) => {
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);
  const monacoRef = useRef<Monaco | null>(null);

  // Force theme update when theme prop changes
  useEffect(() => {
    if (monacoRef.current && theme) {
      monacoRef.current.editor.setTheme(theme);
    }
  }, [theme]);

  useImperativeHandle(ref, () => ({
    setValue: (newValue: string) => {
      if (editorRef.current) {
        editorRef.current.setValue(newValue);
      }
    },
    getValue: () => {
      return editorRef.current?.getValue() || '';
    },
    focus: () => {
      editorRef.current?.focus();
    },
  }));

  const handleBeforeMount = (monaco: Monaco) => {
    // Set initial theme
    if (monaco && monaco.editor) {
      try {
        monaco.editor.setTheme(theme);
      } catch (e) {
        console.log('Theme will be set on mount');
      }
    }
  };

  const handleEditorDidMount = (editor: editor.IStandaloneCodeEditor, monaco: Monaco) => {
    editorRef.current = editor;
    monacoRef.current = monaco;
    
    // Set theme again after mount
    monaco.editor.setTheme(theme || 'vs');

    // Register ReifyDB SQL language configuration
    monaco.languages.registerCompletionItemProvider('sql', {
      provideCompletionItems: (model, position) => {
        const suggestions = [
          // ReifyDB specific keywords
          ...['REIFY', 'FLOW', 'STREAM', 'MATERIALIZE'].map(keyword => ({
            label: keyword,
            kind: monaco.languages.CompletionItemKind.Keyword,
            insertText: keyword,
            documentation: `ReifyDB keyword: ${keyword}`,
          })),
          // Common SQL keywords
          ...['SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 
              'TABLE', 'INDEX', 'VIEW', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'ON', 
              'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'OFFSET'].map(keyword => ({
            label: keyword,
            kind: monaco.languages.CompletionItemKind.Keyword,
            insertText: keyword,
          })),
          // Functions
          ...['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'NOW', 'CURRENT_TIMESTAMP'].map(func => ({
            label: func,
            kind: monaco.languages.CompletionItemKind.Function,
            insertText: `${func}()`,
          })),
        ];

        return { suggestions };
      },
    });

    // Add execute shortcut
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
      if (onExecute) {
        onExecute();
      }
    });

    // Configure editor options
    editor.updateOptions({
      minimap: { enabled: false },
      fontSize: 14,
      lineNumbers: 'on',
      roundedSelection: false,
      scrollBeyondLastLine: false,
      automaticLayout: true,
      tabSize: 2,
      wordWrap: 'on',
      suggest: {
        showKeywords: true,
        showSnippets: true,
        showFunctions: true,
      },
    });
  };

  return (
    <div className={styles.editorContainer}>
      <div className={styles.editorHeader}>
        <span className={styles.editorTitle}>RQL Query Editor</span>
        <span className={styles.shortcutHint}>Ctrl+Enter to execute</span>
      </div>
      <Editor
        height="100%"
        language={language}
        theme={theme}
        value={value}
        onChange={(value) => onChange(value || '')}
        beforeMount={handleBeforeMount}
        onMount={handleEditorDidMount}
        options={{
          readOnly,
          automaticLayout: true
        }}
      />
    </div>
  );
});

CodeEditor.displayName = 'CodeEditor';

export default CodeEditor;