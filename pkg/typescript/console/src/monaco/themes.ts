// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { editor } from 'monaco-editor';

export const brutalist_light_theme: editor.IStandaloneThemeData = {
  base: 'vs',
  inherit: true,
  rules: [
    { token: 'keyword', foreground: '6366F1', fontStyle: 'bold' },
    { token: 'string', foreground: '16A34A' },
    { token: 'string.quote', foreground: '16A34A' },
    { token: 'number', foreground: 'B91C1C', fontStyle: 'bold' },
    { token: 'comment', foreground: '6B7280', fontStyle: 'italic' },
    { token: 'operator', foreground: '383838' },
    { token: 'identifier', foreground: '1A1A1A' },
    { token: 'key', foreground: '6366F1', fontStyle: 'bold' },
  ],
  colors: {
    'editor.background': '#FFFFFF',
    'editor.foreground': '#1A1A1A',
    'editor.lineHighlightBackground': '#F8F8F7',
    'editor.selectionBackground': '#818CF840',
    'editorCursor.foreground': '#818CF8',
    'editorLineNumber.foreground': '#9CA3AF',
    'editorLineNumber.activeForeground': '#383838',
  },
};

export const brutalist_dark_theme: editor.IStandaloneThemeData = {
  base: 'vs-dark',
  inherit: true,
  rules: [
    { token: 'keyword', foreground: '818CF8', fontStyle: 'bold' },
    { token: 'string', foreground: 'A5B4FC' },
    { token: 'string.quote', foreground: 'A5B4FC' },
    { token: 'number', foreground: 'A5B4FC', fontStyle: 'bold' },
    { token: 'comment', foreground: '585858', fontStyle: 'italic' },
    { token: 'operator', foreground: 'a0a0a0' },
    { token: 'identifier', foreground: 'ffffff' },
    { token: 'key', foreground: '818CF8', fontStyle: 'bold' },
  ],
  colors: {
    'editor.background': '#000000',
    'editor.foreground': '#ffffff',
    'editor.lineHighlightBackground': '#141414',
    'editor.selectionBackground': '#818CF830',
    'editorCursor.foreground': '#818CF8',
    'editorLineNumber.foreground': '#585858',
    'editorLineNumber.activeForeground': '#a0a0a0',
    'focusBorder': '#00000000',
    'editorWidget.border': '#343434',
    'editorOverviewRuler.border': '#00000000',
    'editorGroup.border': '#343434',
    'editorGutter.background': '#000000',
    'input.border': '#343434',
    'inputOption.activeBorder': '#343434',
  },
};

export const premium_light_theme: editor.IStandaloneThemeData = {
  base: 'vs',
  inherit: true,
  rules: [
    { token: 'keyword', foreground: '6366F1', fontStyle: 'bold' },
    { token: 'string', foreground: '059669' },
    { token: 'string.quote', foreground: '059669' },
    { token: 'number', foreground: 'DB2777', fontStyle: 'bold' },
    { token: 'comment', foreground: '9CA3AF', fontStyle: 'italic' },
    { token: 'operator', foreground: '525252' },
    { token: 'identifier', foreground: '1A1A1A' },
    { token: 'key', foreground: '6366F1', fontStyle: 'bold' },
  ],
  colors: {
    'editor.background': '#FFFFFF',
    'editor.foreground': '#1A1A1A',
    'editor.lineHighlightBackground': '#F5F5F5',
    'editor.selectionBackground': '#818CF840',
    'editorCursor.foreground': '#6366F1',
    'editorLineNumber.foreground': '#9CA3AF',
    'editorLineNumber.activeForeground': '#525252',
  },
};

export const premium_dark_theme: editor.IStandaloneThemeData = {
  base: 'vs-dark',
  inherit: true,
  rules: [
    { token: 'keyword', foreground: '818cf8', fontStyle: 'bold' },
    { token: 'string', foreground: 'a5b4fc' },
    { token: 'string.quote', foreground: 'a5b4fc' },
    { token: 'number', foreground: 'a5b4fc', fontStyle: 'bold' },
    { token: 'comment', foreground: '585858', fontStyle: 'italic' },
    { token: 'operator', foreground: 'a0a0a0' },
    { token: 'identifier', foreground: 'ffffff' },
    { token: 'key', foreground: '818cf8', fontStyle: 'bold' },
  ],
  colors: {
    'editor.background': '#000000',
    'editor.foreground': '#ffffff',
    'editor.lineHighlightBackground': '#141414',
    'editor.selectionBackground': '#818cf840',
    'editorCursor.foreground': '#818cf8',
    'editorLineNumber.foreground': '#585858',
    'editorLineNumber.activeForeground': '#a0a0a0',
    'focusBorder': '#00000000',
    'editorWidget.border': '#343434',
    'editorOverviewRuler.border': '#00000000',
    'editorGroup.border': '#343434',
    'editorGutter.background': '#000000',
    'input.border': '#343434',
    'inputOption.activeBorder': '#343434',
  },
};
