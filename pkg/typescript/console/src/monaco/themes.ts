// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { editor } from 'monaco-editor';

export const brutalistLightTheme: editor.IStandaloneThemeData = {
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

export const brutalistDarkTheme: editor.IStandaloneThemeData = {
  base: 'vs-dark',
  inherit: true,
  rules: [
    { token: 'keyword', foreground: '818CF8', fontStyle: 'bold' },
    { token: 'string', foreground: '4ADE80' },
    { token: 'string.quote', foreground: '4ADE80' },
    { token: 'number', foreground: 'F87171', fontStyle: 'bold' },
    { token: 'comment', foreground: '9CA3AF', fontStyle: 'italic' },
    { token: 'operator', foreground: 'E5E7EB' },
    { token: 'identifier', foreground: 'F3F4F6' },
    { token: 'key', foreground: '60A5FA', fontStyle: 'bold' },
  ],
  colors: {
    'editor.background': '#1A1A1A',
    'editor.foreground': '#F3F4F6',
    'editor.lineHighlightBackground': '#2A2A2A',
    'editor.selectionBackground': '#818CF830',
    'editorCursor.foreground': '#818CF8',
    'editorLineNumber.foreground': '#6B7280',
    'editorLineNumber.activeForeground': '#E5E7EB',
  },
};

export const premiumLightTheme: editor.IStandaloneThemeData = {
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

export const premiumDarkTheme: editor.IStandaloneThemeData = {
  base: 'vs-dark',
  inherit: true,
  rules: [
    { token: 'keyword', foreground: '818cf8', fontStyle: 'bold' },
    { token: 'string', foreground: '22d3ee' },
    { token: 'string.quote', foreground: '22d3ee' },
    { token: 'number', foreground: '22d3ee', fontStyle: 'bold' },
    { token: 'comment', foreground: '666666', fontStyle: 'italic' },
    { token: 'operator', foreground: 'A0A0A0' },
    { token: 'identifier', foreground: 'F0F0F0' },
    { token: 'key', foreground: '818cf8', fontStyle: 'bold' },
  ],
  colors: {
    'editor.background': '#1a1a1a',
    'editor.foreground': '#F0F0F0',
    'editor.lineHighlightBackground': '#222222',
    'editor.selectionBackground': '#818cf840',
    'editorCursor.foreground': '#818cf8',
    'editorLineNumber.foreground': '#666666',
    'editorLineNumber.activeForeground': '#A0A0A0',
  },
};
