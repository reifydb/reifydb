// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import type { editor } from 'monaco-editor';

export const brutalistLightTheme: editor.IStandaloneThemeData = {
  base: 'vs',
  inherit: true,
  rules: [
    { token: 'keyword', foreground: '383838', fontStyle: 'bold' },
    { token: 'string', foreground: '16A34A' },
    { token: 'string.quote', foreground: '16A34A' },
    { token: 'number', foreground: 'B91C1C', fontStyle: 'bold' },
    { token: 'comment', foreground: '6B7280', fontStyle: 'italic' },
    { token: 'operator', foreground: '383838' },
    { token: 'identifier', foreground: '1A1A1A' },
    { token: 'key', foreground: '2563EB', fontStyle: 'bold' },
  ],
  colors: {
    'editor.background': '#FFFFFF',
    'editor.foreground': '#1A1A1A',
    'editor.lineHighlightBackground': '#F8F8F7',
    'editor.selectionBackground': '#FFD4A3',
    'editorCursor.foreground': '#FF8A3D',
    'editorLineNumber.foreground': '#9CA3AF',
    'editorLineNumber.activeForeground': '#383838',
  },
};

export const brutalistDarkTheme: editor.IStandaloneThemeData = {
  base: 'vs-dark',
  inherit: true,
  rules: [
    { token: 'keyword', foreground: 'FF8A3D', fontStyle: 'bold' },
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
    'editor.selectionBackground': '#4B3D2F',
    'editorCursor.foreground': '#FF8A3D',
    'editorLineNumber.foreground': '#6B7280',
    'editorLineNumber.activeForeground': '#E5E7EB',
  },
};

export const premiumDarkTheme: editor.IStandaloneThemeData = {
  base: 'vs-dark',
  inherit: true,
  rules: [
    { token: 'keyword', foreground: 'FB923C', fontStyle: 'bold' },
    { token: 'string', foreground: '34D399' },
    { token: 'string.quote', foreground: '34D399' },
    { token: 'number', foreground: 'F472B6', fontStyle: 'bold' },
    { token: 'comment', foreground: '525252', fontStyle: 'italic' },
    { token: 'operator', foreground: 'A3A3A3' },
    { token: 'identifier', foreground: 'E5E5E5' },
    { token: 'key', foreground: 'FB923C', fontStyle: 'bold' },
  ],
  colors: {
    'editor.background': '#0C0C0C',
    'editor.foreground': '#E5E5E5',
    'editor.lineHighlightBackground': '#1C1C1C',
    'editor.selectionBackground': '#FB923C40',
    'editorCursor.foreground': '#FB923C',
    'editorLineNumber.foreground': '#525252',
    'editorLineNumber.activeForeground': '#A3A3A3',
  },
};
