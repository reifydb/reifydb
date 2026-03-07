// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { editor } from 'monaco-editor';

export const brutalistLightTheme: editor.IStandaloneThemeData = {
  base: 'vs',
  inherit: true,
  rules: [
    { token: 'keyword', foreground: '0D9488', fontStyle: 'bold' },
    { token: 'string', foreground: '16A34A' },
    { token: 'string.quote', foreground: '16A34A' },
    { token: 'number', foreground: 'B91C1C', fontStyle: 'bold' },
    { token: 'comment', foreground: '6B7280', fontStyle: 'italic' },
    { token: 'operator', foreground: '383838' },
    { token: 'identifier', foreground: '1A1A1A' },
    { token: 'key', foreground: '0D9488', fontStyle: 'bold' },
  ],
  colors: {
    'editor.background': '#FFFFFF',
    'editor.foreground': '#1A1A1A',
    'editor.lineHighlightBackground': '#F8F8F7',
    'editor.selectionBackground': '#A7F3D0',
    'editorCursor.foreground': '#14B8A6',
    'editorLineNumber.foreground': '#9CA3AF',
    'editorLineNumber.activeForeground': '#383838',
  },
};

export const brutalistDarkTheme: editor.IStandaloneThemeData = {
  base: 'vs-dark',
  inherit: true,
  rules: [
    { token: 'keyword', foreground: '14B8A6', fontStyle: 'bold' },
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
    'editor.selectionBackground': '#1A3A36',
    'editorCursor.foreground': '#14B8A6',
    'editorLineNumber.foreground': '#6B7280',
    'editorLineNumber.activeForeground': '#E5E7EB',
  },
};

export const premiumLightTheme: editor.IStandaloneThemeData = {
  base: 'vs',
  inherit: true,
  rules: [
    { token: 'keyword', foreground: '0D9488', fontStyle: 'bold' },
    { token: 'string', foreground: '059669' },
    { token: 'string.quote', foreground: '059669' },
    { token: 'number', foreground: 'DB2777', fontStyle: 'bold' },
    { token: 'comment', foreground: '9CA3AF', fontStyle: 'italic' },
    { token: 'operator', foreground: '525252' },
    { token: 'identifier', foreground: '1A1A1A' },
    { token: 'key', foreground: '0D9488', fontStyle: 'bold' },
  ],
  colors: {
    'editor.background': '#FFFFFF',
    'editor.foreground': '#1A1A1A',
    'editor.lineHighlightBackground': '#F5F5F5',
    'editor.selectionBackground': '#99F6E4',
    'editorCursor.foreground': '#0D9488',
    'editorLineNumber.foreground': '#9CA3AF',
    'editorLineNumber.activeForeground': '#525252',
  },
};

export const premiumDarkTheme: editor.IStandaloneThemeData = {
  base: 'vs-dark',
  inherit: true,
  rules: [
    { token: 'keyword', foreground: '14B8A6', fontStyle: 'bold' },
    { token: 'string', foreground: '34D399' },
    { token: 'string.quote', foreground: '34D399' },
    { token: 'number', foreground: 'F472B6', fontStyle: 'bold' },
    { token: 'comment', foreground: '525252', fontStyle: 'italic' },
    { token: 'operator', foreground: 'A3A3A3' },
    { token: 'identifier', foreground: 'E5E5E5' },
    { token: 'key', foreground: '14B8A6', fontStyle: 'bold' },
  ],
  colors: {
    'editor.background': '#1A1A1A',
    'editor.foreground': '#E5E5E5',
    'editor.lineHighlightBackground': '#252525',
    'editor.selectionBackground': '#14B8A640',
    'editorCursor.foreground': '#14B8A6',
    'editorLineNumber.foreground': '#525252',
    'editorLineNumber.activeForeground': '#A3A3A3',
  },
};
