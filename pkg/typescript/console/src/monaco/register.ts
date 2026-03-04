// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

import { rqlLanguageDefinition, rqlLanguageConfiguration } from './rql-language';
import { premiumDarkTheme, brutalistDarkTheme, brutalistLightTheme } from './themes';

let registered = false;

export function registerRqlLanguage(monaco: typeof import('monaco-editor')): void {
  if (registered) return;

  monaco.languages.register({ id: 'rql' });
  monaco.languages.setMonarchTokensProvider('rql', rqlLanguageDefinition);
  monaco.languages.setLanguageConfiguration('rql', rqlLanguageConfiguration);
  monaco.editor.defineTheme('premium-dark', premiumDarkTheme);
  monaco.editor.defineTheme('brutalist-dark', brutalistDarkTheme);
  monaco.editor.defineTheme('brutalist-light', brutalistLightTheme);

  registered = true;
}
