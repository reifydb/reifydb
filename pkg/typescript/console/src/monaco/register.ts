// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import { rql_language_definition, rql_language_configuration } from './rql-language';
import { premium_dark_theme, premium_light_theme, brutalist_dark_theme, brutalist_light_theme } from './themes';

let registered = false;

export function register_rql_language(monaco: typeof import('monaco-editor')): void {
  if (registered) return;

  monaco.languages.register({ id: 'rql' });
  monaco.languages.setMonarchTokensProvider('rql', rql_language_definition);
  monaco.languages.setLanguageConfiguration('rql', rql_language_configuration);
  monaco.editor.defineTheme('premium-dark', premium_dark_theme);
  monaco.editor.defineTheme('premium-light', premium_light_theme);
  monaco.editor.defineTheme('brutalist-dark', brutalist_dark_theme);
  monaco.editor.defineTheme('brutalist-light', brutalist_light_theme);

  registered = true;
}
