// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

interface SnippetErrorProps {
  message: string;
}

function lineClass(line: string): string | undefined {
  if (/^\s*(↳ )?Error \S+/.test(line)) return 'rdb-snippet-error__title';
  return undefined;
}

export function SnippetError({ message }: SnippetErrorProps) {
  const lines = message.replace(/\n+$/, '').split('\n');

  return (
    <pre className="rdb-snippet-error">
      {lines.map((line, i) => (
        <span key={i} className={lineClass(line)}>
          {line}
          {i < lines.length - 1 ? '\n' : ''}
        </span>
      ))}
    </pre>
  );
}
