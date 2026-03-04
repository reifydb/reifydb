// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

interface ResultsErrorProps {
  message: string;
}

export function ResultsError({ message }: ResultsErrorProps) {
  return (
    <div className="rdb-results__error">
      <pre>ERR: {message}</pre>
    </div>
  );
}
