// SPDX-License-Identifier: Apache-2.0
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
