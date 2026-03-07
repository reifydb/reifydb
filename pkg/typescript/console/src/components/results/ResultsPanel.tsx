// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { ExecutionResult } from '../../types';
import { ResultsTable } from './ResultsTable';
import { ResultsError } from './ResultsError';
import { ResultsEmpty } from './ResultsEmpty';
import { ResultsStatusBar } from './ResultsStatusBar';

interface ResultsPanelProps {
  result: ExecutionResult | null;
}

export function ResultsPanel({ result }: ResultsPanelProps) {
  if (!result) {
    return (
      <div className="rdb-results__empty">
        $ run a query to see results
      </div>
    );
  }

  if (!result.success && result.error) {
    return <ResultsError message={result.error} />;
  }

  const data = result.data ?? [];
  if (data.length === 0) {
    return <ResultsEmpty />;
  }

  return (
    <>
      <ResultsTable data={data} />
      <ResultsStatusBar
        rowCount={data.length}
        executionTime={result.executionTime}
        data={data}
      />
    </>
  );
}
