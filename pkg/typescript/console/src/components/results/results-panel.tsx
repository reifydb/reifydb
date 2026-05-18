// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

import type { ExecutionResult } from '../../types';
import { ResultsTable } from './results-table';
import { ResultsError } from './results-error';
import { ResultsEmpty } from './results-empty';
import { ResultsStatusBar } from './results-status-bar';

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
    return <ResultsError message={result.error} diagnostic={result.diagnostic} />;
  }

  const data = result.data ?? [];
  if (data.length === 0) {
    return <ResultsEmpty />;
  }

  return (
    <>
      <ResultsTable data={data} />
      <ResultsStatusBar
        row_count={data.length}
        execution_time={result.execution_time}
        data={data}
      />
    </>
  );
}
