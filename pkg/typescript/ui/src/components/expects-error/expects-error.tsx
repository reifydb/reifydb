// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

import { Badge } from "../badge/index.js";

export interface ExpectsErrorProps {
  code: string;
  className?: string;
}

export function ExpectsError({ code, className }: ExpectsErrorProps) {
  return (
    <Badge variant="warning" className={className}>
      fails with {code}
    </Badge>
  );
}
