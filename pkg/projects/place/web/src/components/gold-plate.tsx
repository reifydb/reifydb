// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

export function GoldPlate({ className = '' }: { className?: string }) {
  return (
    <a href="https://reifydb.com" target="_blank" rel="noopener noreferrer" className={`gold-plate ${className}`}>
      Powered by ReifyDB
    </a>
  )
}
