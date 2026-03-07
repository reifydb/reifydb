// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::multi::transaction::MultiTransaction;

pub fn test_multi() -> MultiTransaction {
	MultiTransaction::testing()
}

#[cfg(test)]
mod oracle_stress;
