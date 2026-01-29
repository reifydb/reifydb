// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::multi::transaction::MultiTransaction;

pub fn test_multi() -> MultiTransaction {
	MultiTransaction::testing()
}

#[cfg(test)]
mod oracle_stress;
