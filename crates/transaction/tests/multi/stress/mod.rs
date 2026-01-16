// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::multi::transaction::TransactionMulti;

pub fn test_multi() -> TransactionMulti {
	TransactionMulti::testing()
}

#[cfg(test)]
mod oracle_stress;
