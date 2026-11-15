// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::catalog::object::ObjectId;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{error::ValueError, value::datetime::DateTime};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Advanced {
	Inserted,
	Forward,
	Backwards(DateTime),
}

pub trait CompletenessWriter: Send + Sync {
	fn advance(
		&self,
		tx: &mut Transaction<'_>,
		advances: &[(ObjectId, DateTime)],
	) -> Result<Vec<Advanced>, ValueError>;
}

pub type CompletenessWriterRef = Arc<dyn CompletenessWriter>;
