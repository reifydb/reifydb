// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod cdc;
pub mod interceptor;
mod transaction;
mod unversioned;
mod versioned;

use crate::interface::TableDef;
use crate::row::EncodedRow;
pub use crate::transaction::{StandardTransaction, Transaction};
use crate::RowNumber;
pub use cdc::{
	CdcQueryTransaction, CdcTransaction, StandardCdcQueryTransaction,
	StandardCdcTransaction,
};
pub use transaction::{
	CommandTransaction, QueryTransaction,
};
pub use unversioned::*;
pub use versioned::*;

#[derive(Debug, Clone)]
pub enum PendingWrite {
	TableInsert {
		table: TableDef,
		id: RowNumber,
		row: EncodedRow,
	},
	TableUpdate {
		table: TableDef,
		id: RowNumber,
		row: EncodedRow,
	},
	TableRemove {
		table: TableDef,
		id: RowNumber,
	},
}
