// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{RowId, interface::TableDef, row::EncodedRow};

#[derive(Debug, Clone)]
pub enum PendingWrite {
	InsertIntoTable {
		table: TableDef,
		id: RowId,
		row: EncodedRow,
	},
	Update {
		table: TableDef,
		id: RowId,
		row: EncodedRow,
	},
	Remove {
		table: TableDef,
		id: RowId,
	},
}
