// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{RowId, impl_hook, interface::TableDef, row::EncodedRow};

pub struct TablePreInsertHook {
	pub table: TableDef,
	pub row: EncodedRow,
}

impl_hook!(TablePreInsertHook);

pub struct TablePostInsertHook {
	pub table: TableDef,
	pub id: RowId,
	pub row: EncodedRow,
}

impl_hook!(TablePostInsertHook);
