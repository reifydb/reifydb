// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{interface::TableDef, row::EncodedRow};

#[derive(Debug)]
pub struct PreInsertHook<'a> {
	pub table: &'a TableDef,
	pub row: &'a EncodedRow,
}
