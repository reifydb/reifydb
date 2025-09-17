// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{impl_event, interface::TableDef, row::Row};

pub struct TableInsertedEvent {
	pub table: TableDef,
	pub row: Row,
}

impl_event!(TableInsertedEvent);
