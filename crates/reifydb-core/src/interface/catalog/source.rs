// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

use crate::interface::{TableDef, TableId, ViewDef, ViewId};

#[derive(
	Debug,
	Copy,
	Clone,
	PartialOrd,
	PartialEq,
	Ord,
	Eq,
	Hash,
	Serialize,
	Deserialize,
)]
pub enum SourceId {
	Table(TableId),
	View(ViewId),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SourceDef {
	Table(TableDef),
	View(ViewDef),
}

impl SourceDef {
	pub fn id(&self) -> SourceId {
		match self {
			SourceDef::Table(table) => SourceId::Table(table.id),
			SourceDef::View(view) => SourceId::View(view.id),
		}
	}
}
