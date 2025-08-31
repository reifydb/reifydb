// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod column;
mod find;
pub mod flow;
mod generator;
mod get;
mod layout;
mod list;
mod row;
mod system;

pub use column::ColumnSequence;
use reifydb_core::interface::{SchemaId, SequenceId};
pub use row::RowSequence;
pub(crate) use system::SystemSequence;

pub struct Sequence {
	pub id: SequenceId,
	pub schema: SchemaId,
	pub name: String,
	pub value: u64,
}
