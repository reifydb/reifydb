// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

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
use reifydb_core::interface::{NamespaceId, SequenceId};
pub use row::RowSequence;
pub(crate) use system::SystemSequence;

pub struct Sequence {
	pub id: SequenceId,
	pub namespace: NamespaceId,
	pub name: String,
	pub value: u64,
}
