// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod column;
pub mod find;
pub mod flow;
pub mod generator;
pub mod get;
pub mod list;
pub mod reducer;
pub mod row;
pub mod schema;
pub mod system;

use reifydb_core::interface::catalog::id::{NamespaceId, SequenceId};

pub struct Sequence {
	pub id: SequenceId,
	pub namespace: NamespaceId,
	pub name: String,
	pub value: u64,
}
