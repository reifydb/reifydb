// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod column;
pub mod find;
pub mod flow;
pub mod generator;
pub mod get;
pub mod list;
pub mod row;
pub mod sink;
pub mod source;
pub mod system;
pub mod windowed_view_group;

use reifydb_core::interface::catalog::id::{NamespaceId, SequenceId};

pub struct Sequence {
	pub id: SequenceId,
	pub namespace: NamespaceId,
	pub name: String,
	pub value: u64,
}
