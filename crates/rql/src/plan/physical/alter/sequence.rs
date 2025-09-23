// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::QueryTransaction;

use crate::plan::{
	logical,
	physical::{AlterSequenceNode, Compiler, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_alter_sequence<'a>(
		_rx: &mut impl QueryTransaction,
		alter: logical::AlterSequenceNode<'a>,
	) -> crate::Result<PhysicalPlan<'a>> {
		// For ALTER SEQUENCE, we just pass through the logical plan
		// The actual execution will happen in the engine
		Ok(PhysicalPlan::AlterSequence(AlterSequenceNode {
			sequence: alter.sequence,
			column: alter.column,
			value: alter.value,
		}))
	}
}
