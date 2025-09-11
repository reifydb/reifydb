// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::QueryTransaction;

use crate::plan::{
	logical::AlterSequenceNode,
	physical::{AlterSequencePlan, Compiler, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_alter_sequence<'a>(
		_rx: &mut impl QueryTransaction,
		alter: AlterSequenceNode<'a>,
	) -> crate::Result<PhysicalPlan<'a>> {
		// For ALTER SEQUENCE, we just pass through the logical plan
		// info The actual execution will happen in the engine
		Ok(PhysicalPlan::AlterSequence(AlterSequencePlan {
			sequence: alter.sequence,
			column: alter.column,
			value: alter.value,
		}))
	}
}
