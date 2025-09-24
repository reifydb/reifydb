// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	NamespaceDef, NamespaceId, QueryTransaction,
	resolved::{ResolvedNamespace, ResolvedSequence, SequenceDef},
};
use reifydb_type::Fragment;

use crate::plan::{
	logical::{self, resolver::DEFAULT_NAMESPACE},
	physical::{AlterSequenceNode, Compiler, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_alter_sequence<'a>(
		_rx: &mut impl QueryTransaction,
		alter: logical::AlterSequenceNode<'a>,
	) -> crate::Result<PhysicalPlan<'a>> {
		// Resolve the sequence identifier to a ResolvedSequence
		// For now, create a basic resolved sequence (in a real implementation,
		// this would query the catalog to get the actual sequence definition)
		let namespace_name = alter.sequence.namespace.as_ref().map(|f| f.text()).unwrap_or(DEFAULT_NAMESPACE);

		let namespace = ResolvedNamespace::new(
			Fragment::owned_internal(namespace_name),
			NamespaceDef {
				id: NamespaceId(1), // In real implementation, this would come from catalog
				name: namespace_name.to_string(),
			},
		);

		let sequence_def = SequenceDef {
			name: alter.sequence.name.text().to_string(),
			current_value: 1, // In real implementation, this would come from catalog
			increment: 1,
		};

		let resolved_sequence = ResolvedSequence::new(alter.sequence.name.clone(), namespace, sequence_def);

		Ok(PhysicalPlan::AlterSequence(AlterSequenceNode {
			sequence: resolved_sequence,
			column: alter.column,
			value: alter.value,
		}))
	}
}
