// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod flow_edge {
	use once_cell::sync::Lazy;
	use reifydb_core::value::encoded::EncodedValuesLayout;
	use reifydb_type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const FLOW: usize = 1;
	pub(crate) const SOURCE: usize = 2;
	pub(crate) const TARGET: usize = 3;

	pub(crate) static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| {
		EncodedValuesLayout::new(&[
			Type::Uint8, // id (FlowEdgeId)
			Type::Uint8, // flow (FlowId)
			Type::Uint8, // source (FlowNodeId)
			Type::Uint8, // target (FlowNodeId)
		])
	});
}

pub(crate) mod flow_edge_by_flow {
	use once_cell::sync::Lazy;
	use reifydb_core::value::encoded::EncodedValuesLayout;
	use reifydb_type::Type;

	pub(crate) const FLOW: usize = 0;
	pub(crate) const ID: usize = 1;

	pub(crate) static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| {
		EncodedValuesLayout::new(&[
			Type::Uint8, // flow (FlowId)
			Type::Uint8, // id (FlowEdgeId)
		])
	});
}
