// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub(crate) mod flow_node {
	use once_cell::sync::Lazy;
	use reifydb_core::value::encoded::layout::EncodedValuesLayout;
	use reifydb_type::value::r#type::Type;

	pub(crate) const ID: usize = 0;
	pub(crate) const FLOW: usize = 1;
	pub(crate) const TYPE: usize = 2;
	pub(crate) const DATA: usize = 3;

	pub(crate) static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| {
		EncodedValuesLayout::new(&[
			Type::Uint8, // id (FlowNodeId)
			Type::Uint8, // flow (FlowId)
			Type::Uint1, // type (FlowNodeType discriminator)
			Type::Blob,  // data (serialized type-specific data)
		])
	});
}

pub(crate) mod flow_node_by_flow {
	use once_cell::sync::Lazy;
	use reifydb_core::value::encoded::layout::EncodedValuesLayout;
	use reifydb_type::value::r#type::Type;

	pub(crate) const FLOW: usize = 0;
	pub(crate) const ID: usize = 1;

	pub(crate) static LAYOUT: Lazy<EncodedValuesLayout> = Lazy::new(|| {
		EncodedValuesLayout::new(&[
			Type::Uint8, // flow (FlowId)
			Type::Uint8, // id (FlowNodeId)
		])
	});
}
