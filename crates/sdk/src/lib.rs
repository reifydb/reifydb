// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! ReifyDB Operator SDK

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod catalog;
pub mod error;
pub mod ffi;
pub mod flow;
pub mod marshal;
pub mod operator;
pub mod state;
pub mod store;
pub mod testing;

pub use catalog::Catalog;
pub use error::{FFIError, Result};
pub use flow::{FlowChange, FlowChangeBuilder, FlowChangeOrigin, FlowDiff};
pub use operator::{FFIOperator, FFIOperatorMetadata, FFIOperatorWithMetadata, OperatorColumnDef, OperatorContext};
pub use reifydb_core::{
	CommitVersion, CowVec, Row,
	interface::{
		ColumnDef, ColumnId, FlowNodeId, NamespaceDef, NamespaceId, PrimaryKeyDef, PrimaryKeyId, PrimitiveId,
		TableDef, TableId,
	},
	key::EncodableKey,
	value::{
		column::Columns,
		encoded::{EncodedKey, EncodedValues, EncodedValuesLayout},
	},
};
pub use state::{
	FFIKeyedStateful, FFIRawStatefulOperator, FFISingleStateful, FFIWindowStateful, RowNumberProvider, State,
};
pub use store::Store;

// Prelude module for convenient imports
pub mod prelude {
	// Capabilities from ABI
	pub use reifydb_abi::{
		CAPABILITY_ALL_STANDARD, CAPABILITY_DELETE, CAPABILITY_DROP, CAPABILITY_INSERT, CAPABILITY_PULL,
		CAPABILITY_TICK, CAPABILITY_UPDATE, has_capability,
	};
	// Core types from reifydb-core
	pub use reifydb_core::{
		CommitVersion, CowVec, Row,
		interface::{
			ColumnDef, ColumnId, FlowNodeId, NamespaceDef, NamespaceId, PrimaryKeyDef, PrimaryKeyId,
			PrimitiveId, TableDef, TableId,
		},
		key::EncodableKey,
		value::{
			column::Columns,
			encoded::{EncodedKey, EncodedValues, EncodedValuesLayout},
		},
	};
	// Type system from reifydb-type
	pub use reifydb_type::{RowNumber, Type, TypeConstraint, Value};

	// Main SDK types
	pub use crate::{
		Catalog, FFIKeyedStateful, FFIOperator, FFIOperatorMetadata, FFIOperatorWithMetadata,
		FFIRawStatefulOperator, FFISingleStateful, FFIWindowStateful, FlowChange, FlowChangeBuilder,
		FlowChangeOrigin, FlowDiff, OperatorColumnDef, OperatorContext, RowNumberProvider, State, Store,
		error::{FFIError, Result},
	};
}
