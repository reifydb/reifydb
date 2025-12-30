// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

//! Operator metadata trait for diagnostics.
//!
//! This module provides the `OperatorInfo` trait that all operators implement
//! to provide metadata for error diagnostics and debugging.

use reifydb_core::interface::FlowNodeId;

/// Trait for operators to provide metadata for diagnostics.
///
/// All operators (native and FFI) should implement this trait to provide
/// consistent metadata that can be included in error diagnostics.
pub trait OperatorInfo {
	/// Returns the operator name (e.g., "Filter", "Map", "Join").
	fn operator_name(&self) -> &'static str;

	/// Returns the operator version.
	/// Defaults to the crate version.
	fn operator_version(&self) -> &'static str {
		env!("CARGO_PKG_VERSION")
	}

	/// Returns the operator's node ID.
	fn operator_id(&self) -> FlowNodeId;
}

/// Macro to implement OperatorInfo for a struct with a `node` field.
///
/// Usage:
/// ```ignore
/// impl_operator_info!(FilterOperator, "Filter");
/// ```
#[macro_export]
macro_rules! impl_operator_info {
	($type:ty, $name:literal) => {
		impl $crate::operator::info::OperatorInfo for $type {
			fn operator_name(&self) -> &'static str {
				$name
			}

			fn operator_id(&self) -> reifydb_core::interface::FlowNodeId {
				self.node
			}
		}
	};
	($type:ty, $name:literal, $id_field:ident) => {
		impl $crate::operator::info::OperatorInfo for $type {
			fn operator_name(&self) -> &'static str {
				$name
			}

			fn operator_id(&self) -> reifydb_core::interface::FlowNodeId {
				self.$id_field
			}
		}
	};
}
