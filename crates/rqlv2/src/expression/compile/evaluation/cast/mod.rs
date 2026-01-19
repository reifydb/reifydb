// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Cast operations for type conversions

mod any;
mod blob;
mod boolean;
mod number;
mod temporal;
mod text;
mod uuid;

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::r#type::Type;

use crate::expression::types::{EvalError, EvalResult};

/// Main entry point for casting column data to a target type.
///
/// This dispatcher routes to the appropriate conversion function based on the target type.
pub(crate) fn cast_column_data(data: &ColumnData, target: Type) -> EvalResult<ColumnData> {
	// Handle Undefined columns - they stay undefined in the target type
	if let ColumnData::Undefined(container) = data {
		let mut result = ColumnData::with_capacity(target, container.len());
		for _ in 0..container.len() {
			result.push_undefined();
		}
		return Ok(result);
	}

	let source_type = data.get_type();

	// Same type - no conversion needed
	if target == source_type {
		return Ok(data.clone());
	}

	// Route to appropriate converter based on target type
	match (source_type, target) {
		// From Any type
		(Type::Any, _) => any::from_any(data, target),

		// To number types
		(_, target) if target.is_number() => number::to_number(data, target),

		// To blob
		(_, target) if target.is_blob() => blob::to_blob(data),

		// To boolean
		(_, target) if target.is_bool() => boolean::to_boolean(data),

		// To text
		(_, target) if target.is_utf8() => text::to_text(data),

		// To temporal
		(_, target) if target.is_temporal() => temporal::to_temporal(data, target),

		// To UUID
		(_, target) if target.is_uuid() => uuid::to_uuid(data, target),
		(source, target) if source.is_uuid() || target.is_uuid() => uuid::to_uuid(data, target),

		// Unsupported cast
		_ => Err(EvalError::UnsupportedCast {
			from: format!("{:?}", source_type),
			to: format!("{:?}", target),
		}),
	}
}
