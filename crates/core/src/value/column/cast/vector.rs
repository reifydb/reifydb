// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::iter::repeat_n;

use num_traits::ToPrimitive;
use reifydb_value::{
	Result,
	error::{ConstraintKind, TypeError},
	fragment::LazyFragment,
	value::{Value, value_type::ValueType, vector::VectorValue},
};

use crate::value::column::buffer::ColumnBuffer;

pub fn to_vector(data: &ColumnBuffer, dims: u32, lazy_fragment: impl LazyFragment) -> Result<ColumnBuffer> {
	let rows: Vec<Option<Vec<f32>>> = match data {
		ColumnBuffer::Any(container) => {
			let mut rows = Vec::with_capacity(container.len());
			for idx in 0..container.len() {
				if !container.is_defined(idx) {
					rows.push(None);
					continue;
				}
				match container.get_value(idx) {
					Value::Any(inner) => rows.push(list_to_f32(&inner, &lazy_fragment)?),
					other => rows.push(list_to_f32(&other, &lazy_fragment)?),
				}
			}
			rows
		}
		ColumnBuffer::Blob {
			container,
			..
		} => {
			let mut rows = Vec::with_capacity(container.len());
			for idx in 0..container.len() {
				match container.get(idx) {
					Some(bytes) if container.is_defined(idx) => {
						if bytes.len() % 4 != 0 {
							return Err(TypeError::UnsupportedCast {
								from: ValueType::Blob,
								to: ValueType::Vector(0),
								fragment: lazy_fragment.fragment(),
							}
							.into());
						}
						rows.push(Some(VectorValue::from_le_bytes(bytes).as_slice().to_vec()));
					}
					_ => rows.push(None),
				}
			}
			rows
		}

		ColumnBuffer::Vector(container) => (0..container.len())
			.map(|idx| container.is_defined(idx).then(|| container.get(idx).unwrap_or_default().to_vec()))
			.collect(),
		other => {
			return Err(TypeError::UnsupportedCast {
				from: other.get_type(),
				to: ValueType::Vector(0),
				fragment: lazy_fragment.fragment(),
			}
			.into());
		}
	};

	let mut values: Vec<f32> = Vec::with_capacity(rows.len() * dims as usize);
	let mut bitvec: Vec<bool> = Vec::with_capacity(rows.len());
	for row in &rows {
		match row {
			Some(row) if row.len() == dims as usize => {
				values.extend_from_slice(row);
				bitvec.push(true);
			}
			Some(row) => {
				return Err(TypeError::ConstraintViolation {
					kind: ConstraintKind::VectorDimension {
						actual: row.len(),
						expected: dims as usize,
					},
					message: format!(
						"VECTOR value has {} dimensions (column requires {})",
						row.len(),
						dims
					),
					fragment: lazy_fragment.fragment(),
				}
				.into());
			}
			None => {
				values.extend(repeat_n(0.0f32, dims as usize));
				bitvec.push(false);
			}
		}
	}

	Ok(ColumnBuffer::vector_with_bitvec(dims, values, bitvec))
}

fn list_to_f32(value: &Value, lazy_fragment: &impl LazyFragment) -> Result<Option<Vec<f32>>> {
	match value {
		Value::None {
			..
		} => Ok(None),
		Value::Vector(v) => Ok(Some(v.as_slice().to_vec())),
		Value::List(items) => {
			let mut out = Vec::with_capacity(items.len());
			for item in items {
				match numeric_to_f32(item) {
					Some(f) => out.push(f),
					None => {
						return Err(TypeError::UnsupportedCast {
							from: item.get_type(),
							to: ValueType::Vector(0),
							fragment: lazy_fragment.fragment(),
						}
						.into());
					}
				}
			}
			Ok(Some(out))
		}
		other => Err(TypeError::UnsupportedCast {
			from: other.get_type(),
			to: ValueType::Vector(0),
			fragment: lazy_fragment.fragment(),
		}
		.into()),
	}
}

fn numeric_to_f32(value: &Value) -> Option<f32> {
	match value {
		Value::Float4(v) => Some(v.value()),
		Value::Float8(v) => Some(v.value() as f32),
		Value::Int1(v) => Some(*v as f32),
		Value::Int2(v) => Some(*v as f32),
		Value::Int4(v) => Some(*v as f32),
		Value::Int8(v) => Some(*v as f32),
		Value::Int16(v) => Some(*v as f32),
		Value::Uint1(v) => Some(*v as f32),
		Value::Uint2(v) => Some(*v as f32),
		Value::Uint4(v) => Some(*v as f32),
		Value::Uint8(v) => Some(*v as f32),
		Value::Uint16(v) => Some(*v as f32),

		Value::Decimal(v) => v.0.to_f64().map(|f| f as f32),
		Value::Int(v) => v.0.to_f64().map(|f| f as f32),
		Value::Uint(v) => v.0.to_f64().map(|f| f as f32),
		_ => None,
	}
}
