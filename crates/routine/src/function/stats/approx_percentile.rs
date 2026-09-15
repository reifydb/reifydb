// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use num_traits::ToPrimitive;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	Value,
	digest::DigestError,
	value_type::{ValueType, input_types::InputTypes},
};

pub struct ApproxPercentile {
	info: RoutineInfo,
}

impl Default for ApproxPercentile {
	fn default() -> Self {
		Self::new()
	}
}

impl ApproxPercentile {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("stats::approx_percentile"),
		}
	}
}

fn output_type(input: &ValueType) -> ValueType {
	match input {
		ValueType::Option(inner) => output_type(inner),
		ValueType::Digest {
			inner,
			..
		} if **inner == ValueType::Duration => ValueType::Duration,
		_ => ValueType::Float8,
	}
}

fn is_untyped_none(column: &ColumnBuffer) -> bool {
	let (data, bitvec) = column.unwrap_option();
	matches!(data.get_type(), ValueType::Any | ValueType::Boolean)
		&& bitvec.is_some_and(|bits| bits.count_ones() == 0)
}

fn failed(ctx: &FunctionContext, reason: String) -> RoutineError {
	RoutineError::FunctionExecutionFailed {
		function: ctx.fragment.clone(),
		reason,
	}
}

fn percentile_at(ctx: &FunctionContext, p: &ColumnBuffer, row: usize) -> Result<f64, RoutineError> {
	let value = p.get_value(row);
	let number = match &value {
		Value::Float4(v) => Some(f64::from(v.value())),
		Value::Float8(v) => Some(v.value()),
		Value::Int1(v) => Some(f64::from(*v)),
		Value::Int2(v) => Some(f64::from(*v)),
		Value::Int4(v) => Some(f64::from(*v)),
		Value::Int8(v) => Some(*v as f64),
		Value::Int16(v) => Some(*v as f64),
		Value::Uint1(v) => Some(f64::from(*v)),
		Value::Uint2(v) => Some(f64::from(*v)),
		Value::Uint4(v) => Some(f64::from(*v)),
		Value::Uint8(v) => Some(*v as f64),
		Value::Uint16(v) => Some(*v as f64),
		Value::Int(v) => v.0.to_f64(),
		Value::Uint(v) => v.0.to_f64(),
		Value::Decimal(v) => v.0.to_f64(),
		_ => None,
	};
	let number = number.ok_or_else(|| failed(ctx, format!("p {value} cannot be read as a number")))?;
	if !(0.0..=1.0).contains(&number) {
		return Err(failed(ctx, DigestError::PercentileOutOfRange.to_string()));
	}
	Ok(number)
}

impl<'a> Routine<FunctionContext<'a>> for ApproxPercentile {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, input_types: &[ValueType]) -> ValueType {
		input_types.first().map(output_type).unwrap_or(ValueType::Float8)
	}

	fn propagates_options(&self) -> bool {
		false
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let digest_column = &args[0];
		let percentile_column = &args[1];
		let (digest_data, _) = digest_column.unwrap_option();
		let (percentile_data, _) = percentile_column.unwrap_option();

		let row_count = digest_column.len();
		let container = match digest_data {
			ColumnBuffer::Digest {
				container,
				..
			} => container,
			_ if args.len() == 2 && is_untyped_none(digest_column) => {
				return Ok(Columns::new(vec![ColumnWithName::new(
					ctx.fragment.clone(),
					ColumnBuffer::none_typed(ValueType::Float8, row_count),
				)]));
			}
			other => {
				return Err(failed(
					ctx,
					format!(
						"a {} input needs an accuracy and is only supported inside window or aggregate",
						other.get_type()
					),
				));
			}
		};

		if args.len() == 3 {
			return Err(failed(
				ctx,
				"accuracy comes from the digest type, remove the argument".to_string(),
			));
		}

		let result_type = output_type(&digest_data.get_type());
		if is_untyped_none(percentile_column) {
			return Ok(Columns::new(vec![ColumnWithName::new(
				ctx.fragment.clone(),
				ColumnBuffer::none_typed(result_type, row_count),
			)]));
		}

		if !percentile_data.get_type().is_number() {
			return Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: InputTypes::numeric().expected_at(0).to_vec(),
				actual: percentile_data.get_type(),
			});
		}

		let mut result = ColumnBuffer::with_capacity(result_type, row_count);
		for row in 0..row_count {
			if !digest_column.is_defined(row) || !percentile_column.is_defined(row) {
				result.push_none();
				continue;
			}
			let p = percentile_at(ctx, percentile_data, row)?;
			let digest = container
				.get(row)
				.unwrap_or_else(|| panic!("defined digest row {row} holds no digest"));
			let value = digest.percentile_value(p).map_err(|error| failed(ctx, error.to_string()))?;
			result.push_value(value);
		}

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), result)]))
	}
}

impl Function for ApproxPercentile {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Range(2, 3)
	}
}
