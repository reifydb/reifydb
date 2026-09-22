// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use reifydb_core::{
	error::diagnostic::operation,
	metrics::heap::HeapSize,
	value::column::{
		buffer::ColumnBuffer,
		builder::ColumnBuilder,
		columns::Columns,
		view::group_by::{GroupId, GroupRows, GroupSlots},
	},
};
use reifydb_routine_abi::{
	Accumulator, Arity, Function, FunctionKind, LiteralArgument, LiteralKind, Routine, RoutineInfo,
	context::FunctionContext, error::RoutineError,
};
use reifydb_value::{
	error,
	fragment::Fragment,
	value::{
		Value,
		container::digest_array,
		digest::{Digest, DigestError, literal::parse_accuracy},
		value_type::ValueType,
	},
};

pub struct StatsDigest {
	info: RoutineInfo,
}

impl Default for StatsDigest {
	fn default() -> Self {
		Self::new()
	}
}

impl StatsDigest {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("stats::digest"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for StatsDigest {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Any
	}

	fn propagates_options(&self) -> bool {
		false
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, _args: &Columns) -> Result<Columns, RoutineError> {
		Err(RoutineError::FunctionExecutionFailed {
			function: ctx.fragment.clone(),
			reason: "stats::digest is only supported inside window or aggregate".to_string(),
		})
	}
}

impl Function for StatsDigest {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Aggregate]
	}

	fn arity(&self) -> Arity {
		Arity::Any
	}

	fn accumulator(
		&self,
		ctx: &mut FunctionContext<'_>,
		literals: &[LiteralArgument],
	) -> Result<Option<Box<dyn Accumulator>>, RoutineError> {
		let accuracy = match literals {
			[] => None,
			[literal] => Some(accuracy_literal(literal)?),
			_ => {
				return Err(RoutineError::FunctionArityMismatch {
					function: ctx.fragment.clone(),
					expected: 2,
					actual: literals.len() + 1,
				});
			}
		};
		Ok(Some(Box::new(DigestAccumulator {
			function: ctx.fragment.clone(),
			accuracy,
			seen: None,
			digests: GroupSlots::new(),
		})))
	}

	fn max_literal_arguments(&self) -> usize {
		1
	}
}

fn accuracy_literal(literal: &LiteralArgument) -> Result<u32, RoutineError> {
	let parsed = match literal.kind {
		LiteralKind::Number => parse_accuracy(literal.fragment.text()),
		LiteralKind::None
		| LiteralKind::Bool
		| LiteralKind::Text
		| LiteralKind::Temporal
		| LiteralKind::Duration => Err(DigestError::AccuracyNotANumber),
	};
	parsed.map_err(|failure| {
		let fragment = literal.fragment.clone();
		let diagnostic = match failure {
			DigestError::AccuracyOutOfRange => operation::aggregate_accuracy_out_of_range(fragment),
			DigestError::AccuracyNotWholePpm => operation::aggregate_accuracy_not_whole_ppm(fragment),
			DigestError::AccuracyNotANumber => operation::aggregate_accuracy_not_a_number(fragment),
			other => {
				return RoutineError::FunctionExecutionFailed {
					function: fragment,
					reason: other.to_string(),
				};
			}
		};
		RoutineError::from(error!(diagnostic))
	})
}

fn input_error(function: &Fragment, failure: DigestError) -> RoutineError {
	let fragment = function.clone();
	let diagnostic = match failure {
		DigestError::UnsupportedInnerType {
			inner,
		} => operation::aggregate_digest_unsupported_input(fragment, function.text(), inner),
		DigestError::InputTypeMismatch {
			expected,
			actual,
		} => operation::aggregate_digest_input_mismatch(fragment, expected, actual),
		DigestError::MergeMismatch {
			left_inner,
			left_accuracy,
			right_inner,
			right_accuracy,
		} => operation::aggregate_digest_merge_mismatch(
			fragment,
			digest_type(left_inner, left_accuracy),
			digest_type(right_inner, right_accuracy),
		),
		DigestError::DurationMonthPart => operation::aggregate_digest_month_part(fragment),
		DigestError::DurationConversion(conversion) => return RoutineError::from(conversion),
		other => {
			return RoutineError::FunctionExecutionFailed {
				function: fragment,
				reason: other.to_string(),
			};
		}
	};
	RoutineError::from(error!(diagnostic))
}

fn digest_type(inner: ValueType, accuracy: u32) -> ValueType {
	ValueType::Digest {
		inner: Box::new(inner),
		accuracy,
	}
}

struct DigestAccumulator {
	function: Fragment,
	accuracy: Option<u32>,
	seen: Option<(ValueType, u32)>,
	digests: GroupSlots<Option<Digest>>,
}

impl DigestAccumulator {
	fn create(&mut self, inner: ValueType, accuracy: u32) -> Result<Digest, RoutineError> {
		let (inner, accuracy) = self.seen.clone().unwrap_or((inner, accuracy));
		let digest = Digest::new(inner, accuracy).map_err(|failure| input_error(&self.function, failure))?;
		self.seen = Some((digest.inner().clone(), digest.accuracy()));
		Ok(digest)
	}

	fn add(&mut self, slot: &mut Option<Digest>, value: &Value) -> Result<(), RoutineError> {
		if let Value::Digest(part) = value {
			return self.merge(slot, part);
		}
		let Some(accuracy) = self.accuracy else {
			return Err(error!(operation::aggregate_accuracy_required(
				self.function.clone(),
				self.function.text()
			))
			.into());
		};
		let digest = match slot {
			Some(digest) => digest,
			None => slot.insert(self.create(value.get_type(), accuracy)?),
		};
		digest.add_value(value).map_err(|failure| input_error(&self.function, failure))
	}

	fn merge(&mut self, slot: &mut Option<Digest>, part: &Digest) -> Result<(), RoutineError> {
		if self.accuracy.is_some() {
			return Err(error!(operation::aggregate_accuracy_from_digest_type(
				self.function.clone(),
				self.function.text()
			))
			.into());
		}
		let digest = match slot {
			Some(digest) => digest,
			None => slot.insert(self.create(part.inner().clone(), part.accuracy())?),
		};
		digest.merge(part).map_err(|failure| input_error(&self.function, failure))
	}
}

impl Accumulator for DigestAccumulator {
	fn heap_size(&self) -> usize {
		self.digests.heap_size()
	}

	fn update(&mut self, args: &Columns, groups: &GroupRows) -> Result<(), RoutineError> {
		let column = &args[0];
		let (data, _) = column.unwrap_option();
		if let ColumnBuffer::Digest {
			inner,
			accuracy,
			..
		} = data && self.seen.is_none()
		{
			self.seen = Some((inner.clone(), *accuracy));
		}
		for &(group, ref rows) in groups.iter() {
			let mut slot = self.digests.remove(group).flatten();
			for &row in rows {
				match data {
					ColumnBuffer::Digest {
						container,
						..
					} => {
						if !column.is_defined(row) {
							continue;
						}
						let part = digest_array::get(container, row).unwrap_or_else(|| {
							panic!("defined digest row {row} holds no digest")
						});
						self.merge(&mut slot, &part)?;
					}
					_ => {
						let value = column.get_value(row);
						if !matches!(value, Value::None { .. }) {
							self.add(&mut slot, &value)?;
						}
					}
				}
			}
			self.digests.insert(group, slot);
		}
		Ok(())
	}

	fn finalize(&mut self) -> Result<(Vec<GroupId>, ColumnBuffer), RoutineError> {
		let digests = mem::take(&mut self.digests);
		let output = match (self.seen.take(), self.accuracy) {
			(Some((inner, accuracy)), _) => digest_type(inner, accuracy),
			(None, Some(accuracy)) => digest_type(ValueType::Float8, accuracy),
			(None, None) => {
				let keys: Vec<GroupId> = digests.into_iter().map(|(group, _)| group).collect();
				let data = ColumnBuffer::none_typed(ValueType::Any, keys.len());
				return Ok((keys, data));
			}
		};
		let mut keys = Vec::with_capacity(digests.len());
		let mut data = ColumnBuilder::with_capacity(output, digests.len());
		for (group, digest) in digests {
			keys.push(group);
			match digest {
				Some(digest) => data.push_value(Value::Digest(Box::new(digest))),
				None => data.push_none(),
			}
		}
		Ok((keys, data.finish()))
	}
}
