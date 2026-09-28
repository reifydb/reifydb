// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef, UInt64Array};
use arrow_buffer::{BooleanBuffer, NullBuffer, ScalarBuffer};
use reifydb_value::{
	Result,
	util::{bitmap, kernel},
	value::container::{bool_array, fixed_array, primitive, varlen_array},
};

use crate::{
	internal_err,
	value::column::{
		ColumnBuffer,
		buffer::{map_decimal, on_decimal, with_container},
		builder::ColumnBuilder,
	},
};

macro_rules! map_container {
	($self:expr, |$a:ident| $native:expr, |$u:ident| $fixed:expr, |$v:ident| $varlen:expr) => {
		match $self {
			ColumnBuffer::Float4($a) => ColumnBuffer::Float4($native),
			ColumnBuffer::Float8($a) => ColumnBuffer::Float8($native),
			ColumnBuffer::Int1($a) => ColumnBuffer::Int1($native),
			ColumnBuffer::Int2($a) => ColumnBuffer::Int2($native),
			ColumnBuffer::Int4($a) => ColumnBuffer::Int4($native),
			ColumnBuffer::Int8($a) => ColumnBuffer::Int8($native),
			ColumnBuffer::Uint1($a) => ColumnBuffer::Uint1($native),
			ColumnBuffer::Uint2($a) => ColumnBuffer::Uint2($native),
			ColumnBuffer::Uint4($a) => ColumnBuffer::Uint4($native),
			ColumnBuffer::Uint8($a) => ColumnBuffer::Uint8($native),
			ColumnBuffer::Int16($u) => ColumnBuffer::Int16($fixed),
			ColumnBuffer::Uint16($u) => ColumnBuffer::Uint16($fixed),
			ColumnBuffer::Bool(_) => {
				unreachable!(
					"map_container! must not be called on Bool variant directly; handle it explicitly"
				)
			}
			ColumnBuffer::Utf8 {
				container: $v,
				max_bytes,
			} => ColumnBuffer::Utf8 {
				container: $varlen,
				max_bytes: *max_bytes,
			},
			ColumnBuffer::Date($a) => ColumnBuffer::Date($native),
			ColumnBuffer::DateTime($a) => ColumnBuffer::DateTime($native),
			ColumnBuffer::Time($a) => ColumnBuffer::Time($native),
			ColumnBuffer::Duration($a) => ColumnBuffer::Duration($native),

			ColumnBuffer::IdentityId($u) => ColumnBuffer::IdentityId($fixed),
			ColumnBuffer::DictionaryId {
				..
			} => {
				unreachable!(
					"map_container! must not be called on DictionaryId variant directly; handle it explicitly"
				)
			}
			ColumnBuffer::Uuid4($u) => ColumnBuffer::Uuid4($fixed),
			ColumnBuffer::Uuid7($u) => ColumnBuffer::Uuid7($fixed),
			ColumnBuffer::Blob {
				container: $v,
				max_bytes,
			} => ColumnBuffer::Blob {
				container: $varlen,
				max_bytes: *max_bytes,
			},
			ColumnBuffer::Decimal(d) => ColumnBuffer::Decimal(map_decimal!(d, |$a| $native)),
			ColumnBuffer::Any {
				container: $v,
				declared_type,
			} => ColumnBuffer::Any {
				container: $varlen,
				declared_type: declared_type.clone(),
			},
			ColumnBuffer::Digest {
				container: $v,
				inner,
				accuracy,
			} => ColumnBuffer::Digest {
				container: $varlen,
				inner: inner.clone(),
				accuracy: *accuracy,
			},
			ColumnBuffer::None {
				..
			} => {
				unreachable!(
					"map_container! must not be called on the None variant directly; handle it explicitly"
				)
			}
		}
	};
}

impl ColumnBuffer {
	pub fn take(&self, num: usize) -> ColumnBuffer {
		match self {
			ColumnBuffer::Bool(a) => ColumnBuffer::Bool(bool_array::take(a, num)),
			ColumnBuffer::None {
				array,
				nulls,
			} => ColumnBuffer::none_sized(num.min(array.len()), nulls.is_none()),
			ColumnBuffer::DictionaryId {
				container,
				dictionary_id,
			} => ColumnBuffer::DictionaryId {
				container: fixed_array::take(container, num),
				dictionary_id: *dictionary_id,
			},
			_ => map_container!(self, |a| primitive::take(a, num), |u| fixed_array::take(u, num), |v| {
				varlen_array::take(v, num)
			}),
		}
	}

	pub fn slice(&self, start: usize, end: usize) -> ColumnBuffer {
		if let Some(nulls) = self.nulls() {
			assert!(start <= end, "ColumnBuffer::slice: start {start} > end {end}");
			assert!(end <= nulls.len(), "ColumnBuffer::slice: end {end} > len {}", nulls.len());
		}
		match self {
			ColumnBuffer::Bool(a) => ColumnBuffer::Bool(bool_array::slice(a, start, end)),
			ColumnBuffer::None {
				nulls,
				..
			} => ColumnBuffer::none_sized(end - start, nulls.is_none()),
			ColumnBuffer::DictionaryId {
				container,
				dictionary_id,
			} => ColumnBuffer::DictionaryId {
				container: fixed_array::slice(container, start, end),
				dictionary_id: *dictionary_id,
			},
			_ => map_container!(
				self,
				|a| primitive::slice(a, start, end),
				|u| fixed_array::slice(u, start, end),
				|v| varlen_array::slice(v, start, end)
			),
		}
	}

	pub fn extract_rows(&self, indices: &[usize]) -> Result<ColumnBuffer> {
		rows_in_range(indices.iter().copied(), self.len())?;
		let kept = BooleanBuffer::collect_bool(indices.len(), |row| !self.none_at(indices[row]));
		Ok(self.selected(indices, kept))
	}

	pub fn extract_rows_or_none(&self, picks: &[Option<usize>]) -> Result<ColumnBuffer> {
		rows_in_range(picks.iter().flatten().copied(), self.len())?;
		let indices: Vec<usize> = picks.iter().map(|pick| pick.unwrap_or(0)).collect();
		let kept = BooleanBuffer::collect_bool(picks.len(), |row| {
			picks[row].is_some_and(|index| !self.none_at(index))
		});
		Ok(self.selected(&indices, kept))
	}

	fn selected(&self, indices: &[usize], kept: BooleanBuffer) -> ColumnBuffer {
		if let ColumnBuffer::None {
			..
		} = self
		{
			return ColumnBuffer::none_sized(indices.len(), self.nulls().is_none());
		}
		let all_kept = kept.count_set_bits() == kept.len();
		let selected = if all_kept {
			take_rows(self, &picks(indices))
		} else {
			fill_rows(self, indices, &kept)
		};
		if self.nulls().is_some() || !all_kept {
			return selected.replace_nulls(Some(NullBuffer::new(kept)));
		}
		selected
	}

	pub fn gather(&self, indices: &[usize]) -> Result<ColumnBuffer> {
		if let ColumnBuffer::None {
			..
		} = self
		{
			rows_in_range(indices.iter().copied(), self.len())?;
			return Ok(ColumnBuffer::none_sized(indices.len(), self.nulls().is_none()));
		}
		match self.clone().split_nulls() {
			(inner, Some(nulls)) if !self.keeps_own_nulls() => {
				let inner = inner.gather(indices)?;
				let bits = bitmap::reorder(nulls.inner(), indices);
				Ok(inner.replace_nulls(Some(NullBuffer::new(bits))))
			}
			(mut inner, _) => {
				inner.reorder(indices)?;
				Ok(inner)
			}
		}
	}
}

fn rows_in_range(mut indices: impl Iterator<Item = usize>, len: usize) -> Result<()> {
	match indices.find(|&index| index >= len) {
		Some(index) => internal_err!("row index {} out of range for a column of {} rows", index, len),
		None => Ok(()),
	}
}

fn picks(indices: &[usize]) -> UInt64Array {
	UInt64Array::new(ScalarBuffer::from(indices.iter().map(|&row| row as u64).collect::<Vec<_>>()), None)
}

fn take_rows(source: &ColumnBuffer, picks: &UInt64Array) -> ColumnBuffer {
	match source {
		ColumnBuffer::Bool(a) => ColumnBuffer::Bool(kernel::taken(a, picks)),
		ColumnBuffer::Uint16(a) => ColumnBuffer::Uint16(kernel::taken(a, picks)),
		ColumnBuffer::DictionaryId {
			container,
			dictionary_id,
		} => ColumnBuffer::DictionaryId {
			container: kernel::taken(container, picks),
			dictionary_id: *dictionary_id,
		},
		_ => map_container!(source, |a| kernel::taken(a, picks), |u| kernel::taken(u, picks), |v| {
			kernel::taken(v, picks)
		}),
	}
}

fn fill_rows(source: &ColumnBuffer, indices: &[usize], kept: &BooleanBuffer) -> ColumnBuffer {
	let row = default_row(source);
	let filler = as_array(&row);
	let pairs: Vec<(usize, usize)> = (0..indices.len())
		.map(|slot| {
			if kept.value(slot) {
				(0, indices[slot])
			} else {
				(1, 0)
			}
		})
		.collect();
	match source {
		ColumnBuffer::Bool(a) => ColumnBuffer::Bool(kernel::interleaved(a, filler, &pairs)),
		ColumnBuffer::Uint16(a) => ColumnBuffer::Uint16(kernel::interleaved(a, filler, &pairs)),
		ColumnBuffer::DictionaryId {
			container,
			dictionary_id,
		} => ColumnBuffer::DictionaryId {
			container: kernel::interleaved(container, filler, &pairs),
			dictionary_id: *dictionary_id,
		},
		_ => map_container!(
			source,
			|a| kernel::interleaved(a, filler, &pairs),
			|u| kernel::interleaved(u, filler, &pairs),
			|v| kernel::interleaved(v, filler, &pairs)
		),
	}
}

pub(crate) fn as_array(buffer: &ColumnBuffer) -> &dyn Array {
	match buffer {
		ColumnBuffer::Bool(a) => a,
		ColumnBuffer::Uint16(a) => a,
		ColumnBuffer::None {
			array,
			..
		} => array,
		ColumnBuffer::Decimal(d) => {
			on_decimal!(d, |a| a as &dyn Array)
		}
		_ => with_container!(buffer, |a| a as &dyn Array, |t| t as &dyn Array, |u| u as &dyn Array, |v| v
			as &dyn Array),
	}
}

impl ColumnBuffer {
	pub fn to_array_ref(&self) -> ArrayRef {
		let array = as_array(self);
		array.slice(0, array.len())
	}
}

pub(crate) fn wrap_array(shell: &ColumnBuffer, array: &dyn Array) -> ColumnBuffer {
	match shell {
		ColumnBuffer::Bool(_) => ColumnBuffer::Bool(kernel::downcast(array)),
		ColumnBuffer::Uint16(_) => ColumnBuffer::Uint16(kernel::downcast(array)),
		ColumnBuffer::None {
			nulls,
			..
		} => ColumnBuffer::none_sized(array.len(), nulls.is_none()),
		ColumnBuffer::DictionaryId {
			dictionary_id,
			..
		} => ColumnBuffer::DictionaryId {
			container: kernel::downcast(array),
			dictionary_id: *dictionary_id,
		},
		_ => map_container!(shell, |_a| kernel::downcast(array), |_u| kernel::downcast(array), |_v| {
			kernel::downcast(array)
		}),
	}
}

pub(crate) fn default_row(source: &ColumnBuffer) -> ColumnBuffer {
	let (bare, _) = source.clone().split_nulls();
	let mut builder = ColumnBuilder::like(&bare, 1);
	builder.push_none();
	builder.finish()
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{Value, value_type::ValueType};

	use crate::value::column::ColumnBuffer;

	#[test]
	fn extract_rows_past_the_end_fails() {
		// A row past the end is a bug, so it must fail naming the row and length, never read as a none row.
		let column = ColumnBuffer::int4([1, 2]);

		let error = column.extract_rows(&[0, 2]).unwrap_err();

		assert!(error.diagnostic().message.contains("row index 2 out of range for a column of 2 rows"));
	}

	#[test]
	fn extract_rows_keeps_a_none_row_at_a_valid_index() {
		// The range check must not turn a real none row into an error or a value.
		let column = ColumnBuffer::int4_with_bitvec([1, 2], vec![false, true]);

		let extracted = column.extract_rows(&[1, 0]).unwrap();

		assert_eq!(extracted.get_value(0), Value::Int4(2));
		assert_eq!(extracted.get_value(1), Value::none_of(ValueType::Int4));
	}

	#[test]
	fn extract_rows_or_none_gives_a_none_row_for_no_pick() {
		// A left join row with no match must read as none, never as the row at a placeholder index.
		let column = ColumnBuffer::int4([7, 8]);

		let extracted = column.extract_rows_or_none(&[Some(1), None, Some(0)]).unwrap();

		assert_eq!(extracted.get_value(0), Value::Int4(8));
		assert_eq!(extracted.get_value(1), Value::none_of(ValueType::Int4));
		assert_eq!(extracted.get_value(2), Value::Int4(7));
	}

	#[test]
	fn extract_rows_or_none_fails_on_a_pick_past_the_end() {
		// A matched pick past the end is a bug, so it must fail like extract rows does.
		let column = ColumnBuffer::int4([7, 8]);

		let error = column.extract_rows_or_none(&[None, Some(2)]).unwrap_err();

		assert!(error.diagnostic().message.contains("row index 2 out of range for a column of 2 rows"));
	}
}
