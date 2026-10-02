// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	iter::{Enumerate, FilterMap},
	mem,
	sync::Arc,
	vec::IntoIter as VecIntoIter,
};

use arrow_array::{
	Array, ArrayRef, Decimal128Array, Decimal256Array, Float32Array, Float64Array, PrimitiveArray, UInt32Array,
	make_array,
};
use arrow_buffer::{NullBuffer, ScalarBuffer, i256};
use arrow_row::{RowConverter, Rows, SortField};
use arrow_schema::{ArrowError, FieldRef};
use arrow_select::take::take;
use indexmap::{IndexMap, IndexSet};
use reifydb_codec::key::{encoded::EncodedKey, serializer::KeySerializer};
use reifydb_value::{
	Result,
	error::{Error, TypeError},
	fragment::Fragment,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		constraint::{precision::Precision, scale::Scale},
		container::decimal_array::{DECIMAL128_MAX_PRECISION, DecimalArray, data_type},
		decimal::unscaled,
		value_type::ValueType,
	},
};

use crate::{
	internal_error,
	metrics::heap::HeapSize,
	value::column::{builder::ColumnBuilder, factory::none, key::extend_keys},
};

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(pub u32);

impl GroupId {
	pub fn index(self) -> usize {
		self.0 as usize
	}
}

pub type GroupRows = Vec<(GroupId, Vec<usize>)>;

#[derive(Debug, Clone)]
pub struct GroupSlots<T> {
	slots: Vec<Option<T>>,
	occupied: usize,
}

impl<T> Default for GroupSlots<T> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T> GroupSlots<T> {
	pub fn new() -> Self {
		Self {
			slots: Vec::new(),
			occupied: 0,
		}
	}

	pub fn len(&self) -> usize {
		self.occupied
	}

	pub fn is_empty(&self) -> bool {
		self.occupied == 0
	}

	pub fn get(&self, group: GroupId) -> Option<&T> {
		self.slots.get(group.index()).and_then(Option::as_ref)
	}

	pub fn insert(&mut self, group: GroupId, value: T) {
		self.reserve_for(group);
		if self.slots[group.index()].replace(value).is_none() {
			self.occupied += 1;
		}
	}

	pub fn get_or_insert_with(&mut self, group: GroupId, default: impl FnOnce() -> T) -> &mut T {
		self.reserve_for(group);
		if self.slots[group.index()].is_none() {
			self.occupied += 1;
		}
		self.slots[group.index()].get_or_insert_with(default)
	}

	pub fn or_insert(&mut self, group: GroupId, default: T) -> &mut T {
		self.get_or_insert_with(group, || default)
	}

	pub fn remove(&mut self, group: GroupId) -> Option<T> {
		let removed = self.slots.get_mut(group.index()).and_then(Option::take);
		if removed.is_some() {
			self.occupied -= 1;
		}
		removed
	}

	fn reserve_for(&mut self, group: GroupId) {
		if self.slots.len() <= group.index() {
			self.slots.resize_with(group.index() + 1, || None);
		}
	}
}

impl<T: HeapSize> HeapSize for GroupSlots<T> {
	fn heap_size(&self) -> usize {
		self.slots.heap_size()
	}
}

fn occupied_slot<T>((index, slot): (usize, Option<T>)) -> Option<(GroupId, T)> {
	slot.map(|value| (GroupId(index as u32), value))
}

impl<T> IntoIterator for GroupSlots<T> {
	type Item = (GroupId, T);
	type IntoIter = FilterMap<Enumerate<VecIntoIter<Option<T>>>, fn((usize, Option<T>)) -> Option<(GroupId, T)>>;

	fn into_iter(self) -> Self::IntoIter {
		self.slots
			.into_iter()
			.enumerate()
			.filter_map(occupied_slot as fn((usize, Option<T>)) -> Option<(GroupId, T)>)
	}
}

#[derive(Debug, Default, Clone)]
pub struct GroupKeyDict {
	entries: IndexSet<EncodedKey>,
	row_lens: Vec<usize>,
	converter: Option<Arc<RowConverter>>,
	key_types: Vec<ValueType>,
	key_rows: Vec<Vec<(FieldRef, ArrayRef)>>,
}

impl GroupKeyDict {
	pub fn new() -> Self {
		Self {
			entries: IndexSet::new(),
			row_lens: Vec::new(),
			converter: None,
			key_types: Vec::new(),
			key_rows: Vec::new(),
		}
	}

	pub fn len(&self) -> usize {
		self.entries.len()
	}

	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}

	pub fn key_values(&self, group: GroupId) -> Option<Vec<Value>> {
		self.key_rows
			.iter()
			.map(|parts| {
				let mut index = group.index();
				for (field, array) in parts {
					if index < array.len() {
						return ColumnView::try_from((array, field.as_ref()))
							.ok()
							.map(|view| view.get_value(index));
					}
					index -= array.len();
				}
				None
			})
			.collect()
	}

	pub fn key_column(
		&self,
		column: usize,
		declared: Option<ValueType>,
		name: &str,
	) -> Result<(FieldRef, ArrayRef)> {
		let parts = self.key_rows.get(column).map(Vec::as_slice).unwrap_or_default();
		let views = parts.iter().map(ColumnView::try_from).collect::<Result<Vec<_>>>()?;
		let first = views.iter().find_map(|view| {
			(0..view.len())
				.map(|index| view.get_value(index))
				.find(|value| !matches!(value, Value::None { .. }))
		});
		let mut builder = match first.map(|value| value.get_type()).or(declared) {
			Some(key_type) => ColumnBuilder::with_capacity(key_type, self.len()),
			None => ColumnBuilder::from_view(&ColumnView::try_from(&none(name, 0))?),
		};
		for (part, view) in parts.iter().zip(&views) {
			match canonical_floats(part, view) {
				Some(canonical) => builder.append_values(&ColumnView::try_from(&canonical)?)?,
				None => builder.append_values(view)?,
			}
		}
		Ok(builder.finish(name))
	}

	pub(crate) fn intern(&mut self, encoded: &[u8], row_len: usize) -> (GroupId, bool) {
		if let Some(index) = self.entries.get_index_of(encoded) {
			return (GroupId(index as u32), false);
		}
		let (index, _) = self.entries.insert_full(EncodedKey::new(encoded));
		self.row_lens.push(row_len);
		(GroupId(index as u32), true)
	}

	fn remember(&mut self, views: &[ColumnView<'_>], first_rows: &[usize]) -> Result<()> {
		if self.key_rows.len() < views.len() {
			self.key_rows.resize_with(views.len(), Vec::new);
		}
		let indices = UInt32Array::from_iter_values(first_rows.iter().map(|&row| row as u32));
		for (parts, view) in self.key_rows.iter_mut().zip(views) {
			let taken = take(view.array(), &indices, None).map_err(group_key_error)?;
			parts.push((Arc::new(view.field.clone()), taken));
		}
		Ok(())
	}

	pub(crate) fn row_keys(&mut self, views: &[&ColumnView]) -> Result<Rows> {
		let batch_types: Vec<ValueType> = views.iter().map(|view| view.get_type()).collect();
		if self.converter.is_some() {
			let targets: Vec<ValueType> = self
				.key_types
				.iter()
				.zip(&batch_types)
				.map(|(cached, batch)| common_key_type(cached, batch).unwrap_or_else(|| batch.clone()))
				.collect();
			if targets != self.key_types {
				self.rekey(targets)?;
			}
		} else {
			let fields =
				views.iter().map(|view| SortField::new(view.array().data_type().clone())).collect();
			self.converter = Some(Arc::new(RowConverter::new(fields).map_err(group_key_error)?));
			self.key_types = batch_types;
		}
		let converter = self.converter.clone().ok_or_else(|| internal_error!("group key converter missing"))?;
		let mut arrays: Vec<ArrayRef> = Vec::with_capacity(views.len());
		for (view, target) in views.iter().zip(&self.key_types) {
			let (cast, dropped) = cast_key(&make_array(view.array().to_data()), target);
			if dropped > 0 {
				return Err(key_out_of_range(target));
			}
			arrays.push(cast);
		}
		converter.convert_columns(&arrays).map_err(group_key_error)
	}

	fn rekey(&mut self, targets: Vec<ValueType>) -> Result<()> {
		let old =
			self.converter.take().ok_or_else(|| internal_error!("group keys rekeyed before any batch"))?;
		let parser = old.parser();
		let rows =
			self.entries.iter().zip(&self.row_lens).map(|(key, &len)| parser.parse(&key.as_bytes()[..len]));
		let decoded = old.convert_rows(rows).map_err(group_key_error)?;
		let mut arrays: Vec<ArrayRef> = Vec::with_capacity(decoded.len());
		for (array, target) in decoded.into_iter().zip(&targets) {
			let (cast, dropped) = cast_key_array(array, target);
			if dropped > 0 {
				return Err(key_out_of_range(target));
			}
			arrays.push(cast);
		}
		let fields = arrays.iter().map(|array| SortField::new(array.data_type().clone())).collect();
		let converter = Arc::new(RowConverter::new(fields).map_err(group_key_error)?);
		let rows = converter.convert_columns(&arrays).map_err(group_key_error)?;
		let entries = mem::take(&mut self.entries);
		let mut row_lens = Vec::with_capacity(self.row_lens.len());
		for (index, (key, old_len)) in entries.into_iter().zip(&self.row_lens).enumerate() {
			let row = rows.row(index);
			let mut bytes = row.as_ref().to_vec();
			bytes.extend_from_slice(&key.as_bytes()[*old_len..]);
			row_lens.push(row.as_ref().len());
			self.entries.insert(EncodedKey::new(&bytes));
		}
		self.row_lens = row_lens;
		self.converter = Some(converter);
		self.key_types = targets;
		Ok(())
	}
}

fn key_out_of_range(target: &ValueType) -> Error {
	TypeError::NumberOutOfRange {
		target: target.clone(),
		fragment: Fragment::None,
		descriptor: None,
	}
	.into()
}

pub fn common_key_type(left: &ValueType, right: &ValueType) -> Option<ValueType> {
	match (left, right) {
		_ if left == right => Some(left.clone()),
		(
			ValueType::Decimal {
				precision: lp,
				scale: ls,
			},
			ValueType::Decimal {
				precision: rp,
				scale: rs,
			},
		) => {
			let scale = ls.value().max(rs.value());
			let digits = (lp.value() - ls.value()).max(rp.value() - rs.value());
			let precision = (digits + scale).min(unscaled::MAX_DIGITS);
			Some(ValueType::decimal(Precision::new(precision), Scale::new(scale)))
		}
		_ => None,
	}
}

pub fn cast_key(array: &ArrayRef, target: &ValueType) -> (ArrayRef, usize) {
	let (Some(precision), Some(scale)) = (target.precision(), target.scale()) else {
		return (array.clone(), 0);
	};
	if *array.data_type() == data_type(precision, scale) {
		return (array.clone(), 0);
	}
	cast_key_array(array.clone(), target)
}

fn cast_key_array(array: ArrayRef, target: &ValueType) -> (ArrayRef, usize) {
	let (Some(precision), Some(scale)) = (target.precision(), target.scale()) else {
		return (array, 0);
	};
	let decimal = if let Some(array) = array.as_any().downcast_ref::<Decimal128Array>() {
		DecimalArray::Decimal128(array.clone())
	} else if let Some(array) = array.as_any().downcast_ref::<Decimal256Array>() {
		DecimalArray::Decimal256(array.clone())
	} else {
		return (array, 0);
	};
	let (cast, dropped) = rescale_exact(&decimal, precision, scale);
	let cast: ArrayRef = match cast {
		DecimalArray::Decimal128(array) => Arc::new(array),
		DecimalArray::Decimal256(array) => Arc::new(array),
	};
	(cast, dropped)
}

fn rescale_exact(array: &DecimalArray, precision: Precision, scale: Scale) -> (DecimalArray, usize) {
	let from = array.scale().value();
	let nulls = match array {
		DecimalArray::Decimal128(array) => array.logical_nulls(),
		DecimalArray::Decimal256(array) => array.logical_nulls(),
	};
	let mut dropped = 0;
	let mut valid = Vec::with_capacity(array.len());
	let mut values = Vec::with_capacity(array.len());
	for (index, value) in array.unscaled_values().into_iter().enumerate() {
		let present = nulls.as_ref().is_none_or(|nulls| nulls.is_valid(index));
		let exact = present.then(|| rescaled(value, from, scale.value(), precision.value())).flatten();
		if present && exact.is_none() {
			dropped += 1;
		}
		valid.push(exact.is_some());
		values.push(exact.unwrap_or(i256::ZERO));
	}
	let nulls = (nulls.is_some() || dropped > 0).then(|| NullBuffer::from(valid));
	let cast = if precision.value() <= DECIMAL128_MAX_PRECISION {
		let natives: Vec<i128> = values
			.into_iter()
			.map(|value| value.to_i128().expect("a value within precision 38 fits i128"))
			.collect();
		DecimalArray::Decimal128(
			PrimitiveArray::new(ScalarBuffer::from(natives), nulls)
				.with_data_type(data_type(precision, scale)),
		)
	} else {
		DecimalArray::Decimal256(
			PrimitiveArray::new(ScalarBuffer::from(values), nulls)
				.with_data_type(data_type(precision, scale)),
		)
	};
	(cast, dropped)
}

fn rescaled(value: i256, from: u8, to: u8, precision: u8) -> Option<i256> {
	let value = if to >= from {
		unscaled::upscale(value, to - from)?
	} else {
		let divisor = unscaled::pow10(from - to)?;
		if value.checked_rem(divisor)? != i256::ZERO {
			return None;
		}
		value.checked_div(divisor)?
	};
	(unscaled::digits(value) <= precision).then_some(value)
}

fn group_key_error(error: ArrowError) -> Error {
	internal_error!("Failed to build group keys: {}", error)
}

fn row_format_matches_value_key(view: &ColumnView) -> bool {
	let ty = view.get_type();
	ty.is_scalar() && !matches!(ty.inner_type(), ValueType::Float4 | ValueType::Float8)
}

impl HeapSize for GroupKeyDict {
	fn heap_size(&self) -> usize {
		self.entries.capacity() * (mem::size_of::<EncodedKey>() + mem::size_of::<usize>())
			+ self.row_lens.capacity() * mem::size_of::<usize>()
			+ self.entries.iter().map(HeapSize::heap_size).sum::<usize>()
			+ self.key_rows.iter().flatten().map(|(_, array)| array.get_array_memory_size()).sum::<usize>()
	}
}

fn canonical_floats(part: &(FieldRef, ArrayRef), view: &ColumnView) -> Option<(FieldRef, ArrayRef)> {
	let array: ArrayRef = match &view.data {
		ViewData::Float4(array) => Arc::new(Float32Array::from_iter(array.iter().map(|value| {
			value.filter(|v| !v.is_nan()).map(|v| {
				if v == 0.0 {
					0.0
				} else {
					v
				}
			})
		}))),
		ViewData::Float8(array) => Arc::new(Float64Array::from_iter(array.iter().map(|value| {
			value.filter(|v| !v.is_nan()).map(|v| {
				if v == 0.0 {
					0.0
				} else {
					v
				}
			})
		}))),
		_ => return None,
	};
	Some((part.0.clone(), array))
}

pub(crate) fn group_rows(views: &[ColumnView<'_>], row_count: usize, dict: &mut GroupKeyDict) -> Result<GroupRows> {
	let row_columns: Vec<&ColumnView> = views.iter().filter(|view| row_format_matches_value_key(view)).collect();
	let value_columns: Vec<&ColumnView> = views.iter().filter(|view| !row_format_matches_value_key(view)).collect();
	let row_keys = match row_columns.is_empty() {
		true => None,
		false => Some(dict.row_keys(&row_columns)?),
	};

	let mut value_keys: Vec<KeySerializer> = Vec::new();
	if !value_columns.is_empty() {
		value_keys = (0..row_count).map(|_| KeySerializer::new()).collect();
		for view in &value_columns {
			extend_keys(view, &mut value_keys)?;
		}
	}
	let mut value_keys = value_keys.into_iter();

	let mut rows_by_group: IndexMap<GroupId, Vec<usize>> = IndexMap::new();
	let mut first_rows: Vec<usize> = Vec::new();
	let mut bytes: Vec<u8> = Vec::new();

	for row in 0..row_count {
		bytes.clear();
		if let Some(row_keys) = &row_keys {
			bytes.extend_from_slice(row_keys.row(row).as_ref());
		}
		if let Some(value_key) = value_keys.next() {
			bytes.extend_from_slice(value_key.to_encoded_key().as_bytes());
		}

		let row_len = row_keys.as_ref().map_or(0, |row_keys| row_keys.row(row).as_ref().len());
		let (group, new) = dict.intern(&bytes, row_len);
		if new {
			first_rows.push(row);
		}
		rows_by_group.entry(group).or_default().push(row);
	}
	if !first_rows.is_empty() {
		dict.remember(views, &first_rows)?;
	}

	Ok(rows_by_group.into_iter().collect())
}
