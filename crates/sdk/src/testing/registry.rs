// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{cell::Cell, collections::HashMap, fmt, mem, ptr, slice, str, sync::Mutex};

use postcard::from_bytes as postcard_decode;
use reifydb_abi::{
	callbacks::builder::{ColumnBufferHandle, EmitDiffKind},
	context::context::ContextFFI,
	data::column::ColumnTypeCode,
};
use reifydb_core::{
	interface::change::{Diff, Diffs},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_type::{
	fragment::Fragment,
	util::{bitvec::BitVec, cowvec::CowVec},
	value::{
		Value,
		constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
		container::{
			any::AnyContainer, blob::BlobContainer, bool::BoolContainer, dictionary::DictionaryContainer,
			identity_id::IdentityIdContainer, number::NumberContainer, temporal::TemporalContainer,
			utf8::Utf8Container, uuid::UuidContainer,
		},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::DictionaryEntryId,
		duration::Duration,
		identity::IdentityId,
		int::Int,
		is::IsNumber,
		row_number::RowNumber,
		time::Time,
		uint::Uint,
		uuid::{Uuid4, Uuid7},
	},
};
use serde::de::DeserializeOwned;

pub struct TestBuilderRegistry {
	inner: Mutex<RegistryInner>,
}

struct RegistryInner {
	slots: HashMap<u64, Slot>,
	accumulator: Vec<EmittedDiff>,
	next_id: u64,
}

enum Slot {
	Active(Active),
	Committed(Committed),
}

pub struct Active {
	pub type_code: ColumnTypeCode,
	pub data: Vec<u8>,
	pub offsets: Option<Vec<u64>>,
	pub bitvec: Option<Vec<u8>>,
	pub generation: u64,
}

pub struct Committed {
	pub buffer: ColumnBuffer,
	pub row_count: usize,
}

pub struct EmittedDiff {
	pub kind: EmitDiffKind,
	pub pre: Option<Columns>,
	pub post: Option<Columns>,
}

impl Default for TestBuilderRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl TestBuilderRegistry {
	pub fn new() -> Self {
		Self {
			inner: Mutex::new(RegistryInner {
				slots: HashMap::new(),
				accumulator: Vec::new(),
				next_id: 1,
			}),
		}
	}

	pub fn drain_diffs(&self) -> Vec<EmittedDiff> {
		let mut inner = self.inner.lock().unwrap();
		inner.slots.clear();
		mem::take(&mut inner.accumulator)
	}
}

#[derive(Clone, Copy)]
struct Handle {
	id: u64,
	generation: u64,
}

impl Handle {
	fn encode(self) -> *mut ColumnBufferHandle {
		assert!(self.id != 0 && self.id < (1 << 48));
		assert!(self.generation < (1 << 16));
		(self.id | (self.generation << 48)) as *mut ColumnBufferHandle
	}

	fn decode(ptr: *mut ColumnBufferHandle) -> Self {
		let packed = ptr as u64;
		Self {
			id: packed & ((1 << 48) - 1),
			generation: packed >> 48,
		}
	}
}

thread_local! {
	static REGISTRY: Cell<Option<&'static TestBuilderRegistry>> = const { Cell::new(None) };
}

pub fn with_registry<R>(registry: &TestBuilderRegistry, f: impl FnOnce() -> R) -> R {
	// SAFETY: we only hold the pointer for the duration of `f`. The
	// registry outlives `f`.
	let extended: &'static TestBuilderRegistry = unsafe { mem::transmute(registry) };
	let prev = REGISTRY.with(|cell| cell.replace(Some(extended)));
	let r = f();
	REGISTRY.with(|cell| cell.set(prev));
	r
}

fn current() -> Option<&'static TestBuilderRegistry> {
	REGISTRY.with(|cell| cell.get())
}

fn elem_size_for(type_code: ColumnTypeCode) -> usize {
	match type_code {
		ColumnTypeCode::Bool => 1,
		ColumnTypeCode::Float4 | ColumnTypeCode::Int4 | ColumnTypeCode::Uint4 | ColumnTypeCode::Date => 4,
		ColumnTypeCode::Int1 | ColumnTypeCode::Uint1 => 1,
		ColumnTypeCode::Int2 | ColumnTypeCode::Uint2 => 2,
		ColumnTypeCode::Float8
		| ColumnTypeCode::Int8
		| ColumnTypeCode::Uint8
		| ColumnTypeCode::DateTime
		| ColumnTypeCode::Time => 8,
		ColumnTypeCode::Int16 | ColumnTypeCode::Uint16 => 16,
		ColumnTypeCode::Duration
		| ColumnTypeCode::IdentityId
		| ColumnTypeCode::Uuid4
		| ColumnTypeCode::Uuid7
		| ColumnTypeCode::DictionaryId => 16,
		ColumnTypeCode::Utf8 | ColumnTypeCode::Blob => 1,
		ColumnTypeCode::Int | ColumnTypeCode::Uint | ColumnTypeCode::Decimal | ColumnTypeCode::Any => 1,
		ColumnTypeCode::Undefined => 1,
	}
}

fn is_var_len(type_code: ColumnTypeCode) -> bool {
	matches!(
		type_code,
		ColumnTypeCode::Utf8
			| ColumnTypeCode::Blob
			| ColumnTypeCode::Int | ColumnTypeCode::Uint
			| ColumnTypeCode::Decimal
			| ColumnTypeCode::Any | ColumnTypeCode::DictionaryId
	)
}

pub(crate) unsafe extern "C" fn test_acquire(
	_ctx: *mut ContextFFI,
	type_code: ColumnTypeCode,
	capacity: usize,
) -> *mut ColumnBufferHandle {
	let Some(registry) = current() else {
		return ptr::null_mut();
	};
	let mut inner = registry.inner.lock().unwrap();
	let id = inner.next_id;
	inner.next_id = inner.next_id.checked_add(1).unwrap_or(1);

	let elem = elem_size_for(type_code);
	let active = Active {
		type_code,
		data: Vec::with_capacity(capacity.saturating_mul(elem)),
		offsets: if is_var_len(type_code) {
			let mut o = Vec::with_capacity(capacity + 1);
			o.push(0);
			Some(o)
		} else {
			None
		},
		bitvec: None,
		generation: 1,
	};
	inner.slots.insert(id, Slot::Active(active));
	Handle {
		id,
		generation: 1,
	}
	.encode()
}

pub(crate) unsafe extern "C" fn test_data_ptr(handle: *mut ColumnBufferHandle) -> *mut u8 {
	let Some(registry) = current() else {
		return ptr::null_mut();
	};
	let h = Handle::decode(handle);
	let mut inner = registry.inner.lock().unwrap();
	match inner.slots.get_mut(&h.id) {
		Some(Slot::Active(a)) if a.generation == h.generation => a.data.as_mut_ptr(),
		_ => ptr::null_mut(),
	}
}

pub(crate) unsafe extern "C" fn test_offsets_ptr(handle: *mut ColumnBufferHandle) -> *mut u64 {
	let Some(registry) = current() else {
		return ptr::null_mut();
	};
	let h = Handle::decode(handle);
	let mut inner = registry.inner.lock().unwrap();
	match inner.slots.get_mut(&h.id) {
		Some(Slot::Active(a)) if a.generation == h.generation => match &mut a.offsets {
			Some(o) => o.as_mut_ptr(),
			None => ptr::null_mut(),
		},
		_ => ptr::null_mut(),
	}
}

pub(crate) unsafe extern "C" fn test_bitvec_ptr(handle: *mut ColumnBufferHandle) -> *mut u8 {
	let Some(registry) = current() else {
		return ptr::null_mut();
	};
	let h = Handle::decode(handle);
	let mut inner = registry.inner.lock().unwrap();
	match inner.slots.get_mut(&h.id) {
		Some(Slot::Active(a)) if a.generation == h.generation => {
			if a.bitvec.is_none() {
				let cap = a.data.capacity() / elem_size_for(a.type_code).max(1);
				a.bitvec = Some(vec![0u8; cap.div_ceil(8)]);
			}
			a.bitvec.as_mut().unwrap().as_mut_ptr()
		}
		_ => ptr::null_mut(),
	}
}

pub(crate) unsafe extern "C" fn test_grow(handle: *mut ColumnBufferHandle, additional: usize) -> i32 {
	let Some(registry) = current() else {
		return -1;
	};
	let h = Handle::decode(handle);
	let mut inner = registry.inner.lock().unwrap();
	match inner.slots.get_mut(&h.id) {
		Some(Slot::Active(a)) if a.generation == h.generation => {
			let elem = elem_size_for(a.type_code);
			a.data.reserve(additional.saturating_mul(elem));
			if let Some(o) = a.offsets.as_mut() {
				o.reserve(additional);
			}
			0
		}
		_ => -1,
	}
}

pub(crate) unsafe extern "C" fn test_commit(handle: *mut ColumnBufferHandle, written_count: usize) -> i32 {
	let Some(registry) = current() else {
		return -1;
	};
	let h = Handle::decode(handle);
	let mut inner = registry.inner.lock().unwrap();
	let slot = match inner.slots.remove(&h.id) {
		Some(s) => s,
		None => return -1,
	};
	let mut active = match slot {
		Slot::Active(a) if a.generation == h.generation => a,
		other => {
			inner.slots.insert(h.id, other);
			return -1;
		}
	};

	// Sync the underlying Vec lengths to match what the guest wrote
	// through the raw pointers handed out by `data_ptr` / `offsets_ptr`.
	let elem = elem_size_for(active.type_code);
	// For var-len types: extend offsets first so `.last()` reflects the
	// guest-written end-of-payload byte count.
	if let Some(offsets) = active.offsets.as_mut() {
		let offsets_len = written_count + 1;
		if offsets_len > offsets.capacity() {
			return -1;
		}
		unsafe {
			offsets.set_len(offsets_len);
		}
	}
	let data_byte_len = if is_var_len(active.type_code) {
		// Var-len `written_count` is the element count; the byte count
		// comes from the last offset.
		match active.offsets.as_ref() {
			Some(o) if !o.is_empty() => *o.last().unwrap() as usize,
			_ => 0,
		}
	} else {
		written_count.saturating_mul(elem)
	};
	if data_byte_len > active.data.capacity() {
		// Guest claimed to write more bytes than the buffer can hold.
		return -1;
	}
	unsafe {
		active.data.set_len(data_byte_len);
	}
	if let Some(bitvec) = active.bitvec.as_mut() {
		let needed = written_count.div_ceil(8);
		if needed > bitvec.capacity() {
			return -1;
		}
		unsafe {
			bitvec.set_len(needed);
		}
	}

	let buffer = match finalize_buffer(active.type_code, active.data, active.offsets, active.bitvec, written_count)
	{
		Some(b) => b,
		None => return -1,
	};
	inner.slots.insert(
		h.id,
		Slot::Committed(Committed {
			buffer,
			row_count: written_count,
		}),
	);
	0
}

pub(crate) unsafe extern "C" fn test_release(handle: *mut ColumnBufferHandle) {
	let Some(registry) = current() else {
		return;
	};
	let h = Handle::decode(handle);
	let mut inner = registry.inner.lock().unwrap();
	inner.slots.remove(&h.id);
}

pub(crate) unsafe extern "C" fn test_emit_diff(
	_ctx: *mut ContextFFI,
	kind: EmitDiffKind,
	pre_handles_ptr: *const *mut ColumnBufferHandle,
	pre_name_ptrs: *const *const u8,
	pre_name_lens: *const usize,
	pre_count: usize,
	pre_row_count: usize,
	pre_row_numbers_ptr: *const u64,
	pre_row_numbers_len: usize,
	post_handles_ptr: *const *mut ColumnBufferHandle,
	post_name_ptrs: *const *const u8,
	post_name_lens: *const usize,
	post_count: usize,
	post_row_count: usize,
	post_row_numbers_ptr: *const u64,
	post_row_numbers_len: usize,
) -> i32 {
	let Some(registry) = current() else {
		return -1;
	};
	let mut inner = registry.inner.lock().unwrap();
	let now = DateTime::default();

	let pre = if pre_count > 0 {
		let ptrs = ColumnsPtrs {
			handles: pre_handles_ptr,
			names: pre_name_ptrs,
			name_lens: pre_name_lens,
			count: pre_count,
		};
		match assemble(&mut inner, ptrs, pre_row_count, pre_row_numbers_ptr, pre_row_numbers_len, now) {
			Ok(c) => Some(c),
			Err(code) => return code,
		}
	} else {
		None
	};
	let post = if post_count > 0 {
		let ptrs = ColumnsPtrs {
			handles: post_handles_ptr,
			names: post_name_ptrs,
			name_lens: post_name_lens,
			count: post_count,
		};
		match assemble(&mut inner, ptrs, post_row_count, post_row_numbers_ptr, post_row_numbers_len, now) {
			Ok(c) => Some(c),
			Err(code) => return code,
		}
	} else {
		None
	};

	inner.accumulator.push(EmittedDiff {
		kind,
		pre,
		post,
	});
	0
}

struct ColumnsPtrs {
	handles: *const *mut ColumnBufferHandle,
	names: *const *const u8,
	name_lens: *const usize,
	count: usize,
}

fn assemble(
	inner: &mut RegistryInner,
	ptrs: ColumnsPtrs,
	row_count: usize,
	row_numbers_ptr: *const u64,
	row_numbers_len: usize,
	now: DateTime,
) -> Result<Columns, i32> {
	if ptrs.handles.is_null() || ptrs.names.is_null() || ptrs.name_lens.is_null() {
		return Err(-1);
	}
	if row_numbers_len != row_count {
		return Err(-1);
	}
	if row_count > 0 && row_numbers_ptr.is_null() {
		return Err(-1);
	}
	let count = ptrs.count;
	let handles = unsafe { slice::from_raw_parts(ptrs.handles, count) };
	let names = unsafe { slice::from_raw_parts(ptrs.names, count) };
	let lens = unsafe { slice::from_raw_parts(ptrs.name_lens, count) };

	let mut cols: Vec<ColumnWithName> = Vec::with_capacity(count);
	for i in 0..count {
		let h = Handle::decode(handles[i]);
		let slot = inner.slots.remove(&h.id).ok_or(-1)?;
		let committed = match slot {
			Slot::Committed(c) => c,
			Slot::Active(a) => {
				inner.slots.insert(h.id, Slot::Active(a));
				return Err(-1);
			}
		};
		let name = if names[i].is_null() || lens[i] == 0 {
			""
		} else {
			let s = unsafe { slice::from_raw_parts(names[i], lens[i]) };
			str::from_utf8(s).unwrap_or("")
		};
		cols.push(ColumnWithName::new(Fragment::internal(name), committed.buffer));
	}
	let row_numbers: Vec<RowNumber> = if row_count == 0 {
		Vec::new()
	} else {
		let raw = unsafe { slice::from_raw_parts(row_numbers_ptr, row_count) };
		raw.iter().copied().map(RowNumber).collect()
	};
	let timestamps: Vec<DateTime> = vec![now; row_count];
	Ok(Columns::with_system_columns(cols, row_numbers, timestamps.clone(), timestamps))
}

/// Produce a native ColumnBuffer from raw bytes + offsets + bitvec.
pub(crate) fn finalize_buffer(
	type_code: ColumnTypeCode,
	mut data: Vec<u8>,
	offsets: Option<Vec<u64>>,
	bitvec: Option<Vec<u8>>,
	written_count: usize,
) -> Option<ColumnBuffer> {
	let make_option_wrapped = |inner: ColumnBuffer| match bitvec {
		Some(bytes) => {
			let bv = BitVec::from_raw(bytes, written_count);
			ColumnBuffer::Option {
				inner: Box::new(inner),
				bitvec: bv,
			}
		}
		None => inner,
	};

	let inner = match type_code {
		ColumnTypeCode::Bool => {
			let bv = BitVec::from_raw(data, written_count);
			ColumnBuffer::Bool(BoolContainer::from_parts(bv))
		}
		ColumnTypeCode::Float4 => to_numeric::<f32>(&data, written_count, ColumnBuffer::Float4)?,
		ColumnTypeCode::Float8 => to_numeric::<f64>(&data, written_count, ColumnBuffer::Float8)?,
		ColumnTypeCode::Int1 => to_numeric::<i8>(&data, written_count, ColumnBuffer::Int1)?,
		ColumnTypeCode::Int2 => to_numeric::<i16>(&data, written_count, ColumnBuffer::Int2)?,
		ColumnTypeCode::Int4 => to_numeric::<i32>(&data, written_count, ColumnBuffer::Int4)?,
		ColumnTypeCode::Int8 => to_numeric::<i64>(&data, written_count, ColumnBuffer::Int8)?,
		ColumnTypeCode::Int16 => to_numeric::<i128>(&data, written_count, ColumnBuffer::Int16)?,
		ColumnTypeCode::Uint1 => to_numeric::<u8>(&data, written_count, ColumnBuffer::Uint1)?,
		ColumnTypeCode::Uint2 => to_numeric::<u16>(&data, written_count, ColumnBuffer::Uint2)?,
		ColumnTypeCode::Uint4 => to_numeric::<u32>(&data, written_count, ColumnBuffer::Uint4)?,
		ColumnTypeCode::Uint8 => to_numeric::<u64>(&data, written_count, ColumnBuffer::Uint8)?,
		ColumnTypeCode::Uint16 => to_numeric::<u128>(&data, written_count, ColumnBuffer::Uint16)?,
		ColumnTypeCode::Date => {
			let v = bytes_to_vec::<Date>(&data, written_count)?;
			ColumnBuffer::Date(TemporalContainer::from_parts(CowVec::new(v)))
		}
		ColumnTypeCode::DateTime => {
			let v = bytes_to_vec::<DateTime>(&data, written_count)?;
			ColumnBuffer::DateTime(TemporalContainer::from_parts(CowVec::new(v)))
		}
		ColumnTypeCode::Time => {
			let v = bytes_to_vec::<Time>(&data, written_count)?;
			ColumnBuffer::Time(TemporalContainer::from_parts(CowVec::new(v)))
		}
		ColumnTypeCode::Duration => {
			let v = bytes_to_vec::<Duration>(&data, written_count)?;
			ColumnBuffer::Duration(TemporalContainer::from_parts(CowVec::new(v)))
		}
		ColumnTypeCode::IdentityId => {
			let v = bytes_to_vec::<IdentityId>(&data, written_count)?;
			ColumnBuffer::IdentityId(IdentityIdContainer::from_parts(CowVec::new(v)))
		}
		ColumnTypeCode::Uuid4 => {
			let v = bytes_to_vec::<Uuid4>(&data, written_count)?;
			ColumnBuffer::Uuid4(UuidContainer::from_parts(CowVec::new(v)))
		}
		ColumnTypeCode::Uuid7 => {
			let v = bytes_to_vec::<Uuid7>(&data, written_count)?;
			ColumnBuffer::Uuid7(UuidContainer::from_parts(CowVec::new(v)))
		}
		ColumnTypeCode::Utf8 => {
			let offsets = offsets.unwrap_or_else(|| vec![0u64]);
			let payload_len = *offsets.last().unwrap_or(&0) as usize;
			data.truncate(payload_len);
			ColumnBuffer::Utf8 {
				container: Utf8Container::from_bytes_offsets(data, offsets),
				max_bytes: MaxBytes::MAX,
			}
		}
		ColumnTypeCode::Blob => {
			let offsets = offsets.unwrap_or_else(|| vec![0u64]);
			let payload_len = *offsets.last().unwrap_or(&0) as usize;
			data.truncate(payload_len);
			ColumnBuffer::Blob {
				container: BlobContainer::from_bytes_offsets(data, offsets),
				max_bytes: MaxBytes::MAX,
			}
		}
		ColumnTypeCode::Int => {
			let v = postcard_per_element::<Int>(&data, &offsets, written_count)?;
			ColumnBuffer::Int {
				container: NumberContainer::from_vec(v),
				max_bytes: MaxBytes::MAX,
			}
		}
		ColumnTypeCode::Uint => {
			let v = postcard_per_element::<Uint>(&data, &offsets, written_count)?;
			ColumnBuffer::Uint {
				container: NumberContainer::from_vec(v),
				max_bytes: MaxBytes::MAX,
			}
		}
		ColumnTypeCode::Decimal => {
			let v = postcard_per_element::<Decimal>(&data, &offsets, written_count)?;
			ColumnBuffer::Decimal {
				container: NumberContainer::from_vec(v),
				precision: Precision::MAX,
				scale: Scale::MIN,
			}
		}
		ColumnTypeCode::Any => {
			let values: Vec<Value> = postcard_per_element::<Value>(&data, &offsets, written_count)?;
			let boxed: Vec<Box<Value>> = values.into_iter().map(Box::new).collect();
			ColumnBuffer::Any(AnyContainer::from_vec(boxed))
		}
		ColumnTypeCode::DictionaryId => {
			let entries: Vec<DictionaryEntryId> =
				postcard_per_element::<DictionaryEntryId>(&data, &offsets, written_count)?;
			ColumnBuffer::DictionaryId(DictionaryContainer::from_vec(entries))
		}
		_ => return None,
	};
	Some(make_option_wrapped(inner))
}

fn postcard_per_element<T: DeserializeOwned>(data: &[u8], offsets: &Option<Vec<u64>>, count: usize) -> Option<Vec<T>> {
	let offsets = offsets.as_ref()?;
	if offsets.len() < count + 1 {
		return None;
	}
	let mut out: Vec<T> = Vec::with_capacity(count);
	for i in 0..count {
		let start = offsets[i] as usize;
		let end = offsets[i + 1] as usize;
		if end > data.len() || start > end {
			return None;
		}
		let value: T = postcard_decode(&data[start..end]).ok()?;
		out.push(value);
	}
	Some(out)
}

fn bytes_to_vec<T: Copy>(data: &[u8], count: usize) -> Option<Vec<T>> {
	let needed = count.checked_mul(mem::size_of::<T>())?;
	if data.len() < needed {
		return None;
	}
	let mut v: Vec<T> = Vec::with_capacity(count);
	unsafe {
		ptr::copy_nonoverlapping(data.as_ptr() as *const T, v.as_mut_ptr(), count);
		v.set_len(count);
	}
	Some(v)
}

fn to_numeric<T: Copy + IsNumber + fmt::Debug + Default>(
	data: &[u8],
	count: usize,
	wrap: fn(NumberContainer<T>) -> ColumnBuffer,
) -> Option<ColumnBuffer> {
	let v = bytes_to_vec::<T>(data, count)?;
	Some(wrap(NumberContainer::from_parts(CowVec::new(v))))
}

/// Convenience: drain `EmittedDiff`s into a `Diffs` collection.
pub fn into_diffs(emitted: Vec<EmittedDiff>) -> Diffs {
	emitted.into_iter()
		.map(|d| match d.kind {
			EmitDiffKind::Insert => Diff::insert(d.post.unwrap_or_else(Columns::empty)),
			EmitDiffKind::Update => Diff::update(
				d.pre.unwrap_or_else(Columns::empty),
				d.post.unwrap_or_else(Columns::empty),
			),
			EmitDiffKind::Remove => Diff::remove(d.pre.unwrap_or_else(Columns::empty)),
		})
		.collect()
}
