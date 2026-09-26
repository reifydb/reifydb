// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cell::Cell, collections::HashMap, ffi::c_void, mem, ptr, slice, str};

use arrow_array::{BooleanArray, LargeBinaryArray, LargeStringArray};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use reifydb_codec::{
	extern_c::cells::{decode_any_cell, decode_dictionary_id_cell},
	tag::ValueKind,
};
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_sdk::common::{
	extern_c::wire::{
		callbacks::builder::{ColumnBufferHandle, EmitDiffKind},
		status::{
			EXTERN_C_ERROR_INTERNAL, EXTERN_C_ERROR_INVALID_UTF8, EXTERN_C_ERROR_MARSHAL,
			EXTERN_C_ERROR_NULL_PTR, EXTERN_C_OK,
		},
	},
	family::{cell_width, decode_family_column, family_params, is_family},
};
use reifydb_value::{
	fragment::Fragment,
	reifydb_assertions,
	value::{
		Value,
		constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
		container::{
			any_array::any_array,
			temporal_array::{date_array, datetime_array, duration_array, time_array},
			uuid_array::{identity_id_array, uuid4_array, uuid7_array},
		},
		date::Date,
		datetime::DateTime,
		dictionary::DictionaryEntryId,
		duration::Duration,
		identity::IdentityId,
		row_number::RowNumber,
		system_columns::SystemColumns,
		time::Time,
		uuid::{Uuid4, Uuid7},
	},
};

pub struct BuilderRegistry {
	inner: Mutex<RegistryInner>,
}

struct RegistryInner {
	slots: HashMap<u64, BuilderSlot>,

	accumulator: Vec<EmittedDiff>,

	next_id: u64,
}

enum BuilderSlot {
	Active(ActiveBuilder),

	Committed(CommittedBuilder),
}

pub struct ActiveBuilder {
	pub type_code: ValueKind,
	pub family: Option<(Precision, Scale)>,
	pub elem_size: usize,
	pub data: Vec<u8>,
	pub offsets: Option<Vec<u64>>,
	pub bitvec: Option<Vec<u8>>,

	pub generation: u64,
}

pub struct CommittedBuilder {
	pub type_code: ValueKind,
	pub buffer: ColumnBuffer,
	pub row_count: usize,
}

pub struct EmittedDiff {
	pub kind: EmitDiffKind,
	pub pre: Option<Columns>,
	pub post: Option<Columns>,
}

impl Default for BuilderRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl BuilderRegistry {
	pub fn new() -> Self {
		Self {
			inner: Mutex::new(RegistryInner {
				slots: HashMap::new(),
				accumulator: Vec::new(),
				next_id: 1,
			}),
		}
	}

	pub fn drain(&self) -> Vec<EmittedDiff> {
		let mut inner = self.inner.lock();
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
		reifydb_assertions! {
			assert!(self.id != 0, "handle id 0 reserved");
		}

		assert!(self.id < (1 << 48), "handle id overflow");
		assert!(self.generation < (1 << 16), "handle generation overflow");
		let packed = self.id | (self.generation << 48);
		packed as *mut ColumnBufferHandle
	}

	fn decode(ptr: *mut ColumnBufferHandle) -> Self {
		let packed = ptr as u64;
		Self {
			id: packed & ((1 << 48) - 1),
			generation: packed >> 48,
		}
	}
}

fn current_registry() -> Option<&'static BuilderRegistry> {
	REGISTRY.with(|cell| cell.get())
}

thread_local! {
	static REGISTRY: Cell<Option<&'static BuilderRegistry>> = const { Cell::new(None) };
}

pub fn with_registry<R>(registry: &BuilderRegistry, f: impl FnOnce() -> R) -> R {
	// SAFETY: the 'static is confined to this frame - the thread-local is restored before returning.
	let extended: &'static BuilderRegistry = unsafe { mem::transmute(registry) };
	let prev = REGISTRY.with(|cell| cell.replace(Some(extended)));
	let result = f();
	REGISTRY.with(|cell| cell.set(prev));
	result
}

/// # Safety
/// `_ctx` may be null; all pointer access is guarded internally.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn host_builder_acquire(
	_ctx: *mut c_void,
	type_code: ValueKind,
	precision: u8,
	scale: u8,
	capacity: usize,
) -> *mut ColumnBufferHandle {
	let Some(registry) = current_registry() else {
		return ptr::null_mut();
	};
	let family = if is_family(type_code) {
		match family_params(type_code, precision, scale) {
			Some(params) => Some(params),
			None => return ptr::null_mut(),
		}
	} else {
		None
	};
	let elem_size = match family {
		Some((precision, _)) => cell_width(precision),
		None => elem_size_for(type_code),
	};
	let mut inner = registry.inner.lock();
	let id = inner.next_id;
	inner.next_id = inner.next_id.checked_add(1).unwrap_or(1);

	let initial_data_capacity = capacity.saturating_mul(elem_size);
	let active = ActiveBuilder {
		type_code,
		family,
		elem_size,
		data: Vec::with_capacity(initial_data_capacity),
		offsets: if is_var_len(type_code) {
			let mut o = Vec::with_capacity(capacity + 1);
			o.push(0u64);
			Some(o)
		} else {
			None
		},
		bitvec: None,
		generation: 1,
	};
	let handle = Handle {
		id,
		generation: 1,
	};
	inner.slots.insert(id, BuilderSlot::Active(active));
	handle.encode()
}

/// # Safety
/// `handle` must be a value returned by `host_builder_acquire`, or null. The returned pointer is
/// invalidated by the next call to `host_builder_grow` on the same handle; the caller must re-fetch it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn host_builder_data_ptr(handle: *mut ColumnBufferHandle) -> *mut u8 {
	let Some(registry) = current_registry() else {
		return ptr::null_mut();
	};
	let h = Handle::decode(handle);
	let mut inner = registry.inner.lock();
	match inner.slots.get_mut(&h.id) {
		Some(BuilderSlot::Active(active)) if active.generation == h.generation => active.data.as_mut_ptr(),
		_ => ptr::null_mut(),
	}
}

/// # Safety
/// `handle` must be a value returned by `host_builder_acquire`, or null. The returned pointer is
/// invalidated by the next call to `host_builder_grow` on the same handle; the caller must re-fetch it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn host_builder_offsets_ptr(handle: *mut ColumnBufferHandle) -> *mut u64 {
	let Some(registry) = current_registry() else {
		return ptr::null_mut();
	};
	let h = Handle::decode(handle);
	let mut inner = registry.inner.lock();
	match inner.slots.get_mut(&h.id) {
		Some(BuilderSlot::Active(active)) if active.generation == h.generation => match &mut active.offsets {
			Some(offsets) => offsets.as_mut_ptr(),
			None => ptr::null_mut(),
		},
		_ => ptr::null_mut(),
	}
}

/// # Safety
/// `handle` must be a value returned by `host_builder_acquire`, or null. The returned pointer is
/// invalidated by the next call to `host_builder_grow` on the same handle; the caller must re-fetch it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn host_builder_bitvec_ptr(handle: *mut ColumnBufferHandle) -> *mut u8 {
	let Some(registry) = current_registry() else {
		return ptr::null_mut();
	};
	let h = Handle::decode(handle);
	let mut inner = registry.inner.lock();
	match inner.slots.get_mut(&h.id) {
		Some(BuilderSlot::Active(active)) if active.generation == h.generation => {
			if active.bitvec.is_none() {
				let elem_cap = active.data.capacity() / active.elem_size.max(1);
				active.bitvec = Some(vec![0u8; elem_cap.div_ceil(8)]);
			}
			active.bitvec.as_mut().unwrap().as_mut_ptr()
		}
		_ => ptr::null_mut(),
	}
}

/// # Safety
/// `handle` must be a value returned by `host_builder_acquire`, or null. A successful grow may
/// reallocate the builder's buffers, invalidating any pointer obtained before this call from
/// `host_builder_data_ptr`, `host_builder_offsets_ptr`, or `host_builder_bitvec_ptr`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn host_builder_grow(handle: *mut ColumnBufferHandle, additional: usize) -> i32 {
	let Some(registry) = current_registry() else {
		return EXTERN_C_ERROR_INTERNAL;
	};
	let h = Handle::decode(handle);
	let mut inner = registry.inner.lock();
	match inner.slots.get_mut(&h.id) {
		Some(BuilderSlot::Active(active)) if active.generation == h.generation => {
			let elem = active.elem_size;
			let extra_bytes = additional.saturating_mul(elem);
			let target_cap = active.data.capacity().saturating_add(extra_bytes);
			let needed_reserve = target_cap.saturating_sub(active.data.len());
			active.data.reserve(needed_reserve);
			if let Some(offsets) = active.offsets.as_mut() {
				let target_off_cap = offsets.capacity().saturating_add(additional);
				let needed_off_reserve = target_off_cap.saturating_sub(offsets.len());
				offsets.reserve(needed_off_reserve);
			}
			if let Some(bitvec) = active.bitvec.as_mut() {
				let needed_bytes = (additional + active.data.len() / elem.max(1)).div_ceil(8);
				if bitvec.len() < needed_bytes {
					bitvec.resize(needed_bytes, 0);
				}
			}
			EXTERN_C_OK
		}
		_ => EXTERN_C_ERROR_INTERNAL,
	}
}

/// # Safety
/// `handle` must be a value returned by `host_builder_acquire`, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn host_builder_commit(handle: *mut ColumnBufferHandle, written_count: usize) -> i32 {
	let Some(registry) = current_registry() else {
		return EXTERN_C_ERROR_INTERNAL;
	};
	let h = Handle::decode(handle);
	let mut inner = registry.inner.lock();

	let mut active = match inner.take_active(h) {
		Ok(active) => active,
		Err(code) => return code,
	};
	if let Err(code) = active.set_committed_lengths(written_count) {
		return code;
	}
	inner.store_committed(h, active, written_count)
}

impl RegistryInner {
	#[inline]
	fn take_active(&mut self, h: Handle) -> Result<ActiveBuilder, i32> {
		let slot = match self.slots.remove(&h.id) {
			Some(slot) => slot,
			None => return Err(EXTERN_C_ERROR_INTERNAL),
		};
		match slot {
			BuilderSlot::Active(a) if a.generation == h.generation => Ok(a),
			other => {
				self.slots.insert(h.id, other);
				Err(EXTERN_C_ERROR_INTERNAL)
			}
		}
	}

	#[inline]
	fn store_committed(&mut self, h: Handle, active: ActiveBuilder, written_count: usize) -> i32 {
		let buffer = match finalize_buffer(
			active.type_code,
			active.family,
			active.data,
			active.offsets,
			active.bitvec,
			written_count,
		) {
			Ok(b) => b,
			Err(code) => return code,
		};
		self.slots.insert(
			h.id,
			BuilderSlot::Committed(CommittedBuilder {
				type_code: active.type_code,
				buffer,
				row_count: written_count,
			}),
		);
		EXTERN_C_OK
	}
}

impl ActiveBuilder {
	#[inline]
	fn set_committed_lengths(&mut self, written_count: usize) -> Result<(), i32> {
		reifydb_assertions! {
			let var_len = is_var_len(self.type_code);
			assert!(
				!var_len || self.offsets.is_some(),
				"var-len builder (type_code {:?}) committed without an offsets vector; \
				 host_builder_acquire allocates offsets for every var-len type, so a missing \
				 offsets here would silently set data_byte_len to 0 and truncate the column to empty",
				self.type_code
			);
		}

		let elem = self.elem_size;

		if let Some(offsets) = self.offsets.as_mut() {
			let offsets_len = written_count + 1;
			if offsets_len > offsets.capacity() {
				return Err(EXTERN_C_ERROR_INTERNAL);
			}
			// SAFETY: within capacity (checked) and the guest filled every slot before commit.
			unsafe {
				offsets.set_len(offsets_len);
			}
		}
		let data_byte_len = if is_var_len(self.type_code) {
			match self.offsets.as_ref() {
				Some(o) if !o.is_empty() => *o.last().unwrap() as usize,
				_ => 0,
			}
		} else {
			written_count.saturating_mul(elem)
		};
		if data_byte_len > self.data.capacity() {
			return Err(EXTERN_C_ERROR_INTERNAL);
		}
		// SAFETY: within capacity (checked) and the guest wrote that many bytes before commit.
		unsafe {
			self.data.set_len(data_byte_len);
		}
		if let Some(bitvec) = self.bitvec.as_mut() {
			let needed = written_count.div_ceil(8);
			if needed > bitvec.capacity() {
				return Err(EXTERN_C_ERROR_INTERNAL);
			}
			// SAFETY: within capacity (checked) and the guest wrote the bits before commit.
			unsafe {
				bitvec.set_len(needed);
			}
		}
		Ok(())
	}
}

/// # Safety
/// `handle` must be a value returned by `host_builder_acquire`, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn host_builder_release(handle: *mut ColumnBufferHandle) {
	let Some(registry) = current_registry() else {
		return;
	};
	let h = Handle::decode(handle);
	let mut inner = registry.inner.lock();
	inner.slots.remove(&h.id);
}

/// # Safety
/// All handle/name pointer arrays must be valid for the given counts, or null when the
/// corresponding count is zero.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn host_builder_emit_diff(
	written_at_nanos: i64,
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
	let Some(registry) = current_registry() else {
		return EXTERN_C_ERROR_INTERNAL;
	};

	let mut inner = registry.inner.lock();
	let now = DateTime::from_nanos(written_at_nanos);

	let pre_columns = match assemble_columns_opt(
		&mut inner,
		ColumnsPtrs {
			handles: pre_handles_ptr,
			names: pre_name_ptrs,
			name_lens: pre_name_lens,
			count: pre_count,
		},
		pre_row_count,
		pre_row_numbers_ptr,
		pre_row_numbers_len,
		now,
	) {
		Ok(c) => c,
		Err(code) => return code,
	};
	let post_columns = match assemble_columns_opt(
		&mut inner,
		ColumnsPtrs {
			handles: post_handles_ptr,
			names: post_name_ptrs,
			name_lens: post_name_lens,
			count: post_count,
		},
		post_row_count,
		post_row_numbers_ptr,
		post_row_numbers_len,
		now,
	) {
		Ok(c) => c,
		Err(code) => return code,
	};

	inner.accumulator.push(EmittedDiff {
		kind,
		pre: pre_columns,
		post: post_columns,
	});
	EXTERN_C_OK
}

fn assemble_columns_opt(
	inner: &mut RegistryInner,
	ptrs: ColumnsPtrs,
	row_count: usize,
	row_numbers_ptr: *const u64,
	row_numbers_len: usize,
	now: DateTime,
) -> Result<Option<Columns>, i32> {
	if ptrs.count == 0 {
		return Ok(None);
	}
	assemble_columns(inner, ptrs, row_count, row_numbers_ptr, row_numbers_len, now).map(Some)
}

struct ColumnsPtrs {
	handles: *const *mut ColumnBufferHandle,
	names: *const *const u8,
	name_lens: *const usize,
	count: usize,
}

fn assemble_columns(
	inner: &mut RegistryInner,
	ptrs: ColumnsPtrs,
	row_count: usize,
	row_numbers_ptr: *const u64,
	row_numbers_len: usize,
	now: DateTime,
) -> Result<Columns, i32> {
	let ColumnsPtrs {
		handles: handles_ptr,
		names: name_ptrs,
		name_lens,
		count,
	} = ptrs;
	if handles_ptr.is_null() || name_ptrs.is_null() || name_lens.is_null() {
		return Err(EXTERN_C_ERROR_NULL_PTR);
	}
	if row_numbers_len != row_count {
		return Err(EXTERN_C_ERROR_INTERNAL);
	}
	if row_count > 0 && row_numbers_ptr.is_null() {
		return Err(EXTERN_C_ERROR_NULL_PTR);
	}
	// SAFETY: all three are non-null here and the caller guarantees `count` initialized elements.
	let handles = unsafe { slice::from_raw_parts(handles_ptr, count) };
	let names = unsafe { slice::from_raw_parts(name_ptrs, count) };
	let lens = unsafe { slice::from_raw_parts(name_lens, count) };

	let mut cols: Vec<ColumnWithName> = Vec::with_capacity(count);
	for i in 0..count {
		let h = Handle::decode(handles[i]);
		let slot = inner.slots.remove(&h.id).ok_or(EXTERN_C_ERROR_INTERNAL)?;
		let committed = match slot {
			BuilderSlot::Committed(c) => c,
			BuilderSlot::Active(a) => {
				inner.slots.insert(h.id, BuilderSlot::Active(a));
				return Err(EXTERN_C_ERROR_INTERNAL);
			}
		};
		let name_bytes = if names[i].is_null() || lens[i] == 0 {
			""
		} else {
			// SAFETY: names[i] is non-null and readable for lens[i] bytes per the caller.
			let s = unsafe { slice::from_raw_parts(names[i], lens[i]) };
			str::from_utf8(s).unwrap_or("")
		};
		cols.push(ColumnWithName::new(Fragment::internal(name_bytes), committed.buffer));
	}

	let row_numbers: Vec<RowNumber> = if row_count == 0 {
		Vec::new()
	} else {
		// SAFETY: row_count > 0 passed the null check and row_numbers_len was verified equal to it.
		let raw = unsafe { slice::from_raw_parts(row_numbers_ptr, row_count) };
		raw.iter().copied().map(RowNumber).collect()
	};
	let timestamps: Vec<DateTime> = vec![now; row_count];
	Ok(Columns::with_system(
		cols,
		SystemColumns::new(
			row_numbers,
			Vec::new(),
			timestamps.clone(),
			timestamps.clone(),
			timestamps,
			Vec::new(),
		),
	))
}

fn elem_size_for(type_code: ValueKind) -> usize {
	match type_code {
		ValueKind::Boolean => 1,
		ValueKind::Float4 | ValueKind::Int4 | ValueKind::Uint4 | ValueKind::Date => 4,
		ValueKind::Int1 | ValueKind::Uint1 => 1,
		ValueKind::Int2 | ValueKind::Uint2 => 2,
		ValueKind::Float8 | ValueKind::Int8 | ValueKind::Uint8 | ValueKind::DateTime | ValueKind::Time => 8,
		ValueKind::Int16 | ValueKind::Uint16 => 16,
		ValueKind::Duration => 16,
		ValueKind::IdentityId | ValueKind::Uuid4 | ValueKind::Uuid7 => 16,
		ValueKind::Utf8 | ValueKind::Blob => 1,
		ValueKind::DictionaryId => 16,
		ValueKind::Any => 1,
		ValueKind::Decimal
		| ValueKind::None
		| ValueKind::Type
		| ValueKind::List
		| ValueKind::Record
		| ValueKind::Tuple
		| ValueKind::Digest => 1,
	}
}

fn is_var_len(type_code: ValueKind) -> bool {
	matches!(type_code, ValueKind::Utf8 | ValueKind::Blob | ValueKind::Any | ValueKind::DictionaryId)
}

fn finalize_buffer(
	type_code: ValueKind,
	family: Option<(Precision, Scale)>,
	mut data: Vec<u8>,
	offsets: Option<Vec<u64>>,
	bitvec: Option<Vec<u8>>,
	written_count: usize,
) -> Result<ColumnBuffer, i32> {
	let make_option_wrapped = |inner: ColumnBuffer| match bitvec {
		Some(mut bytes) => {
			bytes.truncate(written_count.div_ceil(8));
			inner.with_nulls(NullBuffer::new(BooleanBuffer::new(Buffer::from_vec(bytes), 0, written_count)))
		}
		None => inner,
	};

	let inner = match type_code {
		ValueKind::Boolean => {
			data.truncate(written_count.div_ceil(8));
			ColumnBuffer::Bool(BooleanArray::from(BooleanBuffer::new(
				Buffer::from_vec(data),
				0,
				written_count,
			)))
		}
		ValueKind::Float4 => ColumnBuffer::float4(
			numeric_bytes_to_vec::<f32>(&data, written_count).ok_or(EXTERN_C_ERROR_INTERNAL)?,
		),
		ValueKind::Float8 => ColumnBuffer::float8(
			numeric_bytes_to_vec::<f64>(&data, written_count).ok_or(EXTERN_C_ERROR_INTERNAL)?,
		),
		ValueKind::Int1 => ColumnBuffer::int1(
			numeric_bytes_to_vec::<i8>(&data, written_count).ok_or(EXTERN_C_ERROR_INTERNAL)?,
		),
		ValueKind::Int2 => ColumnBuffer::int2(
			numeric_bytes_to_vec::<i16>(&data, written_count).ok_or(EXTERN_C_ERROR_INTERNAL)?,
		),
		ValueKind::Int4 => ColumnBuffer::int4(
			numeric_bytes_to_vec::<i32>(&data, written_count).ok_or(EXTERN_C_ERROR_INTERNAL)?,
		),
		ValueKind::Int8 => ColumnBuffer::int8(
			numeric_bytes_to_vec::<i64>(&data, written_count).ok_or(EXTERN_C_ERROR_INTERNAL)?,
		),
		ValueKind::Int16 => ColumnBuffer::int16(
			numeric_bytes_to_vec::<i128>(&data, written_count).ok_or(EXTERN_C_ERROR_INTERNAL)?,
		),
		ValueKind::Uint1 => ColumnBuffer::uint1(
			numeric_bytes_to_vec::<u8>(&data, written_count).ok_or(EXTERN_C_ERROR_INTERNAL)?,
		),
		ValueKind::Uint2 => ColumnBuffer::uint2(
			numeric_bytes_to_vec::<u16>(&data, written_count).ok_or(EXTERN_C_ERROR_INTERNAL)?,
		),
		ValueKind::Uint4 => ColumnBuffer::uint4(
			numeric_bytes_to_vec::<u32>(&data, written_count).ok_or(EXTERN_C_ERROR_INTERNAL)?,
		),
		ValueKind::Uint8 => ColumnBuffer::uint8(
			numeric_bytes_to_vec::<u64>(&data, written_count).ok_or(EXTERN_C_ERROR_INTERNAL)?,
		),
		ValueKind::Uint16 => ColumnBuffer::uint16(
			numeric_bytes_to_vec::<u128>(&data, written_count).ok_or(EXTERN_C_ERROR_INTERNAL)?,
		),
		ValueKind::Date => {
			let v = numeric_bytes_to_vec::<Date>(&data, written_count).ok_or(EXTERN_C_ERROR_INTERNAL)?;
			ColumnBuffer::Date(date_array(v))
		}
		ValueKind::DateTime => {
			let v = numeric_bytes_to_vec::<DateTime>(&data, written_count)
				.ok_or(EXTERN_C_ERROR_INTERNAL)?;
			ColumnBuffer::DateTime(datetime_array(v))
		}
		ValueKind::Time => {
			let v = numeric_bytes_to_vec::<Time>(&data, written_count).ok_or(EXTERN_C_ERROR_INTERNAL)?;
			ColumnBuffer::Time(time_array(v))
		}
		ValueKind::Duration => {
			let v = numeric_bytes_to_vec::<Duration>(&data, written_count)
				.ok_or(EXTERN_C_ERROR_INTERNAL)?;
			ColumnBuffer::Duration(duration_array(v))
		}
		ValueKind::IdentityId => {
			let v = numeric_bytes_to_vec::<IdentityId>(&data, written_count)
				.ok_or(EXTERN_C_ERROR_INTERNAL)?;
			ColumnBuffer::IdentityId(identity_id_array(v))
		}
		ValueKind::Uuid4 => {
			let v = numeric_bytes_to_vec::<Uuid4>(&data, written_count).ok_or(EXTERN_C_ERROR_INTERNAL)?;
			ColumnBuffer::Uuid4(uuid4_array(v))
		}
		ValueKind::Uuid7 => {
			let v = numeric_bytes_to_vec::<Uuid7>(&data, written_count).ok_or(EXTERN_C_ERROR_INTERNAL)?;
			ColumnBuffer::Uuid7(uuid7_array(v))
		}
		ValueKind::Utf8 => {
			let offsets = offsets.unwrap_or_else(|| vec![0u64]);

			let payload_len = *offsets.last().unwrap_or(&0) as usize;
			data.truncate(payload_len);
			let (offsets, values) = checked_varlen_parts(data, offsets)?;
			ColumnBuffer::Utf8 {
				container: LargeStringArray::try_new(offsets, values, None)
					.map_err(|_| EXTERN_C_ERROR_INVALID_UTF8)?,
				max_bytes: MaxBytes::MAX,
			}
		}
		ValueKind::Blob => {
			let offsets = offsets.unwrap_or_else(|| vec![0u64]);
			let payload_len = *offsets.last().unwrap_or(&0) as usize;
			data.truncate(payload_len);
			let (offsets, values) = checked_varlen_parts(data, offsets)?;
			ColumnBuffer::Blob {
				container: LargeBinaryArray::try_new(offsets, values, None)
					.map_err(|_| EXTERN_C_ERROR_MARSHAL)?,
				max_bytes: MaxBytes::MAX,
			}
		}
		ValueKind::Decimal => {
			let (precision, scale) = family.ok_or(EXTERN_C_ERROR_INTERNAL)?;
			decode_family_column(type_code, precision, scale, &data, written_count)
				.map_err(|_| EXTERN_C_ERROR_MARSHAL)?
		}
		ValueKind::Any => {
			let values: Vec<Value> = decode_per_element::<Value>(&data, &offsets, written_count, |bytes| {
				decode_any_cell(bytes).ok()
			})
			.ok_or(EXTERN_C_ERROR_INTERNAL)?;
			ColumnBuffer::Any {
				container: any_array(values),
				declared_type: None,
			}
		}
		ValueKind::DictionaryId => {
			let entries: Vec<DictionaryEntryId> =
				decode_per_element::<DictionaryEntryId>(&data, &offsets, written_count, |bytes| {
					decode_dictionary_id_cell(bytes).ok()
				})
				.ok_or(EXTERN_C_ERROR_INTERNAL)?;
			ColumnBuffer::dictionary_id(entries)
		}
		_ => return Err(EXTERN_C_ERROR_INTERNAL),
	};
	Ok(make_option_wrapped(inner))
}

fn checked_varlen_parts(data: Vec<u8>, offsets: Vec<u64>) -> Result<(OffsetBuffer<i64>, Buffer), i32> {
	let offsets = offsets
		.into_iter()
		.map(i64::try_from)
		.collect::<Result<Vec<i64>, _>>()
		.map_err(|_| EXTERN_C_ERROR_MARSHAL)?;
	let within_data =
		offsets.last().is_some_and(|&last| usize::try_from(last).is_ok_and(|last| last <= data.len()));
	if !within_data || !offsets.is_sorted() {
		return Err(EXTERN_C_ERROR_MARSHAL);
	}
	Ok((OffsetBuffer::new(ScalarBuffer::from(offsets)), Buffer::from_vec(data)))
}

fn decode_per_element<T>(
	data: &[u8],
	offsets: &Option<Vec<u64>>,
	count: usize,
	decode: impl Fn(&[u8]) -> Option<T>,
) -> Option<Vec<T>> {
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
		out.push(decode(&data[start..end])?);
	}
	Some(out)
}

fn numeric_bytes_to_vec<T: Copy>(data: &[u8], count: usize) -> Option<Vec<T>> {
	let needed = count.checked_mul(mem::size_of::<T>())?;
	if data.len() < needed {
		return None;
	}
	let mut v: Vec<T> = Vec::with_capacity(count);
	// SAFETY: bounds checked above and `v` is fresh, so no overlap; bytes are copied, so `data` needs no alignment.
	unsafe {
		ptr::copy_nonoverlapping(data.as_ptr(), v.as_mut_ptr() as *mut u8, needed);
		v.set_len(count);
	}
	Some(v)
}

#[cfg(test)]
mod tests {
	use std::ptr;

	use arrow_array::Array;
	use postcard::to_allocvec;
	use reifydb_codec::tag::ValueKind;
	use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
	use reifydb_sdk::{
		common::extern_c::wire::{
			callbacks::builder::ColumnBufferHandle,
			status::{EXTERN_C_ERROR_INVALID_UTF8, EXTERN_C_ERROR_MARSHAL, EXTERN_C_OK},
		},
		flow::operator::{change::BorrowedColumns, extern_c::binding::arena::Arena},
	};
	use reifydb_value::{
		fragment::Fragment,
		value::{
			blob::Blob,
			constraint::{precision::Precision, scale::Scale},
			container::decimal_array::{INT16_DATA_TYPE, UINT16_DATA_TYPE, u128s},
			decimal::Decimal,
			dictionary::DictionaryEntryId,
		},
	};
	use serde_json::to_string;

	use super::{
		BuilderRegistry, BuilderSlot, Handle, finalize_buffer, host_builder_acquire, host_builder_commit,
		host_builder_data_ptr, host_builder_offsets_ptr, numeric_bytes_to_vec, with_registry,
	};

	fn decimals(texts: &[&str]) -> Vec<Decimal> {
		texts.iter().map(|t| Decimal::parse(t).unwrap()).collect()
	}

	fn commit_varlen(
		registry: &BuilderRegistry,
		type_code: ValueKind,
		data: &[u8],
		offsets: &[u64],
	) -> (i32, *mut ColumnBufferHandle) {
		// Capacity must cover every byte and offset, otherwise the commit fails before the checks run.
		let rows = offsets.len() - 1;
		with_registry(registry, || {
			// SAFETY: a registry is installed and both copies stay within the capacity acquired for them.
			unsafe {
				let handle =
					host_builder_acquire(ptr::null_mut(), type_code, 0, 0, data.len().max(rows));
				ptr::copy_nonoverlapping(data.as_ptr(), host_builder_data_ptr(handle), data.len());
				ptr::copy_nonoverlapping(
					offsets.as_ptr(),
					host_builder_offsets_ptr(handle),
					offsets.len(),
				);
				(host_builder_commit(handle, rows), handle)
			}
		})
	}

	fn committed_buffer(registry: &BuilderRegistry, handle: *mut ColumnBufferHandle) -> ColumnBuffer {
		match registry.inner.lock().slots.remove(&Handle::decode(handle).id) {
			Some(BuilderSlot::Committed(committed)) => committed.buffer,
			_ => panic!("a successful commit must leave a committed column behind"),
		}
	}

	fn host_to_guest_to_host(input: ColumnBuffer) -> ColumnBuffer {
		let columns = Columns::new(vec![ColumnWithName::new(Fragment::internal("c"), input)]);
		let mut arena = Arena::new();
		let ffi = arena.marshal_columns(&columns);
		// SAFETY: `ffi` points into `arena` and `columns`, and both outlive every read below.
		let borrowed = unsafe { BorrowedColumns::from_extern_c(&ffi) };
		let column = borrowed.column_at_index(0).expect("one column was marshalled");
		let (data, offsets, rows) = (column.data_bytes(), column.offsets(), column.row_count());
		let registry = BuilderRegistry::new();
		let (code, handle) = with_registry(&registry, || {
			// SAFETY: a registry is installed and both copies stay within the capacity acquired for them.
			unsafe {
				let handle = host_builder_acquire(
					ptr::null_mut(),
					column.type_code(),
					column.precision(),
					column.scale(),
					data.len().max(rows),
				);
				ptr::copy_nonoverlapping(data.as_ptr(), host_builder_data_ptr(handle), data.len());
				if !offsets.is_empty() {
					ptr::copy_nonoverlapping(
						offsets.as_ptr(),
						host_builder_offsets_ptr(handle),
						offsets.len(),
					);
				}
				(host_builder_commit(handle, rows), handle)
			}
		});
		assert_eq!(code, EXTERN_C_OK);
		committed_buffer(&registry, handle)
	}

	#[test]
	fn int16_extremes_round_trip_through_the_host_builder() {
		// A narrowed row or an arrow default scale 10 on the rebuilt column corrupts every 128 bit value.
		let values = [i128::MIN, i128::MAX, 0, -1];
		let output = host_to_guest_to_host(ColumnBuffer::int16(values));
		let ColumnBuffer::Int16(array) = &output else {
			panic!("expected a plain Int16 column, got {:?}", output.get_type())
		};
		assert_eq!(&array.values()[..], &values);
		assert_eq!(array.data_type(), &INT16_DATA_TYPE);
	}

	#[test]
	fn uint16_past_64_bits_round_trip_through_the_host_builder() {
		// Any hop through a 64 bit conversion of the 256 bit native loses 2^64 and u128::MAX.
		let values = [1u128 << 64, u128::MAX, (1u128 << 64) - 1, 1u128 << 127, 0];
		let output = host_to_guest_to_host(ColumnBuffer::uint16(values));
		let ColumnBuffer::Uint16(array) = &output else {
			panic!("expected a plain Uint16 column, got {:?}", output.get_type())
		};
		assert_eq!(u128s(array), values);
		assert_eq!(array.data_type(), &UINT16_DATA_TYPE);
	}

	#[test]
	fn every_dictionary_width_round_trips_through_the_host_builder() {
		// A slip between the cell's byte count width and the row's width tag changes the entry id.
		let entries = vec![
			DictionaryEntryId::U1(0),
			DictionaryEntryId::U1(u8::MAX),
			DictionaryEntryId::U2(0),
			DictionaryEntryId::U2(u16::MAX),
			DictionaryEntryId::U4(0),
			DictionaryEntryId::U4(u32::MAX),
			DictionaryEntryId::U8(0),
			DictionaryEntryId::U8(u64::MAX),
			DictionaryEntryId::U16(0),
			DictionaryEntryId::U16(u128::MAX),
		];
		let output = host_to_guest_to_host(ColumnBuffer::dictionary_id(entries.clone()));
		assert_eq!(output, ColumnBuffer::dictionary_id(entries));
	}

	#[test]
	fn guest_utf8_round_trips_through_checked_construction() {
		// Checking offsets and UTF-8 must not reject or reshape well formed guest strings.
		let registry = BuilderRegistry::new();
		let (code, handle) = commit_varlen(&registry, ValueKind::Utf8, "abcdéf".as_bytes(), &[0, 1, 3, 7]);
		assert_eq!(code, EXTERN_C_OK);
		assert_eq!(committed_buffer(&registry, handle), ColumnBuffer::utf8(["a", "bc", "déf"]));
	}

	#[test]
	fn guest_blob_round_trips_through_checked_construction() {
		// Checking offsets must not reject or reshape well formed guest blobs, including non UTF-8 bytes.
		let registry = BuilderRegistry::new();
		let (code, handle) = commit_varlen(&registry, ValueKind::Blob, &[0xff, 1, 2, 0xfe], &[0, 1, 1, 4]);
		assert_eq!(code, EXTERN_C_OK);
		assert_eq!(
			committed_buffer(&registry, handle),
			ColumnBuffer::blob([Blob::new(vec![0xff]), Blob::new(vec![]), Blob::new(vec![1, 2, 0xfe])])
		);
	}

	#[test]
	fn guest_invalid_utf8_is_rejected_without_a_column() {
		// Unchecked construction would hand bytes that are not UTF-8 to code that assumes they are.
		let registry = BuilderRegistry::new();
		let (code, _) = commit_varlen(&registry, ValueKind::Utf8, &[b'a', 0xff, 0xfe], &[0, 1, 3]);
		assert_eq!(code, EXTERN_C_ERROR_INVALID_UTF8);
		assert!(registry.inner.lock().slots.is_empty(), "a rejected commit must not leave a column behind");
	}

	#[test]
	fn guest_utf8_offset_inside_a_char_is_rejected_without_a_column() {
		// Validating only the whole buffer misses rows that split a multi-byte char.
		let registry = BuilderRegistry::new();
		let (code, _) = commit_varlen(&registry, ValueKind::Utf8, "é".as_bytes(), &[0, 1, 2]);
		assert_eq!(code, EXTERN_C_ERROR_INVALID_UTF8);
		assert!(registry.inner.lock().slots.is_empty(), "a rejected commit must not leave a column behind");
	}

	#[test]
	fn guest_utf8_non_monotonic_offsets_are_a_marshal_error() {
		// A decreasing offset would describe a row with negative length.
		let registry = BuilderRegistry::new();
		let (code, _) = commit_varlen(&registry, ValueKind::Utf8, b"abc", &[0, 3, 1]);
		assert_eq!(code, EXTERN_C_ERROR_MARSHAL);
		assert!(registry.inner.lock().slots.is_empty(), "a rejected commit must not leave a column behind");
	}

	#[test]
	fn guest_blob_non_monotonic_offsets_are_a_marshal_error() {
		// A decreasing offset would describe a row with negative length.
		let registry = BuilderRegistry::new();
		let (code, _) = commit_varlen(&registry, ValueKind::Blob, &[1, 2, 3], &[0, 3, 1]);
		assert_eq!(code, EXTERN_C_ERROR_MARSHAL);
		assert!(registry.inner.lock().slots.is_empty(), "a rejected commit must not leave a column behind");
	}

	#[test]
	fn utf8_last_offset_past_the_data_is_a_marshal_error() {
		// A last offset past the data would read bytes the guest never wrote.
		let result = finalize_buffer(ValueKind::Utf8, None, b"ab".to_vec(), Some(vec![0, 1, 5]), None, 2);
		assert_eq!(result.err(), Some(EXTERN_C_ERROR_MARSHAL));
	}

	#[test]
	fn blob_last_offset_past_the_data_is_a_marshal_error() {
		// A last offset past the data would read bytes the guest never wrote.
		let result = finalize_buffer(ValueKind::Blob, None, vec![1, 2], Some(vec![0, 1, 5]), None, 2);
		assert_eq!(result.err(), Some(EXTERN_C_ERROR_MARSHAL));
	}

	#[test]
	fn utf8_offset_beyond_i64_is_a_marshal_error() {
		// Arrow offsets are i64, so a larger u64 offset cannot be represented and must not wrap negative.
		let result = finalize_buffer(ValueKind::Utf8, None, b"ab".to_vec(), Some(vec![0, u64::MAX]), None, 1);
		assert_eq!(result.err(), Some(EXTERN_C_ERROR_MARSHAL));
	}

	#[test]
	fn guest_bool_column_serializes_like_an_equal_host_column() {
		// A guest hands over one data byte per row, yet equal Bool columns must serialize byte for byte alike.
		let guest =
			finalize_buffer(ValueKind::Boolean, None, vec![5, 0, 0], None, None, 3).expect("a Bool column");
		let host = ColumnBuffer::bool([true, false, true]);
		assert_eq!(to_string(&guest).unwrap(), to_string(&host).unwrap());
		assert_eq!(to_allocvec(&guest).unwrap(), to_allocvec(&host).unwrap());
		let ColumnBuffer::Bool(bits) = &guest else {
			panic!("expected a Bool column, got {:?}", guest.get_type())
		};
		assert_eq!(bits.values().inner().len(), 1, "3 rows must pack into exactly ceil(3 / 8) bytes");
	}

	#[test]
	fn int16_rows_copy_out_of_a_byte_buffer_at_any_alignment() {
		// A Vec<u8> only promises alignment 1, so a typed i128 copy out of it is a misaligned read.
		let values = [i128::MIN, 1, i128::MAX];
		let bytes: Vec<u8> = [0u8].into_iter().chain(values.iter().flat_map(|v| v.to_ne_bytes())).collect();
		assert_eq!(numeric_bytes_to_vec::<i128>(&bytes[1..], values.len()), Some(values.to_vec()));
	}

	#[test]
	fn decimal_at_both_widths_round_trips_through_the_host_builder() {
		// Dropping the scale on the way back would read every unscaled value as a whole number.
		let (narrow, narrow_scale) = (Precision::new(38), Scale::new(10));
		let narrow_values =
			decimals(&["9999999999999999999999999999.9999999999", "-0.0000000001", "0", "12.5"]);
		let output = host_to_guest_to_host(ColumnBuffer::decimal(narrow, narrow_scale, narrow_values.clone()));
		assert_eq!(output, ColumnBuffer::decimal(narrow, narrow_scale, narrow_values));

		let (wide, wide_scale) = (Precision::MAX, Scale::new(20));
		let wide_values = decimals(&[
			"99999999999999999999999999999999999999999999999999999999.99999999999999999999",
			"-99999999999999999999999999999999999999999999999999999999.99999999999999999999",
			"0.00000000000000000001",
		]);
		let output = host_to_guest_to_host(ColumnBuffer::decimal(wide, wide_scale, wide_values.clone()));
		assert_eq!(output, ColumnBuffer::decimal(wide, wide_scale, wide_values));
	}

	#[test]
	fn family_acquire_with_invalid_precision_or_scale_returns_null() {
		// Without the check a guest could size cells from a precision the column type cannot hold.
		let registry = BuilderRegistry::new();
		let bad = [(ValueKind::Decimal, 10, 11), (ValueKind::Decimal, 0, 0)];
		for (kind, precision, scale) in bad {
			// SAFETY: a registry is installed and nothing is written through the handle.
			let handle = with_registry(&registry, || unsafe {
				host_builder_acquire(ptr::null_mut(), kind, precision, scale, 4)
			});
			assert!(
				handle.is_null(),
				"{kind:?} with precision {precision} and scale {scale} must be rejected"
			);
		}
		assert!(registry.inner.lock().slots.is_empty(), "a rejected acquire must not leave a builder behind");
	}

	#[test]
	fn family_cell_with_more_digits_than_the_precision_is_a_marshal_error() {
		// Building the column anyway would panic in arrow or store a value the column type forbids.
		let precision = Precision::new(5);
		let mut cells = 100_000i128.to_le_bytes().to_vec();
		cells.extend_from_slice(&7i128.to_le_bytes());
		for kind in [ValueKind::Decimal] {
			let result = finalize_buffer(kind, Some((precision, Scale::MIN)), cells.clone(), None, None, 2);
			assert_eq!(result.err(), Some(EXTERN_C_ERROR_MARSHAL), "{kind:?} must reject a 6 digit cell");
		}
	}
}
