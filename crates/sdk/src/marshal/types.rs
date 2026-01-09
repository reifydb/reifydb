// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Type-specific unmarshalling functions

use reifydb_abi::{BufferFFI, ColumnDataFFI};
use reifydb_core::value::container::BoolContainer;
use reifydb_type::{Date, DateTime, Duration, IdentityId, IsNumber, Time, Uuid4, Uuid7, Value};
use serde::de::DeserializeOwned;

use crate::ffi::Arena;

impl Arena {
	pub(super) fn unmarshal_bool_data(&self, ffi: &ColumnDataFFI, bitvec: reifydb_type::BitVec) -> BoolContainer {
		use reifydb_core::value::container::BoolContainer;

		let row_count = ffi.row_count;
		if ffi.data.is_empty() {
			return BoolContainer::new(vec![false; row_count], bitvec);
		}

		unsafe {
			let bytes = std::slice::from_raw_parts(ffi.data.ptr, ffi.data.len);
			let mut values = Vec::with_capacity(row_count);
			for i in 0..row_count {
				let byte_idx = i / 8;
				let bit_idx = i % 8;
				let val = if byte_idx < bytes.len() {
					(bytes[byte_idx] & (1 << bit_idx)) != 0
				} else {
					false
				};
				values.push(val);
			}
			BoolContainer::new(values, bitvec)
		}
	}

	/// Unmarshal numeric data
	pub(super) fn unmarshal_numeric_data<T: Copy + Default + IsNumber>(
		&self,
		ffi: &ColumnDataFFI,
		bitvec: reifydb_type::BitVec,
	) -> reifydb_core::value::container::NumberContainer<T> {
		use reifydb_core::value::container::NumberContainer;

		let row_count = ffi.row_count;
		if ffi.data.is_empty() {
			return NumberContainer::new(vec![T::default(); row_count], bitvec);
		}

		unsafe {
			let ptr = ffi.data.ptr as *const T;
			let len = ffi.data.len / size_of::<T>();
			let slice = std::slice::from_raw_parts(ptr, len);
			NumberContainer::new(slice.to_vec(), bitvec)
		}
	}

	/// Unmarshal UTF8 data with offsets
	pub(super) fn unmarshal_utf8_data(
		&self,
		ffi: &ColumnDataFFI,
		bitvec: reifydb_type::BitVec,
	) -> reifydb_core::value::container::Utf8Container {
		use reifydb_core::value::container::Utf8Container;

		let row_count = ffi.row_count;
		if ffi.data.is_empty() || ffi.offsets.is_empty() {
			return Utf8Container::new(vec![String::new(); row_count], bitvec);
		}

		unsafe {
			let data = std::slice::from_raw_parts(ffi.data.ptr, ffi.data.len);
			let offsets = self.read_offsets(&ffi.offsets);

			let mut strings = Vec::with_capacity(row_count);
			for i in 0..row_count {
				let start = offsets[i] as usize;
				let end = offsets[i + 1] as usize;
				let s = std::str::from_utf8(&data[start..end]).unwrap_or("").to_string();
				strings.push(s);
			}

			Utf8Container::new(strings, bitvec)
		}
	}

	/// Unmarshal date data
	pub(super) fn unmarshal_date_data(
		&self,
		ffi: &ColumnDataFFI,
		bitvec: reifydb_type::BitVec,
	) -> reifydb_core::value::container::TemporalContainer<Date> {
		use reifydb_core::value::container::TemporalContainer;

		let row_count = ffi.row_count;
		if ffi.data.is_empty() {
			return TemporalContainer::new(vec![Date::default(); row_count], bitvec);
		}

		unsafe {
			let ptr = ffi.data.ptr as *const i32;
			let len = ffi.data.len / size_of::<i32>();
			let slice = std::slice::from_raw_parts(ptr, len);
			let dates: Vec<Date> = slice
				.iter()
				.map(|&days| Date::from_days_since_epoch(days).unwrap_or_default())
				.collect();
			TemporalContainer::new(dates, bitvec)
		}
	}

	/// Unmarshal datetime data
	pub(super) fn unmarshal_datetime_data(
		&self,
		ffi: &ColumnDataFFI,
		bitvec: reifydb_type::BitVec,
	) -> reifydb_core::value::container::TemporalContainer<DateTime> {
		use reifydb_core::value::container::TemporalContainer;

		let row_count = ffi.row_count;
		if ffi.data.is_empty() {
			return TemporalContainer::new(vec![DateTime::default(); row_count], bitvec);
		}

		unsafe {
			let ptr = ffi.data.ptr as *const i64;
			let len = ffi.data.len / size_of::<i64>();
			let slice = std::slice::from_raw_parts(ptr, len);
			let datetimes: Vec<DateTime> =
				slice.iter().map(|&ts| DateTime::from_timestamp(ts).unwrap_or_default()).collect();
			TemporalContainer::new(datetimes, bitvec)
		}
	}

	/// Unmarshal time data
	pub(super) fn unmarshal_time_data(
		&self,
		ffi: &ColumnDataFFI,
		bitvec: reifydb_type::BitVec,
	) -> reifydb_core::value::container::TemporalContainer<Time> {
		use reifydb_core::value::container::TemporalContainer;

		let row_count = ffi.row_count;
		if ffi.data.is_empty() {
			return TemporalContainer::new(vec![Time::default(); row_count], bitvec);
		}

		unsafe {
			let ptr = ffi.data.ptr as *const u64;
			let len = ffi.data.len / size_of::<u64>();
			let slice = std::slice::from_raw_parts(ptr, len);
			let times: Vec<Time> = slice
				.iter()
				.map(|&ns| Time::from_nanos_since_midnight(ns).unwrap_or_default())
				.collect();
			TemporalContainer::new(times, bitvec)
		}
	}

	/// Unmarshal duration data (deserialize with postcard since Duration has 3 fields)
	pub(super) fn unmarshal_duration_data(
		&self,
		ffi: &ColumnDataFFI,
		bitvec: reifydb_type::BitVec,
	) -> reifydb_core::value::container::TemporalContainer<Duration> {
		use reifydb_core::value::container::TemporalContainer;

		let row_count = ffi.row_count;
		if ffi.data.is_empty() || ffi.offsets.is_empty() {
			return TemporalContainer::new(vec![Duration::default(); row_count], bitvec);
		}

		unsafe {
			let data = std::slice::from_raw_parts(ffi.data.ptr, ffi.data.len);
			let offsets = self.read_offsets(&ffi.offsets);

			let mut durations = Vec::with_capacity(row_count);
			for i in 0..row_count {
				let start = offsets[i] as usize;
				let end = offsets[i + 1] as usize;
				let duration: Duration = postcard::from_bytes(&data[start..end]).unwrap_or_default();
				durations.push(duration);
			}

			TemporalContainer::new(durations, bitvec)
		}
	}

	/// Unmarshal identity ID data
	pub(super) fn unmarshal_identity_id_data(
		&self,
		ffi: &ColumnDataFFI,
		bitvec: reifydb_type::BitVec,
	) -> reifydb_core::value::container::IdentityIdContainer {
		use reifydb_core::value::container::IdentityIdContainer;
		use uuid::Uuid as StdUuid;

		let row_count = ffi.row_count;
		if ffi.data.is_empty() {
			return IdentityIdContainer::new(vec![IdentityId::default(); row_count], bitvec);
		}

		unsafe {
			let bytes = std::slice::from_raw_parts(ffi.data.ptr, ffi.data.len);
			let ids: Vec<IdentityId> = bytes
				.chunks(16)
				.map(|chunk| {
					let mut arr = [0u8; 16];
					arr.copy_from_slice(chunk);
					// IdentityId wraps Uuid7 which wraps StdUuid
					IdentityId(Uuid7(StdUuid::from_bytes(arr)))
				})
				.collect();
			IdentityIdContainer::new(ids, bitvec)
		}
	}

	/// Unmarshal UUID4 data
	pub(super) fn unmarshal_uuid4_data(
		&self,
		ffi: &ColumnDataFFI,
		bitvec: reifydb_type::BitVec,
	) -> reifydb_core::value::container::UuidContainer<Uuid4> {
		use reifydb_core::value::container::UuidContainer;
		use uuid::Uuid as StdUuid;

		let row_count = ffi.row_count;
		if ffi.data.is_empty() {
			return UuidContainer::new(vec![Uuid4::default(); row_count], bitvec);
		}

		unsafe {
			let bytes = std::slice::from_raw_parts(ffi.data.ptr, ffi.data.len);
			let uuids: Vec<Uuid4> = bytes
				.chunks(16)
				.map(|chunk| {
					let mut arr = [0u8; 16];
					arr.copy_from_slice(chunk);
					Uuid4(StdUuid::from_bytes(arr))
				})
				.collect();
			UuidContainer::new(uuids, bitvec)
		}
	}

	/// Unmarshal UUID7 data
	pub(super) fn unmarshal_uuid7_data(
		&self,
		ffi: &ColumnDataFFI,
		bitvec: reifydb_type::BitVec,
	) -> reifydb_core::value::container::UuidContainer<Uuid7> {
		use reifydb_core::value::container::UuidContainer;
		use uuid::Uuid as StdUuid;

		let row_count = ffi.row_count;
		if ffi.data.is_empty() {
			return UuidContainer::new(vec![Uuid7::default(); row_count], bitvec);
		}

		unsafe {
			let bytes = std::slice::from_raw_parts(ffi.data.ptr, ffi.data.len);
			let uuids: Vec<Uuid7> = bytes
				.chunks(16)
				.map(|chunk| {
					let mut arr = [0u8; 16];
					arr.copy_from_slice(chunk);
					Uuid7(StdUuid::from_bytes(arr))
				})
				.collect();
			UuidContainer::new(uuids, bitvec)
		}
	}

	/// Unmarshal blob data with offsets
	pub(super) fn unmarshal_blob_data(
		&self,
		ffi: &ColumnDataFFI,
		bitvec: reifydb_type::BitVec,
	) -> reifydb_core::value::container::BlobContainer {
		use reifydb_core::value::container::BlobContainer;
		use reifydb_type::Blob;

		let row_count = ffi.row_count;
		if ffi.data.is_empty() || ffi.offsets.is_empty() {
			return BlobContainer::new(vec![Blob::empty(); row_count], bitvec);
		}

		unsafe {
			let data = std::slice::from_raw_parts(ffi.data.ptr, ffi.data.len);
			let offsets = self.read_offsets(&ffi.offsets);

			let mut blobs = Vec::with_capacity(row_count);
			for i in 0..row_count {
				let start = offsets[i] as usize;
				let end = offsets[i + 1] as usize;
				blobs.push(Blob::new(data[start..end].to_vec()));
			}

			BlobContainer::new(blobs, bitvec)
		}
	}

	/// Unmarshal serialized data with offsets
	pub(super) fn unmarshal_serialized_data<T: Default + Clone + DeserializeOwned + IsNumber>(
		&self,
		ffi: &ColumnDataFFI,
		bitvec: reifydb_type::BitVec,
	) -> reifydb_core::value::container::NumberContainer<T> {
		use reifydb_core::value::container::NumberContainer;

		let row_count = ffi.row_count;
		if ffi.data.is_empty() || ffi.offsets.is_empty() {
			return NumberContainer::new(vec![T::default(); row_count], bitvec);
		}

		unsafe {
			let data = std::slice::from_raw_parts(ffi.data.ptr, ffi.data.len);
			let offsets = self.read_offsets(&ffi.offsets);

			let mut values = Vec::with_capacity(row_count);
			for i in 0..row_count {
				let start = offsets[i] as usize;
				let end = offsets[i + 1] as usize;
				let value: T = postcard::from_bytes(&data[start..end]).unwrap_or_default();
				values.push(value);
			}

			NumberContainer::new(values, bitvec)
		}
	}

	/// Unmarshal Any data with offsets
	pub(super) fn unmarshal_any_data(
		&self,
		ffi: &ColumnDataFFI,
		bitvec: reifydb_type::BitVec,
	) -> reifydb_core::value::container::AnyContainer {
		use reifydb_core::value::container::AnyContainer;

		let row_count = ffi.row_count;
		if ffi.data.is_empty() || ffi.offsets.is_empty() {
			return AnyContainer::new(vec![Box::new(Value::Undefined); row_count], bitvec);
		}

		unsafe {
			let data = std::slice::from_raw_parts(ffi.data.ptr, ffi.data.len);
			let offsets = self.read_offsets(&ffi.offsets);

			let mut values = Vec::with_capacity(row_count);
			for i in 0..row_count {
				let start = offsets[i] as usize;
				let end = offsets[i + 1] as usize;
				let value: Value = postcard::from_bytes(&data[start..end]).unwrap_or(Value::Undefined);
				values.push(Box::new(value));
			}

			AnyContainer::new(values, bitvec)
		}
	}

	/// Helper: read offsets array from FFI buffer
	pub(super) fn read_offsets(&self, ffi: &BufferFFI) -> Vec<u64> {
		if ffi.is_empty() {
			return Vec::new();
		}
		unsafe {
			let ptr = ffi.ptr as *const u64;
			let len = ffi.len / size_of::<u64>();
			std::slice::from_raw_parts(ptr, len).to_vec()
		}
	}
}
