//! Column marshalling between Rust and FFI types

use std::mem::size_of;

use reifydb_core::value::column::{Column, ColumnData, Columns};
use reifydb_flow_operator_abi::*;
use reifydb_type::{
	Date, DateTime, Decimal, Duration, Fragment, IdentityId, Int, IsNumber, RowNumber, Time, Uint, Uuid4, Uuid7,
	Value,
	value::constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
};
use serde::{Serialize, de::DeserializeOwned};

use crate::marshal::Marshaller;

impl Marshaller {
	/// Marshal Columns to FFI representation
	pub fn marshal_columns(&mut self, columns: &Columns) -> ColumnsFFI {
		let row_count = columns.row_count();
		let column_count = columns.len();

		if row_count == 0 && column_count == 0 {
			return ColumnsFFI::empty();
		}

		// Marshal row numbers
		let row_numbers_ptr = if !columns.row_numbers.is_empty() {
			let size = columns.row_numbers.len() * size_of::<u64>();
			let ptr = self.arena.alloc(size) as *mut u64;
			if !ptr.is_null() {
				unsafe {
					for (i, rn) in columns.row_numbers.iter().enumerate() {
						*ptr.add(i) = (*rn).into();
					}
				}
			}
			ptr as *const u64
		} else {
			std::ptr::null()
		};

		// Marshal each column
		let columns_size = column_count * size_of::<ColumnFFI>();
		let columns_ptr = self.arena.alloc(columns_size) as *mut ColumnFFI;

		if !columns_ptr.is_null() {
			unsafe {
				for (i, col) in columns.iter().enumerate() {
					let col_ffi = self.marshal_column(col);
					*columns_ptr.add(i) = col_ffi;
				}
			}
		}

		ColumnsFFI {
			row_count,
			column_count,
			row_numbers: row_numbers_ptr,
			columns: columns_ptr as *const ColumnFFI,
		}
	}

	/// Marshal a single Column to FFI representation
	fn marshal_column(&mut self, column: &Column) -> ColumnFFI {
		// Marshal column name
		let name_bytes = column.name.text().as_bytes();
		let name_ptr = self.arena.copy_bytes(name_bytes);
		let name = BufferFFI {
			ptr: name_ptr,
			len: name_bytes.len(),
			cap: name_bytes.len(),
		};

		// Marshal column data
		let data = self.marshal_column_data(column.data());

		ColumnFFI {
			name,
			data,
		}
	}

	/// Marshal ColumnData to FFI representation
	fn marshal_column_data(&mut self, data: &ColumnData) -> ColumnDataFFI {
		let row_count = data.len();

		if row_count == 0 {
			return ColumnDataFFI {
				type_code: column_data_to_type_code(data),
				row_count: 0,
				data: BufferFFI::empty(),
				defined_bitvec: BufferFFI::empty(),
				offsets: BufferFFI::empty(),
			};
		}

		let type_code = column_data_to_type_code(data);

		// Marshal bitvec (iterate and pack bits)
		let defined_bitvec = if matches!(data, ColumnData::Undefined(_)) {
			BufferFFI::empty()
		} else {
			self.marshal_bitvec(data.bitvec())
		};

		// Marshal data and offsets based on type
		let (data_buffer, offsets_buffer) = self.marshal_column_data_bytes(data);

		ColumnDataFFI {
			type_code,
			row_count,
			data: data_buffer,
			defined_bitvec,
			offsets: offsets_buffer,
		}
	}

	/// Marshal a bitvec by iterating and packing bits
	fn marshal_bitvec(&mut self, bitvec: &reifydb_type::BitVec) -> BufferFFI {
		let len = bitvec.len();
		if len == 0 {
			return BufferFFI::empty();
		}

		let byte_count = (len + 7) / 8;
		let ptr = self.arena.alloc(byte_count);
		if ptr.is_null() {
			return BufferFFI::empty();
		}

		// Zero-initialize
		unsafe {
			std::ptr::write_bytes(ptr, 0, byte_count);
		}

		// Pack bits
		for (i, bit) in bitvec.iter().enumerate() {
			if bit {
				unsafe {
					*ptr.add(i / 8) |= 1 << (i % 8);
				}
			}
		}

		BufferFFI {
			ptr,
			len: byte_count,
			cap: byte_count,
		}
	}

	/// Marshal the raw data bytes for a ColumnData
	fn marshal_column_data_bytes(&mut self, data: &ColumnData) -> (BufferFFI, BufferFFI) {
		match data {
			// Fixed-size numeric types - use Deref to get slice
			ColumnData::Bool(container) => {
				// BoolContainer stores packed bits internally
				let len = container.len();
				let byte_count = (len + 7) / 8;
				let ptr = self.arena.alloc(byte_count);
				if !ptr.is_null() {
					unsafe {
						std::ptr::write_bytes(ptr, 0, byte_count);
					}
					for i in 0..len {
						if let Some(val) = container.get(i) {
							if val {
								unsafe {
									*ptr.add(i / 8) |= 1 << (i % 8);
								}
							}
						}
					}
				}
				(
					BufferFFI {
						ptr,
						len: byte_count,
						cap: byte_count,
					},
					BufferFFI::empty(),
				)
			}

			// Numeric types - use Deref to [T]
			ColumnData::Float4(container) => self.marshal_numeric_slice::<f32>(&**container),
			ColumnData::Float8(container) => self.marshal_numeric_slice::<f64>(&**container),
			ColumnData::Int1(container) => self.marshal_numeric_slice::<i8>(&**container),
			ColumnData::Int2(container) => self.marshal_numeric_slice::<i16>(&**container),
			ColumnData::Int4(container) => self.marshal_numeric_slice::<i32>(&**container),
			ColumnData::Int8(container) => self.marshal_numeric_slice::<i64>(&**container),
			ColumnData::Int16(container) => self.marshal_numeric_slice::<i128>(&**container),
			ColumnData::Uint1(container) => self.marshal_numeric_slice::<u8>(&**container),
			ColumnData::Uint2(container) => self.marshal_numeric_slice::<u16>(&**container),
			ColumnData::Uint4(container) => self.marshal_numeric_slice::<u32>(&**container),
			ColumnData::Uint8(container) => self.marshal_numeric_slice::<u64>(&**container),
			ColumnData::Uint16(container) => self.marshal_numeric_slice::<u128>(&**container),

			// Temporal types - extract encoded values
			ColumnData::Date(container) => {
				let dates: &[Date] = &**container;
				let encoded: Vec<i32> = dates.iter().map(|d| d.to_days_since_epoch()).collect();
				self.marshal_numeric_slice(&encoded)
			}
			ColumnData::DateTime(container) => {
				let datetimes: &[DateTime] = &**container;
				let encoded: Vec<i64> = datetimes.iter().map(|dt| dt.timestamp()).collect();
				self.marshal_numeric_slice(&encoded)
			}
			ColumnData::Time(container) => {
				let times: &[Time] = &**container;
				let encoded: Vec<u64> = times.iter().map(|t| t.to_nanos_since_midnight()).collect();
				self.marshal_numeric_slice(&encoded)
			}
			ColumnData::Duration(container) => {
				// Duration has 3 fields (months, days, nanos), serialize with postcard
				let durations: &[Duration] = &**container;
				self.marshal_serialized(durations)
			}

			// UUID types - 16 bytes each
			ColumnData::IdentityId(container) => {
				// IdentityId wraps Uuid7, which wraps StdUuid
				let ids: &[IdentityId] = &**container;
				let bytes: Vec<u8> =
					ids.iter().flat_map(|id| id.0.as_bytes().iter().copied()).collect();
				let ptr = self.arena.copy_bytes(&bytes);
				(
					BufferFFI {
						ptr,
						len: bytes.len(),
						cap: bytes.len(),
					},
					BufferFFI::empty(),
				)
			}
			ColumnData::Uuid4(container) => {
				let uuids: &[Uuid4] = &**container;
				let bytes: Vec<u8> =
					uuids.iter().flat_map(|u| u.0.as_bytes().iter().copied()).collect();
				let ptr = self.arena.copy_bytes(&bytes);
				(
					BufferFFI {
						ptr,
						len: bytes.len(),
						cap: bytes.len(),
					},
					BufferFFI::empty(),
				)
			}
			ColumnData::Uuid7(container) => {
				let uuids: &[Uuid7] = &**container;
				let bytes: Vec<u8> =
					uuids.iter().flat_map(|u| u.0.as_bytes().iter().copied()).collect();
				let ptr = self.arena.copy_bytes(&bytes);
				(
					BufferFFI {
						ptr,
						len: bytes.len(),
						cap: bytes.len(),
					},
					BufferFFI::empty(),
				)
			}

			// Variable-length types with offsets
			ColumnData::Utf8 {
				container,
				..
			} => {
				let strings: &[String] = &**container;
				self.marshal_strings(strings)
			}
			ColumnData::Blob {
				container,
				..
			} => {
				// Blob is a newtype around Vec<u8>, get bytes from each
				let blobs: &[reifydb_type::Blob] = &**container;
				self.marshal_blob_slices(blobs)
			}

			// Complex types - serialize with postcard
			ColumnData::Int {
				container,
				..
			} => {
				let values: &[Int] = &**container;
				self.marshal_serialized(values)
			}
			ColumnData::Uint {
				container,
				..
			} => {
				let values: &[Uint] = &**container;
				self.marshal_serialized(values)
			}
			ColumnData::Decimal {
				container,
				..
			} => {
				let values: &[Decimal] = &**container;
				self.marshal_serialized(values)
			}
			ColumnData::Any(container) => {
				let mut offsets: Vec<u64> = Vec::with_capacity(container.len() + 1);
				let mut data_bytes: Vec<u8> = Vec::new();
				offsets.push(0);
				for i in 0..container.len() {
					let value = container.get(i);
					let serialized = postcard::to_allocvec(&value).unwrap_or_default();
					data_bytes.extend_from_slice(&serialized);
					offsets.push(data_bytes.len() as u64);
				}
				self.marshal_with_offsets(&data_bytes, &offsets)
			}

			// Undefined has no data
			ColumnData::Undefined(_) => (BufferFFI::empty(), BufferFFI::empty()),
		}
	}

	/// Marshal a numeric slice to raw bytes
	fn marshal_numeric_slice<T: Copy>(&mut self, slice: &[T]) -> (BufferFFI, BufferFFI) {
		let byte_len = slice.len() * size_of::<T>();
		if byte_len == 0 {
			return (BufferFFI::empty(), BufferFFI::empty());
		}

		let ptr = self.arena.alloc(byte_len);
		if !ptr.is_null() {
			unsafe {
				std::ptr::copy_nonoverlapping(slice.as_ptr() as *const u8, ptr, byte_len);
			}
		}
		(
			BufferFFI {
				ptr,
				len: byte_len,
				cap: byte_len,
			},
			BufferFFI::empty(),
		)
	}

	/// Marshal strings with offsets (Arrow-style)
	fn marshal_strings(&mut self, strings: &[String]) -> (BufferFFI, BufferFFI) {
		let mut offsets: Vec<u64> = Vec::with_capacity(strings.len() + 1);
		let mut data: Vec<u8> = Vec::new();

		offsets.push(0);
		for s in strings {
			data.extend_from_slice(s.as_bytes());
			offsets.push(data.len() as u64);
		}

		self.marshal_with_offsets(&data, &offsets)
	}

	/// Marshal Blob slices with offsets (Arrow-style)
	fn marshal_blob_slices(&mut self, blobs: &[reifydb_type::Blob]) -> (BufferFFI, BufferFFI) {
		let mut offsets: Vec<u64> = Vec::with_capacity(blobs.len() + 1);
		let mut data: Vec<u8> = Vec::new();

		offsets.push(0);
		for blob in blobs {
			data.extend_from_slice(blob.as_bytes());
			offsets.push(data.len() as u64);
		}

		self.marshal_with_offsets(&data, &offsets)
	}

	/// Marshal serialized values with offsets
	fn marshal_serialized<T: Serialize>(&mut self, values: &[T]) -> (BufferFFI, BufferFFI) {
		let mut offsets: Vec<u64> = Vec::with_capacity(values.len() + 1);
		let mut data: Vec<u8> = Vec::new();

		offsets.push(0);
		for value in values {
			let serialized = postcard::to_allocvec(value).unwrap_or_default();
			data.extend_from_slice(&serialized);
			offsets.push(data.len() as u64);
		}

		self.marshal_with_offsets(&data, &offsets)
	}

	/// Helper: marshal data and offsets to arena
	fn marshal_with_offsets(&mut self, data: &[u8], offsets: &[u64]) -> (BufferFFI, BufferFFI) {
		let data_ptr = self.arena.copy_bytes(data);
		let offsets_byte_len = offsets.len() * size_of::<u64>();
		let offsets_ptr = self.arena.alloc(offsets_byte_len) as *mut u64;
		if !offsets_ptr.is_null() {
			unsafe {
				std::ptr::copy_nonoverlapping(offsets.as_ptr(), offsets_ptr, offsets.len());
			}
		}

		(
			BufferFFI {
				ptr: data_ptr,
				len: data.len(),
				cap: data.len(),
			},
			BufferFFI {
				ptr: offsets_ptr as *const u8,
				len: offsets_byte_len,
				cap: offsets_byte_len,
			},
		)
	}

	/// Unmarshal Columns from FFI representation
	pub fn unmarshal_columns(&self, ffi: &ColumnsFFI) -> Columns {
		if ffi.is_empty() || ffi.columns.is_null() {
			return Columns::empty();
		}

		// Unmarshal row numbers
		let row_numbers: Vec<RowNumber> = if !ffi.row_numbers.is_null() && ffi.row_count > 0 {
			unsafe {
				let slice = std::slice::from_raw_parts(ffi.row_numbers, ffi.row_count);
				slice.iter().map(|&n| RowNumber(n)).collect()
			}
		} else {
			Vec::new()
		};

		// Unmarshal columns
		let mut columns: Vec<Column> = Vec::with_capacity(ffi.column_count);
		unsafe {
			let cols_slice = std::slice::from_raw_parts(ffi.columns, ffi.column_count);
			for col_ffi in cols_slice {
				columns.push(self.unmarshal_column(col_ffi, ffi.row_count));
			}
		}

		Columns::with_row_numbers(columns, row_numbers)
	}

	/// Unmarshal a single Column from FFI representation
	fn unmarshal_column(&self, ffi: &ColumnFFI, row_count: usize) -> Column {
		// Unmarshal name
		let name = if !ffi.name.ptr.is_null() && ffi.name.len > 0 {
			unsafe {
				let bytes = std::slice::from_raw_parts(ffi.name.ptr, ffi.name.len);
				let s = std::str::from_utf8(bytes).unwrap_or("");
				Fragment::internal(s)
			}
		} else {
			Fragment::internal("")
		};

		// Unmarshal data
		let data = self.unmarshal_column_data(&ffi.data, row_count);

		Column {
			name,
			data,
		}
	}

	/// Unmarshal ColumnData from FFI representation
	fn unmarshal_column_data(&self, ffi: &ColumnDataFFI, row_count: usize) -> ColumnData {
		if row_count == 0 {
			return ColumnData::undefined(0);
		}

		// Unmarshal bitvec
		let bitvec = self.unmarshal_bitvec(&ffi.defined_bitvec, row_count);

		match ffi.type_code {
			ColumnTypeCode::Bool => {
				let container = self.unmarshal_bool_data(ffi, bitvec);
				ColumnData::Bool(container)
			}
			ColumnTypeCode::Float4 => {
				let container = self.unmarshal_numeric_data::<f32>(ffi, bitvec);
				ColumnData::Float4(container)
			}
			ColumnTypeCode::Float8 => {
				let container = self.unmarshal_numeric_data::<f64>(ffi, bitvec);
				ColumnData::Float8(container)
			}
			ColumnTypeCode::Int1 => {
				let container = self.unmarshal_numeric_data::<i8>(ffi, bitvec);
				ColumnData::Int1(container)
			}
			ColumnTypeCode::Int2 => {
				let container = self.unmarshal_numeric_data::<i16>(ffi, bitvec);
				ColumnData::Int2(container)
			}
			ColumnTypeCode::Int4 => {
				let container = self.unmarshal_numeric_data::<i32>(ffi, bitvec);
				ColumnData::Int4(container)
			}
			ColumnTypeCode::Int8 => {
				let container = self.unmarshal_numeric_data::<i64>(ffi, bitvec);
				ColumnData::Int8(container)
			}
			ColumnTypeCode::Int16 => {
				let container = self.unmarshal_numeric_data::<i128>(ffi, bitvec);
				ColumnData::Int16(container)
			}
			ColumnTypeCode::Uint1 => {
				let container = self.unmarshal_numeric_data::<u8>(ffi, bitvec);
				ColumnData::Uint1(container)
			}
			ColumnTypeCode::Uint2 => {
				let container = self.unmarshal_numeric_data::<u16>(ffi, bitvec);
				ColumnData::Uint2(container)
			}
			ColumnTypeCode::Uint4 => {
				let container = self.unmarshal_numeric_data::<u32>(ffi, bitvec);
				ColumnData::Uint4(container)
			}
			ColumnTypeCode::Uint8 => {
				let container = self.unmarshal_numeric_data::<u64>(ffi, bitvec);
				ColumnData::Uint8(container)
			}
			ColumnTypeCode::Uint16 => {
				let container = self.unmarshal_numeric_data::<u128>(ffi, bitvec);
				ColumnData::Uint16(container)
			}
			ColumnTypeCode::Utf8 => {
				let container = self.unmarshal_utf8_data(ffi, bitvec);
				ColumnData::Utf8 {
					container,
					max_bytes: MaxBytes::MAX,
				}
			}
			ColumnTypeCode::Date => {
				let container = self.unmarshal_date_data(ffi, bitvec);
				ColumnData::Date(container)
			}
			ColumnTypeCode::DateTime => {
				let container = self.unmarshal_datetime_data(ffi, bitvec);
				ColumnData::DateTime(container)
			}
			ColumnTypeCode::Time => {
				let container = self.unmarshal_time_data(ffi, bitvec);
				ColumnData::Time(container)
			}
			ColumnTypeCode::Duration => {
				let container = self.unmarshal_duration_data(ffi, bitvec);
				ColumnData::Duration(container)
			}
			ColumnTypeCode::IdentityId => {
				let container = self.unmarshal_identity_id_data(ffi, bitvec);
				ColumnData::IdentityId(container)
			}
			ColumnTypeCode::Uuid4 => {
				let container = self.unmarshal_uuid4_data(ffi, bitvec);
				ColumnData::Uuid4(container)
			}
			ColumnTypeCode::Uuid7 => {
				let container = self.unmarshal_uuid7_data(ffi, bitvec);
				ColumnData::Uuid7(container)
			}
			ColumnTypeCode::Blob => {
				let container = self.unmarshal_blob_data(ffi, bitvec);
				ColumnData::Blob {
					container,
					max_bytes: MaxBytes::MAX,
				}
			}
			ColumnTypeCode::Int => {
				let container = self.unmarshal_serialized_data::<Int>(ffi, bitvec);
				ColumnData::Int {
					container,
					max_bytes: MaxBytes::MAX,
				}
			}
			ColumnTypeCode::Uint => {
				let container = self.unmarshal_serialized_data::<Uint>(ffi, bitvec);
				ColumnData::Uint {
					container,
					max_bytes: MaxBytes::MAX,
				}
			}
			ColumnTypeCode::Decimal => {
				let container = self.unmarshal_serialized_data::<Decimal>(ffi, bitvec);
				ColumnData::Decimal {
					container,
					precision: Precision::MAX,
					scale: Scale::MIN,
				}
			}
			ColumnTypeCode::Any => {
				let container = self.unmarshal_any_data(ffi, bitvec);
				ColumnData::Any(container)
			}
			ColumnTypeCode::Undefined => ColumnData::undefined(row_count),
		}
	}

	/// Unmarshal bitvec from raw bytes
	fn unmarshal_bitvec(&self, ffi: &BufferFFI, len: usize) -> reifydb_type::BitVec {
		use reifydb_type::BitVec;

		if ffi.is_empty() || len == 0 {
			return BitVec::repeat(len, true); // All defined if no bitvec
		}

		unsafe {
			let bytes = std::slice::from_raw_parts(ffi.ptr, ffi.len);
			let mut bits = Vec::with_capacity(len);
			for i in 0..len {
				let byte_idx = i / 8;
				let bit_idx = i % 8;
				let bit = if byte_idx < bytes.len() {
					(bytes[byte_idx] & (1 << bit_idx)) != 0
				} else {
					true
				};
				bits.push(bit);
			}
			BitVec::from_slice(&bits)
		}
	}

	/// Unmarshal bool data
	fn unmarshal_bool_data(
		&self,
		ffi: &ColumnDataFFI,
		bitvec: reifydb_type::BitVec,
	) -> reifydb_core::value::container::BoolContainer {
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
	fn unmarshal_numeric_data<T: Copy + Default + IsNumber>(
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
	fn unmarshal_utf8_data(
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
	fn unmarshal_date_data(
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
	fn unmarshal_datetime_data(
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
	fn unmarshal_time_data(
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
	fn unmarshal_duration_data(
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
	fn unmarshal_identity_id_data(
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
	fn unmarshal_uuid4_data(
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
	fn unmarshal_uuid7_data(
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
	fn unmarshal_blob_data(
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
	fn unmarshal_serialized_data<T: Default + Clone + DeserializeOwned + IsNumber>(
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
	fn unmarshal_any_data(
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
	fn read_offsets(&self, ffi: &BufferFFI) -> Vec<u64> {
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

/// Convert ColumnData variant to type code
fn column_data_to_type_code(data: &ColumnData) -> ColumnTypeCode {
	match data {
		ColumnData::Bool(_) => ColumnTypeCode::Bool,
		ColumnData::Float4(_) => ColumnTypeCode::Float4,
		ColumnData::Float8(_) => ColumnTypeCode::Float8,
		ColumnData::Int1(_) => ColumnTypeCode::Int1,
		ColumnData::Int2(_) => ColumnTypeCode::Int2,
		ColumnData::Int4(_) => ColumnTypeCode::Int4,
		ColumnData::Int8(_) => ColumnTypeCode::Int8,
		ColumnData::Int16(_) => ColumnTypeCode::Int16,
		ColumnData::Uint1(_) => ColumnTypeCode::Uint1,
		ColumnData::Uint2(_) => ColumnTypeCode::Uint2,
		ColumnData::Uint4(_) => ColumnTypeCode::Uint4,
		ColumnData::Uint8(_) => ColumnTypeCode::Uint8,
		ColumnData::Uint16(_) => ColumnTypeCode::Uint16,
		ColumnData::Utf8 {
			..
		} => ColumnTypeCode::Utf8,
		ColumnData::Date(_) => ColumnTypeCode::Date,
		ColumnData::DateTime(_) => ColumnTypeCode::DateTime,
		ColumnData::Time(_) => ColumnTypeCode::Time,
		ColumnData::Duration(_) => ColumnTypeCode::Duration,
		ColumnData::IdentityId(_) => ColumnTypeCode::IdentityId,
		ColumnData::Uuid4(_) => ColumnTypeCode::Uuid4,
		ColumnData::Uuid7(_) => ColumnTypeCode::Uuid7,
		ColumnData::Blob {
			..
		} => ColumnTypeCode::Blob,
		ColumnData::Int {
			..
		} => ColumnTypeCode::Int,
		ColumnData::Uint {
			..
		} => ColumnTypeCode::Uint,
		ColumnData::Decimal {
			..
		} => ColumnTypeCode::Decimal,
		ColumnData::Any(_) => ColumnTypeCode::Any,
		ColumnData::Undefined(_) => ColumnTypeCode::Undefined,
	}
}
