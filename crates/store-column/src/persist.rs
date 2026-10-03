// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{path::Path, result::Result as StdResult, sync::Arc};

use arrow_array::new_empty_array;
use postcard::{from_bytes, to_stdvec};
use reifydb_runtime::io::fs::{FsError, Len, Pread, Pwrite};
use reifydb_value::{
	Result,
	error::Error,
	reifydb_assertions,
	value::value_type::{
		ValueType,
		field::{FieldType, to_field},
	},
};
use serde::{Deserialize, Serialize};
use vortex_array::{
	ArrayContext,
	dtype::DType,
	serde::{SerializeOptions, SerializedArray},
};
use vortex_buffer::{Alignment, ByteBufferMut};
use vortex_flatbuffers::{FlatBuffer, WriteFlatBufferExt};
use vortex_session::{
	VortexSession,
	registry::{Id, ReadContext},
};

use crate::{
	convert::to_vortex,
	error::{ColumnError, fs, vortex},
	snapshot::{ColumnBlock, ColumnChunks},
};

const MAGIC: &[u8; 4] = b"BORG";
const VERSION: u16 = 1;
const PREFIX: usize = 10;

#[derive(Serialize, Deserialize)]
struct BorgHeader {
	align: u32,
	encodings: Vec<String>,
	columns: Vec<BorgColumn>,
}

#[derive(Serialize, Deserialize)]
struct BorgColumn {
	name: String,
	ty: ValueType,
	nullable: bool,
	field_type: FieldType,
	dtype: Vec<u8>,
	chunks: Vec<BorgChunk>,
}

#[derive(Serialize, Deserialize)]
struct BorgChunk {
	offset: u64,
	len: u64,
	rows: u64,
}

pub fn serialize_block(block: &ColumnBlock, session: &VortexSession) -> Result<Vec<u8>> {
	let array_ctx = ArrayContext::empty();
	let options = SerializeOptions {
		offset: 0,
		include_padding: true,
	};
	let mut align = 1usize;
	let mut serialized = Vec::with_capacity(block.columns.len());
	for ((name, _, _), column) in block.schema.iter().zip(&block.columns) {
		let dtype = expected_dtype(session, name, &column.field_type)?;
		let mut chunks = Vec::with_capacity(column.chunks.len());
		for chunk in &column.chunks {
			reifydb_assertions! {
				assert_eq!(
					chunk.dtype(),
					&dtype,
					"persist: column '{name}' holds a chunk whose dtype differs from its field type, so reload would decode it against the wrong shape"
				);
			}
			let parts = chunk.serialize(&array_ctx, session, &options).map_err(vortex("persist"))?;
			if let Some(first) = parts.first() {
				align = align.max(first.alignment().as_usize());
			}
			chunks.push((parts, chunk.len()));
		}
		serialized.push((dtype, chunks));
	}
	let mut data: Vec<u8> = Vec::new();
	let mut columns = Vec::with_capacity(block.columns.len());
	for (((name, ty, nullable), column), (dtype, chunks)) in block.schema.iter().zip(&block.columns).zip(serialized)
	{
		let mut borg_chunks = Vec::with_capacity(chunks.len());
		for (parts, rows) in chunks {
			data.resize(align_up(data.len(), align), 0);
			let offset = data.len();
			reifydb_assertions! {
				assert_eq!(
					offset % align,
					0,
					"persist: chunk of column '{name}' starts at {offset}, not a multiple of the block alignment {align}, so a read would copy it"
				);
			}
			for part in &parts {
				data.extend_from_slice(part.as_slice());
			}
			borg_chunks.push(BorgChunk {
				offset: offset as u64,
				len: (data.len() - offset) as u64,
				rows: rows as u64,
			});
		}
		columns.push(BorgColumn {
			name: name.clone(),
			ty: ty.clone(),
			nullable: *nullable,
			field_type: column.field_type.clone(),
			dtype: dtype.write_flatbuffer_bytes().map_err(vortex("persist"))?.as_slice().to_vec(),
			chunks: borg_chunks,
		});
	}
	let header = BorgHeader {
		align: align as u32,
		encodings: array_ctx.to_ids().iter().map(|id| id.as_str().to_string()).collect(),
		columns,
	};
	let header = to_stdvec(&header).map_err(|err| ColumnError::PersistSerialize {
		reason: err.to_string(),
	})?;
	let header_len = u32::try_from(header.len()).map_err(|_| ColumnError::PersistSerialize {
		reason: format!("header of {} bytes does not fit u32", header.len()),
	})?;
	let mut out = Vec::with_capacity(PREFIX + header.len() + data.len());
	out.extend_from_slice(MAGIC);
	out.extend_from_slice(&VERSION.to_le_bytes());
	out.extend_from_slice(&header_len.to_le_bytes());
	out.extend_from_slice(&header);
	out.resize(align_up(out.len(), align), 0);
	out.extend_from_slice(&data);
	Ok(out)
}

pub struct BlockHandle<R: Pread + Len> {
	file: R,
	header: BorgHeader,
	data_start: u64,
	session: VortexSession,
}

impl<R: Pread + Len> BlockHandle<R> {
	pub fn open(file: R, session: VortexSession) -> Result<Self> {
		let len = file.len().map_err(fs("open"))?;
		let mut prefix = [0u8; PREFIX];
		if !read_exact(&file, 0, &mut prefix).map_err(fs("open"))? || &prefix[..4] != MAGIC {
			return Err(corrupt("not a BORG column block"));
		}
		let version = u16::from_le_bytes([prefix[4], prefix[5]]);
		if version != VERSION {
			return Err(ColumnError::PersistVersionUnsupported {
				version,
			}
			.into());
		}
		let header_len = u32::from_le_bytes([prefix[6], prefix[7], prefix[8], prefix[9]]);
		let header_end = PREFIX as u64 + header_len as u64;
		if header_end > len {
			return Err(corrupt("header runs past the end of the block"));
		}
		let mut header = vec![0u8; header_len as usize];
		if !read_exact(&file, PREFIX as u64, &mut header).map_err(fs("open"))? {
			return Err(corrupt("header runs past the end of the block"));
		}
		let header: BorgHeader = from_bytes(&header).map_err(|err| ColumnError::PersistDeserialize {
			reason: err.to_string(),
		})?;
		let align = header.align as u64;
		if !align.is_power_of_two() {
			return Err(corrupt("alignment is not a power of two"));
		}
		let data_start = header_end.next_multiple_of(align);
		if data_start > len {
			return Err(corrupt("padding runs past the end of the block"));
		}
		for column in &header.columns {
			for chunk in &column.chunks {
				let end = data_start
					.checked_add(chunk.offset)
					.and_then(|start| start.checked_add(chunk.len));
				if !end.is_some_and(|end| end <= len) {
					return Err(corrupt("chunk runs past the end of the block"));
				}
			}
		}
		Ok(Self {
			file,
			header,
			data_start,
			session,
		})
	}

	pub fn read(&self, columns: Option<&[&str]>) -> Result<ColumnBlock> {
		if let Some(names) = columns
			&& let Some(missing) = names
				.iter()
				.find(|name| !self.header.columns.iter().any(|column| column.name == **name))
		{
			return Err(ColumnError::ColumnNotInSchema {
				operation: "read",
				name: missing.to_string(),
			}
			.into());
		}
		let alignment = Alignment::new(self.header.align as usize);
		let read_ctx = ReadContext::new(self.header.encodings.iter().map(|id| Id::new(id)).collect::<Vec<_>>());
		let mut schema = Vec::new();
		let mut out = Vec::new();
		for column in &self.header.columns {
			if columns.is_some_and(|names| !names.contains(&column.name.as_str())) {
				continue;
			}
			let stored = DType::from_flatbuffer(FlatBuffer::copy_from(&column.dtype), &self.session)
				.map_err(vortex("persist"))?;
			let expected = expected_dtype(&self.session, &column.name, &column.field_type)?;
			if stored != expected {
				return Err(ColumnError::DTypeMismatch {
					column: column.name.clone(),
					stored: stored.to_string(),
					expected: expected.to_string(),
				}
				.into());
			}
			let mut chunks = Vec::with_capacity(column.chunks.len());
			for chunk in &column.chunks {
				let mut buffer = ByteBufferMut::zeroed_aligned(chunk.len as usize, alignment);
				if !read_exact(&self.file, self.data_start + chunk.offset, buffer.as_mut_slice())
					.map_err(fs("read"))?
				{
					return Err(corrupt("chunk runs past the end of the block"));
				}
				let serialized =
					SerializedArray::try_from(buffer.freeze()).map_err(vortex("persist"))?;
				chunks.push(serialized
					.decode(&stored, chunk.rows as usize, &read_ctx, &self.session)
					.map_err(vortex("persist"))?);
			}
			schema.push((column.name.clone(), column.ty.clone(), column.nullable));
			out.push(ColumnChunks::new(
				column.ty.clone(),
				column.nullable,
				column.field_type.clone(),
				chunks,
			));
		}
		Ok(ColumnBlock::new(Arc::new(schema), out))
	}
}

fn expected_dtype(session: &VortexSession, name: &str, field_type: &FieldType) -> Result<DType> {
	let field = Arc::new(to_field(name, field_type));
	let empty = new_empty_array(field.data_type());
	Ok(to_vortex(session, &(field, empty))?.dtype().clone())
}

fn align_up(offset: usize, align: usize) -> usize {
	offset.div_ceil(align) * align
}

fn read_exact<R: Pread>(file: &R, mut offset: u64, buf: &mut [u8]) -> StdResult<bool, FsError> {
	let mut read = 0;
	while read < buf.len() {
		let n = file.pread(offset, &mut buf[read..])?;
		if n == 0 {
			return Ok(false);
		}
		read += n;
		offset += n as u64;
	}
	Ok(true)
}

pub(crate) fn write_all<W: Pwrite>(file: &W, path: &Path, mut offset: u64, buf: &[u8]) -> StdResult<(), FsError> {
	let mut written = 0;
	while written < buf.len() {
		let n = file.pwrite(offset, &buf[written..])?;
		if n == 0 {
			return Err(FsError::Io {
				path: path.to_path_buf(),
				message: format!("write made no progress at offset {offset}"),
			});
		}
		written += n;
		offset += n as u64;
	}
	Ok(())
}

fn corrupt(reason: &str) -> Error {
	ColumnError::PersistDeserialize {
		reason: reason.to_string(),
	}
	.into()
}

#[cfg(test)]
mod tests {
	use std::mem;

	use arrow_array::{Array, ArrayRef as ArrowArrayRef};
	use arrow_schema::FieldRef;
	use reifydb_core::value::column::{builder::ColumnBuilder, factory};
	use reifydb_runtime::{
		io::fs::{
			Create, Open,
			memory::MemoryFs,
			testing::{FileId, ReadOutcome, TestingFs, TestingHooks},
		},
		sync::mutex::Mutex,
	};
	use reifydb_value::value::{
		Value,
		column_view::ColumnView,
		constraint::{precision::Precision, scale::Scale},
		decimal::Decimal,
		dictionary::{DictionaryEntryId, DictionaryId},
		value_type::{ValueType, field::from_field},
	};

	use super::*;
	use crate::{convert::to_arrow, session::new_session};

	fn chunks(ty: ValueType, nullable: bool, column: &(FieldRef, ArrowArrayRef)) -> ColumnChunks {
		let array = to_vortex(&new_session(), column).unwrap();
		ColumnChunks::single(ty, nullable, from_field(&column.0).unwrap(), array)
	}

	fn exported(block: &ColumnBlock) -> Vec<Vec<(FieldRef, ArrowArrayRef)>> {
		let session = new_session();
		block.schema
			.iter()
			.zip(&block.columns)
			.map(|((name, _, _), column)| {
				column.chunks
					.iter()
					.map(|chunk| {
						to_arrow(&session, name, &column.field_type, chunk.clone()).unwrap()
					})
					.collect()
			})
			.collect()
	}

	fn block_values(block: &ColumnBlock) -> Vec<Vec<Value>> {
		exported(block)
			.iter()
			.map(|column| {
				let mut out = Vec::new();
				for chunk in column {
					let view = ColumnView::try_from(chunk).unwrap();
					for i in 0..chunk.1.len() {
						out.push(view.get_value(i));
					}
				}
				out
			})
			.collect()
	}

	fn load(bytes: &[u8], session: &VortexSession) -> Result<ColumnBlock> {
		let fs = MemoryFs::new();
		let path = Path::new("/block.borg");
		let written = fs.create(path, bytes.len() as u64).unwrap().pwrite(0, bytes).unwrap();
		assert_eq!(written, bytes.len(), "the fixture file must hold every byte");
		BlockHandle::open(fs.open(path).unwrap(), session.clone())?.read(None)
	}

	fn round_trip(block: &ColumnBlock) -> ColumnBlock {
		let session = new_session();
		load(&serialize_block(block, &session).unwrap(), &session).unwrap()
	}

	fn assert_round_trips(block: ColumnBlock) {
		let restored = round_trip(&block);
		assert_eq!(*block.schema, *restored.schema, "schema must survive the round trip");
		assert_eq!(block.len(), restored.len(), "row count must survive the round trip");
		assert_eq!(block_values(&block), block_values(&restored), "values must survive the round trip");
		for (original, reloaded) in block.columns.iter().zip(&restored.columns) {
			assert_eq!(original.field_type, reloaded.field_type, "field type must survive the round trip");
		}
	}

	#[test]
	fn round_trips_fixed_width_column() {
		let schema = Arc::new(vec![("a".to_string(), ValueType::Int4, false)]);
		let column = chunks(ValueType::Int4, false, &factory::int4("a", [1i32, 2, 3, 4]));
		assert_round_trips(ColumnBlock::new(schema, vec![column]));
	}

	#[test]
	fn round_trips_varlen_column() {
		let schema = Arc::new(vec![("s".to_string(), ValueType::Utf8, false)]);
		let column = chunks(ValueType::Utf8, false, &factory::utf8("s", ["alpha", "bravo", "charlie"]));
		assert_round_trips(ColumnBlock::new(schema, vec![column]));
	}

	#[test]
	fn round_trips_multi_column_block() {
		let schema = Arc::new(vec![
			("id".to_string(), ValueType::Uint8, false),
			("name".to_string(), ValueType::Utf8, false),
		]);
		let columns = vec![
			chunks(ValueType::Uint8, false, &factory::uint8("id", vec![1u64, 2, 3])),
			chunks(ValueType::Utf8, false, &factory::utf8("name", ["x", "y", "z"])),
		];
		assert_round_trips(ColumnBlock::new(schema, columns));
	}

	#[test]
	fn round_trips_nullable_column_preserving_none_positions() {
		let mut buffer = ColumnBuilder::with_capacity(ValueType::Int4, 4);
		buffer.push::<i32>(10);
		buffer.push_none();
		buffer.push::<i32>(30);
		buffer.push_none();
		let built = buffer.finish("a");
		assert_eq!(built.1.null_count(), 2, "buffer with push_none must build a column carrying nones");

		let column = chunks(ValueType::Int4, true, &built);
		let schema = Arc::new(vec![("a".to_string(), ValueType::Int4, true)]);
		let block = ColumnBlock::new(schema, vec![column]);

		let restored = round_trip(&block);

		assert!(restored.columns[0].nullable, "nullability must survive the round trip");
		assert_eq!(block_values(&block), block_values(&restored));
		let reloaded = &exported(&restored)[0][0].1;
		let is_defined = |row: usize| reloaded.is_valid(row);
		assert!(is_defined(0));
		assert!(!is_defined(1), "none at index 1 must be preserved");
		assert!(is_defined(2));
		assert!(!is_defined(3), "none at index 3 must be preserved");
	}

	#[test]
	fn optional_columns_keep_their_nones_and_field_type_through_persist() {
		// Without the none bitmap and field type on disk, a reload turns nones to zeros and drops type details.
		let d = |text: &str| Decimal::parse(text).unwrap();
		let mut dictionary = ColumnBuilder::with_capacity(ValueType::DictionaryId, 3);
		dictionary.set_dictionary_id(DictionaryId(7));
		dictionary.push(DictionaryEntryId::U4(1));
		dictionary.push_none();
		dictionary.push(DictionaryEntryId::U4(3));
		let columns = vec![
			factory::int4_optional("i", vec![Some(1), None, Some(3)]),
			factory::utf8_with_bitvec("u", ["x", "", "z"], vec![true, false, true]),
			factory::decimal_with_bitvec(
				"d",
				Precision::new(10),
				Scale::new(2),
				[d("1.50"), d("0.00"), d("-2.25")],
				vec![true, false, true],
			),
			factory::any_optional(
				"y",
				vec![Some(Value::Int4(5)), None, Some(Value::Utf8("w".to_string()))],
			),
			dictionary.finish("k"),
		];
		let mut schema = Vec::new();
		let mut block_chunks = Vec::new();
		for column in &columns {
			let field_type = from_field(&column.0).unwrap();
			let Some(ValueType::Option(bare)) = field_type.value_type.clone() else {
				panic!("fixture column {} must be optional, got {field_type:?}", column.0.name());
			};
			schema.push((column.0.name().clone(), *bare.clone(), true));
			block_chunks.push(chunks(*bare, true, column));
		}
		let block = ColumnBlock::new(Arc::new(schema), block_chunks);

		let restored = round_trip(&block);

		assert_eq!(block_values(&block), block_values(&restored), "values must survive the round trip");
		for (index, (original, reloaded)) in exported(&block).iter().zip(exported(&restored)).enumerate() {
			let (original_field, _) = &original[0];
			let (reloaded_field, reloaded_array) = &reloaded[0];
			assert_eq!(
				from_field(reloaded_field).unwrap(),
				from_field(original_field).unwrap(),
				"column {index} must keep its field type"
			);
			let nones =
				reloaded_array.logical_nulls().expect("a column with nones must reload with a bitmap");
			assert_eq!(
				(0..nones.len()).map(|row| nones.is_null(row)).collect::<Vec<_>>(),
				vec![false, true, false],
				"column {index} must keep its none at row 1"
			);
		}
	}

	#[test]
	fn open_rejects_garbage_without_panicking() {
		assert!(load(&[0xff, 0xff, 0xff, 0xff, 0xff], &new_session()).is_err());
	}

	struct ReadLog(Mutex<Vec<(u64, usize)>>);

	impl TestingHooks for ReadLog {
		fn on_pread(&self, _file: FileId, offset: u64, len: usize) -> ReadOutcome {
			self.0.lock().push((offset, len));
			ReadOutcome::Clean
		}
	}

	#[test]
	fn a_column_read_touches_only_that_columns_byte_ranges() {
		let schema = Arc::new(vec![
			("a".to_string(), ValueType::Int4, false),
			("b".to_string(), ValueType::Utf8, false),
			("c".to_string(), ValueType::Uint8, false),
		]);
		let columns = vec![
			chunks(ValueType::Int4, false, &factory::int4("a", [1i32, 2, 3])),
			chunks(ValueType::Utf8, false, &factory::utf8("b", ["x", "y", "z"])),
			chunks(ValueType::Uint8, false, &factory::uint8("c", vec![7u64, 8, 9])),
		];
		let block = ColumnBlock::new(schema, columns);
		let session = new_session();
		let bytes = serialize_block(&block, &session).unwrap();
		let log = Arc::new(ReadLog(Mutex::new(Vec::new())));
		let fs = TestingFs::new(MemoryFs::new(), log.clone());
		let path = Path::new("/block.borg");
		fs.create(path, bytes.len() as u64).unwrap().pwrite(0, &bytes).unwrap();

		let handle = BlockHandle::open(fs.open(path).unwrap(), session).unwrap();
		let header_len = u32::from_le_bytes(bytes[6..10].try_into().unwrap()) as usize;
		assert_eq!(
			mem::take(&mut *log.0.lock()),
			vec![(0, PREFIX), (PREFIX as u64, header_len)],
			"open must read only the prefix and the header"
		);

		let read = handle.read(Some(&["b"])).unwrap();

		let expected: Vec<(u64, usize)> = handle.header.columns[1]
			.chunks
			.iter()
			.map(|chunk| (handle.data_start + chunk.offset, chunk.len as usize))
			.collect();
		assert!(!expected.is_empty(), "column b must own at least one chunk");
		assert_eq!(*log.0.lock(), expected, "reading b must touch only the byte ranges of its chunks");
		assert_eq!(read.schema.iter().map(|(name, _, _)| name.as_str()).collect::<Vec<_>>(), vec!["b"]);
		assert_eq!(block_values(&read), vec![block_values(&block)[1].clone()]);
	}
}
