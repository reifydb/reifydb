// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_catalog::store::series::shape::deserialize_encoding_config;
use reifydb_core::value::batch::{batch, concat_columns};
use reifydb_extension::block_encoding::{error::BlockEncodingError, registry::BlockEncodings};
use reifydb_store_column::{
	convert::{to_arrow, to_vortex},
	predicate::{Predicate, evaluate},
	selection::Selection,
	snapshot::{ColumnBlock, ColumnChunks, Schema},
};
use reifydb_value::{
	fragment::Fragment,
	value::{system_columns::SystemColumn, value_type::field::to_field},
};
use vortex_session::VortexSession;

use crate::Result;

pub(crate) enum Decoded {
	Skip(Schema),
	Block(ColumnBlock),
}

pub(crate) struct BlockDecoder {
	encodings: BlockEncodings,
	session: VortexSession,
}

impl BlockDecoder {
	pub(crate) fn new(encodings: BlockEncodings, session: VortexSession) -> Self {
		Self {
			encodings,
			session,
		}
	}

	pub(crate) fn decode(&self, block: ColumnBlock, predicate: Option<&Predicate>) -> Result<Decoded> {
		let Some(header) = block.block_encoding.clone() else {
			return Ok(Decoded::Block(block));
		};
		if let Some(predicate) = predicate
			&& matches!(evaluate(&block, predicate, &self.session)?, Selection::None_)
		{
			let mut schema: Vec<_> =
				header.user.iter().map(|c| (c.name.clone(), c.ty.clone(), c.nullable)).collect();
			schema.extend(block
				.schema
				.iter()
				.filter(|(name, _, _)| SystemColumn::from_name(name).is_some())
				.cloned());
			return Ok(Decoded::Skip(Arc::new(schema)));
		}
		let encoding = self.encodings.get(&header.name).ok_or_else(|| BlockEncodingError::UnknownName {
			name: header.name.clone(),
			fragment: Fragment::None,
		})?;
		if header.version > encoding.version() {
			return Err(BlockEncodingError::NewerVersion {
				name: header.name.clone(),
				stored: header.version,
				supported: encoding.version(),
			}
			.into());
		}
		let rows = block.len();
		let mut stored = Vec::new();
		let mut system = Vec::new();
		for ((name, ty, nullable), chunks) in block.schema.iter().zip(&block.columns) {
			if SystemColumn::from_name(name).is_some() {
				system.push(((name.clone(), ty.clone(), *nullable), chunks.clone()));
				continue;
			}
			let mut parts = Vec::with_capacity(chunks.chunks.len());
			for chunk in &chunks.chunks {
				parts.push(to_arrow(&self.session, name, &chunks.field_type, chunk.clone())?);
			}
			stored.push(concat_columns(&parts)?);
		}
		let user_fields: Vec<_> = header
			.user
			.iter()
			.map(|c| Arc::new(to_field(&c.name, &c.field_type).with_nullable(c.nullable)))
			.collect();
		let config = deserialize_encoding_config(&header.name, &header.config)?;
		let bound = encoding.bind(&user_fields, &config)?;
		let user = bound.decode(header.version, batch(stored)?)?;
		if user.num_rows() != rows {
			return Err(BlockEncodingError::RowCountChanged {
				name: header.name.clone(),
				before: rows,
				after: user.num_rows(),
			}
			.into());
		}
		let mut schema = Vec::with_capacity(header.user.len() + system.len());
		let mut columns = Vec::with_capacity(header.user.len() + system.len());
		for ((column, field), array) in header.user.iter().zip(&user_fields).zip(user.columns()) {
			let imported = to_vortex(&self.session, &(Arc::clone(field), Arc::clone(array)))?;
			columns.push(ColumnChunks::single(
				column.ty.clone(),
				column.nullable,
				column.field_type.clone(),
				imported,
			));
			schema.push((column.name.clone(), column.ty.clone(), column.nullable));
		}
		for (entry, chunks) in system {
			schema.push(entry);
			columns.push(chunks);
		}
		Ok(Decoded::Block(ColumnBlock::new(Arc::new(schema), columns)))
	}
}

#[cfg(test)]
pub(crate) mod tests {
	use std::sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	};

	use arrow_array::{Array, Int64Array, RecordBatch};
	use arrow_schema::FieldRef;
	use reifydb_catalog::store::series::shape::serialize_encoding_config;
	use reifydb_core::value::{batch::batch, column::factory};
	use reifydb_extension::block_encoding::{BlockEncoding, BoundBlockEncoding, registry::BlockEncodings};
	use reifydb_store_column::{
		convert::to_vortex,
		predicate::Predicate,
		reader::SnapshotReader,
		session::new_session,
		snapshot::{BlockEncodingHeader, BlockUserColumn, ColumnBlock, ColumnChunks},
	};
	use reifydb_test_harness::block_encoding::{DELTA, DeltaEncoding};
	use reifydb_value::{
		Result,
		value::{
			Value,
			datetime::DateTime,
			system_columns::SystemColumn,
			value_type::{ValueType, field::from_field},
		},
	};

	use super::{BlockDecoder, Decoded};

	struct Counting {
		decodes: Arc<AtomicUsize>,
	}

	impl BlockEncoding for Counting {
		fn version(&self) -> u32 {
			1
		}

		fn bind(&self, user: &[FieldRef], config: &[(String, Value)]) -> Result<Box<dyn BoundBlockEncoding>> {
			Ok(Box::new(BoundCounting {
				inner: DeltaEncoding::new().bind(user, config)?,
				decodes: Arc::clone(&self.decodes),
			}))
		}
	}

	struct BoundCounting {
		inner: Box<dyn BoundBlockEncoding>,
		decodes: Arc<AtomicUsize>,
	}

	impl BoundBlockEncoding for BoundCounting {
		fn stored_fields(&self) -> Vec<FieldRef> {
			self.inner.stored_fields()
		}

		fn encode(&self, user: RecordBatch) -> Result<RecordBatch> {
			self.inner.encode(user)
		}

		fn decode(&self, version: u32, stored: RecordBatch) -> Result<RecordBatch> {
			self.decodes.fetch_add(1, Ordering::SeqCst);
			self.inner.decode(version, stored)
		}
	}

	fn config() -> Vec<(String, Value)> {
		vec![("column".to_string(), Value::Utf8("value".to_string()))]
	}

	fn chunks(column: &(FieldRef, Arc<dyn Array>)) -> ColumnChunks {
		let field_type = from_field(&column.0).unwrap();
		ColumnChunks::single(
			field_type.value_type.clone().unwrap(),
			column.0.is_nullable(),
			field_type,
			to_vortex(&new_session(), column).unwrap(),
		)
	}

	pub(crate) fn encoded_block(keys: Vec<i64>, values: Vec<i64>, version: u32) -> ColumnBlock {
		let rows = keys.len();
		let user = vec![factory::int8("ts", keys), factory::int8("value", values)];
		let user_columns = user
			.iter()
			.map(|(field, _)| BlockUserColumn {
				name: field.name().clone(),
				ty: ValueType::Int8,
				nullable: field.is_nullable(),
				field_type: from_field(field).unwrap(),
			})
			.collect();
		let user_batch = batch(user).unwrap();
		let bound = DeltaEncoding::new().bind(user_batch.schema().fields(), &config()).unwrap();
		let stored = bound.encode(user_batch).unwrap();
		let mut schema = Vec::new();
		let mut columns = Vec::new();
		for (field, array) in stored.schema().fields().iter().zip(stored.columns()) {
			columns.push(chunks(&(field.clone(), array.clone())));
			schema.push((field.name().clone(), ValueType::Int8, false));
		}
		let system = [
			factory::uint8(SystemColumn::RowNumbers.name(), (1..=rows as u64).collect::<Vec<_>>()),
			factory::datetime(SystemColumn::CreatedAt.name(), vec![DateTime::default(); rows]),
			factory::datetime(SystemColumn::UpdatedAt.name(), vec![DateTime::default(); rows]),
			factory::uint8(SystemColumn::CommitVersion.name(), vec![7u64; rows]),
		];
		for column in &system {
			columns.push(chunks(column));
			let ty = SystemColumn::from_name(column.0.name()).unwrap().ty();
			schema.push((column.0.name().clone(), ty, false));
		}
		ColumnBlock::new(Arc::new(schema), columns).with_block_encoding(Some(BlockEncodingHeader {
			name: DELTA.to_string(),
			version,
			config: serialize_encoding_config(&config()),
			user: user_columns,
		}))
	}

	fn delta_decoder() -> BlockDecoder {
		BlockDecoder::new(
			BlockEncodings::builder().register(DELTA, DeltaEncoding::new).configure(),
			new_session(),
		)
	}

	fn decoded(result: Result<Decoded>) -> ColumnBlock {
		match result.unwrap() {
			Decoded::Block(block) => block,
			Decoded::Skip(_) => panic!("expected a decoded block, got a skip"),
		}
	}

	fn read_all(block: ColumnBlock, predicate: Option<Predicate>) -> Vec<RecordBatch> {
		let reader = SnapshotReader::new(Arc::new(block), 1024, new_session());
		let reader = match predicate {
			Some(predicate) => reader.with_predicate(predicate),
			None => reader,
		};
		reader.collect::<Result<Vec<_>>>().unwrap()
	}

	fn int8_values(batches: &[RecordBatch], name: &str) -> Vec<i64> {
		batches.iter()
			.flat_map(|batch| {
				batch.column_by_name(name)
					.unwrap()
					.as_any()
					.downcast_ref::<Int64Array>()
					.unwrap()
					.values()
					.to_vec()
			})
			.collect()
	}

	fn error_of(result: Result<Decoded>) -> (String, String) {
		let Err(err) = result else {
			panic!("expected the decode to fail");
		};
		(err.0.code.clone(), err.0.message.clone())
	}

	#[test]
	fn none_selection_skips_decode() {
		// Decoding a block the predicate rejects wastes the whole decode cost on rows nobody reads.
		let decodes = Arc::new(AtomicUsize::new(0));
		let counter = Arc::clone(&decodes);
		let decoder = BlockDecoder::new(
			BlockEncodings::builder()
				.register(DELTA, move || Counting {
					decodes: Arc::clone(&counter),
				})
				.configure(),
			new_session(),
		);
		let block = encoded_block(vec![1, 2, 3, 4], vec![10, 20, 30, 40], 1);
		let outcome = decoder.decode(block, Some(&Predicate::Gt("ts".into(), Value::Int8(1000)))).unwrap();
		assert!(matches!(outcome, Decoded::Skip(_)), "a block with no matching row must be skipped");
		assert_eq!(decodes.load(Ordering::SeqCst), 0, "a skipped block must never reach decode");
	}

	#[test]
	fn mask_decodes_the_whole_block() {
		// Delta values depend on every earlier row; decoding only the masked rows returns wrong values.
		let block = encoded_block(vec![1, 2, 3, 4], vec![10, 25, 31, 47], 1);
		let predicate = Predicate::Gt("ts".into(), Value::Int8(2));
		let block = decoded(delta_decoder().decode(block, Some(&predicate)));
		assert_eq!(block.len(), 4, "the decoder must return every row of the block, not a cut");
		assert_eq!(int8_values(&read_all(block.clone(), None), "value"), vec![10, 25, 31, 47]);
		let kept = read_all(block, Some(predicate));
		assert_eq!(int8_values(&kept, "ts"), vec![3, 4]);
		assert_eq!(int8_values(&kept, "value"), vec![31, 47]);
	}

	#[test]
	fn unknown_name_fails() {
		// A block whose encoding is not registered must fail, never return stored columns as user data.
		let decoder = BlockDecoder::new(BlockEncodings::empty(), new_session());
		let (code, message) = error_of(decoder.decode(encoded_block(vec![1, 2], vec![5, 6], 1), None));
		assert_eq!(code, "BENC_001");
		assert!(message.contains(DELTA), "the error must name the encoding: {message}");
	}

	#[test]
	fn newer_version_fails() {
		// An older binary must refuse a block written by a newer encoding version instead of misreading it.
		let (code, message) = error_of(delta_decoder().decode(encoded_block(vec![1, 2], vec![5, 6], 2), None));
		assert_eq!(code, "BENC_002");
		assert!(message.contains('2') && message.contains('1'), "the error must name both versions: {message}");
	}

	#[test]
	fn skipped_block_schema_matches_a_decoded_block() {
		// An empty scan takes its headers from a skipped block; a different order breaks the result shape.
		let skipped = match delta_decoder()
			.decode(
				encoded_block(vec![1, 2], vec![5, 6], 1),
				Some(&Predicate::Gt("ts".into(), Value::Int8(1000))),
			)
			.unwrap()
		{
			Decoded::Skip(schema) => schema,
			Decoded::Block(_) => panic!("expected a skip"),
		};
		let block = decoded(delta_decoder().decode(encoded_block(vec![1, 2], vec![5, 6], 1), None));
		assert_eq!(*skipped, *block.schema);
		let names: Vec<_> = skipped.iter().map(|(name, _, _)| name.as_str()).collect();
		assert_eq!(
			names,
			vec![
				"ts",
				"value",
				SystemColumn::RowNumbers.name(),
				SystemColumn::CreatedAt.name(),
				SystemColumn::UpdatedAt.name(),
				SystemColumn::CommitVersion.name(),
			]
		);
	}

	#[test]
	fn system_columns_rejoin_in_order() {
		// System columns must come back after the user columns and unchanged, or row metadata shifts.
		let block = decoded(delta_decoder().decode(encoded_block(vec![1, 2, 3], vec![5, 6, 9], 1), None));
		let schema: Vec<_> = block.schema.iter().map(|(name, ty, _)| (name.as_str(), ty.clone())).collect();
		assert_eq!(
			schema,
			vec![
				("ts", ValueType::Int8),
				("value", ValueType::Int8),
				(SystemColumn::RowNumbers.name(), ValueType::Uint8),
				(SystemColumn::CreatedAt.name(), ValueType::DateTime),
				(SystemColumn::UpdatedAt.name(), ValueType::DateTime),
				(SystemColumn::CommitVersion.name(), ValueType::Uint8),
			]
		);
		assert!(block.block_encoding.is_none(), "a decoded block must not carry the header into the reader");
		let batches = read_all(block, None);
		let commit: Vec<u64> = batches
			.iter()
			.flat_map(|batch| {
				batch.column_by_name(SystemColumn::CommitVersion.name())
					.unwrap()
					.as_any()
					.downcast_ref::<arrow_array::UInt64Array>()
					.unwrap()
					.values()
					.to_vec()
			})
			.collect();
		assert_eq!(commit, vec![7, 7, 7]);
	}
}
