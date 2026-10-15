// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{iter::once, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_catalog::store::series::shape::deserialize_encoding_config;
use reifydb_core::{
	common::{CommitVersion, TimeSource},
	interface::catalog::{
		column_snapshot::ColumnStats,
		series::{Series, SeriesEncoding},
	},
	value::{
		batch::{batch, concat_columns},
		column::factory,
	},
};
use reifydb_extension::block_encoding::{error::BlockEncodingError, registry::BlockEncodings};
use reifydb_store_column::{
	compress::Compressor,
	convert::to_vortex,
	snapshot::{BlockEncodingHeader, BlockUserColumn, ColumnBlock, ColumnChunks},
	stats::block_stats,
};
use reifydb_value::{
	Result,
	fragment::Fragment,
	reifydb_assertions,
	value::{
		system_columns::{SystemColumn, created_at, partitions, row_numbers, system_column, time, updated_at},
		value_type::{ValueType, field::from_field},
	},
};
use vortex_session::VortexSession;

use crate::column::error::SubStoreError;

pub fn system_column_schema(time: &TimeSource, partitioned: bool) -> Vec<(String, ValueType)> {
	SystemColumn::ALL
		.into_iter()
		.filter(|sc| *sc != SystemColumn::Time || carries_time(time))
		.filter(|sc| *sc != SystemColumn::Partitions || partitioned)
		.map(|sc| (sc.name().to_string(), sc.ty()))
		.collect()
}

fn carries_time(time: &TimeSource) -> bool {
	match time {
		TimeSource::None => false,
		TimeSource::Event {
			..
		}
		| TimeSource::Processing => true,
	}
}

pub fn arrow_columns_from_batches(
	schema: &[(String, ValueType)],
	batches: &[RecordBatch],
	version: CommitVersion,
) -> Result<Vec<(String, ValueType, (FieldRef, ArrayRef))>> {
	let timed = schema.iter().any(|(name, _)| SystemColumn::from_name(name) == Some(SystemColumn::Time));
	let partitioned =
		schema.iter().any(|(name, _)| SystemColumn::from_name(name) == Some(SystemColumn::Partitions));
	for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
		let partitions_present = system_column(batch, SystemColumn::Partitions).is_some();
		if partitions_present != partitioned {
			return Err(SubStoreError::PartitionMismatch {
				partitioned,
				present: partitions_present,
			}
			.into());
		}
		let time_present = system_column(batch, SystemColumn::Time).is_some();
		if time_present != timed {
			return Err(SubStoreError::TimeMismatch {
				timed,
				present: time_present,
			}
			.into());
		}
	}

	let mut columns = Vec::with_capacity(schema.len());

	#[cfg(reifydb_assertions)]
	let mut block_rows: Option<usize> = None;

	for (name, ty) in schema {
		let combined = match SystemColumn::from_name(name) {
			Some(sc) => system_column_buffer(sc, batches, version)?,
			None => user_column_buffer(name, batches)?,
		};
		reifydb_assertions! {
			let rows = combined.1.len();
			match block_rows {
				None => block_rows = Some(rows),
				Some(expected) => assert!(
					rows == expected,
					"sub-column assembled a ragged column block: column '{}' has {} rows but earlier columns have {}, so a row-wise read of the block would misalign fields or index past a shorter column",
					name,
					rows,
					expected
				),
			}
		}
		columns.push((name.clone(), ty.clone(), combined));
	}
	Ok(columns)
}

pub fn compress_columns(
	columns: Vec<(String, ValueType, (FieldRef, ArrayRef))>,
	compressor: &Compressor,
) -> Result<ColumnBlock> {
	let mut schema = Vec::with_capacity(columns.len());
	let mut chunked = Vec::with_capacity(columns.len());
	for (name, ty, column) in columns {
		let chunks = compressor.compress(ty.clone(), &column)?;
		schema.push((name, ty, chunks.nullable));
		chunked.push(chunks);
	}
	Ok(ColumnBlock::new(Arc::new(schema), chunked))
}

pub fn column_block_from_batches(
	schema: Vec<(String, ValueType)>,
	batches: Vec<RecordBatch>,
	version: CommitVersion,
	compressor: &Compressor,
) -> Result<ColumnBlock> {
	compress_columns(arrow_columns_from_batches(&schema, &batches, version)?, compressor)
}

pub fn stats_from_arrow(
	columns: &[(String, ValueType, (FieldRef, ArrayRef))],
	session: &VortexSession,
) -> Result<Vec<ColumnStats>> {
	let mut schema = Vec::with_capacity(columns.len());
	let mut chunks = Vec::with_capacity(columns.len());
	for (name, ty, column) in columns {
		let field_type = from_field(&column.0)?;
		let nullable = column.0.is_nullable();
		chunks.push(ColumnChunks::single(ty.clone(), nullable, field_type, to_vortex(session, column)?));
		schema.push((name.clone(), ty.clone(), nullable));
	}
	block_stats(&ColumnBlock::new(Arc::new(schema), chunks), session)
}

pub fn encode_series_block(
	columns: Vec<(String, ValueType, (FieldRef, ArrayRef))>,
	series: &Series,
	encoding: &SeriesEncoding,
	encodings: &BlockEncodings,
	compressor: &Compressor,
) -> Result<ColumnBlock> {
	let (system, mut user): (Vec<_>, Vec<_>) =
		columns.into_iter().partition(|(name, _, _)| SystemColumn::from_name(name).is_some());
	let name = encoding.name.as_str();
	let expected = series.encoding_user_fields();
	if user.iter().all(|(_, _, (_, array))| array.is_empty()) {
		for ((column, _, (field, _)), want) in user.iter_mut().zip(&expected) {
			if column == want.name() {
				*field = Arc::clone(want);
			}
		}
	}
	let user_batch = batch(user.iter().map(|(_, _, column)| column.clone()).collect())?;
	if user_batch.schema().fields().iter().ne(expected.iter()) {
		return Err(BlockEncodingError::UserFieldsDiffer {
			name: name.to_string(),
			series: series.name.clone(),
		}
		.into());
	}
	let block_encoding = encodings.get(name).ok_or_else(|| BlockEncodingError::UnknownName {
		name: name.to_string(),
		fragment: Fragment::None,
	})?;
	let config = deserialize_encoding_config(&encoding.name, &encoding.config)?;
	let bound = block_encoding.bind(user_batch.schema().fields(), &config)?;
	let stored = bound.encode(user_batch.clone())?;
	if stored.num_rows() != user_batch.num_rows() {
		return Err(BlockEncodingError::RowCountChanged {
			name: name.to_string(),
			before: user_batch.num_rows(),
			after: stored.num_rows(),
		}
		.into());
	}
	if stored.schema().fields().iter().ne(bound.stored_fields().iter()) {
		return Err(BlockEncodingError::StoredFieldsDiffer {
			name: name.to_string(),
		}
		.into());
	}
	for column in once(series.key.column()).chain(series.tag.is_some().then_some("tag")) {
		let changed = || BlockEncodingError::PassThroughValuesChanged {
			name: name.to_string(),
			column: column.to_string(),
		};
		let user_column = user_batch.column_by_name(column).ok_or_else(changed)?;
		match stored.column_by_name(column) {
			Some(stored_column) if stored_column.as_ref() == user_column.as_ref() => {}
			_ => return Err(changed().into()),
		}
	}
	reifydb_assertions! {
		let decoded = bound.decode(block_encoding.version(), stored.clone())?;
		assert!(
			decoded == user_batch,
			"block encoding '{name}' is lossy: decode(encode(block)) differs from the sealed block, so a column read would return rows that were never written"
		);
	}
	let mut user_columns = Vec::with_capacity(user.len());
	for (column, ty, (field, _)) in &user {
		user_columns.push(BlockUserColumn {
			name: column.clone(),
			ty: ty.clone(),
			nullable: field.is_nullable(),
			field_type: from_field(field)?,
		});
	}
	let mut out = Vec::with_capacity(stored.num_columns() + system.len());
	for (field, array) in stored.schema().fields().iter().zip(stored.columns()) {
		let ty = from_field(field)?.value_type.ok_or_else(|| BlockEncodingError::UnsupportedStoredType {
			name: name.to_string(),
			field: field.name().clone(),
		})?;
		out.push((field.name().clone(), ty, (field.clone(), array.clone())));
	}
	out.extend(system);
	let header = BlockEncodingHeader {
		name: name.to_string(),
		version: block_encoding.version(),
		config: encoding.config.clone(),
		user: user_columns,
	};
	Ok(compress_columns(out, compressor)?.with_block_encoding(Some(header)))
}

fn user_column_buffer(name: &str, batches: &[RecordBatch]) -> Result<(FieldRef, ArrayRef)> {
	let mut parts: Vec<(FieldRef, ArrayRef)> = Vec::with_capacity(batches.len());
	for batch in batches {
		let (index, _) = batch.schema_ref().column_with_name(name).ok_or_else(|| {
			SubStoreError::MissingColumnInBatch {
				column: name.to_string(),
			}
		})?;
		parts.push((batch.schema_ref().fields()[index].clone(), batch.column(index).clone()));
	}
	if parts.is_empty() {
		return Err(SubStoreError::NoBatchesForMaterialization {
			column: name.to_string(),
		}
		.into());
	}
	concat_columns(&parts)
}

fn system_column_buffer(
	sc: SystemColumn,
	batches: &[RecordBatch],
	version: CommitVersion,
) -> Result<(FieldRef, ArrayRef)> {
	if batches.is_empty() {
		return Err(SubStoreError::NoBatchesForMaterialization {
			column: sc.name().to_string(),
		}
		.into());
	}
	match sc {
		SystemColumn::RowNumbers => {
			let mut values = Vec::new();
			for batch in batches {
				for rn in row_numbers(batch)?.iter() {
					values.push(rn.0);
				}
			}
			Ok(factory::uint8(sc.name(), values))
		}
		SystemColumn::Partitions => {
			let mut values = Vec::new();
			for batch in batches {
				for partition in partitions(batch)?.iter() {
					values.push(partition.0);
				}
			}
			Ok(factory::uint16(sc.name(), values))
		}
		SystemColumn::CreatedAt => {
			let mut values = Vec::new();
			for batch in batches {
				for ts in created_at(batch)?.iter() {
					values.push(*ts);
				}
			}
			Ok(factory::datetime(sc.name(), values))
		}
		SystemColumn::UpdatedAt => {
			let mut values = Vec::new();
			for batch in batches {
				for ts in updated_at(batch)?.iter() {
					values.push(*ts);
				}
			}
			Ok(factory::datetime(sc.name(), values))
		}
		SystemColumn::Time => {
			let mut values = Vec::new();
			for batch in batches {
				for ts in time(batch)?.iter() {
					values.push(*ts);
				}
			}
			Ok(factory::datetime(sc.name(), values))
		}
		SystemColumn::CommitVersion => {
			let total: usize = batches.iter().map(|b| b.num_rows()).sum();
			Ok(factory::uint8(sc.name(), vec![version.0; total]))
		}
	}
}

#[cfg(test)]
mod tests {
	use arrow_array::Int64Array;
	use arrow_schema::{DataType, Field, Schema};
	use reifydb_catalog::store::series::shape::serialize_encoding_config;
	use reifydb_core::interface::catalog::{
		column::{Column, ColumnIndex},
		id::{ColumnId, NamespaceId, SeriesId},
		series::SeriesKey,
	};
	use reifydb_extension::block_encoding::{BlockEncoding, BoundBlockEncoding};
	use reifydb_store_column::session::new_session;
	use reifydb_test_harness::block_encoding::{DELTA, DeltaEncoding};
	use reifydb_value::{
		error::Error,
		value::{
			Value, constraint::TypeConstraint, datetime::DateTime, sumtype::SumTypeId,
			system_columns::stamp_system_columns,
		},
	};

	use super::*;

	type Columns = Vec<(String, ValueType, (FieldRef, ArrayRef))>;

	#[derive(Clone, Copy)]
	enum Fault {
		DropRow,
		ExtraField,
		RewriteTag,
		Lossy,
	}

	struct Faulty(Fault);

	impl BlockEncoding for Faulty {
		fn version(&self) -> u32 {
			1
		}

		fn bind(&self, user: &[FieldRef], _config: &[(String, Value)]) -> Result<Box<dyn BoundBlockEncoding>> {
			Ok(Box::new(BoundFaulty {
				fault: self.0,
				user: user.to_vec(),
			}))
		}
	}

	struct BoundFaulty {
		fault: Fault,
		user: Vec<FieldRef>,
	}

	impl BoundBlockEncoding for BoundFaulty {
		fn stored_fields(&self) -> Vec<FieldRef> {
			self.user.clone()
		}

		fn encode(&self, user: RecordBatch) -> Result<RecordBatch> {
			match self.fault {
				Fault::DropRow => Ok(user.slice(0, user.num_rows() - 1)),
				Fault::ExtraField => {
					let mut fields = self.user.clone();
					fields.push(Arc::new(Field::new("extra", DataType::Int64, false)));
					let mut columns = user.columns().to_vec();
					columns.push(Arc::new(Int64Array::from(vec![0; user.num_rows()])));
					Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap())
				}
				Fault::RewriteTag => {
					let index = user.schema().index_of("tag").unwrap();
					let mut columns = user.columns().to_vec();
					columns[index] = factory::uint1("tag", vec![9u8; user.num_rows()]).1;
					Ok(RecordBatch::try_new(user.schema(), columns).unwrap())
				}
				Fault::Lossy => Ok(user),
			}
		}

		fn decode(&self, _version: u32, stored: RecordBatch) -> Result<RecordBatch> {
			match self.fault {
				Fault::Lossy => {
					let index = stored.schema().index_of("value").unwrap();
					let mut columns = stored.columns().to_vec();
					columns[index] = Arc::new(Int64Array::from(vec![0; stored.num_rows()]));
					Ok(RecordBatch::try_new(stored.schema(), columns).unwrap())
				}
				_ => Ok(stored),
			}
		}
	}

	fn column(name: &str, ty: ValueType) -> Column {
		Column {
			id: ColumnId(1),
			name: name.into(),
			constraint: TypeConstraint::unconstrained(ty),
			properties: vec![],
			index: ColumnIndex(0),
			auto_increment: false,
			dictionary_id: None,
		}
	}

	fn series(key: &str, tagged: bool, encoding: Option<(&str, Vec<(String, Value)>)>) -> Series {
		Series {
			id: SeriesId(1),
			namespace: NamespaceId(1),
			name: "prices".into(),
			columns: vec![column("ts", ValueType::Int8), column("value", ValueType::Int8)],
			tag: tagged.then_some(SumTypeId(1)),
			key: SeriesKey::Integer {
				column: key.into(),
			},
			primary_key: None,
			partition_by: vec![],
			time: TimeSource::None,
			encoding: encoding.map(|(name, config)| SeriesEncoding {
				name: name.into(),
				config: serialize_encoding_config(&config),
			}),
		}
	}

	fn delta_on(column: &str) -> Option<(&'static str, Vec<(String, Value)>)> {
		Some((DELTA, vec![("column".to_string(), Value::Utf8(column.to_string()))]))
	}

	fn schema(series: &Series) -> Vec<(String, ValueType)> {
		let mut schema = vec![("ts".to_string(), ValueType::Int8)];
		if series.tag.is_some() {
			schema.push(("tag".to_string(), ValueType::Uint1));
		}
		schema.push(("value".to_string(), ValueType::Int8));
		schema.extend(system_column_schema(&series.time, false));
		schema
	}

	fn batches(series: &Series, keys: Vec<i64>, values: Vec<i64>) -> Vec<RecordBatch> {
		let rows = keys.len();
		let mut user = vec![factory::int8("ts", keys)];
		if series.tag.is_some() {
			user.push(factory::uint1("tag", vec![1u8; rows]));
		}
		user.push(factory::int8("value", values));
		let (fields, arrays): (Vec<FieldRef>, Vec<ArrayRef>) = user.into_iter().unzip();
		let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
		let stamped = stamp_system_columns(
			batch,
			vec![
				(
					SystemColumn::RowNumbers,
					factory::uint8("r", (1..=rows as u64).collect::<Vec<_>>()).1,
				),
				(SystemColumn::CreatedAt, factory::datetime("c", vec![DateTime::default(); rows]).1),
				(SystemColumn::UpdatedAt, factory::datetime("u", vec![DateTime::default(); rows]).1),
			],
		)
		.unwrap();
		vec![stamped]
	}

	fn columns(series: &Series, keys: Vec<i64>, values: Vec<i64>) -> Columns {
		arrow_columns_from_batches(&schema(series), &batches(series, keys, values), CommitVersion(7)).unwrap()
	}

	fn compressor() -> Compressor {
		Compressor::new(new_session())
	}

	fn registry() -> BlockEncodings {
		BlockEncodings::builder()
			.register(DELTA, DeltaEncoding::new)
			.register("drop_row", || Faulty(Fault::DropRow))
			.register("extra_field", || Faulty(Fault::ExtraField))
			.register("rewrite_tag", || Faulty(Fault::RewriteTag))
			.register("lossy", || Faulty(Fault::Lossy))
			.configure()
	}

	fn encode(series: &Series, columns: Columns) -> Result<ColumnBlock> {
		encode_series_block(columns, series, series.encoding.as_ref().unwrap(), &registry(), &compressor())
	}

	#[test]
	fn an_empty_bucket_scanned_with_nullable_fields_encodes_with_the_catalog_fields() {
		// An empty bucket scan marks user fields nullable; rejecting it aborts the seal of any series with a
		// key gap.
		let encoded = series("ts", false, delta_on("value"));
		let nullable: Columns = columns(&encoded, vec![], vec![])
			.into_iter()
			.map(|(name, ty, (field, array))| {
				let field = match SystemColumn::from_name(&name) {
					Some(_) => field,
					None => Arc::new(field.as_ref().clone().with_nullable(true)),
				};
				(name, ty, (field, array))
			})
			.collect();
		let block = encode(&encoded, nullable).unwrap();
		assert_eq!(block.len(), 0);
		let header = block.block_encoding.expect("an encoded block must carry its header");
		assert!(header.user.iter().all(|column| !column.nullable), "{:?}", header.user);
	}

	fn error_of(result: Result<ColumnBlock>) -> (String, String) {
		let Err(Error(diagnostic)) = result else {
			panic!("expected the seal to fail");
		};
		(diagnostic.code.clone(), diagnostic.message.clone())
	}

	fn names(block: &ColumnBlock) -> Vec<String> {
		block.schema.iter().map(|(name, _, _)| name.clone()).collect()
	}

	#[test]
	fn table_build_is_byte_identical_after_split() {
		// The table path must keep building exactly the block it built before the split, and never a header.
		let plain = series("ts", false, None);
		let joined = column_block_from_batches(
			schema(&plain),
			batches(&plain, vec![1, 2, 3], vec![10, 20, 30]),
			CommitVersion(7),
			&compressor(),
		)
		.unwrap();
		let split = compress_columns(columns(&plain, vec![1, 2, 3], vec![10, 20, 30]), &compressor()).unwrap();
		assert_eq!(*joined.schema, *split.schema);
		assert!(joined.block_encoding.is_none(), "a plain build must never carry a block encoding header");
		let session = new_session();
		assert_eq!(block_stats(&joined, &session).unwrap(), block_stats(&split, &session).unwrap());
	}

	#[test]
	fn stats_come_from_user_columns() {
		// Pruning reads stats by user column name; stats on stored columns would name `value_delta`.
		let encoded = series("ts", false, delta_on("value"));
		let stats =
			stats_from_arrow(&columns(&encoded, vec![1, 2, 3], vec![10, 20, 30]), &new_session()).unwrap();
		let names: Vec<_> = stats.iter().map(|stat| stat.column.clone()).collect();
		assert!(names.contains(&"value".to_string()), "stats must name the user column: {names:?}");
		assert!(
			!names.iter().any(|name| name == "value_delta"),
			"stats must not name stored columns: {names:?}"
		);
	}

	#[test]
	fn encode_rejects_a_row_count_change() {
		// A block that loses rows at seal silently drops data that was committed.
		let dropping = series("ts", false, Some(("drop_row", vec![])));
		let (code, message) = error_of(encode(&dropping, columns(&dropping, vec![1, 2, 3], vec![1, 2, 3])));
		assert_eq!(code, "BENC_003");
		assert!(message.contains("drop_row"), "the error must name the encoding: {message}");
	}

	#[test]
	fn encode_rejects_a_changed_key_column() {
		// Key range pruning and filters read the stored key; a rewritten key returns the wrong rows.
		let key_delta = series("ts", false, delta_on("ts"));
		let (code, message) = error_of(encode(&key_delta, columns(&key_delta, vec![5, 7, 9], vec![1, 2, 3])));
		assert_eq!(code, "BENC_005");
		assert!(message.contains("ts"), "the error must name the key column: {message}");
	}

	#[test]
	fn encode_rejects_stored_fields_other_than_declared() {
		// The read path binds from the declared stored fields; an undeclared column would be misread.
		let extra = series("ts", false, Some(("extra_field", vec![])));
		let (code, message) = error_of(encode(&extra, columns(&extra, vec![1, 2], vec![1, 2])));
		assert_eq!(code, "BENC_006");
		assert!(message.contains("extra_field"), "the error must name the encoding: {message}");
	}

	#[test]
	fn seal_rejects_user_fields_that_differ_from_the_catalog() {
		// Binding on fields CREATE never checked would let the seal use a shape the encoding never accepted.
		let encoded = series("ts", false, delta_on("value"));
		let mut drifted = columns(&encoded, vec![1, 2], vec![1, 2]);
		drifted[1].2.0 = Arc::new(Field::new("value", DataType::Int64, true));
		let (code, message) = error_of(encode(&encoded, drifted));
		assert_eq!(code, "BENC_010");
		assert!(message.contains("prices"), "the error must name the series: {message}");
	}

	#[test]
	fn encode_rejects_a_changed_tag_column() {
		// Variant filters read the stored `tag`; a rewritten tag returns rows of the wrong variant.
		let tagged = series("ts", true, Some(("rewrite_tag", vec![])));
		let (code, message) = error_of(encode(&tagged, columns(&tagged, vec![1, 2], vec![1, 2])));
		assert_eq!(code, "BENC_005");
		assert!(message.contains("tag"), "the error must name the tag column: {message}");
	}

	#[test]
	fn plain_series_stats_equal_the_old_compressed_stats() {
		// Moving stats before compression must not change what the catalog stores for a plain series.
		let plain = series("ts", false, None);
		let session = new_session();
		let block = compress_columns(columns(&plain, vec![1, 5, 3], vec![9, 2, 4]), &compressor()).unwrap();
		assert_eq!(
			stats_from_arrow(&columns(&plain, vec![1, 5, 3], vec![9, 2, 4]), &session).unwrap(),
			block_stats(&block, &session).unwrap()
		);
	}

	#[test]
	fn system_columns_bypass_the_encoding() {
		// An encoding that saw `#` columns could rewrite row numbers or commit versions.
		let encoded = series("ts", false, delta_on("value"));
		let block = encode(&encoded, columns(&encoded, vec![1, 2, 3], vec![10, 20, 30])).unwrap();
		let header = block.block_encoding.clone().expect("an encoded block must carry its header");
		assert!(header.user.iter().all(|column| SystemColumn::from_name(&column.name).is_none()));
		let names = names(&block);
		assert_eq!(&names[..2], &["ts".to_string(), "value_delta".to_string()]);
		assert!(names.contains(&SystemColumn::CommitVersion.name().to_string()));
	}

	#[cfg(reifydb_assertions)]
	#[test]
	#[should_panic(expected = "is lossy")]
	fn lossy_encoding_panics_under_assertions() {
		// A lossy encoding would serve rows that were never written; under assertions the seal must stop.
		let lossy = series("ts", false, Some(("lossy", vec![])));
		let _ = encode(&lossy, columns(&lossy, vec![1, 2], vec![3, 4]));
	}
}
