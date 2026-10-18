// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use reifydb_core::{error::diagnostic::internal::internal, interface::catalog::column_block::ColumnBlock};
use reifydb_extension::block_encoding::error::BlockEncodingError;
use reifydb_store_column::{
	block::Schema, device::BlockFileKey, predicate::Predicate, reader::BlockReader, store::ColumnStore,
};
use reifydb_value::error::Error;

use crate::{
	Result,
	vm::volcano::scan::column_decode::{BlockDecoder, Decoded},
};

pub(crate) struct BlockSequenceReader {
	store: Arc<ColumnStore>,
	blocks: Vec<ColumnBlock>,
	batch_size: usize,
	index: usize,
	current: Option<BlockReader>,
	schema: Option<Schema>,
	predicate: Option<Predicate>,
	decoder: Option<BlockDecoder>,
}

impl BlockSequenceReader {
	pub(crate) fn new(store: Arc<ColumnStore>, blocks: Vec<ColumnBlock>, batch_size: usize) -> Self {
		Self {
			store,
			blocks,
			batch_size,
			index: 0,
			current: None,
			schema: None,
			predicate: None,
			decoder: None,
		}
	}

	pub(crate) fn with_predicate(mut self, predicate: Option<Predicate>) -> Self {
		self.predicate = predicate;
		self
	}

	pub(crate) fn with_decoder(mut self, decoder: Option<BlockDecoder>) -> Self {
		self.decoder = decoder;
		self
	}

	pub(crate) fn schema(&self) -> Option<&Schema> {
		self.schema.as_ref()
	}

	pub(crate) fn next(&mut self) -> Result<Option<RecordBatch>> {
		loop {
			if let Some(reader) = self.current.as_mut() {
				match reader.next() {
					Some(batch) => return batch.map(Some),
					None => self.current = None,
				}
			}

			if self.index >= self.blocks.len() {
				return Ok(None);
			}

			let block = &self.blocks[self.index];
			self.index += 1;

			let data = self
				.store
				.open(&BlockFileKey::of(block))?
				.ok_or_else(|| {
					Error(Box::new(internal(format!(
						"column block {} is missing from the column store",
						block.id
					))))
				})?
				.read(None)?;

			let data = match (&self.decoder, &data.block_encoding) {
				(_, None) => data,
				(None, Some(header)) => {
					return Err(BlockEncodingError::NoDecoder {
						name: header.name.clone(),
						block: block.id,
					}
					.into());
				}
				(Some(decoder), Some(_)) => match decoder.decode(data, self.predicate.as_ref())? {
					Decoded::Skip(schema) => {
						if self.schema.is_none() {
							self.schema = Some(schema);
						}
						continue;
					}
					Decoded::Block(data) => data,
				},
			};
			let data = Arc::new(data);

			if self.schema.is_none() {
				self.schema = Some(Arc::clone(&data.schema));
			}

			let reader = BlockReader::new(data, self.batch_size, self.store.session().clone());
			self.current = Some(match self.predicate.clone() {
				Some(predicate) => reader.with_predicate(predicate),
				None => reader,
			});
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_core::{
		common::CommitVersion,
		interface::catalog::{
			column_block::{ColumnBlock, ColumnBlockSource},
			id::{ColumnBlockId, NamespaceId, SeriesId},
		},
	};
	use reifydb_store_column::{device::BlockFileKey, store::ColumnStore};

	use super::BlockSequenceReader;
	use crate::vm::volcano::scan::column_decode::tests::encoded_block;

	#[test]
	fn encoded_block_without_decoder_fails() {
		// A scan without a decoder must never hand stored columns to the query as if they were user columns.
		let block = ColumnBlock {
			id: ColumnBlockId(42),
			namespace: NamespaceId(1),
			source: ColumnBlockSource::SeriesBucket {
				series_id: SeriesId(9),
				bucket_start: 0,
				bucket_width: 100,
				partition: None,
				sequence_counter: 0,
				sealed_at_commit_version: CommitVersion(7),
			},
			row_count: 2,
			partition_values: Vec::new(),
			stats: Vec::new(),
		};
		let store = ColumnStore::memory().unwrap();
		store.write(&BlockFileKey::of(&block), &encoded_block(vec![1, 2], vec![5, 6], 1)).unwrap();
		let mut reader = BlockSequenceReader::new(Arc::new(store), vec![block], 1024);
		let Err(err) = reader.next() else {
			panic!("an encoded block read without a decoder must fail");
		};
		assert_eq!(err.0.code, "BENC_009");
		assert!(err.0.message.contains("42"), "the error must name the block: {}", err.0.message);
	}
}
