// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cell::RefCell, ops::Bound};

use reifydb_core::interface::flow::OperatorCapability;
use reifydb_codec::{
	row::shape::{RowFamily, RowShape, RowShapeField},
	key::{
		encoded::{EncodedKey, EncodedKeyRange},
		serializer::KeySerializer,
	},
};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin, Diff},
	},
	value::column::columns::Columns,
};
use reifydb_runtime::version_epoch::EpochSeconds;
use reifydb_runtime::version_epoch::VersionEpoch;
use reifydb_sdk::flow::operator::timer::Timer as Tick;
use reifydb_value::value::value_type::ValueType;
use reifydb_value::{
	Result,
	error::Error,
	reifydb_assertions,
	value::{duration::Duration, row_number::RowNumber},
};

use crate::flow::{
	error::FlowGraphError,
	operator::{
		Operator, OperatorCell,
		stateful::{
			row::RowNumberProvider,
			utils::{internal_state_drop, internal_state_range_versioned, internal_state_set},
		},
	},
	transaction::FlowTransaction,
};

const TIMESTAMP_PREFIX: u8 = b'T';

pub struct AppendOperator {
	node: OperatorId,

	parents: Vec<OperatorCell>,

	input_nodes: Vec<OperatorId>,

	row_number_provider: RowNumberProvider,

	ttl_nanos: Option<u64>,

	version_epoch: VersionEpoch,

	evict_cursor: RefCell<Option<EncodedKey>>,
}

impl AppendOperator {
	pub fn new(
		node: OperatorId,
		parents: Vec<OperatorCell>,
		input_nodes: Vec<OperatorId>,
		ttl_nanos: Option<u64>,
		version_epoch: VersionEpoch,
	) -> Self {
		reifydb_assertions! {
			assert_eq!(parents.len(), input_nodes.len());
			assert!(parents.len() >= 2, "Append requires at least 2 inputs");
		}

		Self {
			node,
			parents,
			input_nodes,
			row_number_provider: RowNumberProvider::new(node),
			ttl_nanos,
			version_epoch,
			evict_cursor: RefCell::new(None),
		}
	}

	#[cfg(test)]
	pub(crate) fn new_for_state_tests(node: OperatorId, ttl_nanos: Option<u64>) -> Self {
		Self {
			node,
			parents: Vec::new(),
			input_nodes: Vec::new(),
			row_number_provider: RowNumberProvider::new(node),
			ttl_nanos,
			version_epoch: VersionEpoch::new(),
			evict_cursor: RefCell::new(None),
		}
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.parents[0].output_schema()
	}

	fn parent_index_for_origin(&self, origin: &ChangeOrigin) -> Option<usize> {
		match origin {
			ChangeOrigin::Flow(from_node) => self.input_nodes.iter().position(|n| n == from_node),
			ChangeOrigin::Object(_) => None,
		}
	}

	fn make_composite_key(parent_index: u8, source_row: RowNumber) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(parent_index);
		serializer.extend_u64(source_row.0);
		serializer.finish()
	}

	fn make_timestamp_key(composite_key: &EncodedKey) -> EncodedKey {
		let mut bytes = Vec::with_capacity(1 + composite_key.len());
		bytes.push(TIMESTAMP_PREFIX);
		bytes.extend_from_slice(composite_key.as_ref());
		EncodedKey::new(bytes)
	}

	fn touch(&self, txn: &mut FlowTransaction, composite_key: &EncodedKey) -> Result<()> {
		if self.ttl_nanos.is_none() {
			return Ok(());
		}
		let key = Self::make_timestamp_key(composite_key);
		let row = RowShape::new(RowFamily::Operator, vec![RowShapeField::unconstrained("state", ValueType::Blob)]).allocate_operator();
		internal_state_set(self.node, txn, &key, row.freeze())
	}

	fn forget_mapping(&self, txn: &mut FlowTransaction, composite_key: &EncodedKey) -> Result<()> {
		self.row_number_provider.remove_for_key(txn, composite_key)?;
		let ts_key = Self::make_timestamp_key(composite_key);
		internal_state_drop(self.node, txn, &ts_key)
	}
}

impl Operator for AppendOperator {
	fn id(&self) -> OperatorId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn ticks(&self) -> Option<Duration> {
		if self.ttl_nanos.is_some() {
			Some(Duration::from_seconds(1).unwrap())
		} else {
			None
		}
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let parent_origin = change.origin.clone();
		let mut result_diffs = Vec::with_capacity(change.diffs.len());

		for diff in change.diffs {
			let diff_origin = diff.origin().cloned().unwrap_or_else(|| parent_origin.clone());
			let parent_index = self.parent_index_for_origin(&diff_origin).ok_or_else(|| {
				Error::from(FlowGraphError::UnknownDiffOrigin {
					operator: "Append",
					origin: Some(format!("{:?}", diff_origin)),
				})
			})?;
			match diff {
				Diff::Insert {
					post,
					..
				} => {
					if let Some(d) = self.translate_append_insert(txn, parent_index, post)? {
						result_diffs.push(d);
					}
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					if let Some(d) = self.translate_append_update(txn, parent_index, pre, post)? {
						result_diffs.push(d);
					}
				}
				Diff::Remove {
					pre,
					..
				} => {
					if let Some(d) = self.translate_append_remove(txn, parent_index, pre)? {
						result_diffs.push(d);
					}
				}
			}
		}

		Ok(Change::from_flow(self.node, change.version, result_diffs, change.changed_at))
	}

	fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		let Some(ttl_nanos) = self.ttl_nanos else {
			return Ok(None);
		};

		let now_nanos = tick.due.to_nanos();
		let Some(cutoff_nanos) = now_nanos.checked_sub(ttl_nanos) else {
			return Ok(None);
		};
		let Some(cutoff_version) = self.version_epoch.floor_version_at(EpochSeconds::from_nanos(cutoff_nanos)).map(CommitVersion) else {
			return Ok(None);
		};

		const EVICT_BATCH: usize = 4096;
		let prefix = [TIMESTAMP_PREFIX];
		let base = EncodedKeyRange::prefix(&prefix);
		let start = match self.evict_cursor.borrow().clone() {
			Some(cursor) => Bound::Excluded(cursor),
			None => base.start.clone(),
		};
		let range = EncodedKeyRange::new(start, base.end.clone());
		let batch = internal_state_range_versioned(self.node, txn, range)
			.take(EVICT_BATCH)
			.collect::<Result<Vec<_>>>()?;
		let reached_end = batch.len() < EVICT_BATCH;
		let last_key = batch.last().map(|(key, _, _)| key.clone());

		for (storage_key, version, _row) in batch {
			if version > cutoff_version {
				continue;
			}

			let bytes = storage_key.as_ref();
			if bytes.is_empty() || bytes[0] != TIMESTAMP_PREFIX {
				continue;
			}
			let composite_key = EncodedKey::new(bytes[1..].to_vec());
			self.forget_mapping(txn, &composite_key)?;
		}

		*self.evict_cursor.borrow_mut() = if reached_end {
			None
		} else {
			last_key
		};
		Ok(None)
	}
}

impl AppendOperator {
	#[inline]
	fn translate_create_row_numbers(
		&self,
		txn: &mut FlowTransaction,
		parent_index: usize,
		source: &Columns,
	) -> Result<Vec<RowNumber>> {
		let row_count = source.row_count();
		let mut output_row_numbers = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let source_row_number = source.row_numbers()[row_idx];
			let composite_key = Self::make_composite_key(parent_index as u8, source_row_number);
			let (output_row_number, _) =
				self.row_number_provider.get_or_create_row_number(txn, &composite_key)?;
			self.touch(txn, &composite_key)?;
			output_row_numbers.push(output_row_number);
		}
		Ok(output_row_numbers)
	}

	#[inline]
	fn lookup_row_numbers(
		&self,
		txn: &mut FlowTransaction,
		parent_index: usize,
		source: &Columns,
	) -> Result<Option<(Vec<RowNumber>, Vec<EncodedKey>)>> {
		let row_count = source.row_count();
		let mut output_row_numbers = Vec::with_capacity(row_count);
		let mut composite_keys = Vec::with_capacity(row_count);
		for row_idx in 0..row_count {
			let source_row_number = source.row_numbers()[row_idx];
			let composite_key = Self::make_composite_key(parent_index as u8, source_row_number);
			let Some(row_number) = self.row_number_provider.get_row_number(txn, &composite_key)? else {
				return Ok(None);
			};
			output_row_numbers.push(row_number);
			composite_keys.push(composite_key);
		}
		Ok(Some((output_row_numbers, composite_keys)))
	}

	#[inline]
	fn translate_append_insert(
		&self,
		txn: &mut FlowTransaction,
		parent_index: usize,
		post: Columns,
	) -> Result<Option<Diff>> {
		if post.row_count() == 0 {
			return Ok(None);
		}
		let output_row_numbers = self.translate_create_row_numbers(txn, parent_index, &post)?;
		let output = post.with_row_numbers(output_row_numbers);
		Ok(Some(Diff::insert(output)))
	}

	#[inline]
	fn translate_append_update(
		&self,
		txn: &mut FlowTransaction,
		parent_index: usize,
		pre: Columns,
		post: Columns,
	) -> Result<Option<Diff>> {
		if post.row_count() == 0 {
			return Ok(None);
		}
		let Some((output_row_numbers, composite_keys)) = self.lookup_row_numbers(txn, parent_index, &pre)?
		else {
			return Ok(None);
		};
		for composite_key in &composite_keys {
			self.touch(txn, composite_key)?;
		}
		let pre_output = pre.with_row_numbers(output_row_numbers.clone());
		let post_output = post.with_row_numbers(output_row_numbers);
		Ok(Some(Diff::update(pre_output, post_output)))
	}

	#[inline]
	fn translate_append_remove(
		&self,
		txn: &mut FlowTransaction,
		parent_index: usize,
		pre: Columns,
	) -> Result<Option<Diff>> {
		if pre.row_count() == 0 {
			return Ok(None);
		}
		let Some((output_row_numbers, composite_keys)) = self.lookup_row_numbers(txn, parent_index, &pre)?
		else {
			return Ok(None);
		};
		for composite_key in &composite_keys {
			self.forget_mapping(txn, composite_key)?;
		}
		let output = pre.with_row_numbers(output_row_numbers);
		Ok(Some(Diff::remove(output)))
	}
}
