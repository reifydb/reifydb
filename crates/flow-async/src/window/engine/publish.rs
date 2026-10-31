// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{internal_err, key::operator::state::GroupId, metrics::heap::HeapSize, state::timer::StateStore};
use reifydb_macro::operator_state;
use reifydb_value::{Result, value::row_number::RowNumber};

use crate::window::{
	engine::pane::{PaneApplied, PanePublisher, RollingEmit},
	meta::RollingPublished,
};

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct PublishState<O> {
	pub row: Option<O>,
}

impl<O: HeapSize> HeapSize for PublishState<O> {
	fn heap_size(&self) -> usize {
		self.row.heap_size()
	}
}

impl<O> Default for PublishState<O> {
	fn default() -> Self {
		Self {
			row: None,
		}
	}
}

pub struct RowPublisher<'f, G, K, O, A, C> {
	pub key: Option<&'f dyn Fn(&G) -> K>,
	pub finalize: &'f dyn Fn(&G, C, &A) -> Option<O>,
}

impl<G: Clone, K: Clone, O: Clone, A, C> PanePublisher<G, C, A> for RowPublisher<'_, G, K, O, A, C> {
	type Published = RollingPublished<K, O>;
	type Emit = RollingEmit<G, K, O>;

	fn publish(
		&self,
		_store: &mut dyn StateStore,
		group: &G,
		group_id: GroupId,
		newest: C,
		prior: Option<RollingPublished<K, O>>,
		value: Option<&A>,
		applied: &mut PaneApplied<RollingEmit<G, K, O>>,
	) -> Result<Option<RollingPublished<K, O>>> {
		let out = value.and_then(|value| (self.finalize)(group, newest, value));
		match (prior, out) {
			(None, Some(out)) => {
				let Some(key) = self.key else {
					return internal_err!(
						"a rolling expiry would publish a group that had no published row"
					);
				};
				let post = RollingPublished {
					row_number: 0,
					key: key(group),
					last_value: out,
				};
				applied.emits.push(RollingEmit {
					group: group.clone(),
					group_id,
					prior: None,
					post: Some(post.clone()),
				});
				applied.fresh.push((group_id, applied.emits.len() - 1));
				Ok(Some(post))
			}
			(Some(prior), Some(out)) => {
				let post = RollingPublished {
					row_number: prior.row_number,
					key: prior.key.clone(),
					last_value: out,
				};
				applied.emits.push(RollingEmit {
					group: group.clone(),
					group_id,
					prior: Some(prior),
					post: Some(post.clone()),
				});
				Ok(Some(post))
			}
			(Some(prior), None) => {
				applied.withdrawn.push(group_id);
				applied.emits.push(RollingEmit {
					group: group.clone(),
					group_id,
					prior: Some(prior),
					post: None,
				});
				Ok(None)
			}
			(None, None) => Ok(None),
		}
	}

	fn assign_row_number(
		&self,
		emit: &mut Self::Emit,
		published: &mut Self::Published,
		row: RowNumber,
	) -> Result<()> {
		published.row_number = row.0;
		if let Some(post) = emit.post.as_mut() {
			post.row_number = row.0;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::{
		key::encoded::EncodedKey,
		row::operator::state::{OperatorState, decode},
	};
	use reifydb_core::{
		key::operator::state::{GroupId, IntoGroupStateKey, KeyspaceId, OperatorStateKey, group_data_of_inner},
		metrics::heap::HeapSize,
	};
	use reifydb_macro::operator_state;
	use reifydb_value::{util::hash::Hash128, value::datetime::DateTime};

	use super::PublishState;
	use crate::window::engine::PublishKey;

	#[operator_state]
	#[derive(Debug, Clone, PartialEq, HeapSize)]
	struct Published {
		group: String,
		count: u64,
		mean: f64,
		start: DateTime,
		best: Option<i64>,
	}

	#[test]
	fn publish_state_round_trips_every_field() {
		// A field lost in storage would publish a stale pre row or re-publish a window that was never dirty.
		let state = PublishState {
			row: Some(Published {
				group: "BTC".into(),
				count: 7,
				mean: 1.5,
				start: DateTime::from_epoch_millis(1_700_000_000_123).unwrap(),
				best: None,
			}),
		};
		let bytes = state.encode_state().unwrap();
		assert_eq!(decode::<PublishState<Published>>(&bytes).unwrap(), state);
		let empty = PublishState::<Published>::default();
		assert_eq!(decode::<PublishState<Published>>(&empty.encode_state().unwrap()).unwrap(), empty);
	}

	#[test]
	fn a_publish_key_is_data_of_its_window_group() {
		// A publish row outside its window group's data would survive the reap and leak one row per window.
		let group = GroupId::hashed(Hash128(42));
		let key = (&PublishKey::new(group, EncodedKey::new(vec![1, 2, 3]))).into_group_state_key();
		let (_, keyspace, _) = OperatorStateKey::decode_inner(key.as_bytes()).unwrap();
		assert_eq!(keyspace, KeyspaceId::GUEST_WINDOW_PUBLISH);
		assert_eq!(group_data_of_inner(key.as_bytes()), Some(group));
	}
}
