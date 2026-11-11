// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{internal_err, key::operator::state::GroupId, state::timer::StateStore};
use reifydb_value::{Result, value::row_number::RowNumber};

use crate::window::{
	engine::pane::{PaneApplied, PanePublisher, RollingEmit},
	meta::RollingPublished,
	span::WindowAnchor,
};

pub struct RowPublisher<'f, G, K, O, A, C> {
	pub key: Option<&'f dyn Fn(&G) -> K>,
	pub finalize: &'f dyn Fn(&G, C, &A) -> Option<O>,
	pub zero: Option<&'f dyn Fn(&O) -> Result<Option<O>>>,
}

impl<G: Clone, K: Clone, O: Clone, A, C: WindowAnchor> PanePublisher<G, C, A> for RowPublisher<'_, G, K, O, A, C> {
	type Published = RollingPublished<K, O>;
	type Emit = RollingEmit<G, K, O>;

	fn publish(
		&self,
		store: &mut dyn StateStore,
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
					first_time: newest.as_time().unwrap_or_else(|| store.batch_time()),
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
					first_time: prior.first_time,
				};
				applied.emits.push(RollingEmit {
					group: group.clone(),
					group_id,
					prior: Some(prior),
					post: Some(post.clone()),
				});
				Ok(Some(post))
			}
			(Some(prior), None) => match self.zero {
				Some(zero) => {
					let Some(last_value) = zero(&prior.last_value)? else {
						return Ok(Some(prior));
					};
					let post = RollingPublished {
						row_number: prior.row_number,
						key: prior.key.clone(),
						last_value,
						first_time: prior.first_time,
					};
					applied.emits.push(RollingEmit {
						group: group.clone(),
						group_id,
						prior: Some(prior),
						post: Some(post.clone()),
					});
					Ok(Some(post))
				}
				None => {
					applied.withdrawn.push(group_id);
					applied.emits.push(RollingEmit {
						group: group.clone(),
						group_id,
						prior: Some(prior),
						post: None,
					});
					Ok(None)
				}
			},
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
