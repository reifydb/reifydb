// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_cdc::consume::checkpoint::CdcCheckpoint;
use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::CommitVersion,
	encoded::shape::RowShape,
	interface::catalog::flow::FlowId,
	key::{EncodableKey, Key, dictionary::DictionaryEntryKey, kind::KeyKind},
};
use reifydb_transaction::transaction::{Transaction, command::CommandTransaction};
use reifydb_value::{
	Result,
	value::{dictionary::DictionaryId, identity::IdentityId},
};
use tracing::{Span, warn};

use super::{ConsumeContext, CoordinatorActor, CoordinatorState, Phase};

#[inline]
fn apply_pending_writes(transaction: &mut CommandTransaction, combined: &Pending) -> Result<()> {
	for (key, pw) in combined.iter_sorted() {
		match pw {
			PendingWrite::Set(value) => transaction.set(key, value.clone())?,
			PendingWrite::Remove => {
				if matches!(Key::kind(key), Some(KeyKind::Row)) {
					match transaction.get(key)? {
						Some(existing) => transaction.unset(key, existing.row)?,
						None => transaction.remove(key)?,
					}
				} else {
					transaction.remove(key)?;
				}
			}
			PendingWrite::Drop => transaction.drop_key(key)?,
		}
	}
	Ok(())
}

#[inline]
fn persist_flow_checkpoints(
	transaction: &mut CommandTransaction,
	checkpoints: &[(FlowId, CommitVersion)],
) -> Result<()> {
	for (flow_id, version) in checkpoints {
		CdcCheckpoint::persist(transaction, flow_id, *version)?;
	}
	Ok(())
}

impl CoordinatorActor {
	pub(super) fn finish_consume(&self, state: &mut CoordinatorState, consume_ctx: ConsumeContext) {
		Span::current().record("elapsed_us", consume_ctx.consume_start.elapsed().as_micros() as u64);
		state.set_phase(Phase::Idle, self.clock.instant());

		if consume_ctx.is_empty() {
			(consume_ctx.original_reply)(Ok(()));
			return;
		}

		self.persist_consume(consume_ctx);
	}

	#[inline]
	fn persist_consume(&self, consume_ctx: ConsumeContext) {
		let ConsumeContext {
			combined,
			pending_shapes,
			checkpoints,
			positions,
			checkpoint_deletes,
			original_reply,
			view_changes,
			current_version,
			..
		} = consume_ctx;

		let mut transaction = match self.engine.begin_command(IdentityId::system()) {
			Ok(t) => t,
			Err(e) => {
				(original_reply)(Err(e));
				return;
			}
		};

		if let Err(e) = transaction.disable_conflict_tracking() {
			let _ = transaction.rollback();
			(original_reply)(Err(e));
			return;
		}

		if let Err(e) = apply_pending_writes(&mut transaction, &combined) {
			let _ = transaction.rollback();
			(original_reply)(Err(e));
			return;
		}

		for change in view_changes {
			transaction.track_flow_change(change);
		}

		if let Err(e) = persist_flow_checkpoints(&mut transaction, &checkpoints) {
			let _ = transaction.rollback();
			(original_reply)(Err(e));
			return;
		}

		for flow_id in &checkpoint_deletes {
			if let Err(e) = CdcCheckpoint::delete(&mut transaction, flow_id) {
				let _ = transaction.rollback();
				(original_reply)(Err(e));
				return;
			}
		}

		if let Err(e) = CdcCheckpoint::persist(&mut transaction, &self.consumer_id, current_version) {
			let _ = transaction.rollback();
			(original_reply)(Err(e));
			return;
		}

		if let Err(e) =
			self.catalog.persist_pending_shapes(&mut Transaction::Command(&mut transaction), pending_shapes)
		{
			let _ = transaction.rollback();
			(original_reply)(Err(e));
			return;
		}

		match transaction.commit_unchecked() {
			Ok(_) => {
				self.evict_durable_reservations(&combined);
				for (flow_id, version) in checkpoints.iter().chain(positions.iter()) {
					self.flow_tracker.update(*flow_id, *version);
				}
				(original_reply)(Ok(()))
			}
			Err(e) => (original_reply)(Err(e)),
		}
	}

	pub(super) fn commit_tick_writes(&self, pending: Pending, pending_shapes: Vec<RowShape>) {
		let mut transaction = match self.engine.begin_command(IdentityId::system()) {
			Ok(t) => t,
			Err(e) => {
				warn!(error = %e, "failed to begin command for tick commit");
				return;
			}
		};

		if let Err(e) = transaction.disable_conflict_tracking() {
			let _ = transaction.rollback();
			warn!(error = %e, "failed to disable conflict tracking for tick commit");
			return;
		}

		for (key, pw) in pending.iter_sorted() {
			let result = match pw {
				PendingWrite::Set(value) => transaction.set(key, value.clone()),
				PendingWrite::Remove => transaction.remove(key),
				PendingWrite::Drop => transaction.drop_key(key),
			};
			if let Err(e) = result {
				let _ = transaction.rollback();
				warn!(error = %e, "failed to apply tick write");
				return;
			}
		}

		if let Err(e) =
			self.catalog.persist_pending_shapes(&mut Transaction::Command(&mut transaction), pending_shapes)
		{
			let _ = transaction.rollback();
			warn!(error = %e, "failed to persist tick pending shapes");
			return;
		}

		if let Err(e) = transaction.commit_unchecked() {
			warn!(error = %e, "failed to commit tick writes");
		} else {
			self.evict_durable_reservations(&pending);
		}
	}

	fn evict_durable_reservations(&self, committed: &Pending) {
		let registry = self.engine.dictionary_allocators();
		let mut by_dict: HashMap<DictionaryId, Vec<[u8; 16]>> = HashMap::new();
		for (key, _) in committed.iter_sorted() {
			if matches!(Key::kind(key), Some(KeyKind::DictionaryEntry))
				&& let Some(entry) = DictionaryEntryKey::decode(key)
			{
				by_dict.entry(entry.dictionary).or_default().push(entry.hash);
			}
		}
		for (dictionary, hashes) in by_dict {
			registry.mark_durable(dictionary, &hashes);
		}
	}
}
