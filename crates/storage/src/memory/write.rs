// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	collections::HashMap,
	sync::{Arc, mpsc},
	thread,
};

use crossbeam_skiplist::SkipMap;
use mpsc::{Receiver, Sender};
use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, TransactionId,
	delta::Delta,
	interface::{Cdc, CdcSequencedChange},
	value::row::EncodedRow,
};
use reifydb_type::{Result, return_error};

use crate::{
	cdc::generate_cdc_change, commit::CommitBuffer, diagnostic::sequence_exhausted,
	memory::MultiVersionRowContainer,
};

pub enum WriteCommand {
	MultiVersionCommit {
		deltas: CowVec<Delta>,
		version: CommitVersion,
		transaction: TransactionId,
		timestamp: u64,
		respond_to: Sender<Result<()>>,
	},
	SingleVersionCommit {
		deltas: CowVec<Delta>,
		respond_to: Sender<Result<()>>,
	},
	Shutdown,
}

pub struct Writer {
	receiver: Receiver<WriteCommand>,
	multi: Arc<SkipMap<EncodedKey, MultiVersionRowContainer>>,
	single: Arc<SkipMap<EncodedKey, EncodedRow>>,
	cdcs: Arc<SkipMap<CommitVersion, Cdc>>,
	commit_buffer: CommitBuffer,
	// Track pending responses for buffered commits
	pending_responses: HashMap<CommitVersion, Sender<Result<()>>>,
}

impl Writer {
	pub fn spawn(
		multi: Arc<SkipMap<EncodedKey, MultiVersionRowContainer>>,
		single: Arc<SkipMap<EncodedKey, EncodedRow>>,
		cdcs: Arc<SkipMap<CommitVersion, Cdc>>,
	) -> Result<Sender<WriteCommand>> {
		let (sender, receiver) = mpsc::channel();

		thread::spawn(move || {
			let mut actor = Writer {
				receiver,
				multi,
				single,
				cdcs,
				commit_buffer: CommitBuffer::new(),
				pending_responses: HashMap::new(),
			};
			actor.run();
		});

		Ok(sender)
	}

	fn run(&mut self) {
		while let Ok(cmd) = self.receiver.recv() {
			match cmd {
				WriteCommand::MultiVersionCommit {
					deltas,
					version,
					transaction,
					timestamp,
					respond_to,
				} => {
					// Buffer the commit and process any that are ready
					self.buffer_and_apply_commit(
						deltas,
						version,
						transaction,
						timestamp,
						respond_to,
					);
				}
				WriteCommand::SingleVersionCommit {
					deltas,
					respond_to,
				} => {
					let result = self.handle_single_commit(deltas);
					let _ = respond_to.send(result);
				}
				WriteCommand::Shutdown => break,
			}
		}
	}

	fn buffer_and_apply_commit(
		&mut self,
		deltas: CowVec<Delta>,
		version: CommitVersion,
		transaction: TransactionId,
		timestamp: u64,
		respond_to: Sender<Result<()>>,
	) {
		// Store the response sender for later
		self.pending_responses.insert(version, respond_to);

		// Add to buffer
		self.commit_buffer.add_commit(version, deltas, transaction, timestamp);

		// Process all ready commits
		let ready_commits = self.commit_buffer.drain_ready();
		for commit in ready_commits {
			let result = self.apply_multi_commit(
				commit.deltas,
				commit.version,
				commit.transaction,
				commit.timestamp,
			);

			// Send response for this commit if we have one pending
			if let Some(sender) = self.pending_responses.remove(&commit.version) {
				let _ = sender.send(result);
			}
		}
	}

	fn apply_multi_commit(
		&self,
		deltas: CowVec<Delta>,
		version: CommitVersion,
		transaction: TransactionId,
		timestamp: u64,
	) -> Result<()> {
		let mut cdc_changes = Vec::new();

		// Apply deltas and collect CDC changes
		for (idx, delta) in deltas.iter().enumerate() {
			let sequence = match u16::try_from(idx + 1) {
				Ok(seq) => seq,
				Err(_) => return_error!(sequence_exhausted()),
			};

			let pre = self.multi.get(delta.key()).and_then(|entry| {
				let values = entry.value();
				values.get_latest()
			});

			// Apply the delta
			match &delta {
				Delta::Set {
					key,
					row,
				} => {
					let item = self
						.multi
						.get_or_insert_with(key.clone(), MultiVersionRowContainer::new);
					let val = item.value();
					val.insert(version, Some(row.clone()));
				}
				Delta::Remove {
					key,
				} => {
					if let Some(values) = self.multi.get(key) {
						let values = values.value();
						if !values.is_empty() {
							values.insert(version, None);
						}
					}
				}
			}

			cdc_changes.push(CdcSequencedChange {
				sequence,
				change: generate_cdc_change(delta.clone(), pre),
			});
		}

		// Insert CDC if there are changes
		if !cdc_changes.is_empty() {
			let cdc = Cdc::new(version, timestamp, transaction, cdc_changes);
			self.cdcs.insert(version, cdc);
		}

		Ok(())
	}

	fn handle_single_commit(&self, deltas: CowVec<Delta>) -> Result<()> {
		for delta in deltas {
			match delta {
				Delta::Set {
					key,
					row,
				} => {
					self.single.insert(key, row);
				}
				Delta::Remove {
					key,
				} => {
					self.single.remove(&key);
				}
			}
		}
		Ok(())
	}
}
