// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	collections::{BTreeMap, HashMap},
	sync::{Arc, mpsc},
	thread,
};

use parking_lot::RwLock;
use mpsc::{Receiver, Sender};
use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, TransactionId,
	delta::Delta,
	interface::{Cdc, CdcSequencedChange},
	value::encoded::EncodedValues,
};
use reifydb_type::{Result, return_error};

use crate::{
	backend::{commit::CommitBuffer, diagnostic::sequence_exhausted, memory::VersionChain},
	cdc::generate_cdc_change,
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
	multi: Arc<RwLock<BTreeMap<EncodedKey, VersionChain>>>,
	single: Arc<RwLock<BTreeMap<EncodedKey, Option<EncodedValues>>>>,
	cdcs: Arc<RwLock<BTreeMap<CommitVersion, Cdc>>>,
	commit_buffer: CommitBuffer,
	// Track pending responses for buffered commits
	pending_responses: HashMap<CommitVersion, Sender<Result<()>>>,
}

impl Writer {
	pub fn spawn(
		multi: Arc<RwLock<BTreeMap<EncodedKey, VersionChain>>>,
		single: Arc<RwLock<BTreeMap<EncodedKey, Option<EncodedValues>>>>,
		cdcs: Arc<RwLock<BTreeMap<CommitVersion, Cdc>>>,
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

		// Take write lock once for the entire batch
		{
			let mut multi = self.multi.write();

			// Apply deltas and collect CDC changes
			for (idx, delta) in deltas.into_iter().enumerate() {
				let sequence = match u16::try_from(idx + 1) {
					Ok(seq) => seq,
					Err(_) => return_error!(sequence_exhausted()),
				};

				// Get pre-value for CDC
				let pre = multi.get(delta.key())
					.and_then(|chain| chain.get_latest_value());

				// Apply the delta
				match &delta {
					Delta::Set {
						key,
						values,
					} => {
						multi.entry(key.clone())
							.or_insert_with(VersionChain::new)
							.set(version, Some(values.clone()));
					}
					Delta::Remove {
						key,
					} => {
						multi.entry(key.clone())
							.or_insert_with(VersionChain::new)
							.set(version, None); // Tombstone
					}
				}

				cdc_changes.push(CdcSequencedChange {
					sequence,
					change: generate_cdc_change(delta, pre),
				});
			}
		} // Release write lock

		// Insert CDC if there are changes
		if !cdc_changes.is_empty() {
			let mut cdcs = self.cdcs.write();
			let cdc = Cdc::new(version, timestamp, transaction, cdc_changes);
			cdcs.insert(version, cdc);
		}

		Ok(())
	}

	fn handle_single_commit(&self, deltas: CowVec<Delta>) -> Result<()> {
		let mut single = self.single.write();
		for delta in deltas {
			match delta {
				Delta::Set {
					key,
					values,
				} => {
					single.insert(key, Some(values));
				}
				Delta::Remove {
					key,
				} => {
					single.insert(key, None);
				}
			}
		}
		Ok(())
	}
}
