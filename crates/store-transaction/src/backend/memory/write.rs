// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	collections::{BTreeMap, HashMap},
	sync::{Arc, mpsc},
	thread,
};

use mpsc::{Receiver, Sender};
use parking_lot::RwLock;
use reifydb_core::{CommitVersion, CowVec, EncodedKey, delta::Delta, value::encoded::EncodedValues};
use reifydb_type::Result;

use crate::{
	backend::{commit::CommitBuffer, gc::GcStats, memory::VersionChain},
	cdc::{InternalCdc, process_deltas_for_cdc},
};

pub enum WriteCommand {
	MultiVersionCommit {
		deltas: CowVec<Delta>,
		version: CommitVersion,
		timestamp: u64,
		respond_to: Sender<Result<()>>,
	},
	SingleVersionCommit {
		deltas: CowVec<Delta>,
		respond_to: Sender<Result<()>>,
	},
	GarbageCollect {
		operations: Vec<(EncodedKey, CommitVersion)>, // (key, compact_from_version)
		respond_to: Sender<Result<GcStats>>,
	},
	Shutdown,
}

pub struct Writer {
	receiver: Receiver<WriteCommand>,
	multi: Arc<RwLock<BTreeMap<EncodedKey, VersionChain>>>,
	single: Arc<RwLock<BTreeMap<EncodedKey, Option<EncodedValues>>>>,
	cdcs: Arc<RwLock<BTreeMap<CommitVersion, InternalCdc>>>,
	commit_buffer: CommitBuffer,
	// Track pending responses for buffered commits
	pending_responses: HashMap<CommitVersion, Sender<Result<()>>>,
}

impl Writer {
	pub fn spawn(
		multi: Arc<RwLock<BTreeMap<EncodedKey, VersionChain>>>,
		single: Arc<RwLock<BTreeMap<EncodedKey, Option<EncodedValues>>>>,
		cdcs: Arc<RwLock<BTreeMap<CommitVersion, InternalCdc>>>,
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
					timestamp,
					respond_to,
				} => {
					// Buffer the commit and process any that are ready
					self.buffer_and_apply_commit(deltas, version, timestamp, respond_to);
				}
				WriteCommand::SingleVersionCommit {
					deltas,
					respond_to,
				} => {
					let result = self.handle_single_commit(deltas);
					let _ = respond_to.send(result);
				}
				WriteCommand::GarbageCollect {
					operations,
					respond_to,
				} => {
					let result = self.handle_garbage_collect(operations);
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
		timestamp: u64,
		respond_to: Sender<Result<()>>,
	) {
		// Store the response sender for later
		self.pending_responses.insert(version, respond_to);

		// Add to buffer
		self.commit_buffer.add_commit(version, deltas, timestamp);

		// Process all ready commits
		let ready_commits = self.commit_buffer.drain_ready();
		for commit in ready_commits {
			let result = self.apply_multi_commit(commit.deltas, commit.version, commit.timestamp);

			// Send response for this commit if we have one pending
			if let Some(sender) = self.pending_responses.remove(&commit.version) {
				let _ = sender.send(result);
			}
		}
	}

	fn apply_multi_commit(&self, deltas: CowVec<Delta>, version: CommitVersion, timestamp: u64) -> Result<()> {
		let multi = self.multi.clone();

		// Clone deltas for CDC processing
		let deltas_for_cdc = deltas.clone();

		// Capture pre-versions BEFORE applying deltas
		let mut pre_versions = std::collections::HashMap::new();
		{
			let multi_read = multi.read();
			for delta in deltas.iter() {
				let key = delta.key();
				if !pre_versions.contains_key(key) {
					if let Some(chain) = multi_read.get(key) {
						// Only capture pre-version if the key actually exists (not deleted)
						if let Some(pre_version) = chain.get_latest_version() {
							// Check if this version contains a value (not a tombstone)
							if let Some(Some(_)) = chain.get_at(pre_version) {
								pre_versions.insert(key.clone(), pre_version);
							}
						}
					}
				}
			}
		}

		// Apply deltas to storage
		{
			let mut multi_write = multi.write();
			for delta in deltas {
				match delta {
					Delta::Set {
						key,
						values,
					} => {
						multi_write
							.entry(key)
							.or_insert_with(VersionChain::new)
							.set(version, Some(values));
					}
					Delta::Remove {
						key,
					} => {
						multi_write
							.entry(key)
							.or_insert_with(VersionChain::new)
							.set(version, None);
					}
				}
			}
		}

		// Process CDC changes using the shared function
		let cdc_changes = process_deltas_for_cdc(deltas_for_cdc, version, |key| {
			// Return the pre-version we captured before applying deltas
			pre_versions.get(key).copied()
		})?;

		if !cdc_changes.is_empty() {
			let mut cdcs = self.cdcs.write();
			let cdc = InternalCdc {
				version,
				timestamp,
				changes: cdc_changes,
			};
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

	fn handle_garbage_collect(&self, operations: Vec<(EncodedKey, CommitVersion)>) -> Result<GcStats> {
		let mut stats = GcStats::default();

		// Get write lock (safe because we're in the writer thread)
		let mut multi = self.multi.write();

		stats.keys_processed = operations.len();

		// Compact each operator state key
		for (key, compact_from_version) in operations {
			if let Some(chain) = multi.get_mut(&key) {
				let versions_before = chain.len();

				// Compact the version chain
				chain.compact(compact_from_version);
				let versions_after = chain.len();
				stats.versions_removed += versions_before.saturating_sub(versions_after);
			}
		}

		Ok(stats)
	}
}
