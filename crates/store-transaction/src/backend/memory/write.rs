// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	collections::{BTreeMap, HashMap},
	sync::{Arc, mpsc},
	thread,
};

use mpsc::{Receiver, Sender};
use parking_lot::RwLock;
use reifydb_core::{
	CommitVersion, CowVec, EncodedKey,
	delta::Delta,
	interface::{FlowNodeId, SourceId},
	value::encoded::EncodedValues,
};
use reifydb_type::Result;

use super::multi::{StorageType, classify_key};
use crate::{
	backend::{commit::CommitBuffer, delta_optimizer::optimize_deltas_cow, memory::VersionChain},
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
	/// Drop mode cleanup for operators - keep only 1 version
	CleanupOperatorRetention {
		operators: Vec<FlowNodeId>,
		respond_to: Sender<Result<()>>,
	},
	Shutdown,
}

pub struct Writer {
	receiver: Receiver<WriteCommand>,
	sources: Arc<RwLock<HashMap<SourceId, BTreeMap<EncodedKey, VersionChain>>>>,
	operators: Arc<RwLock<HashMap<FlowNodeId, BTreeMap<EncodedKey, VersionChain>>>>,
	multi: Arc<RwLock<BTreeMap<EncodedKey, VersionChain>>>,
	single: Arc<RwLock<BTreeMap<EncodedKey, Option<EncodedValues>>>>,
	cdcs: Arc<RwLock<BTreeMap<CommitVersion, InternalCdc>>>,
	commit_buffer: CommitBuffer,
	// Track pending responses for buffered commits
	pending_responses: HashMap<CommitVersion, Sender<Result<()>>>,
}

impl Writer {
	pub fn spawn(
		sources: Arc<RwLock<HashMap<SourceId, BTreeMap<EncodedKey, VersionChain>>>>,
		operators: Arc<RwLock<HashMap<FlowNodeId, BTreeMap<EncodedKey, VersionChain>>>>,
		multi: Arc<RwLock<BTreeMap<EncodedKey, VersionChain>>>,
		single: Arc<RwLock<BTreeMap<EncodedKey, Option<EncodedValues>>>>,
		cdcs: Arc<RwLock<BTreeMap<CommitVersion, InternalCdc>>>,
	) -> Result<Sender<WriteCommand>> {
		let (sender, receiver) = mpsc::channel();

		thread::spawn(move || {
			let mut actor = Writer {
				receiver,
				sources,
				operators,
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
				WriteCommand::CleanupOperatorRetention {
					operators,
					respond_to,
				} => {
					let mut result = Ok(());
					for flow_node_id in operators {
						if let Err(e) = self.handle_operator_retention_cleanup(flow_node_id) {
							result = Err(e);
							break;
						}
					}
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
		// Capture pre-versions BEFORE applying deltas
		let mut pre_versions = std::collections::HashMap::new();
		{
			let sources_read = self.sources.read();
			let operators_read = self.operators.read();
			let multi_read = self.multi.read();

			for delta in deltas.iter() {
				let key = delta.key();
				if !pre_versions.contains_key(key) {
					// Determine which storage to check based on key type
					let chain = match classify_key(key) {
						StorageType::Source(source_id) => {
							sources_read.get(&source_id).and_then(|table| table.get(key))
						}
						StorageType::Operator(flow_node_id) => operators_read
							.get(&flow_node_id)
							.and_then(|table| table.get(key)),
						StorageType::Multi => multi_read.get(key),
					};

					if let Some(chain) = chain {
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

		// Optimize deltas BEFORE applying to storage to skip unnecessary operations
		let optimized_deltas = optimize_deltas_cow(deltas.clone(), |key| {
			// Key exists in storage if we have a pre-version for it
			pre_versions.contains_key(key)
		});

		// Apply optimized deltas to storage, routing to the correct storage based on key type
		{
			let mut sources_write = self.sources.write();
			let mut operators_write = self.operators.write();
			let mut multi_write = self.multi.write();

			for delta in optimized_deltas.iter() {
				let key = delta.key();
				let value = match delta {
					Delta::Set {
						values,
						..
					} => Some(values.clone()),
					Delta::Remove {
						..
					} => None,
				};

				match classify_key(key) {
					StorageType::Source(source_id) => {
						sources_write
							.entry(source_id)
							.or_insert_with(BTreeMap::new)
							.entry(key.clone())
							.or_insert_with(VersionChain::new)
							.set(version, value);
					}
					StorageType::Operator(flow_node_id) => {
						operators_write
							.entry(flow_node_id)
							.or_insert_with(BTreeMap::new)
							.entry(key.clone())
							.or_insert_with(VersionChain::new)
							.set(version, value);
					}
					StorageType::Multi => {
						multi_write
							.entry(key.clone())
							.or_insert_with(VersionChain::new)
							.set(version, value);
					}
				}
			}
		}

		// Process CDC changes using the OPTIMIZED deltas (optimization already done at delta level)
		let cdc_changes = process_deltas_for_cdc(optimized_deltas, version, |key| {
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

	/// Handle operator retention cleanup - keep only 1 version (Drop mode)
	fn handle_operator_retention_cleanup(&self, flow_node_id: FlowNodeId) -> Result<()> {
		let mut operators_write = self.operators.write();

		if let Some(table) = operators_write.get_mut(&flow_node_id) {
			for (_key, chain) in table.iter_mut() {
				if chain.len() > 1 {
					if let Some(latest_version) = chain.get_latest_version() {
						// Drop mode: compact to keep only latest version
						// No CDC, no tombstones - just remove old versions
						chain.compact(latest_version);
					}
				}
			}
		}

		Ok(())
	}
}
