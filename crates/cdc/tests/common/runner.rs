// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#![allow(dead_code, unused_imports)]

use std::{
	collections::Bound,
	error::Error as StdError,
	fmt::Write as _,
	thread::sleep,
	time::{Duration, Instant},
};

use reifydb_cdc::storage::{CdcStorage as _, CdcStore};
use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::cdc::{Cdc, SystemChange},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::context::clock::MockClock;
use reifydb_testing::testscript::{command::Command, runner::Runner as TsRunner};
use reifydb_transaction::transaction::command::CommandTransaction;
use reifydb_type::{util::cowvec::CowVec, value::identity::IdentityId};

/// Runs commands from `tests/scripts/cdc/*` against a `StandardEngine` +
/// `CdcStore`. Buffers row mutations into a transaction and commits on the
/// `commit` command. Reads from the CDC store after a short bounded poll.
///
/// Version translation: scripts use 1-based version annotations starting
/// from the script writer's chosen baseline (e.g. 1 or 5). The engine
/// assigns its own sequential commit versions (starting from 2; version 1
/// is consumed by bootstrap and produces no CDC). The runner observes the
/// first commit and records `version_offset = script_v - engine_v` so all
/// subsequent queries translate consistently.
pub struct Runner {
	engine: StandardEngine,
	cdc_store: CdcStore,
	mock_clock: MockClock,
	active_txn: Option<CommandTransaction>,
	last_committed: Option<CommitVersion>,
	/// `script_version - engine_version`. None until first commit.
	version_offset: Option<i64>,
	/// Version annotation captured from the first `insert`/`update`/`delete`/`bulk_insert`
	/// of the current transaction; used to compute `version_offset` on commit.
	pending_script_version: Option<u64>,
}

impl Runner {
	pub fn new(engine: StandardEngine, cdc_store: CdcStore, mock_clock: MockClock) -> Self {
		Self {
			engine,
			cdc_store,
			mock_clock,
			active_txn: None,
			last_committed: None,
			version_offset: None,
			pending_script_version: None,
		}
	}

	fn ensure_txn(&mut self) -> Result<&mut CommandTransaction, Box<dyn StdError>> {
		if self.active_txn.is_none() {
			self.active_txn = Some(self.engine.begin_command(IdentityId::system())?);
		}
		Ok(self.active_txn.as_mut().unwrap())
	}

	fn wait_for_cdc(&self, version: CommitVersion) {
		let deadline = Instant::now() + Duration::from_secs(5);
		loop {
			match self.cdc_store.max_version() {
				Ok(Some(max)) if max.0 >= version.0 => return,
				_ => {}
			}
			if Instant::now() >= deadline {
				return;
			}
			sleep(Duration::from_millis(2));
		}
	}

	fn note_pending_version(&mut self, v: u64) {
		if self.pending_script_version.is_none() {
			self.pending_script_version = Some(v);
		}
	}

	fn to_engine_version(&self, script_v: u64) -> CommitVersion {
		let off = self.version_offset.unwrap_or(0);
		// engine = script - offset
		let engine = (script_v as i64) - off;
		CommitVersion(engine.max(0) as u64)
	}

	fn to_script_version(&self, engine_v: CommitVersion) -> u64 {
		let off = self.version_offset.unwrap_or(0);
		((engine_v.0 as i64) + off).max(0) as u64
	}

	fn write_range(
		&self,
		out: &mut String,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
	) -> Result<(), Box<dyn StdError>> {
		let batch = self.cdc_store.read_range(start, end, 1024)?;
		if batch.items.is_empty() {
			writeln!(out, "ok")?;
		} else {
			for cdc in &batch.items {
				let script_v = self.to_script_version(cdc.version);
				for (i, sc) in cdc.system_changes.iter().enumerate() {
					writeln!(out, "v{} {}", script_v, format_change(i + 1, sc))?;
				}
			}
		}
		Ok(())
	}
}

impl TsRunner for Runner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn StdError>> {
		let mut output = String::new();
		match command.name.as_str() {
			// insert VERSION KEY=VALUE
			"insert" | "update" => {
				let mut args = command.consume_args();
				let v_arg = args.next_pos().ok_or("expected VERSION")?;
				let v: u64 = v_arg.parse()?;
				self.note_pending_version(v);
				let kv = args.next_key().ok_or("expected KEY=VALUE")?;
				let key = encoded_key(kv.key.as_deref().unwrap());
				let row = encoded_row(&kv.value);
				args.reject_rest()?;
				let txn = self.ensure_txn()?;
				txn.set(&key, row)?;
			}
			// delete VERSION KEY
			"delete" => {
				let mut args = command.consume_args();
				let v_arg = args.next_pos().ok_or("expected VERSION")?;
				let v: u64 = v_arg.parse()?;
				self.note_pending_version(v);
				let key_arg = args.next_pos().ok_or("expected KEY")?;
				let key = encoded_key(&key_arg.value);
				args.reject_rest()?;
				let txn = self.ensure_txn()?;
				match txn.get(&key)? {
					Some(prev) => txn.unset(&key, prev.row)?,
					None => txn.remove(&key)?,
				}
			}
			// commit
			"commit" => {
				let txn = self.active_txn.take().ok_or("no active transaction")?;
				let mut txn = txn;
				let engine_version = txn.commit()?;
				if self.version_offset.is_none() {
					if let Some(script_v) = self.pending_script_version {
						self.version_offset =
							Some((script_v as i64) - (engine_version.0 as i64));
					}
				}
				self.pending_script_version = None;
				self.last_committed = Some(engine_version);
				self.wait_for_cdc(engine_version);
				writeln!(output, "ok")?;
			}
			// advance_clock MS
			"advance_clock" => {
				let mut args = command.consume_args();
				let arg = args.next_pos().ok_or("expected MS")?;
				let ms: u64 = arg.parse()?;
				args.reject_rest()?;
				self.mock_clock.advance_millis(ms);
				writeln!(output, "ok")?;
			}
			// bulk_insert VERSION COUNT (self-committing)
			"bulk_insert" => {
				let mut args = command.consume_args();
				let v_arg = args.next_pos().ok_or("expected VERSION")?;
				let v: u64 = v_arg.parse()?;
				self.note_pending_version(v);
				let count_arg = args.next_pos().ok_or("expected COUNT")?;
				let count: u64 = count_arg.parse()?;
				args.reject_rest()?;
				{
					let txn = self.ensure_txn()?;
					for i in 0..count {
						let key = encoded_key(&format!("bulk_{}", i));
						let row = encoded_row(&format!("{}", i));
						txn.set(&key, row)?;
					}
				}
				let txn = self.active_txn.take().ok_or("no active transaction")?;
				let mut txn = txn;
				let engine_version = txn.commit()?;
				if self.version_offset.is_none() {
					if let Some(script_v) = self.pending_script_version {
						self.version_offset =
							Some((script_v as i64) - (engine_version.0 as i64));
					}
				}
				self.pending_script_version = None;
				self.last_committed = Some(engine_version);
				self.wait_for_cdc(engine_version);
				writeln!(output, "ok")?;
			}
			// cdc_get VERSION [SEQ]
			"cdc_get" => {
				let mut args = command.consume_args();
				let v_arg = args.next_pos().ok_or("expected VERSION")?;
				let script_v: u64 = v_arg.parse()?;
				let seq = args.next_pos().map(|a| a.value.parse::<usize>()).transpose()?;
				args.reject_rest()?;
				let engine_v = self.to_engine_version(script_v);
				let cdc = self.cdc_store.read(engine_v)?;
				match (cdc, seq) {
					(None, _) => writeln!(output, "None")?,
					(Some(cdc), None) => writeln!(output, "{}", format_cdc(&cdc, script_v))?,
					(Some(cdc), Some(s)) if s >= 1 => match cdc.system_changes.get(s - 1) {
						Some(sc) => writeln!(output, "{}", format_change(s, sc))?,
						None => writeln!(output, "None")?,
					},
					(Some(_), Some(_)) => writeln!(output, "None")?,
				}
			}
			// cdc_range_included FROM TO
			"cdc_range_included" => {
				let (from, to) = parse_two_versions(command)?;
				let from = self.to_engine_version(from.0);
				let to = self.to_engine_version(to.0);
				self.write_range(&mut output, Bound::Included(from), Bound::Included(to))?;
			}
			"cdc_range_included_excluded" => {
				let (from, to) = parse_two_versions(command)?;
				let from = self.to_engine_version(from.0);
				let to = self.to_engine_version(to.0);
				self.write_range(&mut output, Bound::Included(from), Bound::Excluded(to))?;
			}
			"cdc_range_excluded_included" => {
				let (from, to) = parse_two_versions(command)?;
				let from = self.to_engine_version(from.0);
				let to = self.to_engine_version(to.0);
				self.write_range(&mut output, Bound::Excluded(from), Bound::Included(to))?;
			}
			"cdc_range_excluded_excluded" => {
				let (from, to) = parse_two_versions(command)?;
				let from = self.to_engine_version(from.0);
				let to = self.to_engine_version(to.0);
				self.write_range(&mut output, Bound::Excluded(from), Bound::Excluded(to))?;
			}
			"cdc_range_to_included" => {
				let to = parse_one_version(command)?;
				let to = self.to_engine_version(to.0);
				self.write_range(&mut output, Bound::Unbounded, Bound::Included(to))?;
			}
			"cdc_range_to_excluded" => {
				let to = parse_one_version(command)?;
				let to = self.to_engine_version(to.0);
				self.write_range(&mut output, Bound::Unbounded, Bound::Excluded(to))?;
			}
			"cdc_range_from_included" => {
				let from = parse_one_version(command)?;
				let from = self.to_engine_version(from.0);
				self.write_range(&mut output, Bound::Included(from), Bound::Unbounded)?;
			}
			"cdc_range_from_excluded" => {
				let from = parse_one_version(command)?;
				let from = self.to_engine_version(from.0);
				self.write_range(&mut output, Bound::Excluded(from), Bound::Unbounded)?;
			}
			"cdc_range_unbounded" => {
				self.write_range(&mut output, Bound::Unbounded, Bound::Unbounded)?;
			}
			// cdc_scan
			"cdc_scan" => {
				self.write_range(&mut output, Bound::Unbounded, Bound::Unbounded)?;
			}
			// cdc_count VERSION
			"cdc_count" => {
				let v = parse_one_version(command)?;
				let engine_v = self.to_engine_version(v.0);
				let n = self.cdc_store.count(engine_v)?;
				writeln!(output, "count: {}", n)?;
			}
			other => return Err(format!("unknown command: {}", other).into()),
		}
		Ok(output)
	}
}

fn parse_one_version(cmd: &Command) -> Result<CommitVersion, Box<dyn StdError>> {
	let mut args = cmd.consume_args();
	let v: u64 = args.next_pos().ok_or("expected VERSION")?.parse()?;
	args.reject_rest()?;
	Ok(CommitVersion(v))
}

fn parse_two_versions(cmd: &Command) -> Result<(CommitVersion, CommitVersion), Box<dyn StdError>> {
	let mut args = cmd.consume_args();
	let from: u64 = args.next_pos().ok_or("expected FROM")?.parse()?;
	let to: u64 = args.next_pos().ok_or("expected TO")?.parse()?;
	args.reject_rest()?;
	Ok((CommitVersion(from), CommitVersion(to)))
}

fn encoded_key(s: &str) -> EncodedKey {
	EncodedKey(CowVec::new(s.as_bytes().to_vec()))
}

fn encoded_row(s: &str) -> EncodedRow {
	EncodedRow(CowVec::new(s.as_bytes().to_vec()))
}

fn render_bytes(b: &[u8]) -> String {
	String::from_utf8_lossy(b).into_owned()
}

pub fn format_change(seq: usize, sc: &SystemChange) -> String {
	match sc {
		SystemChange::Insert {
			key,
			post,
		} => format!(
			"Change {{ seq: {}, change: Insert {{ key: {:?}, post: {:?} }} }}",
			seq,
			render_bytes(&key.0),
			render_bytes(&post.0),
		),
		SystemChange::Update {
			key,
			pre,
			post,
		} => format!(
			"Change {{ seq: {}, change: Update {{ key: {:?}, pre: {:?}, post: {:?} }} }}",
			seq,
			render_bytes(&key.0),
			render_bytes(&pre.0),
			render_bytes(&post.0),
		),
		SystemChange::Delete {
			key,
			pre,
		} => match pre {
			Some(pre) => format!(
				"Change {{ seq: {}, change: Delete {{ key: {:?}, pre: {:?} }} }}",
				seq,
				render_bytes(&key.0),
				render_bytes(&pre.0),
			),
			None => format!(
				"Change {{ seq: {}, change: Delete {{ key: {:?} }} }}",
				seq,
				render_bytes(&key.0),
			),
		},
	}
}

pub fn format_cdc(cdc: &Cdc, script_version: u64) -> String {
	let ts_millis = cdc.timestamp.to_nanos() / 1_000_000;
	let mut s = format!("Cdc {{ version: {}, ts: {}, changes: [", script_version, ts_millis);
	for (i, sc) in cdc.system_changes.iter().enumerate() {
		if i > 0 {
			s.push_str(", ");
		}
		// Inner format: { seq: N, change: ... } (no outer "Change" wrapper)
		match sc {
			SystemChange::Insert {
				key,
				post,
			} => write!(
				s,
				"{{ seq: {}, change: Insert {{ key: {:?}, post: {:?} }} }}",
				i + 1,
				render_bytes(&key.0),
				render_bytes(&post.0),
			)
			.unwrap(),
			SystemChange::Update {
				key,
				pre,
				post,
			} => write!(
				s,
				"{{ seq: {}, change: Update {{ key: {:?}, pre: {:?}, post: {:?} }} }}",
				i + 1,
				render_bytes(&key.0),
				render_bytes(&pre.0),
				render_bytes(&post.0),
			)
			.unwrap(),
			SystemChange::Delete {
				key,
				pre,
			} => match pre {
				Some(pre) => write!(
					s,
					"{{ seq: {}, change: Delete {{ key: {:?}, pre: {:?} }} }}",
					i + 1,
					render_bytes(&key.0),
					render_bytes(&pre.0),
				)
				.unwrap(),
				None => write!(
					s,
					"{{ seq: {}, change: Delete {{ key: {:?} }} }}",
					i + 1,
					render_bytes(&key.0)
				)
				.unwrap(),
			},
		}
	}
	s.push_str("] }");
	s
}
