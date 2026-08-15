// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{error::Error as StdError, fmt::Write, ops::Bound};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::operator::EncodedOperatorRow,
};
use reifydb_core::{
	common::CommitVersion,
	interface::catalog::flow::{FlowId, OperatorId},
	key::operator_state::{GroupId, Keyspace, OperatorStateKey},
	util::encoding::{
		binary::decode_binary,
		format::{Formatter, raw::Raw},
	},
};
use reifydb_store_operator::{
	store::OperatorStore,
	types::{BufferedState, OperatorWrite},
};
use reifydb_testing::testscript;
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};
use testscript::command::{ArgumentConsumer, Command};

/// Every script names its keys by a short suffix; the runner wraps that suffix in the group/keyspace frame the
/// store expects, so keys carry the keyspace byte the census triggers read while goldens stay readable.
const KEY_PREFIX_LEN: usize = 9;

const DEFAULT_OPERATOR: u64 = 1;

const DEFAULT_GROUP: u64 = 1;

const DEFAULT_KEYSPACE: u8 = 0x10;

const DEFAULT_SIDE: u8 = 0;

const DEFAULT_LIMIT: u64 = 64;

const DEFAULT_BATCH: u64 = 1024;

/// Shared testscript runner for every per-tier test binary. With `auto_flush` each mutating command is followed
/// by `flush_pending_blocking()`; without it the explicit `flush` command is the only thing that moves the commit
/// buffer into sqlite, which is what lets a script control flush timing precisely.
pub struct Runner {
	pub store: OperatorStore,
	pub auto_flush: bool,
}

#[allow(dead_code)]
impl Runner {
	pub fn from_store(store: OperatorStore) -> Self {
		Self {
			store,
			auto_flush: true,
		}
	}

	pub fn from_store_no_auto_flush(store: OperatorStore) -> Self {
		Self {
			store,
			auto_flush: false,
		}
	}

	#[inline]
	fn maybe_flush(&self) {
		if self.auto_flush {
			self.store.flush_pending_blocking();
		}
	}
}

impl testscript::runner::Runner for Runner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn StdError>> {
		let mut output = String::new();
		match command.name.as_str() {
			"set" => {
				let mut args = command.consume_args();
				let operator = operator_of(&mut args)?;
				let keyspace = keyspace_of(&mut args)?;
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = encode_key(
					&decode_binary(&kv.key.expect("next_key yields a keyed argument")),
					keyspace,
				);
				let row = EncodedOperatorRow::timeless(&decode_binary(&kv.value));
				args.reject_rest()?;

				self.store.set(operator, key, row);
				self.maybe_flush();
			}

			"remove" => {
				let mut args = command.consume_args();
				let operator = operator_of(&mut args)?;
				let keyspace = keyspace_of(&mut args)?;
				let key = encode_key(
					&decode_binary(&args.next_pos().ok_or("key not given")?.value),
					keyspace,
				);
				args.reject_rest()?;

				self.store.remove(operator, &key);
				self.maybe_flush();
			}

			"get" => {
				let mut args = command.consume_args();
				let operator = operator_of(&mut args)?;
				let keyspace = keyspace_of(&mut args)?;
				let key = encode_key(
					&decode_binary(&args.next_pos().ok_or("key not given")?.value),
					keyspace,
				);
				args.reject_rest()?;

				let value = self.store.get(operator, &key).map(|row| row.body().to_vec());
				writeln!(output, "{}", Raw::key_maybe_value(&key_name(&key), value))?;
			}

			"contains" => {
				let mut args = command.consume_args();
				let operator = operator_of(&mut args)?;
				let keyspace = keyspace_of(&mut args)?;
				let key = encode_key(
					&decode_binary(&args.next_pos().ok_or("key not given")?.value),
					keyspace,
				);
				args.reject_rest()?;

				writeln!(
					output,
					"{} => {}",
					Raw::key(&key_name(&key)),
					self.store.contains(operator, &key)
				)?;
			}

			"range" => {
				let mut args = command.consume_args();
				let operator = operator_of(&mut args)?;
				let keyspace = keyspace_of(&mut args)?;
				let batch_size: u64 = args.lookup_parse("batch")?.unwrap_or(DEFAULT_BATCH);
				let spec = args.next_pos().map(|a| a.value.clone()).unwrap_or_else(|| "..".to_string());
				args.reject_rest()?;
				if !spec.contains("..") {
					return Err(format!("range '{spec}' is not a range").into());
				}

				let parsed = EncodedKeyRange::parse(&spec);
				let range = EncodedKeyRange::new(
					frame_bound(parsed.start, keyspace),
					frame_bound(parsed.end, keyspace),
				);
				let batch = self.store.range_batch(operator, range, batch_size);
				for (key, row) in &batch.items {
					writeln!(output, "{}", Raw::key_value(&key_name(key), row.body()))?;
				}
				writeln!(output, "has_more={}", batch.has_more)?;
			}

			"drop_state" => {
				let mut args = command.consume_args();
				let operator = operator_of(&mut args)?;
				args.reject_rest()?;

				self.store.drop_operator_state(operator);
				self.maybe_flush();
			}

			"batch" => {
				let (writes, checkpoints, deletes) = parse_batch(command)?;
				if !checkpoints.is_empty() || !deletes.is_empty() {
					return Err("batch takes no checkpoint arguments, use batch_ckpt".into());
				}
				self.store.apply_batch(&writes);
				self.maybe_flush();
			}

			"batch_ckpt" => {
				let (writes, checkpoints, deletes) = parse_batch(command)?;
				self.store.apply_batch_with_checkpoints(&writes, &checkpoints, &deletes);
				self.maybe_flush();
			}

			"ckpt_set" => {
				let mut args = command.consume_args();
				let flow = FlowId(args.next_pos().ok_or("flow not given")?.parse()?);
				let version = CommitVersion(args.next_pos().ok_or("version not given")?.parse()?);
				args.reject_rest()?;

				self.store.checkpoint_set(flow, version);
				self.maybe_flush();
			}

			"ckpt_get" => {
				let mut args = command.consume_args();
				let flow = FlowId(args.next_pos().ok_or("flow not given")?.parse()?);
				args.reject_rest()?;

				match self.store.checkpoint_get(flow) {
					Some(version) => writeln!(output, "flow {} => {}", flow.0, version.0)?,
					None => writeln!(output, "flow {} => None", flow.0)?,
				}
			}

			"ckpt_delete" => {
				let mut args = command.consume_args();
				let flow = FlowId(args.next_pos().ok_or("flow not given")?.parse()?);
				args.reject_rest()?;

				self.store.checkpoint_delete(flow);
				self.maybe_flush();
			}

			"ckpt_list" => {
				command.consume_args().reject_rest()?;

				let flows: Vec<u64> =
					self.store.checkpoint_list().into_iter().map(|flow| flow.0).collect();
				writeln!(output, "flows: {flows:?}")?;
			}

			"ckpt_floor" => {
				command.consume_args().reject_rest()?;

				match self.store.checkpoint_floor() {
					Some(version) => writeln!(output, "floor => {}", version.0)?,
					None => writeln!(output, "floor => None")?,
				}
			}

			"anchor_set" => {
				let mut args = command.consume_args();
				let operator = operator_of(&mut args)?;
				let group = group_of(&mut args)?;
				let side = side_of(&mut args)?;
				let row_number = RowNumber(args.lookup_parse("row")?.ok_or("row not given")?);
				let expiry: u64 = args.lookup_parse("at")?.ok_or("at not given")?;
				args.reject_rest()?;

				self.store.anchor_set(operator, group, side, row_number, DateTime::from_millis(expiry));
				self.maybe_flush();
			}

			"anchor_get" => {
				let mut args = command.consume_args();
				let operator = operator_of(&mut args)?;
				let group = group_of(&mut args)?;
				let side = side_of(&mut args)?;
				let row_number = RowNumber(args.lookup_parse("row")?.ok_or("row not given")?);
				args.reject_rest()?;

				match self.store.anchor_get(operator, group, side, row_number) {
					Some(expiry) => writeln!(
						output,
						"anchor {}/{} => {}",
						side,
						row_number.0,
						expiry.to_millis()
					)?,
					None => writeln!(output, "anchor {}/{} => None", side, row_number.0)?,
				}
			}

			"anchor_remove" => {
				let mut args = command.consume_args();
				let operator = operator_of(&mut args)?;
				let group = group_of(&mut args)?;
				let side = side_of(&mut args)?;
				let row_number = RowNumber(args.lookup_parse("row")?.ok_or("row not given")?);
				args.reject_rest()?;

				self.store.anchor_remove(operator, group, side, row_number);
				self.maybe_flush();
			}

			"anchors_group_remove" => {
				let mut args = command.consume_args();
				let operator = operator_of(&mut args)?;
				let group = group_of(&mut args)?;
				args.reject_rest()?;

				self.store.anchors_remove_group(operator, group);
				self.maybe_flush();
			}

			"anchors_drop" => {
				let mut args = command.consume_args();
				let operator = operator_of(&mut args)?;
				args.reject_rest()?;

				self.store.anchors_drop_operator(operator);
				self.maybe_flush();
			}

			"anchors_by_expiry" => {
				let mut args = command.consume_args();
				let operator = operator_of(&mut args)?;
				let group = group_of(&mut args)?;
				let limit: u64 = args.lookup_parse("limit")?.unwrap_or(DEFAULT_LIMIT);
				args.reject_rest()?;

				let anchors = self.store.anchors_by_expiry(operator, group, limit);
				for anchor in &anchors {
					writeln!(
						output,
						"{}/{} => {}",
						anchor.side,
						anchor.row_number.0,
						anchor.expiry.to_millis()
					)?;
				}
				writeln!(output, "count={}", anchors.len())?;
			}

			"anchors_due" => {
				let mut args = command.consume_args();
				let operator = operator_of(&mut args)?;
				let group = group_of(&mut args)?;
				let limit: u64 = args.lookup_parse("limit")?.unwrap_or(DEFAULT_LIMIT);
				let at: u64 = args.lookup_parse("at")?.ok_or("at not given")?;
				args.reject_rest()?;

				let anchors = self.store.anchors_due(operator, group, DateTime::from_millis(at), limit);
				for anchor in &anchors {
					writeln!(
						output,
						"{}/{} => {}",
						anchor.side,
						anchor.row_number.0,
						anchor.expiry.to_millis()
					)?;
				}
				writeln!(output, "count={}", anchors.len())?;
			}

			"anchor_census" => {
				command.consume_args().reject_rest()?;

				let census = self.store.anchor_census();
				for entry in &census {
					writeln!(output, "operator {} => {}", entry.operator.0, entry.keys)?;
				}
				writeln!(output, "count={}", census.len())?;
			}

			"bytes" => {
				let mut args = command.consume_args();
				let operator = operator_of(&mut args)?;
				args.reject_rest()?;

				writeln!(output, "bytes {} => {}", operator.0, self.store.bytes(operator).as_bytes())?;
			}

			"total_bytes" => {
				command.consume_args().reject_rest()?;

				writeln!(output, "total_bytes => {}", self.store.total_bytes().as_bytes())?;
			}

			"census" => {
				command.consume_args().reject_rest()?;

				let census = self.store.census();
				for entry in &census {
					writeln!(
						output,
						"operator {} keyspace {:#04x} keys={} key_bytes={} value_bytes={}",
						entry.operator.0,
						entry.keyspace.0,
						entry.keys,
						entry.key_bytes.as_bytes(),
						entry.value_bytes.as_bytes()
					)?;
				}
				writeln!(output, "count={}", census.len())?;
			}

			"flush" => {
				command.consume_args().reject_rest()?;

				match self.store.flush_pending_blocking() {
					true => writeln!(output, "ok")?,
					false => writeln!(output, "not flushed")?,
				}
			}

			"commit_get" => {
				let mut args = command.consume_args();
				let operator = operator_of(&mut args)?;
				let keyspace = keyspace_of(&mut args)?;
				let key = encode_key(
					&decode_binary(&args.next_pos().ok_or("key not given")?.value),
					keyspace,
				);
				args.reject_rest()?;

				let buffer = self.store.commit();
				let name = key_name(&key);
				match buffer.lookup_state(operator, &key) {
					BufferedState::Row(row) => {
						writeln!(output, "{}", Raw::key_value(&name, row.body()))?
					}
					BufferedState::Tombstone => writeln!(output, "{} => tombstone", Raw::key(&name))?,
					BufferedState::Dropped | BufferedState::Absent => {
						writeln!(output, "{} => unknown", Raw::key(&name))?
					}
				}
			}

			"persistent_get" => {
				let mut args = command.consume_args();
				let operator = operator_of(&mut args)?;
				let keyspace = keyspace_of(&mut args)?;
				let key = encode_key(
					&decode_binary(&args.next_pos().ok_or("key not given")?.value),
					keyspace,
				);
				args.reject_rest()?;

				let persistent = self.store.persistent().ok_or("persistent tier not configured")?;
				let value = persistent.get(operator, &key).map(|row| row.body().to_vec());
				writeln!(output, "{}", Raw::key_maybe_value(&key_name(&key), value))?;
			}

			"persistent_set" => {
				let mut args = command.consume_args();
				let operator = operator_of(&mut args)?;
				let keyspace = keyspace_of(&mut args)?;
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = encode_key(
					&decode_binary(&kv.key.expect("next_key yields a keyed argument")),
					keyspace,
				);
				let row = EncodedOperatorRow::timeless(&decode_binary(&kv.value));
				args.reject_rest()?;

				let persistent = self.store.persistent().ok_or("persistent tier not configured")?;
				persistent.set(operator, key, row);
			}

			"persistent_delete" => {
				let mut args = command.consume_args();
				let operator = operator_of(&mut args)?;
				let keyspace = keyspace_of(&mut args)?;
				let key = encode_key(
					&decode_binary(&args.next_pos().ok_or("key not given")?.value),
					keyspace,
				);
				args.reject_rest()?;

				let persistent = self.store.persistent().ok_or("persistent tier not configured")?;
				persistent.remove(operator, &key);
			}

			name => {
				return Err(format!("invalid command {name}").into());
			}
		}
		Ok(output)
	}
}

fn encode_key(suffix: &[u8], keyspace: u8) -> EncodedKey {
	OperatorStateKey::inner_encoded(GroupId(DEFAULT_GROUP), Keyspace(keyspace), suffix).as_encoded().clone()
}

fn key_name(key: &EncodedKey) -> Vec<u8> {
	key.as_slice()[KEY_PREFIX_LEN..].to_vec()
}

fn frame_bound(bound: Bound<EncodedKey>, keyspace: u8) -> Bound<EncodedKey> {
	match bound {
		Bound::Unbounded => Bound::Unbounded,
		Bound::Included(key) => Bound::Included(encode_key(key.as_slice(), keyspace)),
		Bound::Excluded(key) => Bound::Excluded(encode_key(key.as_slice(), keyspace)),
	}
}

fn operator_of(args: &mut ArgumentConsumer<'_>) -> Result<OperatorId, Box<dyn StdError>> {
	Ok(OperatorId(args.lookup_parse("op")?.unwrap_or(DEFAULT_OPERATOR)))
}

fn group_of(args: &mut ArgumentConsumer<'_>) -> Result<GroupId, Box<dyn StdError>> {
	Ok(GroupId(args.lookup_parse("group")?.unwrap_or(DEFAULT_GROUP)))
}

fn side_of(args: &mut ArgumentConsumer<'_>) -> Result<u8, Box<dyn StdError>> {
	Ok(args.lookup_parse("side")?.unwrap_or(DEFAULT_SIDE))
}

fn keyspace_of(args: &mut ArgumentConsumer<'_>) -> Result<u8, Box<dyn StdError>> {
	match args.lookup("ks") {
		Some(arg) => parse_keyspace(&arg.value),
		None => Ok(DEFAULT_KEYSPACE),
	}
}

fn parse_keyspace(value: &str) -> Result<u8, Box<dyn StdError>> {
	match value.strip_prefix("0x") {
		Some(hex) => Ok(u8::from_str_radix(hex, 16)?),
		None => Ok(value.parse()?),
	}
}

type BatchArgs = (Vec<OperatorWrite>, Vec<(FlowId, CommitVersion)>, Vec<FlowId>);

fn parse_batch(command: &Command) -> Result<BatchArgs, Box<dyn StdError>> {
	let mut operator = OperatorId(DEFAULT_OPERATOR);
	let mut keyspace = DEFAULT_KEYSPACE;
	let mut writes: Vec<OperatorWrite> = Vec::new();
	let mut checkpoints: Vec<(FlowId, CommitVersion)> = Vec::new();
	let mut deletes: Vec<FlowId> = Vec::new();

	for arg in &command.args {
		let name = arg.key.as_deref().ok_or_else(|| format!("batch argument '{}' needs a key", arg.value))?;
		match name {
			"op" => operator = OperatorId(arg.parse()?),
			"ks" => keyspace = parse_keyspace(&arg.value)?,
			"set" => {
				let (key, body) = arg.value.split_once('/').ok_or("set needs key/value")?;
				writes.push(OperatorWrite::Set {
					operator,
					key: encode_key(&decode_binary(key), keyspace),
					row: EncodedOperatorRow::timeless(&decode_binary(body)),
				});
			}
			"remove" => writes.push(OperatorWrite::Remove {
				operator,
				key: encode_key(&decode_binary(&arg.value), keyspace),
			}),
			"anchor_set" => {
				let parts: Vec<&str> = arg.value.split('/').collect();
				if parts.len() != 4 {
					return Err("anchor_set needs group/side/row/millis".into());
				}
				writes.push(OperatorWrite::AnchorSet {
					operator,
					group: GroupId(parts[0].parse()?),
					side: parts[1].parse()?,
					row_num: RowNumber(parts[2].parse()?),
					expiry: DateTime::from_millis(parts[3].parse()?),
				});
			}
			"anchor_remove" => {
				let parts: Vec<&str> = arg.value.split('/').collect();
				if parts.len() != 3 {
					return Err("anchor_remove needs group/side/row".into());
				}
				writes.push(OperatorWrite::AnchorRemove {
					operator,
					group: GroupId(parts[0].parse()?),
					side: parts[1].parse()?,
					run_num: RowNumber(parts[2].parse()?),
				});
			}
			"ckpt" => {
				let (flow, version) = arg.value.split_once('/').ok_or("ckpt needs flow/version")?;
				checkpoints.push((FlowId(flow.parse()?), CommitVersion(version.parse()?)));
			}
			"ckpt_del" => deletes.push(FlowId(arg.value.parse()?)),
			other => return Err(format!("invalid batch argument '{other}'").into()),
		}
	}

	Ok((writes, checkpoints, deletes))
}
