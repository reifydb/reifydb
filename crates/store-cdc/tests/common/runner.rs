// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::Bound, error::Error as StdError, fmt::Write as _};

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, CdcChange},
};
use reifydb_store_cdc::{
	storage::{CdcStorage, Cutoff},
	store::CdcStore,
	tier::persistent::CdcPersistentTier,
};
use reifydb_testing::testscript::{command::Command, runner::Runner as TsRunner};
use reifydb_value::{util::cowvec::CowVec, value::datetime::DateTime};

const DEFAULT_TIMESTAMP: u64 = 1_700_000_000_000_000_000;

const DEFAULT_KEY: &str = "k";

const DEFAULT_VALUE: &str = "v";

const DEFAULT_BATCH: u64 = 1024;

const SUMMARY_LIMIT: usize = 1024;

/// Nothing it prints may depend on which persistent tier is underneath: block boundaries and version bounds are shared
/// truth, compression and cache residency are not.
pub struct Runner {
	pub store: CdcStore,
	pub persistent: CdcPersistentTier,
}

impl Runner {
	pub fn new(store: CdcStore, persistent: CdcPersistentTier) -> Self {
		Self {
			store,
			persistent,
		}
	}
}

fn parse_bounds(spec: &str) -> Result<(Bound<CommitVersion>, Bound<CommitVersion>), Box<dyn StdError>> {
	// a leading '>' makes the start exclusive, which Rust range syntax cannot express
	let (spec, start_excluded) = match spec.strip_prefix('>') {
		Some(rest) => (rest, true),
		None => (spec, false),
	};
	let Some((head, tail)) = spec.split_once("..") else {
		return Err(format!("range '{spec}' is not a range").into());
	};
	let start = match head.trim() {
		"" if start_excluded => return Err("an exclusive start needs a version".into()),
		"" => Bound::Unbounded,
		v => {
			let version = CommitVersion(v.parse::<u64>()?);
			if start_excluded {
				Bound::Excluded(version)
			} else {
				Bound::Included(version)
			}
		}
	};
	let (tail, inclusive) = match tail.strip_prefix('=') {
		Some(rest) => (rest, true),
		None => (tail, false),
	};
	let end = match tail.trim() {
		"" if inclusive => return Err("an inclusive end needs a version".into()),
		"" => Bound::Unbounded,
		v => {
			let version = CommitVersion(v.parse::<u64>()?);
			if inclusive {
				Bound::Included(version)
			} else {
				Bound::Excluded(version)
			}
		}
	};
	Ok((start, end))
}

fn render_version(version: Option<CommitVersion>) -> String {
	match version {
		Some(version) => version.0.to_string(),
		None => "None".to_string(),
	}
}

fn render_cutoff(cutoff: Option<Cutoff>) -> String {
	match cutoff {
		Some(Cutoff::Version(version)) => version.0.to_string(),
		Some(Cutoff::Unbounded) => "unbounded".to_string(),
		None => "None".to_string(),
	}
}

impl TsRunner for Runner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn StdError>> {
		let mut output = String::new();
		match command.name.as_str() {
			"write" => {
				let mut args = command.consume_args();
				let version: u64 = args.next_pos().ok_or("version not given")?.parse()?;
				let timestamp: u64 = args.lookup_parse("ts")?.unwrap_or(DEFAULT_TIMESTAMP);
				let changes: usize = args.lookup_parse("changes")?.unwrap_or(1);
				let key =
					args.lookup("key").map(|a| a.value.clone()).unwrap_or(DEFAULT_KEY.to_string());
				let value = args
					.lookup("value")
					.map(|a| a.value.clone())
					.unwrap_or(DEFAULT_VALUE.to_string());
				args.reject_rest()?;

				let cdc = Cdc::new(
					CommitVersion(version),
					DateTime::from_nanos(timestamp),
					(0..changes)
						.map(|i| CdcChange::Insert {
							key: EncodedKey::new(format!("{key}{i}").into_bytes()),
							post: EncodedBytes(CowVec::new(
								format!("{value}{i}").into_bytes(),
							)),
						})
						.collect(),
				);
				self.store.write(&cdc)?;
			}

			"flush" => {
				command.consume_args().reject_rest()?;
				if !self.store.flush_pending() {
					return Err("flush timed out".into());
				}
			}

			"read" => {
				let mut args = command.consume_args();
				let version = CommitVersion(args.next_pos().ok_or("version not given")?.parse()?);
				args.reject_rest()?;

				match self.store.read(version)? {
					Some(cdc) => writeln!(
						output,
						"v{} => changes={} ts={}",
						version.0,
						cdc.changes.len(),
						cdc.timestamp.to_nanos()
					)?,
					None => writeln!(output, "v{} => None", version.0)?,
				}
			}

			"count" => {
				let mut args = command.consume_args();
				let version = CommitVersion(args.next_pos().ok_or("version not given")?.parse()?);
				args.reject_rest()?;

				writeln!(output, "v{} => count={}", version.0, self.store.count(version)?)?;
			}

			"range" => {
				let mut args = command.consume_args();
				let batch_size: u64 = args.lookup_parse("batch")?.unwrap_or(DEFAULT_BATCH);
				let spec = args.next_pos().map(|a| a.value.clone()).unwrap_or_else(|| "..".to_string());
				args.reject_rest()?;

				let (start, end) = parse_bounds(&spec)?;
				let batch = self.store.read_range(start, end, batch_size)?;
				for cdc in &batch.items {
					writeln!(output, "v{}", cdc.version.0)?;
				}
				writeln!(output, "has_more={}", batch.has_more)?;
			}

			"drop_before" => {
				let mut args = command.consume_args();
				let version = CommitVersion(args.next_pos().ok_or("version not given")?.parse()?);
				let limit: usize = args.lookup_parse("limit")?.unwrap_or(usize::MAX);
				args.reject_rest()?;

				let result = self.store.drop_before(Cutoff::Version(version), limit)?;
				writeln!(
					output,
					"dropped={} sources={} more={}",
					result.count.as_u64(),
					result.entries.len(),
					result.more_remaining
				)?;
			}

			"bounds" => {
				command.consume_args().reject_rest()?;
				writeln!(
					output,
					"min={} max={} truncated_before={}",
					render_version(self.store.min_version()?),
					render_version(self.store.max_version()?),
					self.store.truncated_before()?.0
				)?;
			}

			"blocks" => {
				// the sealed block layout is the one piece of tier internals every combination must
				// agree on, because retention drops whole blocks
				command.consume_args().reject_rest()?;
				let summaries = self.persistent.summaries_from(CommitVersion(0), SUMMARY_LIMIT)?;
				write!(output, "blocks={}", summaries.len())?;
				for summary in &summaries {
					write!(
						output,
						" [{}..{} n={}]",
						summary.min_version.0,
						summary.max_version.0,
						summary.count.as_u64()
					)?;
				}
				writeln!(output)?;
			}

			"ttl_cutoff" => {
				let mut args = command.consume_args();
				let nanos: u64 = args.next_pos().ok_or("timestamp not given")?.parse()?;
				args.reject_rest()?;

				writeln!(
					output,
					"cutoff={}",
					render_cutoff(self.store.find_ttl_cutoff(DateTime::from_nanos(nanos))?)
				)?;
			}

			name => return Err(format!("unknown command {name}").into()),
		}
		Ok(output)
	}
}
