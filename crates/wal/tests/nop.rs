// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::log::Position;
use reifydb_store_log::segment::{Scan, Stop};
use reifydb_value::value::datetime::DateTime;
use reifydb_wal::{
	body::Body,
	error::{Result, WalError},
	lsn::Lsn,
	nop::NopDevice,
	wal::Wal,
};

#[derive(Debug, PartialEq)]
struct Ping(u64);

impl Body for Ping {
	fn kind(&self) -> u32 {
		1
	}

	fn encode(&self, out: &mut Vec<u8>) {
		out.extend_from_slice(&self.0.to_le_bytes());
	}

	fn decode(_kind: u32, bytes: &[u8]) -> Result<Self> {
		let bytes: [u8; 8] = bytes.try_into().map_err(|_| WalError::Body("ping is not 8 bytes".into()))?;
		Ok(Ping(u64::from_le_bytes(bytes)))
	}
}

fn open() -> Wal<Ping, NopDevice> {
	let scan = Scan {
		records: Vec::new(),
		end: Position::ZERO,
		stop: Stop::Unwritten,
	};
	let (wal, _) = Wal::open(NopDevice::new(), vec![scan]).unwrap();
	wal
}

#[test]
fn nop_lsns_count_up_by_one_per_append() {
	// Floors and the per-version lsn map key on these numbers, so a gap or a reuse breaks single and multi
	// checkpoints.
	let wal = open();
	let lsns: Vec<u64> = (0..3).map(|n| wal.append(DateTime::from_millis(1), &Ping(n)).unwrap().as_u64()).collect();
	assert_eq!(lsns, vec![1, 2, 3]);
}

#[test]
fn nop_durable_follows_the_last_append() {
	// Without this, an fsync commit with the wal disabled would call sync and still see itself as not durable.
	let wal = open();
	assert_eq!(wal.durable(), None);
	wal.append(DateTime::from_millis(1), &Ping(7)).unwrap();
	let last = wal.append(DateTime::from_millis(2), &Ping(8)).unwrap();
	assert_eq!(wal.sync().unwrap(), Some(last));
	assert_eq!(wal.durable(), Some(last));
}

#[test]
fn nop_floor_advances_without_floor_ahead() {
	// A store flush moves its floor to the lsn it saved; a FloorAhead here panics every flush with the wal
	// disabled.
	let wal = open();
	let floor = wal.floor("multi").unwrap();
	let lsn = wal.append(DateTime::from_millis(1), &Ping(1)).unwrap();
	wal.sync().unwrap();
	floor.advance(lsn).unwrap();
	assert_eq!(floor.position(), Some(lsn));
	wal.gc().unwrap();
}

#[test]
fn nop_replay_yields_no_records() {
	// Boot replays from here; any record coming back would be installed twice on top of the stores.
	let wal = open();
	wal.append(DateTime::from_millis(1), &Ping(1)).unwrap();
	wal.append(DateTime::from_millis(2), &Ping(2)).unwrap();
	let replayed: Vec<_> = wal.replay(Lsn::FIRST).unwrap().collect();
	assert!(replayed.is_empty(), "the nop device kept {} records", replayed.len());
}
