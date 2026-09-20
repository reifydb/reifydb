// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::VecDeque, marker::PhantomData};

use reifydb_codec::log::record::Record;
use reifydb_value::value::datetime::DateTime;

use crate::{
	body::Body,
	device::{ReadFrom, RecordCursor},
	error::{Result, WalError},
	lsn::Lsn,
};

const BATCH: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replayed<T> {
	pub lsn: Lsn,
	pub at: DateTime,
	pub body: T,
}

pub struct Replay<'a, T, L: ReadFrom> {
	cursor: L::Cursor<'a>,
	buffered: VecDeque<Record>,
	drained: bool,
	body: PhantomData<T>,
}

impl<'a, T, L: ReadFrom> Replay<'a, T, L> {
	pub(crate) fn new(cursor: L::Cursor<'a>) -> Self {
		Self {
			cursor,
			buffered: VecDeque::new(),
			drained: false,
			body: PhantomData,
		}
	}
}

impl<T: Body, L: ReadFrom> Iterator for Replay<'_, T, L> {
	type Item = Result<Replayed<T>>;

	fn next(&mut self) -> Option<Self::Item> {
		while self.buffered.is_empty() {
			if self.drained {
				return None;
			}
			match self.cursor.next_batch(BATCH) {
				Ok(records) => {
					self.drained = records.len() < BATCH;
					self.buffered.extend(records);
				}
				Err(error) => {
					self.drained = true;
					return Some(Err(error));
				}
			}
		}
		self.buffered.pop_front().map(decode)
	}
}

fn decode<T: Body>(record: Record) -> Result<Replayed<T>> {
	let lsn = Lsn::from_version(record.version).unwrap_or(Lsn::FIRST);
	let kind = record.kind.as_u32();
	match T::decode(kind, &record.payload) {
		Ok(body) => Ok(Replayed {
			lsn,
			at: record.timestamp,
			body,
		}),
		Err(error) => Err(WalError::Decode {
			lsn,
			kind,
			reason: error.to_string(),
		}),
	}
}
