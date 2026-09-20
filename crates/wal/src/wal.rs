// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::marker::PhantomData;

use reifydb_codec::log::{RecordKind, Term, record::Record};
use reifydb_runtime::sync::{Arc, condvar::Condvar, mutex::Mutex};
use reifydb_store_log::segment::Scan;
use reifydb_value::{reifydb_assertions, value::datetime::DateTime};

use crate::{
	body::Body,
	device::{Append, Flush, Mark, ReadFrom, Reclaim},
	error::{Result, WalError},
	lsn::Lsn,
	recovered::Recovered,
};

pub struct Wal<T, L>(Arc<Inner<T, L>>);

struct Inner<T, L> {
	device: L,
	appender: Mutex<Appender>,
	progress: Mutex<Progress>,
	flushed: Condvar,
	body: PhantomData<T>,
}

struct Appender {
	last: Option<Lsn>,
}

struct Progress {
	durable: Option<Lsn>,
	failed: Option<WalError>,
}

impl<T, L> Clone for Wal<T, L> {
	fn clone(&self) -> Self {
		Self(Arc::clone(&self.0))
	}
}

impl<T: Body, L: Append + Flush + ReadFrom + Reclaim + Mark> Wal<T, L> {
	pub fn open(device: L, scans: Vec<Scan>) -> Result<(Self, Recovered)> {
		let [scan] = scans.as_slice() else {
			return Err(WalError::Partitions {
				found: scans.len() as u32,
			});
		};
		let stop = scan.stop;
		let head = device.head()?.and_then(Lsn::from_version);
		let durable = device.durable()?.and_then(Lsn::from_version);
		let start = device.start()?.and_then(Lsn::from_version);
		let floors =
			device.readers()?.into_iter().map(|(name, hint)| (name, Lsn::from_version(hint))).collect();
		let wal = Self(Arc::new(Inner {
			device,
			appender: Mutex::new(Appender {
				last: head,
			}),
			progress: Mutex::new(Progress {
				durable,
				failed: None,
			}),
			flushed: Condvar::new(),
			body: PhantomData,
		}));
		Ok((
			wal,
			Recovered {
				head,
				durable,
				start,
				floors,
				stop,
			},
		))
	}
}

impl<T: Body, L: Append + Flush> Wal<T, L> {
	pub fn append(&self, at: DateTime, body: &T) -> Result<Lsn> {
		if let Some(failed) = &self.0.progress.lock().failed {
			return Err(failed.clone());
		}
		let mut appender = self.0.appender.lock();
		let lsn = match appender.last {
			None => Lsn::FIRST,
			Some(last) => Lsn::new(last.as_u64() + 1),
		};
		reifydb_assertions! {
			assert!(
				appender.last.is_none_or(|last| lsn.as_u64() == last.as_u64() + 1),
				"the next lsn is not one past the last appended one, so a gap or a reuse in the lsn \
				 sequence lets replay read a record under a version another record already claimed \
				 (next lsn={lsn:?}, last appended={:?})",
				appender.last
			);
		}
		let mut payload = Vec::new();
		body.encode(&mut payload);
		let record =
			Record::new(lsn.into(), lsn.into(), Term::new(0), at, RecordKind::new(body.kind()), payload);
		self.0.device.append(&record)?;
		appender.last = Some(lsn);
		Ok(lsn)
	}

	pub fn sync(&self) -> Result<Option<Lsn>> {
		if let Some(failed) = &self.0.progress.lock().failed {
			return Err(failed.clone());
		}
		let outcome = self.0.device.flush().and_then(|()| self.0.device.durable());
		let found = match outcome {
			Ok(found) => found.and_then(Lsn::from_version),
			Err(error) => {
				self.0.progress.lock().failed = Some(error.clone());
				self.0.flushed.notify_all();
				return Err(error);
			}
		};
		reifydb_assertions! {
			let appended = self.0.appender.lock().last;
			assert!(
				found <= appended,
				"the durable mark passed the last appended lsn, so a flush claims a record durable \
				 that was never framed (durable={found:?}, appended={appended:?})"
			);
		}
		self.0.progress.lock().durable = found;
		self.0.flushed.notify_all();
		Ok(found)
	}

	pub fn durable(&self) -> Option<Lsn> {
		self.0.progress.lock().durable
	}

	#[cfg(not(reifydb_single_threaded))]
	pub fn wait_durable(&self, lsn: Lsn) -> Result<()> {
		let mut progress = self.0.progress.lock();
		while progress.durable.is_none_or(|found| found < lsn) {
			if let Some(failed) = &progress.failed {
				return Err(failed.clone());
			}
			self.0.flushed.wait(&mut progress);
		}
		Ok(())
	}
}
