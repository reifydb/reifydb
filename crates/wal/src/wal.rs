// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, marker::PhantomData};

use reifydb_codec::log::{LogIndex, RecordKind, Term, record::Record};
use reifydb_runtime::{
	actor::{
		context::CancellationToken,
		mailbox::{ActorRef, SendError},
		system::{ActorHandle, ActorSpawner},
	},
	sync::{Arc, condvar::Condvar, mutex::Mutex},
};
use reifydb_store_log::{error::LogError, segment::Scan};
use reifydb_value::{reifydb_assertions, value::datetime::DateTime};

use crate::{
	body::Body,
	cursors::Cursors,
	device::{Append, Flush, Mark, ReadFrom, Reclaim},
	error::{Result, WalError},
	floor::Floor,
	lsn::Lsn,
	recovered::Recovered,
	replay::Replay,
	sync::{SyncActor, SyncMessage, SyncMode},
};

pub struct Wal<T, L>(Arc<Inner<T, L>>);

struct Inner<T, L> {
	device: L,
	appender: Mutex<Appender>,
	progress: Mutex<Progress>,
	held: Mutex<BTreeSet<String>>,
	syncer: Mutex<Option<Syncer>>,
	flushed: Condvar,
	body: PhantomData<T>,
}

struct Appender {
	last: Option<Lsn>,
}

struct Syncer {
	actor: ActorRef<SyncMessage>,
	cancel: Option<CancellationToken>,
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
			held: Mutex::new(BTreeSet::new()),
			syncer: Mutex::new(None),
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

impl<T, L> Wal<T, L> {
	pub(crate) fn device(&self) -> &L {
		&self.0.device
	}

	pub(crate) fn release(&self, name: &str) {
		self.0.held.lock().remove(name);
	}
}

impl<T, L: Mark> Wal<T, L> {
	pub fn floor(&self, name: &str) -> Result<Floor<T, L>> {
		if !self.0.held.lock().insert(name.to_string()) {
			return Err(WalError::FloorHeld(name.to_string()));
		}
		match self.position_of(name) {
			Ok(position) => Ok(Floor::new(self.clone(), name.to_string(), position)),
			Err(error) => {
				self.release(name);
				Err(error)
			}
		}
	}

	fn position_of(&self, name: &str) -> Result<Option<Lsn>> {
		match self.0.device.register(name) {
			Ok(()) => return Ok(None),
			Err(WalError::Log(LogError::AlreadyExists(_))) => {}
			Err(error) => return Err(error),
		}
		let stored = self.0.device.readers()?.into_iter().find(|(id, _)| id == name).map(|(_, hint)| hint);
		Ok(stored.and_then(Lsn::from_version))
	}
}

impl<T, L: Reclaim + Mark> Wal<T, L> {
	pub fn gc(&self) -> Result<Option<Lsn>> {
		let lowest =
			self.0.device.readers()?.into_iter().map(|(_, hint)| Lsn::from_version(hint)).min().flatten();
		if let Some(lowest) = lowest {
			reifydb_assertions! {
				let appended = self.0.appender.lock().last;
				assert!(
					appended.is_some_and(|last| lowest <= last),
					"the lowest floor is past the last appended lsn, so gc would drop every sealed \
					 segment under a reader position no record ever had (floor={lowest:?}, \
					 appended={appended:?})"
				);
			}
			let ceiling = LogIndex::from(lowest).max(self.0.device.committed()?);
			self.0.device.drop_below(ceiling)?;
		}
		self.start()
	}

	pub fn cursors(&self) -> Result<Cursors> {
		let appended = self.0.appender.lock().last;
		let durable = self.0.progress.lock().durable;
		let floors =
			self.0.device
				.readers()?
				.into_iter()
				.map(|(name, hint)| (name, Lsn::from_version(hint)))
				.collect();
		Ok(Cursors {
			appended,
			durable,
			start: self.start()?,
			floors,
			bytes: self.0.device.bytes()?,
		})
	}

	fn start(&self) -> Result<Option<Lsn>> {
		Ok(self.0.device.start()?.and_then(Lsn::from_version))
	}
}

impl<T: Body, L: ReadFrom + Reclaim> Wal<T, L> {
	pub fn replay(&self, from: Lsn) -> Result<Replay<'_, T, L>> {
		let oldest = self.0.device.start()?.and_then(Lsn::from_version);
		let at = match oldest {
			Some(oldest) if from < oldest => {
				return Err(WalError::Purged {
					requested: from,
					oldest,
				});
			}
			_ => from,
		};
		match self.0.device.read_from(at.into()) {
			Ok(cursor) => Ok(Replay::new(cursor)),
			Err(WalError::Log(LogError::Purged {
				oldest,
				..
			})) => Err(WalError::Purged {
				requested: from,
				oldest: Lsn::from_version(oldest).unwrap_or(Lsn::FIRST),
			}),
			Err(error) => Err(error),
		}
	}
}

impl<T: Body, L: Append + Flush> Wal<T, L> {
	pub fn append(&self, at: DateTime, body: &T) -> Result<Lsn> {
		if let Some(failed) = &self.0.progress.lock().failed {
			return Err(failed.clone());
		}
		let lsn = {
			let mut appender = self.0.appender.lock();
			let lsn = match appender.last {
				None => Lsn::FIRST,
				Some(last) => Lsn::new(last.as_u64() + 1),
			};
			reifydb_assertions! {
				assert!(
					appender.last.is_none_or(|last| lsn.as_u64() == last.as_u64() + 1),
					"the next lsn is not one past the last appended one, so a gap or a reuse in \
					 the lsn sequence lets replay read a record under a version another record \
					 already claimed (next lsn={lsn:?}, last appended={:?})",
					appender.last
				);
			}
			let mut payload = Vec::new();
			body.encode(&mut payload);
			let record = Record::new(
				lsn.into(),
				lsn.into(),
				Term::new(0),
				at,
				RecordKind::new(body.kind()),
				payload,
			);
			self.0.device.append(&record)?;
			appender.last = Some(lsn);
			lsn
		};
		self.notify()?;
		Ok(lsn)
	}

	fn notify(&self) -> Result<()> {
		let syncer = self.0.syncer.lock();
		let Some(syncer) = syncer.as_ref() else {
			return Ok(());
		};
		match syncer.actor.send(SyncMessage::Appended) {
			Ok(()) | Err(SendError::Full(_)) => Ok(()),
			Err(SendError::Closed(_)) => {
				if syncer.cancel.as_ref().is_some_and(CancellationToken::is_cancelled) {
					return Ok(());
				}
				Err(WalError::SyncStopped)
			}
		}
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
		let durable = {
			let mut progress = self.0.progress.lock();
			progress.durable = progress.durable.max(found);
			progress.durable
		};
		self.0.flushed.notify_all();
		Ok(durable)
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

impl<T: Body + Send + Sync + 'static, L: Append + Flush> Wal<T, L> {
	pub fn spawn_sync(&self, spawner: &ActorSpawner, mode: SyncMode) -> Option<ActorHandle<SyncMessage>> {
		let window = match mode {
			SyncMode::Never => return None,
			SyncMode::GroupCommit {
				window,
			} => Some(window),
			SyncMode::EveryAppend => None,
		};
		let handle = spawner.spawn_coordination("wal-sync", SyncActor::new(self.clone(), window));
		if mode == SyncMode::EveryAppend {
			*self.0.syncer.lock() = Some(Syncer {
				actor: handle.actor_ref().clone(),
				cancel: spawner.cancellation_token(),
			});
		}
		Some(handle)
	}
}
