// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{num::NonZeroU64, sync::Arc};

use arrow_array::RecordBatch;
use reifydb_core::{common::CommitVersion, interface::catalog::object::ObjectId};
use reifydb_value::{Result, error::Error};

use crate::backfill::Scan;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Continue {
	Yes,
	Crash,
}

#[derive(Debug, PartialEq)]
pub enum Outcome {
	Land,
	Err(Error),
}

pub trait ScanHooks: Send + Sync {
	fn on_open(&self, _source: ObjectId) -> Outcome {
		Outcome::Land
	}

	fn on_next(&self, _source: ObjectId, _pull: u64) -> Outcome {
		Outcome::Land
	}

	fn during_open(&self, _source: ObjectId) {}

	fn during_next(&self, _source: ObjectId, _pull: u64) {}

	fn on_call(&self, _call: u64) -> Continue {
		Continue::Yes
	}
}

pub struct NoFaults;

impl ScanHooks for NoFaults {}

#[derive(Clone)]
pub struct InstalledScanHooks(pub Arc<dyn ScanHooks>);

pub struct TestingScan<S> {
	scan: S,
	hooks: Arc<dyn ScanHooks>,
	calls: u64,
	opened: Option<ObjectId>,
	pulls: u64,
}

impl<S> TestingScan<S> {
	pub fn over(scan: S, hooks: Arc<dyn ScanHooks>) -> Self {
		Self {
			scan,
			hooks,
			calls: 0,
			opened: None,
			pulls: 0,
		}
	}

	pub fn scan_mut(&mut self) -> &mut S {
		&mut self.scan
	}

	pub fn calls(&self) -> u64 {
		self.calls
	}

	fn call(&mut self) {
		self.calls += 1;
		let call = self.calls;
		if self.hooks.on_call(call) == Continue::Crash {
			panic!("simulated crash at call {call}");
		}
	}
}

impl<S: Scan> Scan for TestingScan<S> {
	fn version(&self) -> CommitVersion {
		self.scan.version()
	}

	fn open(&mut self, source: ObjectId, batch_size: NonZeroU64) -> Result<()> {
		self.call();
		self.hooks.during_open(source);
		if let Outcome::Err(error) = self.hooks.on_open(source) {
			return Err(error);
		}
		self.scan.open(source, batch_size)?;
		self.opened = Some(source);
		self.pulls = 0;
		Ok(())
	}

	fn next(&mut self) -> Result<Option<RecordBatch>> {
		self.call();
		let Some(source) = self.opened else {
			panic!("testing scan pulled before any source was opened");
		};
		let pull = self.pulls;
		self.hooks.during_next(source, pull);
		if let Outcome::Err(error) = self.hooks.on_next(source, pull) {
			return Err(error);
		}
		let chunk = self.scan.next()?;
		self.pulls += 1;
		Ok(chunk)
	}
}
