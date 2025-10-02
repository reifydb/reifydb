// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::mpsc;

use reifydb_core::{CowVec, Result, delta::Delta};
use reifydb_type::Error;

use crate::{
	backend::{memory::write::WriteCommand, single::BackendSingleVersionCommit},
	memory::MemoryBackend,
	storage_internal_error,
};

impl BackendSingleVersionCommit for MemoryBackend {
	fn commit(&mut self, delta: CowVec<Delta>) -> Result<()> {
		let (respond_to, response) = mpsc::channel();

		self.writer
			.send(WriteCommand::SingleVersionCommit {
				deltas: delta,
				respond_to,
			})
			.map_err(|_| Error(storage_internal_error!("Memory writer disconnected")))?;

		match response.recv() {
			Ok(result) => result,
			Err(_) => Err(Error(storage_internal_error!("Memory writer failed to respond"))),
		}
	}
}
