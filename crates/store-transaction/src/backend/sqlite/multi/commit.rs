// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use std::sync::mpsc;

use reifydb_core::{CommitVersion, CowVec, Result, delta::Delta, interface::TransactionId, util::now_millis};
use reifydb_type::Error;

use crate::{
	MultiVersionCommit,
	backend::sqlite::{SqliteBackend, write::WriteCommand},
	storage_internal_error,
};

impl MultiVersionCommit for SqliteBackend {
	fn commit(&self, deltas: CowVec<Delta>, version: CommitVersion, transaction: TransactionId) -> Result<()> {
		let (respond_to, response) = mpsc::channel();

		self.writer
			.send(WriteCommand::MultiVersionCommit {
				deltas,
				version,
				transaction,
				timestamp: now_millis(),
				respond_to,
			})
			.map_err(|_| Error(storage_internal_error!("Writer disconnected")))?;

		match response.recv() {
			Ok(result) => result,
			Err(_) => Err(Error(storage_internal_error!("Writer failed to response"))),
		}
	}
}
