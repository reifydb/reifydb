// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::mpsc;

use reifydb_core::{CowVec, Result, delta::Delta, interface::SingleVersionCommit};

use crate::sqlite::{Sqlite, write::WriteCommand};

impl SingleVersionCommit for Sqlite {
	fn commit(&mut self, deltas: CowVec<Delta>) -> Result<()> {
		let (tx, rx) = mpsc::channel();
		self.writer
			.send(WriteCommand::SingleVersionCommit {
				operations: self.convert_deltas_to_operations(deltas),
				response: tx,
			})
			.map_err(|_| {
				reifydb_type::Error(crate::storage_internal_error!("Writer actor disconnected"))
			})?;
		match rx.recv() {
			Ok(result) => result,
			Err(_) => {
				Err(reifydb_type::Error(crate::storage_internal_error!("Writer actor response failed")))
			}
		}
	}
}
