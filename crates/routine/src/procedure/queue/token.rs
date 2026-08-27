// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::id::QueueId;
use reifydb_routine_abi::error::QueueError;
use reifydb_value::{fragment::Fragment, value::row_number::RowNumber};

const PREFIX: &str = "qt1";
const FIELDS: usize = 6;

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimToken {
	pub queue: QueueId,
	pub partition: u16,
	pub row: RowNumber,
	pub attempt: u32,
	pub worker: String,
}

impl ClaimToken {
	pub fn format(&self) -> String {
		format!(
			"{}:{}:{}:{}:{}:{}",
			PREFIX, self.queue.0, self.partition, self.row.0, self.attempt, self.worker
		)
	}

	pub fn parse(procedure: &'static str, fragment: &Fragment, token: &str) -> Result<Self, QueueError> {
		Self::split(token).ok_or_else(|| QueueError::TokenInvalid {
			procedure,
			fragment: fragment.clone(),
			token: token.to_string(),
		})
	}

	fn split(token: &str) -> Option<Self> {
		let mut parts = token.splitn(FIELDS, ':');

		if parts.next()? != PREFIX {
			return None;
		}

		let queue = parts.next()?.parse::<u64>().ok()?;
		let partition = parts.next()?.parse::<u16>().ok()?;
		let row = parts.next()?.parse::<u64>().ok()?;
		let attempt = parts.next()?.parse::<u32>().ok()?;
		let worker = parts.next()?;

		if worker.is_empty() {
			return None;
		}

		Some(Self {
			queue: QueueId(queue),
			partition,
			row: RowNumber(row),
			attempt,
			worker: worker.to_string(),
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	const PROCEDURE: &str = "queue::test";

	fn token(worker: &str) -> ClaimToken {
		ClaimToken {
			queue: QueueId(9),
			partition: 3,
			row: RowNumber(17),
			attempt: 2,
			worker: worker.to_string(),
		}
	}

	#[test]
	fn test_a_token_survives_a_round_trip() {
		// Every ack and extend is authorised purely by what this parser reads back out. A field
		// that shifts by one position would hand attempt 2's outcome to attempt 3, or transition
		// a different item entirely.
		let original = token("worker-1");

		assert_eq!(ClaimToken::parse(PROCEDURE, &Fragment::none(), &original.format()).unwrap(), original);
	}

	#[test]
	fn test_a_worker_id_containing_colons_survives() {
		// Worker ids are caller-supplied and routinely look like "host:port" or a URI. Splitting
		// on every colon instead of the first five would truncate the id and silently
		// mis-attribute the ack to a different worker.
		let original = token("10.0.0.1:8080:pool:a");

		let parsed = ClaimToken::parse(PROCEDURE, &Fragment::none(), &original.format()).unwrap();

		assert_eq!(parsed.worker, "10.0.0.1:8080:pool:a");
		assert_eq!(parsed, original);
	}

	#[test]
	fn test_the_boundary_values_of_every_numeric_field_survive() {
		// The token is the only carrier of the CAS discriminators. A field parsed as a narrower
		// type would wrap at the boundary and address a different partition or attempt.
		let original = ClaimToken {
			queue: QueueId(u64::MAX),
			partition: u16::MAX,
			row: RowNumber(u64::MAX),
			attempt: u32::MAX,
			worker: "w".to_string(),
		};

		assert_eq!(ClaimToken::parse(PROCEDURE, &Fragment::none(), &original.format()).unwrap(), original);
	}

	#[test]
	fn test_malformed_tokens_are_rejected_rather_than_panicking() {
		// Tokens arrive from the network, so every one of these is reachable by a hostile or
		// merely buggy client. A panic here takes the server down; a silent default would forge
		// authority over an item the caller never claimed.
		for malformed in [
			"",
			"qt1",
			"qt1:9:3:17:2",
			"qt0:9:3:17:2:w",
			"qt1:nine:3:17:2:w",
			"qt1:9:70000:17:2:w",
			"qt1:9:3:17:99999999999999999999:w",
			"qt1:-9:3:17:2:w",
			"qt1:9:3:17:2:",
		] {
			assert!(
				matches!(
					ClaimToken::parse(PROCEDURE, &Fragment::none(), malformed),
					Err(QueueError::TokenInvalid { .. })
				),
				"expected {malformed:?} to be rejected"
			);
		}
	}
}
