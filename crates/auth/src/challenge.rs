// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::RwLock, time::Duration};

use reifydb_runtime::context::{
	clock::{Clock, Instant},
	rng::Rng,
};
use uuid::Builder;

struct ChallengeEntry {
	pub identifier: String,
	pub method: String,
	pub payload: HashMap<String, String>,
	pub created_at: Instant,
}

pub struct ChallengeInfo {
	pub identifier: String,
	pub method: String,
	pub payload: HashMap<String, String>,
}

pub struct ChallengeStore {
	entries: RwLock<HashMap<String, ChallengeEntry>>,
	ttl: Duration,
}

impl ChallengeStore {
	pub fn new(ttl: Duration) -> Self {
		Self {
			entries: RwLock::new(HashMap::new()),
			ttl,
		}
	}

	pub fn create(
		&self,
		identifier: String,
		method: String,
		payload: HashMap<String, String>,
		clock: &Clock,
		rng: &Rng,
	) -> String {
		let millis = clock.now_millis();
		let random_bytes = rng.infra_bytes_10();
		let challenge_id = Builder::from_unix_timestamp_millis(millis, &random_bytes).into_uuid().to_string();
		let entry = ChallengeEntry {
			identifier,
			method,
			payload,
			created_at: clock.instant(),
		};
		let mut entries = self.entries.write().unwrap();
		entries.insert(challenge_id.clone(), entry);
		challenge_id
	}

	pub fn consume(&self, challenge_id: &str) -> Option<ChallengeInfo> {
		let mut entries = self.entries.write().unwrap();
		let entry = entries.remove(challenge_id)?;

		if entry.created_at.elapsed() > self.ttl {
			return None;
		}

		Some(ChallengeInfo {
			identifier: entry.identifier,
			method: entry.method,
			payload: entry.payload,
		})
	}

	pub fn cleanup_expired(&self) {
		let ttl = self.ttl;
		let mut entries = self.entries.write().unwrap();
		entries.retain(|_, e| e.created_at.elapsed() <= ttl);
	}
}

#[cfg(test)]
mod tests {
	use reifydb_runtime::context::clock::MockClock;

	use super::*;

	fn test_clock_and_rng() -> (Clock, MockClock, Rng) {
		let mock = MockClock::from_millis(1000);
		(Clock::Mock(mock.clone()), mock, Rng::seeded(42))
	}

	#[test]
	fn test_create_and_consume() {
		let (clock, _, rng) = test_clock_and_rng();
		let store = ChallengeStore::new(Duration::from_secs(60));
		let data = HashMap::from([("nonce".to_string(), "abc123".to_string())]);

		let id = store.create("alice".to_string(), "solana".to_string(), data, &clock, &rng);
		let info = store.consume(&id).unwrap();

		assert_eq!(info.identifier, "alice");
		assert_eq!(info.method, "solana");
		assert_eq!(info.payload.get("nonce").unwrap(), "abc123");
	}

	#[test]
	fn test_one_time_use() {
		let (clock, _, rng) = test_clock_and_rng();
		let store = ChallengeStore::new(Duration::from_secs(60));
		let id = store.create("alice".to_string(), "solana".to_string(), HashMap::new(), &clock, &rng);

		assert!(store.consume(&id).is_some());
		assert!(store.consume(&id).is_none()); // second attempt fails
	}

	#[test]
	fn test_unknown_challenge() {
		let store = ChallengeStore::new(Duration::from_secs(60));
		assert!(store.consume("nonexistent").is_none());
	}

	#[test]
	fn test_expired_challenge() {
		let (clock, mock, rng) = test_clock_and_rng();
		let store = ChallengeStore::new(Duration::from_millis(1));
		let id = store.create("alice".to_string(), "solana".to_string(), HashMap::new(), &clock, &rng);

		mock.advance_millis(10);
		assert!(store.consume(&id).is_none());
	}
}
