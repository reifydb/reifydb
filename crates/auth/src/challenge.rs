// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! In-memory challenge store for multi-step authentication flows.
//!
//! Challenges are one-time-use and expire after a configurable TTL.

use std::{
	collections::HashMap,
	sync::RwLock,
	time::{Duration, Instant},
};

use reifydb_type::value::uuid::Uuid7;

/// A pending authentication challenge.
struct ChallengeEntry {
	pub username: String,
	pub method: String,
	pub payload: HashMap<String, String>,
	pub created_at: Instant,
}

/// Stored challenge info returned when consuming a challenge.
pub struct ChallengeInfo {
	pub username: String,
	pub method: String,
	pub payload: HashMap<String, String>,
}

/// In-memory store for pending authentication challenges.
///
/// Challenges are created during multi-step authentication (e.g., wallet signing)
/// and consumed on the client's response. Each challenge is one-time-use and
/// expires after the configured TTL.
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

	/// Create a new challenge and return its ID.
	pub fn create(&self, username: String, method: String, payload: HashMap<String, String>) -> String {
		let challenge_id = Uuid7::generate().to_string();
		let entry = ChallengeEntry {
			username,
			method,
			payload,
			created_at: Instant::now(),
		};
		let mut entries = self.entries.write().unwrap();
		entries.insert(challenge_id.clone(), entry);
		challenge_id
	}

	/// Consume a challenge by ID. Returns the challenge data if valid and not expired.
	/// The challenge is removed after consumption (one-time use).
	pub fn consume(&self, challenge_id: &str) -> Option<ChallengeInfo> {
		let mut entries = self.entries.write().unwrap();
		let entry = entries.remove(challenge_id)?;

		if entry.created_at.elapsed() > self.ttl {
			return None;
		}

		Some(ChallengeInfo {
			username: entry.username,
			method: entry.method,
			payload: entry.payload,
		})
	}

	/// Remove all expired entries.
	pub fn cleanup_expired(&self) {
		let ttl = self.ttl;
		let mut entries = self.entries.write().unwrap();
		entries.retain(|_, e| e.created_at.elapsed() <= ttl);
	}
}

#[cfg(test)]
mod tests {
	use std::thread;

	use super::*;

	#[test]
	fn test_create_and_consume() {
		let store = ChallengeStore::new(Duration::from_secs(60));
		let data = HashMap::from([("nonce".to_string(), "abc123".to_string())]);

		let id = store.create("alice".to_string(), "solana".to_string(), data);
		let info = store.consume(&id).unwrap();

		assert_eq!(info.username, "alice");
		assert_eq!(info.method, "solana");
		assert_eq!(info.payload.get("nonce").unwrap(), "abc123");
	}

	#[test]
	fn test_one_time_use() {
		let store = ChallengeStore::new(Duration::from_secs(60));
		let id = store.create("alice".to_string(), "solana".to_string(), HashMap::new());

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
		let store = ChallengeStore::new(Duration::from_millis(1));
		let id = store.create("alice".to_string(), "solana".to_string(), HashMap::new());

		thread::sleep(Duration::from_millis(10));
		assert!(store.consume(&id).is_none());
	}
}
