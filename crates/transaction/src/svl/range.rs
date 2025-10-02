// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{cmp, vec::IntoIter};

use reifydb_core::{EncodedKey, delta::Delta, interface::SingleVersionValues};

/// Iterator for scanning a range in an SVL WriteTransaction with owned values.
/// This avoids lifetime issues with the storage lock.
pub struct SvlRangeIter {
	/// Iterator over committed data
	committed: IntoIter<SingleVersionValues>,
	/// Iterator over pending changes
	pending: IntoIter<(EncodedKey, Delta)>,
	/// Next item from pending buffer
	next_pending: Option<(EncodedKey, Delta)>,
	/// Next item from committed storage
	next_committed: Option<SingleVersionValues>,
	/// Track the last key we yielded to avoid duplicates
	last_yielded_key: Option<EncodedKey>,
}

impl SvlRangeIter {
	pub fn new(pending: IntoIter<(EncodedKey, Delta)>, committed: IntoIter<SingleVersionValues>) -> Self {
		let mut iterator = SvlRangeIter {
			pending,
			committed,
			next_pending: None,
			next_committed: None,
			last_yielded_key: None,
		};

		iterator.advance_pending();
		iterator.advance_committed();

		iterator
	}

	fn advance_pending(&mut self) {
		self.next_pending = self.pending.next();
	}

	fn advance_committed(&mut self) {
		self.next_committed = self.committed.next();
	}
}

impl Iterator for SvlRangeIter {
	type Item = SingleVersionValues;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			match (self.next_pending.as_ref(), self.next_committed.as_ref()) {
				// Both pending and committed iterators have
				// items
				(Some((pending_key, _delta)), Some(committed)) => {
					match pending_key.cmp(&committed.key) {
						// Pending item has a smaller
						// key, yield it
						cmp::Ordering::Less => {
							let (key, delta) = self.next_pending.take().unwrap();
							self.advance_pending();
							self.last_yielded_key = Some(key.clone());

							match delta {
								Delta::Set {
									values,
									..
								} => {
									return Some(SingleVersionValues {
										key,
										values,
									});
								}
								Delta::Remove {
									..
								} => {
									// Skip removed entries
									continue;
								}
							}
						}
						// Keys are equal - pending
						// overrides committed
						cmp::Ordering::Equal => {
							let (key, delta) = self.next_pending.take().unwrap();
							self.advance_pending();
							self.advance_committed(); // Skip the committed version
							self.last_yielded_key = Some(key.clone());

							match delta {
								Delta::Set {
									values,
									..
								} => {
									return Some(SingleVersionValues {
										key,
										values,
									});
								}
								Delta::Remove {
									..
								} => {
									// Skip removed entries
									continue;
								}
							}
						}
						// Committed item has a smaller
						// key, yield it
						cmp::Ordering::Greater => {
							let committed = self.next_committed.take().unwrap();
							self.advance_committed();

							// Check if this key was
							// already yielded
							if self.last_yielded_key
								.as_ref()
								.is_none_or(|k| k != &committed.key)
							{
								self.last_yielded_key = Some(committed.key.clone());
								return Some(committed);
							}
						}
					}
				}
				// Only pending items left
				(Some((_key, _delta)), None) => {
					let (key, delta) = self.next_pending.take().unwrap();
					self.advance_pending();
					self.last_yielded_key = Some(key.clone());

					match delta {
						Delta::Set {
							values,
							..
						} => {
							return Some(SingleVersionValues {
								key,
								values,
							});
						}
						Delta::Remove {
							..
						} => {
							// Skip removed entries
							continue;
						}
					}
				}
				// Only committed items left
				(None, Some(_)) => {
					// Check if this key was already yielded
					let committed = self.next_committed.as_ref().unwrap();
					if self.last_yielded_key.as_ref().is_none_or(|k| k != &committed.key) {
						let committed = self.next_committed.take().unwrap();
						self.advance_committed();
						self.last_yielded_key = Some(committed.key.clone());
						return Some(committed);
					} else {
						// Already yielded, skip
						self.advance_committed();
						continue;
					}
				}
				// No items left
				(None, None) => return None,
			}
		}
	}
}
