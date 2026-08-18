// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::cmp;

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Delta {
	Set {
		key: EncodedKey,
		bytes: EncodedBytes,
	},

	Remove {
		key: EncodedKey,
		announce: RemoveAnnounce,
	},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RemoveVisibility {
	Silent,

	Announced,

	Unobserved,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RemoveAnnounce {
	Silent,

	Announced {
		pre: EncodedBytes,
	},

	Unobserved {
		pre: EncodedBytes,
	},
}

impl RemoveAnnounce {
	pub fn announces(&self) -> bool {
		!matches!(self, Self::Silent)
	}

	pub fn visible(&self) -> bool {
		matches!(self, Self::Announced { .. })
	}

	pub fn pre(&self) -> Option<&EncodedBytes> {
		match self {
			Self::Silent => None,
			Self::Announced {
				pre,
			}
			| Self::Unobserved {
				pre,
			} => Some(pre),
		}
	}
}

impl PartialOrd for Delta {
	fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Delta {
	fn cmp(&self, other: &Self) -> cmp::Ordering {
		self.key().cmp(other.key())
	}
}

impl Delta {
	pub fn remove_silent(key: EncodedKey) -> Self {
		Self::Remove {
			key,
			announce: RemoveAnnounce::Silent,
		}
	}

	pub fn remove_announced(key: EncodedKey, pre: EncodedBytes) -> Self {
		Self::Remove {
			key,
			announce: RemoveAnnounce::Announced {
				pre,
			},
		}
	}

	pub fn remove_unobserved(key: EncodedKey, pre: EncodedBytes) -> Self {
		Self::Remove {
			key,
			announce: RemoveAnnounce::Unobserved {
				pre,
			},
		}
	}

	pub fn key(&self) -> &EncodedKey {
		match self {
			Self::Set {
				key,
				..
			} => key,
			Self::Remove {
				key,
				..
			} => key,
		}
	}

	pub fn bytes(&self) -> Option<&EncodedBytes> {
		match self {
			Self::Set {
				bytes,
				..
			} => Some(bytes),
			Self::Remove {
				..
			} => None,
		}
	}

	pub fn announces(&self) -> bool {
		match self {
			Self::Set {
				..
			} => true,
			Self::Remove {
				announce,
				..
			} => announce.announces(),
		}
	}
}
