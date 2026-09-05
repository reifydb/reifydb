// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::bytes::EncodedBytes;

use crate::key::any::AnyKey;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delta {
	Set {
		key: AnyKey,
		bytes: EncodedBytes,
	},

	Remove {
		key: AnyKey,
		announce: RemoveAnnounce,
	},
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoveVisibility {
	Silent,

	Announced,

	Unobserved,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

impl Delta {
	pub fn remove_silent(key: AnyKey) -> Self {
		Self::Remove {
			key,
			announce: RemoveAnnounce::Silent,
		}
	}

	pub fn remove_announced(key: AnyKey, pre: EncodedBytes) -> Self {
		Self::Remove {
			key,
			announce: RemoveAnnounce::Announced {
				pre,
			},
		}
	}

	pub fn remove_unobserved(key: AnyKey, pre: EncodedBytes) -> Self {
		Self::Remove {
			key,
			announce: RemoveAnnounce::Unobserved {
				pre,
			},
		}
	}

	pub fn key(&self) -> &AnyKey {
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
