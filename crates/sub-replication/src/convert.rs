// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::cdc::{Cdc, CdcChange},
};
use reifydb_value::util::cowvec::CowVec;

use crate::generated::{CdcChangeProto, CdcEntry, DeleteChange, InsertChange, UpdateChange, cdc_change_proto::Change};

pub fn cdc_to_proto(cdc: &Cdc) -> CdcEntry {
	CdcEntry {
		version: cdc.version.0,
		timestamp: cdc.timestamp.to_nanos(),
		changes: cdc.changes.iter().map(cdc_change_to_proto).collect(),
	}
}

pub fn cdc_change_to_proto(cc: &CdcChange) -> CdcChangeProto {
	let change = match cc {
		CdcChange::Insert {
			key,
			post,
		} => Change::Insert(InsertChange {
			key: key.as_ref().to_vec(),
			post: post.as_slice().to_vec(),
		}),
		CdcChange::Update {
			key,
			pre,
			post,
		} => Change::Update(UpdateChange {
			key: key.as_ref().to_vec(),
			pre: pre.as_slice().to_vec(),
			post: post.as_slice().to_vec(),
		}),
		CdcChange::Delete {
			key,
			pre,
			visible,
		} => {
			let (pre_bytes, has_pre) = match pre {
				Some(p) => (p.as_slice().to_vec(), true),
				None => (Vec::new(), false),
			};
			Change::Delete(DeleteChange {
				key: key.as_ref().to_vec(),
				pre: pre_bytes,
				has_pre,
				visible: *visible,
			})
		}
	};
	CdcChangeProto {
		change: Some(change),
	}
}

pub fn proto_to_cdc_change(proto: &CdcChangeProto) -> Option<CdcChange> {
	match proto.change.as_ref()? {
		Change::Insert(ic) => Some(CdcChange::Insert {
			key: EncodedKey::new(ic.key.clone()),
			post: EncodedBytes(CowVec::new(ic.post.clone())),
		}),
		Change::Update(uc) => Some(CdcChange::Update {
			key: EncodedKey::new(uc.key.clone()),
			pre: EncodedBytes(CowVec::new(uc.pre.clone())),
			post: EncodedBytes(CowVec::new(uc.post.clone())),
		}),
		Change::Delete(dc) => {
			let pre = if dc.has_pre {
				Some(EncodedBytes(CowVec::new(dc.pre.clone())))
			} else {
				None
			};
			Some(CdcChange::Delete {
				key: EncodedKey::new(dc.key.clone()),
				pre,
				visible: dc.visible,
			})
		}
	}
}

pub fn cdc_change_to_delta(cc: &CdcChange) -> Delta {
	match cc {
		CdcChange::Insert {
			key,
			post,
		} => Delta::Set {
			key: key.clone(),
			bytes: post.clone(),
		},
		CdcChange::Update {
			key,
			post,
			..
		} => Delta::Set {
			key: key.clone(),
			bytes: post.clone(),
		},
		CdcChange::Delete {
			key,
			pre,
			visible,
		} => match (pre, visible) {
			(Some(pre), true) => Delta::remove_announced(key.clone(), pre.clone()),
			(Some(pre), false) => Delta::remove_unobserved(key.clone(), pre.clone()),
			(None, _) => Delta::remove_silent(key.clone()),
		},
	}
}

pub fn proto_entry_to_deltas(entry: &CdcEntry) -> (CommitVersion, Vec<Delta>) {
	let version = CommitVersion(entry.version);
	let deltas = entry
		.changes
		.iter()
		.filter_map(|cc| {
			let cdc_change = proto_to_cdc_change(cc)?;
			Some(cdc_change_to_delta(&cdc_change))
		})
		.collect();
	(version, deltas)
}

pub fn proto_entry_to_cdc_changes(entry: &CdcEntry) -> (CommitVersion, Vec<CdcChange>) {
	let version = CommitVersion(entry.version);
	let changes = entry.changes.iter().filter_map(proto_to_cdc_change).collect();
	(version, changes)
}

#[cfg(test)]
mod tests {
	use reifydb_core::delta::RemoveAnnounce;

	use super::*;

	#[test]
	fn test_insert_roundtrip() {
		let cc = CdcChange::Insert {
			key: EncodedKey::new(vec![1, 2, 3]),
			post: EncodedBytes(CowVec::new(vec![10, 20, 30])),
		};
		let proto = cdc_change_to_proto(&cc);
		let back = proto_to_cdc_change(&proto).unwrap();
		assert_eq!(cc, back);
	}

	#[test]
	fn test_update_roundtrip() {
		let cc = CdcChange::Update {
			key: EncodedKey::new(vec![4, 5]),
			pre: EncodedBytes(CowVec::new(vec![10])),
			post: EncodedBytes(CowVec::new(vec![20])),
		};
		let proto = cdc_change_to_proto(&cc);
		let back = proto_to_cdc_change(&proto).unwrap();
		assert_eq!(cc, back);
	}

	#[test]
	fn test_delete_with_pre_roundtrip() {
		let cc = CdcChange::Delete {
			key: EncodedKey::new(vec![6]),
			pre: Some(EncodedBytes(CowVec::new(vec![99]))),
			visible: true,
		};
		let proto = cdc_change_to_proto(&cc);
		let back = proto_to_cdc_change(&proto).unwrap();
		assert_eq!(cc, back);
	}

	#[test]
	fn test_delete_without_pre_roundtrip() {
		let cc = CdcChange::Delete {
			key: EncodedKey::new(vec![7]),
			pre: None,
			visible: true,
		};
		let proto = cdc_change_to_proto(&cc);
		let back = proto_to_cdc_change(&proto).unwrap();
		assert_eq!(cc, back);
	}

	#[test]
	fn test_delete_hidden_from_the_change_stream_stays_hidden_across_the_wire() {
		// Without the proto carrying visibility, a replica republishes a ttl removal the primary withheld.
		let cc = CdcChange::Delete {
			key: EncodedKey::new(vec![8]),
			pre: Some(EncodedBytes(CowVec::new(vec![42]))),
			visible: false,
		};
		let proto = cdc_change_to_proto(&cc);
		let back = proto_to_cdc_change(&proto).unwrap();
		assert_eq!(cc, back);

		match back {
			CdcChange::Delete {
				pre,
				visible,
				..
			} => {
				assert!(!visible, "a hidden delete must not become visible on the replica");
				assert!(
					pre.is_some(),
					"and it must keep its pre-image, otherwise replication cannot apply it"
				);
			}
			_ => panic!("expected a delete"),
		}
	}

	#[test]
	fn test_insert_to_delta() {
		let cc = CdcChange::Insert {
			key: EncodedKey::new(vec![1]),
			post: EncodedBytes(CowVec::new(vec![2])),
		};
		let delta = cdc_change_to_delta(&cc);
		match delta {
			Delta::Set {
				key,
				bytes,
			} => {
				assert_eq!(key.as_ref(), &[1]);
				assert_eq!(bytes.as_slice(), &[2]);
			}
			_ => panic!("Expected Delta::Set"),
		}
	}

	#[test]
	fn test_update_to_delta() {
		let cc = CdcChange::Update {
			key: EncodedKey::new(vec![1]),
			pre: EncodedBytes(CowVec::new(vec![2])),
			post: EncodedBytes(CowVec::new(vec![3])),
		};
		let delta = cdc_change_to_delta(&cc);
		match delta {
			Delta::Set {
				key,
				bytes,
			} => {
				assert_eq!(key.as_ref(), &[1]);
				assert_eq!(bytes.as_slice(), &[3]); // post value
			}
			_ => panic!("Expected Delta::Set"),
		}
	}

	#[test]
	fn test_delete_to_delta_with_pre() {
		// A replicated Delete must stay announced, otherwise it strands the replica's own subscribers.
		let cc = CdcChange::Delete {
			key: EncodedKey::new(vec![1]),
			pre: Some(EncodedBytes(CowVec::new(vec![2]))),
			visible: true,
		};
		match cdc_change_to_delta(&cc) {
			Delta::Remove {
				key,
				announce: RemoveAnnounce::Announced {
					pre,
				},
			} => {
				assert_eq!(key.as_ref(), &[1]);
				assert_eq!(pre.as_slice(), &[2], "the primary's pre-image must survive replication");
			}
			other => panic!("Expected an announced Delta::Remove, got {other:?}"),
		}
	}

	#[test]
	fn test_delete_without_pre_applies_silently() {
		// A Delete without a pre-image never came from a reifydb primary; inventing one would fabricate a row.
		let cc = CdcChange::Delete {
			key: EncodedKey::new(vec![1]),
			pre: None,
			visible: true,
		};
		match cdc_change_to_delta(&cc) {
			Delta::Remove {
				key,
				announce,
			} => {
				assert_eq!(key.as_ref(), &[1]);
				assert_eq!(announce, RemoveAnnounce::Silent);
			}
			other => panic!("Expected Delta::Remove, got {other:?}"),
		}
	}
}
