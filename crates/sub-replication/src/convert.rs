// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::cdc::{Cdc, SystemChange},
};
use reifydb_value::util::cowvec::CowVec;

use crate::generated::{
	CdcEntry, DeleteChange, InsertChange, SystemChangeProto, UpdateChange, system_change_proto::Change,
};

pub fn cdc_to_proto(cdc: &Cdc) -> CdcEntry {
	CdcEntry {
		version: cdc.version.0,
		timestamp: cdc.timestamp.to_nanos(),
		system_changes: cdc.system_changes.iter().map(system_change_to_proto).collect(),
	}
}

pub fn system_change_to_proto(sc: &SystemChange) -> SystemChangeProto {
	let change = match sc {
		SystemChange::Insert {
			key,
			post,
		} => Change::Insert(InsertChange {
			key: key.as_ref().to_vec(),
			post: post.as_slice().to_vec(),
		}),
		SystemChange::Update {
			key,
			pre,
			post,
		} => Change::Update(UpdateChange {
			key: key.as_ref().to_vec(),
			pre: pre.as_slice().to_vec(),
			post: post.as_slice().to_vec(),
		}),
		SystemChange::Delete {
			key,
			pre,
		} => {
			let (pre_bytes, has_pre) = match pre {
				Some(p) => (p.as_slice().to_vec(), true),
				None => (Vec::new(), false),
			};
			Change::Delete(DeleteChange {
				key: key.as_ref().to_vec(),
				pre: pre_bytes,
				has_pre,
			})
		}
	};
	SystemChangeProto {
		change: Some(change),
	}
}

pub fn proto_to_system_change(proto: &SystemChangeProto) -> Option<SystemChange> {
	match proto.change.as_ref()? {
		Change::Insert(ic) => Some(SystemChange::Insert {
			key: EncodedKey::new(ic.key.clone()),
			post: EncodedRow(CowVec::new(ic.post.clone())),
		}),
		Change::Update(uc) => Some(SystemChange::Update {
			key: EncodedKey::new(uc.key.clone()),
			pre: EncodedRow(CowVec::new(uc.pre.clone())),
			post: EncodedRow(CowVec::new(uc.post.clone())),
		}),
		Change::Delete(dc) => {
			let pre = if dc.has_pre {
				Some(EncodedRow(CowVec::new(dc.pre.clone())))
			} else {
				None
			};
			Some(SystemChange::Delete {
				key: EncodedKey::new(dc.key.clone()),
				pre,
			})
		}
	}
}

pub fn system_change_to_delta(sc: &SystemChange) -> Delta {
	match sc {
		SystemChange::Insert {
			key,
			post,
		} => Delta::Set {
			key: key.clone(),
			row: post.clone(),
		},
		SystemChange::Update {
			key,
			post,
			..
		} => Delta::Set {
			key: key.clone(),
			row: post.clone(),
		},
		SystemChange::Delete {
			key,
			pre,
		} => match pre {
			Some(pre) => Delta::remove_announced(key.clone(), pre.clone()),
			None => Delta::remove_silent(key.clone()),
		},
	}
}

pub fn proto_entry_to_deltas(entry: &CdcEntry) -> (CommitVersion, Vec<Delta>) {
	let version = CommitVersion(entry.version);
	let deltas = entry
		.system_changes
		.iter()
		.filter_map(|sc| {
			let system_change = proto_to_system_change(sc)?;
			Some(system_change_to_delta(&system_change))
		})
		.collect();
	(version, deltas)
}

pub fn proto_entry_to_system_changes(entry: &CdcEntry) -> (CommitVersion, Vec<SystemChange>) {
	let version = CommitVersion(entry.version);
	let changes = entry.system_changes.iter().filter_map(proto_to_system_change).collect();
	(version, changes)
}

#[cfg(test)]
mod tests {
	use reifydb_core::delta::RemoveAnnounce;

	use super::*;

	#[test]
	fn test_insert_roundtrip() {
		let sc = SystemChange::Insert {
			key: EncodedKey::new(vec![1, 2, 3]),
			post: EncodedRow(CowVec::new(vec![10, 20, 30])),
		};
		let proto = system_change_to_proto(&sc);
		let back = proto_to_system_change(&proto).unwrap();
		assert_eq!(sc, back);
	}

	#[test]
	fn test_update_roundtrip() {
		let sc = SystemChange::Update {
			key: EncodedKey::new(vec![4, 5]),
			pre: EncodedRow(CowVec::new(vec![10])),
			post: EncodedRow(CowVec::new(vec![20])),
		};
		let proto = system_change_to_proto(&sc);
		let back = proto_to_system_change(&proto).unwrap();
		assert_eq!(sc, back);
	}

	#[test]
	fn test_delete_with_pre_roundtrip() {
		let sc = SystemChange::Delete {
			key: EncodedKey::new(vec![6]),
			pre: Some(EncodedRow(CowVec::new(vec![99]))),
		};
		let proto = system_change_to_proto(&sc);
		let back = proto_to_system_change(&proto).unwrap();
		assert_eq!(sc, back);
	}

	#[test]
	fn test_delete_without_pre_roundtrip() {
		let sc = SystemChange::Delete {
			key: EncodedKey::new(vec![7]),
			pre: None,
		};
		let proto = system_change_to_proto(&sc);
		let back = proto_to_system_change(&proto).unwrap();
		assert_eq!(sc, back);
	}

	#[test]
	fn test_insert_to_delta() {
		let sc = SystemChange::Insert {
			key: EncodedKey::new(vec![1]),
			post: EncodedRow(CowVec::new(vec![2])),
		};
		let delta = system_change_to_delta(&sc);
		match delta {
			Delta::Set {
				key,
				row,
			} => {
				assert_eq!(key.as_ref(), &[1]);
				assert_eq!(row.as_slice(), &[2]);
			}
			_ => panic!("Expected Delta::Set"),
		}
	}

	#[test]
	fn test_update_to_delta() {
		let sc = SystemChange::Update {
			key: EncodedKey::new(vec![1]),
			pre: EncodedRow(CowVec::new(vec![2])),
			post: EncodedRow(CowVec::new(vec![3])),
		};
		let delta = system_change_to_delta(&sc);
		match delta {
			Delta::Set {
				key,
				row,
			} => {
				assert_eq!(key.as_ref(), &[1]);
				assert_eq!(row.as_slice(), &[3]); // post value
			}
			_ => panic!("Expected Delta::Set"),
		}
	}

	/// A replicated Delete always announces: it arrived as a CDC record, so re-silencing it on the
	/// replica would strand the replica's own subscribers. The pre-image rides along when the primary
	/// captured one.
	#[test]
	fn test_delete_to_delta_with_pre() {
		let sc = SystemChange::Delete {
			key: EncodedKey::new(vec![1]),
			pre: Some(EncodedRow(CowVec::new(vec![2]))),
		};
		match system_change_to_delta(&sc) {
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

	/// Our own producer always attaches a pre-image to an announced delete, so a Delete arriving with
	/// none did not come from a reifydb primary. There is nothing to announce with, and inventing an
	/// empty before-image would hand the replica's subscribers a fabricated row, so it applies silently.
	#[test]
	fn test_delete_without_pre_applies_silently() {
		let sc = SystemChange::Delete {
			key: EncodedKey::new(vec![1]),
			pre: None,
		};
		match system_change_to_delta(&sc) {
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
