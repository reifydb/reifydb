// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::{
	catalog::{object::ObjectId, storage::StorageId},
	store::EntryKind,
};

const CURRENT_SUFFIX: &str = "__current";

pub(super) fn entry_id_to_name(kind: EntryKind) -> String {
	match kind {
		EntryKind::Multi => "multi".to_string(),
		EntryKind::Source(id) => format!("source_{}_{}", id.type_tag(), id.as_u64()),
		EntryKind::PartitionedSource(id) => format!("partsource_{}_{}", id.type_tag(), id.as_u64()),
	}
}

pub(super) fn name_to_entry_id(name: &str) -> Option<EntryKind> {
	if name == "multi" {
		return Some(EntryKind::Multi);
	}
	if let Some(rest) = name.strip_prefix("source_") {
		let (tag, id) = split_tag_and_id(rest)?;
		return StorageId::from_type_tag(tag, id).map(EntryKind::Source);
	}
	if let Some(rest) = name.strip_prefix("partsource_") {
		let (tag, id) = split_tag_and_id(rest)?;
		return ObjectId::from_type_tag(tag, id).map(EntryKind::PartitionedSource);
	}
	None
}

fn split_tag_and_id(rest: &str) -> Option<(u8, u64)> {
	let (tag, id) = rest.split_once('_')?;
	Some((tag.parse().ok()?, id.parse().ok()?))
}

pub(super) fn current_table_name(kind: EntryKind) -> String {
	format!("{}{}", entry_id_to_name(kind), CURRENT_SUFFIX)
}

pub(super) fn current_table_name_to_entry(name: &str) -> Option<EntryKind> {
	name.strip_suffix(CURRENT_SUFFIX).and_then(name_to_entry_id)
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::catalog::id::{RingBufferId, TableId};

	use super::*;

	#[test]
	fn every_constructible_entry_kind_survives_the_name_round_trip() {
		// The persistent tier enumerates its tables by name, so a kind whose name cannot be parsed back becomes
		// invisible to every maintenance pass that walks them.
		let mut covered = 0;
		for tag in 0..=u8::MAX {
			for id in [0u64, 1, 16_391, u64::MAX] {
				if let Some(storage) = StorageId::from_type_tag(tag, id) {
					let kind = EntryKind::Source(storage);
					assert_eq!(
						current_table_name_to_entry(&current_table_name(kind)),
						Some(kind),
						"source kind at type tag {tag} did not survive the round trip"
					);
					covered += 1;
				}
				if let Some(object) = ObjectId::from_type_tag(tag, id) {
					let kind = EntryKind::PartitionedSource(object);
					assert_eq!(
						current_table_name_to_entry(&current_table_name(kind)),
						Some(kind),
						"partitioned kind at type tag {tag} did not survive the round trip"
					);
					covered += 1;
				}
			}
		}

		assert_eq!(current_table_name_to_entry(&current_table_name(EntryKind::Multi)), Some(EntryKind::Multi));
		assert!(covered > 0, "the sweep constructed no kinds at all, so it proved nothing about any of them");
	}

	#[test]
	fn two_storage_variants_at_the_same_id_get_distinct_table_names() {
		// Rendering a storage id without its variant collapses distinct objects onto one table, so their rows
		// and tombstones would share a keyspace.
		let table = EntryKind::Source(StorageId::table(TableId(5)));
		let ringbuffer = EntryKind::Source(StorageId::ringbuffer(RingBufferId(5)));

		assert_ne!(
			current_table_name(table),
			current_table_name(ringbuffer),
			"a table and a ring buffer sharing id 5 must not share a persistent table"
		);
	}

	#[test]
	fn a_name_the_formatter_never_produces_is_rejected_rather_than_guessed() {
		// A foreign table in the same database must not resolve to a real kind, or maintenance would attribute
		// its rows to an object that does not own them.
		assert_eq!(current_table_name_to_entry("sqlite_sequence"), None);
		assert_eq!(current_table_name_to_entry("source_1_5"), None);
		assert_eq!(current_table_name_to_entry("source_1__current"), None);
		assert_eq!(current_table_name_to_entry("source_0_5__current"), None);
		assert_eq!(current_table_name_to_entry("source_notatag_5__current"), None);
	}
}
