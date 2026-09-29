// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use arrow_array::RecordBatch;
use indexmap::IndexMap;
use reifydb_value::{
	Result, reifydb_assertions,
	value::{diff_type::DiffType, row_number::RowNumber, system_columns::row_numbers},
};

use crate::{
	interface::change::{ChangeOrigin, Diff},
	value::batch::{append, take_rows},
};

pub fn coalesce_diffs(diffs: Vec<Diff>) -> Result<Vec<Diff>> {
	if has_cross_kind_overlap(&diffs)? {
		consolidate_diffs(diffs)
	} else {
		merge_adjacent(diffs)
	}
}

pub fn consolidate_diffs(diffs: Vec<Diff>) -> Result<Vec<Diff>> {
	let diffs: Vec<Diff> = diffs.into_iter().filter(|diff| diff.row_count() > 0).collect();
	if diffs.is_empty() {
		return Ok(diffs);
	}
	if !all_row_keyed(diffs.iter())? {
		return merge_adjacent(diffs);
	}
	consolidate_row_keyed(diffs)
}

fn has_cross_kind_overlap(diffs: &[Diff]) -> Result<bool> {
	let mut first_kind: Option<DiffType> = None;
	let mut mixed = false;
	for diff in diffs {
		if diff.row_count() == 0 {
			continue;
		}
		match first_kind {
			None => first_kind = Some(diff.kind()),
			Some(kind) if kind != diff.kind() => {
				mixed = true;
				break;
			}
			Some(_) => {}
		}
	}
	if !mixed {
		return Ok(false);
	}
	if !all_row_keyed(diffs.iter().filter(|diff| diff.row_count() > 0))? {
		return Ok(false);
	}
	let mut seen: HashMap<(Option<&ChangeOrigin>, RowNumber), u8> = HashMap::new();
	for diff in diffs {
		if diff.row_count() == 0 {
			continue;
		}
		let bit = kind_bit(diff.kind());
		let origin = diff.origin();
		for &row in key_rows(diff)? {
			let mask = seen.entry((origin, row)).or_insert(0);
			if *mask & !bit != 0 {
				return Ok(true);
			}
			*mask |= bit;
		}
	}
	Ok(false)
}

fn kind_bit(kind: DiffType) -> u8 {
	match kind {
		DiffType::Insert => 1,
		DiffType::Update => 2,
		DiffType::Remove => 4,
	}
}

fn key_rows(diff: &Diff) -> Result<&[RowNumber]> {
	match diff {
		Diff::Insert {
			post,
			..
		} => row_numbers(post),
		Diff::Update {
			post,
			..
		} => row_numbers(post),
		Diff::Remove {
			pre,
			..
		} => row_numbers(pre),
	}
}

fn merge_adjacent(diffs: Vec<Diff>) -> Result<Vec<Diff>> {
	let mut merged: Vec<Diff> = Vec::with_capacity(diffs.len());
	for diff in diffs {
		if diff.row_count() == 0 {
			continue;
		}
		let same_kind_and_origin = match (merged.last(), &diff) {
			(Some(last), next) => last.kind() == next.kind() && last.origin() == next.origin(),
			_ => false,
		};
		if same_kind_and_origin {
			let last = merged.last_mut().expect("non-empty by same_kind_and_origin branch");
			merge_into(last, diff)?;
		} else {
			merged.push(diff);
		}
	}
	Ok(merged)
}

fn merge_into(target: &mut Diff, source: Diff) -> Result<()> {
	match (target, source) {
		(
			Diff::Insert {
				post: t,
				..
			},
			Diff::Insert {
				post: s,
				..
			},
		) => append_in_place(t, &s),
		(
			Diff::Update {
				pre: tp,
				post: tpost,
				..
			},
			Diff::Update {
				pre: sp,
				post: spost,
				..
			},
		) => {
			append_in_place(tp, &sp)?;
			append_in_place(tpost, &spost)
		}
		(
			Diff::Remove {
				pre: t,
				..
			},
			Diff::Remove {
				pre: s,
				..
			},
		) => append_in_place(t, &s),
		_ => unreachable!("merge_into requires matching diff kinds"),
	}
}

enum RowState {
	Inserted {
		post: RecordBatch,
	},
	Updated {
		pre: RecordBatch,
		post: RecordBatch,
	},
	Removed {
		pre: RecordBatch,
	},
}

type StateKey = (Option<ChangeOrigin>, RowNumber);

#[derive(Default)]
struct OriginGroup {
	inserts: Option<RecordBatch>,
	update_pre: Option<RecordBatch>,
	update_post: Option<RecordBatch>,
	removes: Option<RecordBatch>,
}

fn consolidate_row_keyed(diffs: Vec<Diff>) -> Result<Vec<Diff>> {
	let mut states: IndexMap<StateKey, RowState> = IndexMap::new();
	for diff in diffs {
		match diff {
			Diff::Insert {
				post,
				origin,
			} => {
				for (i, &row) in row_numbers(&post)?.iter().enumerate() {
					apply_insert(&mut states, (origin.clone(), row), take_rows(&post, &[i])?);
				}
			}
			Diff::Update {
				pre,
				post,
				origin,
			} => {
				reifydb_assertions! {
					assert!(
						row_numbers(&pre)? == row_numbers(&post)?,
						"diff consolidation keys an update row by its post row number and pairs \
						 the pre row positionally; a pre row carrying a different row number \
						 would retract a different row than the one this update claims to \
						 replace (pre={:?}, post={:?})",
						row_numbers(&pre)?,
						row_numbers(&post)?
					);
				}
				for (i, &row) in row_numbers(&post)?.iter().enumerate() {
					apply_update(
						&mut states,
						(origin.clone(), row),
						take_rows(&pre, &[i])?,
						take_rows(&post, &[i])?,
					);
				}
			}
			Diff::Remove {
				pre,
				origin,
			} => {
				for (i, &row) in row_numbers(&pre)?.iter().enumerate() {
					apply_remove(&mut states, (origin.clone(), row), take_rows(&pre, &[i])?);
				}
			}
		}
	}

	let mut groups: IndexMap<Option<ChangeOrigin>, OriginGroup> = IndexMap::new();
	for ((origin, _), state) in states {
		let group = groups.entry(origin).or_default();
		match state {
			RowState::Inserted {
				post,
			} => append_into(&mut group.inserts, post)?,
			RowState::Updated {
				pre,
				post,
			} => {
				append_into(&mut group.update_pre, pre)?;
				append_into(&mut group.update_post, post)?;
			}
			RowState::Removed {
				pre,
			} => append_into(&mut group.removes, pre)?,
		}
	}

	let mut result: Vec<Diff> = Vec::with_capacity(groups.len() * 3);
	for (origin, group) in groups {
		if let Some(post) = group.inserts {
			result.push(Diff::Insert {
				post,
				origin: origin.clone(),
			});
		}
		if let (Some(pre), Some(post)) = (group.update_pre, group.update_post) {
			result.push(Diff::Update {
				pre,
				post,
				origin: origin.clone(),
			});
		}
		if let Some(pre) = group.removes {
			result.push(Diff::Remove {
				pre,
				origin,
			});
		}
	}
	Ok(result)
}

fn all_row_keyed<'a>(mut diffs: impl Iterator<Item = &'a Diff>) -> Result<bool> {
	diffs.try_fold(true, |keyed, diff| Ok(keyed && diff_is_row_keyed(diff)?))
}

fn diff_is_row_keyed(diff: &Diff) -> Result<bool> {
	match diff {
		Diff::Insert {
			post,
			..
		} => batch_row_keyed(post),
		Diff::Update {
			pre,
			post,
			..
		} => Ok(batch_row_keyed(pre)? && batch_row_keyed(post)? && pre.num_rows() == post.num_rows()),
		Diff::Remove {
			pre,
			..
		} => batch_row_keyed(pre),
	}
}

fn batch_row_keyed(batch: &RecordBatch) -> Result<bool> {
	Ok(batch.num_rows() > 0 && row_numbers(batch)?.len() == batch.num_rows())
}

fn apply_insert(states: &mut IndexMap<StateKey, RowState>, key: StateKey, post: RecordBatch) {
	let next = match states.get(&key) {
		Some(RowState::Updated {
			pre,
			..
		})
		| Some(RowState::Removed {
			pre,
		}) => RowState::Updated {
			pre: pre.clone(),
			post,
		},
		_ => RowState::Inserted {
			post,
		},
	};
	states.insert(key, next);
}

fn apply_update(states: &mut IndexMap<StateKey, RowState>, key: StateKey, pre: RecordBatch, post: RecordBatch) {
	let next = match states.get(&key) {
		None => RowState::Updated {
			pre,
			post,
		},
		Some(RowState::Inserted {
			..
		}) => RowState::Inserted {
			post,
		},
		Some(RowState::Updated {
			pre: pre0,
			..
		})
		| Some(RowState::Removed {
			pre: pre0,
		}) => RowState::Updated {
			pre: pre0.clone(),
			post,
		},
	};
	states.insert(key, next);
}

fn apply_remove(states: &mut IndexMap<StateKey, RowState>, key: StateKey, pre: RecordBatch) {
	match states.get(&key) {
		None => {
			states.insert(
				key,
				RowState::Removed {
					pre,
				},
			);
		}
		Some(RowState::Inserted {
			..
		}) => {
			states.shift_remove(&key);
		}
		Some(RowState::Updated {
			pre: pre0,
			..
		}) => {
			let pre0 = pre0.clone();
			states.insert(
				key,
				RowState::Removed {
					pre: pre0,
				},
			);
		}
		Some(RowState::Removed {
			..
		}) => {}
	}
}

fn append_into(target: &mut Option<RecordBatch>, source: RecordBatch) -> Result<()> {
	match target {
		Some(existing) => append_in_place(existing, &source),
		None => {
			*target = Some(source);
			Ok(())
		}
	}
}

fn append_in_place(target: &mut RecordBatch, source: &RecordBatch) -> Result<()> {
	*target = append(target, source)?;
	Ok(())
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow_array::UInt64Array;
	use reifydb_value::value::{
		Value,
		system_columns::{SystemColumn, column_view, with_system_column},
	};

	use super::*;
	use crate::{
		interface::catalog::{id::TableId, object::ObjectId},
		value::{
			batch::{batch, empty_batch},
			column::factory::int4,
		},
	};

	fn cols(rows: &[(u64, i32)]) -> RecordBatch {
		let rns: Vec<u64> = rows.iter().map(|&(rn, _)| rn).collect();
		let vals: Vec<i32> = rows.iter().map(|&(_, v)| v).collect();
		with_system_column(
			batch(vec![int4("v", vals)]).unwrap(),
			SystemColumn::RowNumbers,
			Arc::new(UInt64Array::from(rns)),
		)
		.unwrap()
	}

	fn insert(rows: &[(u64, i32)]) -> Diff {
		Diff::insert(cols(rows))
	}

	fn update(pre: &[(u64, i32)], post: &[(u64, i32)]) -> Diff {
		Diff::update(cols(pre), cols(post))
	}

	fn remove(rows: &[(u64, i32)]) -> Diff {
		Diff::remove(cols(rows))
	}

	fn rows_of(columns: &RecordBatch) -> Vec<(u64, i32)> {
		(0..columns.num_rows())
			.map(|i| {
				let rn = row_numbers(columns).unwrap()[i].value();
				let val = match column_view(columns, "v").unwrap().unwrap().get_value(i) {
					Value::Int4(v) => v,
					other => panic!("expected Int4, got {:?}", other),
				};
				(rn, val)
			})
			.collect()
	}

	#[test]
	fn insert_then_remove_annihilates() {
		// The whole point of consolidation: a row inserted and removed within one batch never
		// existed for downstream consumers, so neither diff may survive.
		let out = consolidate_diffs(vec![insert(&[(1, 10)]), remove(&[(1, 10)])]).unwrap();
		assert!(out.is_empty(), "insert+remove of the same row must annihilate, got {:?}", out.len());
	}

	#[test]
	fn remove_then_insert_fuses_to_update() {
		// A remove followed by a re-insert of the same row is a replacement; downstream must see
		// one update whose pre is the removed image and whose post is the re-inserted image.
		let out = consolidate_diffs(vec![remove(&[(1, 10)]), insert(&[(1, 20)])]).unwrap();
		assert_eq!(out.len(), 1);
		match &out[0] {
			Diff::Update {
				pre,
				post,
				..
			} => {
				assert_eq!(rows_of(pre), vec![(1, 10)]);
				assert_eq!(rows_of(post), vec![(1, 20)]);
			}
			other => panic!("expected update, got {:?}", other.kind()),
		}
	}

	#[test]
	fn insert_then_update_collapses_to_insert() {
		// A row born and immediately rewritten in the same batch is still a birth; the pre image
		// was never visible, so the surviving diff must be an insert of the latest post.
		let out = consolidate_diffs(vec![insert(&[(1, 10)]), update(&[(1, 10)], &[(1, 20)])]).unwrap();
		assert_eq!(out.len(), 1);
		match &out[0] {
			Diff::Insert {
				post,
				..
			} => assert_eq!(rows_of(post), vec![(1, 20)]),
			other => panic!("expected insert, got {:?}", other.kind()),
		}
	}

	#[test]
	fn update_then_update_keeps_first_pre_and_last_post() {
		// Chained updates must collapse to one transition from the oldest visible image to the
		// newest, or downstream would double-apply the intermediate image.
		let out = consolidate_diffs(vec![update(&[(1, 10)], &[(1, 20)]), update(&[(1, 20)], &[(1, 30)])])
			.unwrap();
		assert_eq!(out.len(), 1);
		match &out[0] {
			Diff::Update {
				pre,
				post,
				..
			} => {
				assert_eq!(rows_of(pre), vec![(1, 10)]);
				assert_eq!(rows_of(post), vec![(1, 30)]);
			}
			other => panic!("expected update, got {:?}", other.kind()),
		}
	}

	#[test]
	fn update_then_remove_keeps_first_pre() {
		// When an updated row is removed in the same batch, the retraction downstream must undo
		// the image that was actually visible before the batch, not the intermediate post.
		let out = consolidate_diffs(vec![update(&[(1, 10)], &[(1, 20)]), remove(&[(1, 20)])]).unwrap();
		assert_eq!(out.len(), 1);
		match &out[0] {
			Diff::Remove {
				pre,
				..
			} => assert_eq!(rows_of(pre), vec![(1, 10)]),
			other => panic!("expected remove, got {:?}", other.kind()),
		}
	}

	#[test]
	fn later_insert_supersedes_earlier() {
		// Two inserts of the same row number describe one row; the later image wins and the row
		// must not be duplicated in the surviving batch.
		let out = consolidate_diffs(vec![insert(&[(1, 10)]), insert(&[(1, 20)])]).unwrap();
		assert_eq!(out.len(), 1);
		match &out[0] {
			Diff::Insert {
				post,
				..
			} => assert_eq!(rows_of(post), vec![(1, 20)]),
			other => panic!("expected insert, got {:?}", other.kind()),
		}
	}

	#[test]
	fn same_row_number_different_origin_does_not_interact() {
		// Row numbers are only unique per origin; rn 1 of table A and rn 1 of table B are
		// unrelated rows and fusing them would corrupt both objects' change streams.
		let origin_a = Some(ChangeOrigin::Object(ObjectId::Table(TableId(1))));
		let origin_b = Some(ChangeOrigin::Object(ObjectId::Table(TableId(2))));
		let mut removed = remove(&[(1, 10)]);
		removed.set_origin(origin_a.clone());
		let mut inserted = insert(&[(1, 20)]);
		inserted.set_origin(origin_b.clone());

		let out = consolidate_diffs(vec![removed, inserted]).unwrap();

		assert_eq!(out.len(), 2, "cross-origin diffs must not fuse into an update");
		assert_eq!(out[0].kind(), DiffType::Remove);
		assert_eq!(out[0].origin(), origin_a.as_ref());
		assert_eq!(out[1].kind(), DiffType::Insert);
		assert_eq!(out[1].origin(), origin_b.as_ref());
	}

	#[test]
	fn multi_row_batch_splits_and_keeps_survivor_order() {
		// Cancellation is per row: removing one row out of a three-row insert batch must rebuild
		// the batch with the two survivors, in their original arrival order (ordinal assignment
		// downstream depends on arrival order, not row-number order).
		let out = consolidate_diffs(vec![insert(&[(3, 30), (1, 10), (2, 20)]), remove(&[(1, 10)])]).unwrap();
		assert_eq!(out.len(), 1);
		match &out[0] {
			Diff::Insert {
				post,
				..
			} => assert_eq!(rows_of(post), vec![(3, 30), (2, 20)]),
			other => panic!("expected insert, got {:?}", other.kind()),
		}
	}

	#[test]
	fn update_batch_split_keeps_pre_post_alignment() {
		// Splitting a multi-row update batch must keep pre and post paired by index; a survivor
		// whose pre image slipped to another row's position would retract the wrong row.
		let out = consolidate_diffs(vec![update(&[(1, 10), (2, 20)], &[(1, 11), (2, 21)]), remove(&[(1, 11)])])
			.unwrap();
		assert_eq!(out.len(), 2);
		match &out[0] {
			Diff::Update {
				pre,
				post,
				..
			} => {
				assert_eq!(rows_of(pre), vec![(2, 20)]);
				assert_eq!(rows_of(post), vec![(2, 21)]);
			}
			other => panic!("expected update, got {:?}", other.kind()),
		}
		match &out[1] {
			Diff::Remove {
				pre,
				..
			} => assert_eq!(rows_of(pre), vec![(1, 10)]),
			other => panic!("expected remove, got {:?}", other.kind()),
		}
	}

	#[test]
	fn zero_row_diff_does_not_block_consolidation() {
		// The accumulator used to fall back to append-only merging whenever any diff was empty,
		// letting an unrelated empty diff disable annihilation for the whole batch.
		let out = consolidate_diffs(vec![insert(&[(1, 10)]), Diff::insert(empty_batch()), remove(&[(1, 10)])])
			.unwrap();
		assert!(out.is_empty(), "an empty diff must not disable row consolidation");
	}

	#[test]
	fn coalesce_drops_zero_row_diffs_on_the_fast_path() {
		// The fast path must still drop empty diffs, otherwise every hop re-ships zero-row
		// batches through the whole DAG.
		let out = coalesce_diffs(vec![
			insert(&[(1, 10)]),
			Diff::update(empty_batch(), empty_batch()),
			remove(&[(2, 20)]),
		])
		.unwrap();
		assert_eq!(out.len(), 2);
		assert_eq!(out[0].kind(), DiffType::Insert);
		assert_eq!(out[1].kind(), DiffType::Remove);
	}

	#[test]
	fn fast_path_without_overlap_preserves_diff_order_and_shape() {
		// Mixed kinds without row overlap must take the cheap path: original diff order and
		// batch boundaries stay untouched instead of being regrouped by kind.
		let out = coalesce_diffs(vec![remove(&[(2, 20)]), insert(&[(1, 10)])]).unwrap();
		assert_eq!(out.len(), 2);
		assert_eq!(out[0].kind(), DiffType::Remove);
		assert_eq!(out[1].kind(), DiffType::Insert);
		assert_eq!(rows_of(out[0].pre().unwrap()), vec![(2, 20)]);
		assert_eq!(rows_of(out[1].post().unwrap()), vec![(1, 10)]);
	}

	#[test]
	fn insert_only_batches_take_the_fast_path_untouched() {
		// Insert-only is the dominant hot-path workload: it must keep today's cheap adjacent
		// append (one merged batch, rows in arrival order) and never pay row-level
		// consolidation, even when a row number repeats within the same kind.
		let out = coalesce_diffs(vec![insert(&[(1, 10)]), insert(&[(1, 20)])]).unwrap();
		assert_eq!(out.len(), 1);
		match &out[0] {
			Diff::Insert {
				post,
				..
			} => assert_eq!(rows_of(post), vec![(1, 10), (1, 20)]),
			other => panic!("expected insert, got {:?}", other.kind()),
		}
	}

	#[test]
	fn cross_kind_overlap_triggers_consolidation() {
		// Detection has to catch a remove hitting a row inserted earlier in the same batch and
		// route the whole batch through the row algebra.
		let out = coalesce_diffs(vec![insert(&[(1, 10), (2, 20)]), remove(&[(1, 10)])]).unwrap();
		assert_eq!(out.len(), 1);
		match &out[0] {
			Diff::Insert {
				post,
				..
			} => assert_eq!(rows_of(post), vec![(2, 20)]),
			other => panic!("expected insert, got {:?}", other.kind()),
		}
	}

	#[test]
	fn insert_update_overlap_triggers_consolidation() {
		// Updates must participate in overlap detection with their own kind bit; mapping them
		// onto the insert bit would make insert+update of one row look like a same-kind repeat
		// and skip consolidation.
		let out = coalesce_diffs(vec![insert(&[(1, 10)]), update(&[(1, 10)], &[(1, 20)])]).unwrap();
		assert_eq!(out.len(), 1);
		match &out[0] {
			Diff::Insert {
				post,
				..
			} => assert_eq!(rows_of(post), vec![(1, 20)]),
			other => panic!("expected insert, got {:?}", other.kind()),
		}
	}
}
