// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::bytes::EncodedBytes,
};
use reifydb_value::{Result, util::cowvec::CowVec};

use crate::{
	common::CommitVersion,
	delta::Delta,
	interface::catalog::storage::StorageId,
	key::{
		EncodableKeyRange,
		kind::KeyKind,
		row::{
			PartitionedRowKey, PartitionedRowKeyRange, RowKey, RowKeyRange, StoragePartitionedRowKey,
			StorageRowKey,
		},
		series::{
			PartitionedSeriesRowKey, PartitionedSeriesRowKeyRange, SeriesRowKey, SeriesRowKeyRange,
			StoragePartitionedSeriesKey, StorageSeriesKey,
		},
		typed::{TypedKey, key::Key},
	},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tier {
	Buffer,
	Persistent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryLayout {
	Row,
	Series,
}

impl EntryLayout {
	pub fn type_tag(&self) -> u8 {
		match self {
			Self::Row => 1,
			Self::Series => 2,
		}
	}

	pub fn from_type_tag(tag: u8) -> Option<Self> {
		match tag {
			1 => Some(Self::Row),
			2 => Some(Self::Series),
			_ => None,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryKind {
	Multi,

	Source(StorageId, EntryLayout),

	PartitionedSource(StorageId, EntryLayout),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheTiers {
	Neither,
	Point,
	Range,
	Both,
}

impl CacheTiers {
	pub fn caches_points(&self) -> bool {
		matches!(self, Self::Point | Self::Both)
	}

	pub fn caches_ranges(&self) -> bool {
		matches!(self, Self::Range | Self::Both)
	}
}

impl EntryKind {
	pub fn cache_tiers(&self) -> CacheTiers {
		match self {
			Self::Source(_, _) => CacheTiers::Both,
			Self::Multi | Self::PartitionedSource(_, _) => CacheTiers::Point,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageKey {
	Table(StorageRowKey),
	RingBuffer(StorageRowKey),
	Queue(StorageRowKey),
	View(StorageRowKey),

	PartitionedTable(StoragePartitionedRowKey),
	PartitionedRingBuffer(StoragePartitionedRowKey),
	PartitionedQueue(StoragePartitionedRowKey),
	PartitionedView(StoragePartitionedRowKey),

	Series(StorageSeriesKey),
	SeriesView(StorageSeriesKey),

	PartitionedSeries(StoragePartitionedSeriesKey),
	PartitionedSeriesView(StoragePartitionedSeriesKey),
}

fn row_storage_key(storage: StorageId, row: StorageRowKey) -> Option<StorageKey> {
	match storage {
		StorageId::Table(_) => Some(StorageKey::Table(row)),
		StorageId::RingBuffer(_) => Some(StorageKey::RingBuffer(row)),
		StorageId::Queue(_) => Some(StorageKey::Queue(row)),
		StorageId::View(_) => Some(StorageKey::View(row)),
		StorageId::Series(_) => None,
	}
}

fn partitioned_row_storage_key(storage: StorageId, row: StoragePartitionedRowKey) -> Option<StorageKey> {
	match storage {
		StorageId::Table(_) => Some(StorageKey::PartitionedTable(row)),
		StorageId::RingBuffer(_) => Some(StorageKey::PartitionedRingBuffer(row)),
		StorageId::Queue(_) => Some(StorageKey::PartitionedQueue(row)),
		StorageId::View(_) => Some(StorageKey::PartitionedView(row)),
		StorageId::Series(_) => None,
	}
}

fn series_storage_key(storage: StorageId, series: StorageSeriesKey) -> Option<StorageKey> {
	match storage {
		StorageId::Series(_) => Some(StorageKey::Series(series)),
		StorageId::View(_) => Some(StorageKey::SeriesView(series)),
		StorageId::Table(_) | StorageId::RingBuffer(_) | StorageId::Queue(_) => None,
	}
}

fn partitioned_series_storage_key(storage: StorageId, series: StoragePartitionedSeriesKey) -> Option<StorageKey> {
	match storage {
		StorageId::Series(_) => Some(StorageKey::PartitionedSeries(series)),
		StorageId::View(_) => Some(StorageKey::PartitionedSeriesView(series)),
		StorageId::Table(_) | StorageId::RingBuffer(_) | StorageId::Queue(_) => None,
	}
}

fn source_entry(storage: StorageId, layout: EntryLayout, key: Option<StorageKey>) -> (EntryKind, Option<StorageKey>) {
	match key {
		Some(key) => (EntryKind::Source(storage, layout), Some(key)),
		None => (EntryKind::Multi, None),
	}
}

fn partitioned_source_entry(
	storage: StorageId,
	layout: EntryLayout,
	key: Option<StorageKey>,
) -> (EntryKind, Option<StorageKey>) {
	match key {
		Some(key) => (EntryKind::PartitionedSource(storage, layout), Some(key)),
		None => (EntryKind::Multi, None),
	}
}

pub fn storage_key(key: &EncodedKey) -> (EntryKind, Option<StorageKey>) {
	match KeyKind::of(key) {
		Some(KeyKind::Row) => match RowKey::decode(key) {
			Some(row_key) => source_entry(
				row_key.storage,
				EntryLayout::Row,
				row_storage_key(row_key.storage, StorageRowKey::new(row_key.row)),
			),
			None => (EntryKind::Multi, None),
		},
		Some(KeyKind::SeriesRow) => match SeriesRowKey::decode(key) {
			Some(series_key) => source_entry(
				series_key.storage,
				EntryLayout::Series,
				series_storage_key(series_key.storage, StorageSeriesKey::from(series_key)),
			),
			None => (EntryKind::Multi, None),
		},
		Some(KeyKind::PartitionedRow) => match PartitionedRowKey::decode(key) {
			Some(partitioned_key) => partitioned_source_entry(
				partitioned_key.storage,
				EntryLayout::Row,
				partitioned_row_storage_key(
					partitioned_key.storage,
					StoragePartitionedRowKey::new(partitioned_key.partition, partitioned_key.row),
				),
			),
			None => (EntryKind::Multi, None),
		},
		Some(KeyKind::PartitionedSeriesRow) => match PartitionedSeriesRowKey::decode(key) {
			Some(partitioned_key) => partitioned_source_entry(
				partitioned_key.storage,
				EntryLayout::Series,
				partitioned_series_storage_key(
					partitioned_key.storage,
					StoragePartitionedSeriesKey::from(partitioned_key),
				),
			),
			None => (EntryKind::Multi, None),
		},
		_ => (EntryKind::Multi, None),
	}
}

pub fn classify_key(key: &EncodedKey) -> EntryKind {
	storage_key(key).0
}

pub fn classify_range(range: &EncodedKeyRange) -> Option<EntryKind> {
	if let (Some(start), Some(_end)) = RowKeyRange::decode(range) {
		return row_storage_key(start.storage, <StorageRowKey as TypedKey>::low())
			.map(|_| EntryKind::Source(start.storage, EntryLayout::Row));
	}

	if let (Some(start), Some(_end)) = SeriesRowKeyRange::decode(range) {
		return series_storage_key(start, <StorageSeriesKey as TypedKey>::low())
			.map(|_| EntryKind::Source(start, EntryLayout::Series));
	}

	if let (Some(start), Some(_end)) = PartitionedRowKeyRange::decode(range) {
		return partitioned_row_storage_key(start.storage, <StoragePartitionedRowKey as TypedKey>::low())
			.map(|_| EntryKind::PartitionedSource(start.storage, EntryLayout::Row));
	}

	if let (Some(start), Some(_end)) = PartitionedSeriesRowKeyRange::decode(range) {
		return partitioned_series_storage_key(start, <StoragePartitionedSeriesKey as TypedKey>::low())
			.map(|_| EntryKind::PartitionedSource(start, EntryLayout::Series));
	}

	None
}

#[derive(Debug, Clone)]
pub struct MultiVersionRow<K = EncodedKey> {
	pub key: K,
	pub bytes: EncodedBytes,
	pub version: CommitVersion,
}

#[derive(Debug, Clone)]
pub struct SingleVersionRow {
	pub key: EncodedKey,
	pub bytes: EncodedBytes,
}

#[derive(Debug, Clone)]
pub struct MultiVersionBatch<K = EncodedKey> {
	pub items: Vec<MultiVersionRow<K>>,

	pub has_more: bool,
}

impl<K> MultiVersionBatch<K> {
	pub fn empty() -> Self {
		Self {
			items: Vec::new(),
			has_more: false,
		}
	}

	pub fn is_empty(&self) -> bool {
		self.items.is_empty()
	}
}

pub trait MultiVersionCommit: Send + Sync {
	fn commit(&self, deltas: CowVec<Delta>, version: CommitVersion) -> Result<()>;
}

pub trait MultiVersionGet: Send + Sync {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionRow>>;
}

pub trait MultiVersionContains: Send + Sync {
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool>;
}

pub trait MultiVersionGetPrevious: Send + Sync {
	fn get_previous_version(
		&self,
		key: &EncodedKey,
		before_version: CommitVersion,
	) -> Result<Option<MultiVersionRow>>;
}

pub trait MultiVersionStore:
	Send + Sync + Clone + MultiVersionCommit + MultiVersionGet + MultiVersionGetPrevious + MultiVersionContains + 'static
{
}

#[derive(Debug, Clone)]
pub struct SingleVersionBatch {
	pub items: Vec<SingleVersionRow>,

	pub has_more: bool,
}

impl SingleVersionBatch {
	pub fn empty() -> Self {
		Self {
			items: Vec::new(),
			has_more: false,
		}
	}

	pub fn is_empty(&self) -> bool {
		self.items.is_empty()
	}
}

pub trait SingleVersionCommit: Send + Sync {
	fn commit(&mut self, deltas: CowVec<Delta>) -> Result<()>;
}

pub trait SingleVersionGet: Send + Sync {
	fn get(&self, key: &EncodedKey) -> Result<Option<SingleVersionRow>>;
}

pub trait SingleVersionContains: Send + Sync {
	fn contains(&self, key: &EncodedKey) -> Result<bool>;
}

pub trait SingleVersionSet: SingleVersionCommit {
	fn set(&mut self, key: &EncodedKey, bytes: EncodedBytes) -> Result<()> {
		Self::commit(
			self,
			CowVec::new(vec![Delta::Set {
				key: key.clone(),
				bytes: bytes.clone(),
			}]),
		)
	}
}

pub trait SingleVersionRemove: SingleVersionCommit {
	fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		Self::commit(self, CowVec::new(vec![Delta::remove_silent(key.clone())]))
	}
}

pub trait SingleVersionRange: Send + Sync {
	fn range_batch(&self, range: EncodedKeyRange, batch_size: u64) -> Result<SingleVersionBatch>;

	fn range(&self, range: EncodedKeyRange) -> Result<SingleVersionBatch> {
		self.range_batch(range, 1024)
	}

	fn prefix(&self, prefix: &EncodedKey) -> Result<SingleVersionBatch> {
		self.range(EncodedKeyRange::prefix(prefix))
	}
}

pub trait SingleVersionRangeRev: Send + Sync {
	fn range_rev_batch(&self, range: EncodedKeyRange, batch_size: u64) -> Result<SingleVersionBatch>;

	fn range_rev(&self, range: EncodedKeyRange) -> Result<SingleVersionBatch> {
		self.range_rev_batch(range, 1024)
	}

	fn prefix_rev(&self, prefix: &EncodedKey) -> Result<SingleVersionBatch> {
		self.range_rev(EncodedKeyRange::prefix(prefix))
	}
}

pub trait SingleVersionStore:
	Send
	+ Sync
	+ Clone
	+ SingleVersionCommit
	+ SingleVersionGet
	+ SingleVersionContains
	+ SingleVersionSet
	+ SingleVersionRemove
	+ SingleVersionRange
	+ SingleVersionRangeRev
	+ 'static
{
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_value::value::{Value, partition::Partition, row_number::RowNumber};

	use super::{EntryKind, EntryLayout, StorageKey, classify_key, classify_range, storage_key};
	use crate::{
		interface::catalog::{
			id::{SeriesId, TableId, ViewId},
			storage::StorageId,
		},
		key::{
			row::{
				ClusteredRowKey, PartitionedClusteredRowKey, PartitionedRowKey, RowKey, RowSequenceKey,
				StoragePartitionedRowKey, StorageRowKey,
			},
			series::{
				PartitionedSeriesRowKey, PartitionedSeriesRowKeyRange, SeriesRowKey, SeriesRowKeyRange,
				StoragePartitionedSeriesKey, StorageSeriesKey,
			},
			typed::key::Key,
		},
	};

	fn part(v: &str) -> Partition {
		Partition::of(&[Value::Utf8(v.to_string())])
	}

	#[test]
	fn storage_key_hands_back_the_identity_it_decoded() {
		// the store classifies a key and the tier below then decodes the same bytes again for the row
		// number; the route must carry that identity so the second decode has nothing left to do
		let storage = StorageId::Table(TableId(7));

		let row = RowKey::encoded(storage, RowNumber(5));
		assert_eq!(
			storage_key(&row),
			(
				EntryKind::Source(storage, EntryLayout::Row),
				Some(StorageKey::Table(StorageRowKey::new(RowNumber(5))))
			)
		);

		let partitioned = PartitionedRowKey::encoded(storage, part("us"), RowNumber(5));
		assert_eq!(
			storage_key(&partitioned),
			(
				EntryKind::PartitionedSource(storage, EntryLayout::Row),
				Some(StorageKey::PartitionedTable(StoragePartitionedRowKey::new(
					part("us"),
					RowNumber(5)
				)))
			)
		);
	}

	#[test]
	fn storage_key_names_the_storage_a_row_belongs_to() {
		// the variant is what keeps a queue row out of a ringbuffer's cache drawer, so every storage kind
		// that writes plain rows must get its own one rather than a shared Row
		let row = StorageRowKey::new(RowNumber(5));
		for (storage, expected) in [
			(StorageId::table(7), StorageKey::Table(row)),
			(StorageId::ringbuffer(7), StorageKey::RingBuffer(row)),
			(StorageId::queue(7), StorageKey::Queue(row)),
			(StorageId::view(7), StorageKey::View(row)),
		] {
			assert_eq!(storage_key(&RowKey::encoded(storage, RowNumber(5))).1, Some(expected));
		}
	}

	#[test]
	fn a_series_key_carries_its_own_identity_on_a_series_and_on_a_view() {
		// a view's series rows and its plain rows both classify to Source(view); the storage key is what
		// keeps them in separate cache drawers now that neither is stored by its whole encoded key
		let series = StorageSeriesKey::new(None, 5, 1);
		for (storage, expected) in [
			(StorageId::series(7), StorageKey::Series(series)),
			(StorageId::view(7), StorageKey::SeriesView(series)),
		] {
			let series = SeriesRowKey {
				storage,
				variant_tag: None,
				key: 5,
				sequence: 1,
			}
			.encode();
			assert_eq!(
				storage_key(&series),
				(EntryKind::Source(storage, EntryLayout::Series), Some(expected))
			);
		}

		let partitioned = StoragePartitionedSeriesKey::new(part("us"), None, 5, 1);
		for (storage, expected) in [
			(StorageId::series(7), StorageKey::PartitionedSeries(partitioned)),
			(StorageId::view(7), StorageKey::PartitionedSeriesView(partitioned)),
		] {
			let partitioned = PartitionedSeriesRowKey::encoded(storage, part("us"), None, 5, 1);
			assert_eq!(
				storage_key(&partitioned),
				(EntryKind::PartitionedSource(storage, EntryLayout::Series), Some(expected))
			);
		}
	}

	#[test]
	fn a_view_row_and_a_view_series_row_do_not_share_a_storage_key() {
		// a view is one storage kind holding one layout, so the two must land on distinct entries and
		// distinct identities, or the point tier hands a series row back for a plain row lookup
		let storage = StorageId::view(7);
		let row = storage_key(&RowKey::encoded(storage, RowNumber(5))).1.unwrap();
		let series = storage_key(
			&SeriesRowKey {
				storage,
				variant_tag: None,
				key: 5,
				sequence: 5,
			}
			.encode(),
		)
		.1
		.unwrap();
		assert_ne!(row, series);
	}

	#[test]
	fn a_layout_its_storage_cannot_hold_claims_no_entry_at_all() {
		// a plain row key naming a series storage is nonsense; letting it claim a source entry would put a
		// key with no identity in a bucket every other member of has one
		let storage = StorageId::series(7);
		assert_eq!(storage_key(&RowKey::encoded(storage, RowNumber(5))), (EntryKind::Multi, None));

		let series_on_a_table = SeriesRowKey {
			storage: StorageId::table(7),
			variant_tag: None,
			key: 5,
			sequence: 1,
		}
		.encode();
		assert_eq!(storage_key(&series_on_a_table), (EntryKind::Multi, None));
	}

	#[test]
	fn a_source_entry_never_holds_a_key_without_an_identity() {
		// every tier below reads the identity, not the bytes, once a key is in a source bucket; a member
		// with none would have to be found by a scan the bucket has no index for
		let series = |storage| {
			SeriesRowKey {
				storage,
				variant_tag: None,
				key: 5,
				sequence: 1,
			}
			.encode()
		};

		for storage in [
			StorageId::table(7),
			StorageId::ringbuffer(7),
			StorageId::queue(7),
			StorageId::view(7),
			StorageId::series(7),
		] {
			for key in [
				RowKey::encoded(storage, RowNumber(5)),
				PartitionedRowKey::encoded(storage, part("us"), RowNumber(5)),
				series(storage),
				PartitionedSeriesRowKey::encoded(storage, part("us"), None, 5, 1),
				RowSequenceKey::encoded(storage),
			] {
				let (entry, ident) = storage_key(&key);
				match entry {
					EntryKind::Multi => assert_eq!(ident, None),
					EntryKind::Source(_, _) | EntryKind::PartitionedSource(_, _) => {
						assert!(ident.is_some(), "{entry:?} took a key with no identity")
					}
				}
			}
		}
	}

	#[test]
	fn an_entry_never_mixes_the_two_layouts() {
		// the narrow sqlite schema and the point tier drawers are both picked from the entry alone, so an
		// entry that admitted both layouts would store a series sequence in a column that has no room
		let storage = StorageId::view(7);
		let row = storage_key(&RowKey::encoded(storage, RowNumber(5))).0;
		let series = storage_key(
			&SeriesRowKey {
				storage,
				variant_tag: None,
				key: 5,
				sequence: 1,
			}
			.encode(),
		)
		.0;

		assert_eq!(row, EntryKind::Source(storage, EntryLayout::Row));
		assert_eq!(series, EntryKind::Source(storage, EntryLayout::Series));

		let partitioned_row = storage_key(&PartitionedRowKey::encoded(storage, part("us"), RowNumber(5))).0;
		let partitioned_series =
			storage_key(&PartitionedSeriesRowKey::encoded(storage, part("us"), None, 5, 1)).0;

		assert_eq!(partitioned_row, EntryKind::PartitionedSource(storage, EntryLayout::Row));
		assert_eq!(partitioned_series, EntryKind::PartitionedSource(storage, EntryLayout::Series));
	}

	#[test]
	fn storage_key_agrees_with_classify_key_on_the_entry() {
		let storage = StorageId::Table(TableId(7));
		for key in [
			RowKey::encoded(storage, RowNumber(5)),
			PartitionedRowKey::encoded(storage, part("us"), RowNumber(5)),
			RowSequenceKey::encoded(storage),
		] {
			assert_eq!(storage_key(&key).0, classify_key(&key));
		}
	}

	#[test]
	fn storage_key_sends_a_sorted_view_clustered_key_to_multi() {
		// A sorted view sink writes kind ++ storage ++ sort values ++ row, which carries
		// KeyKind::Row but is longer than the fixed row layout. It must land in Multi: the source
		// bucket keys on StorageRowKey alone, so two clustered keys differing only in their sort
		// prefix would alias onto one identity and a get could return another row's bytes.
		let storage = StorageId::view(3);
		let mut clustered = RowKey::encoded(storage, RowNumber(1)).as_slice().to_vec();
		clustered.extend_from_slice(&[0xAA; 8]);
		clustered.extend_from_slice(&99u64.to_be_bytes());

		assert_eq!(storage_key(&EncodedKey::new(clustered)), (EntryKind::Multi, None));
	}

	#[test]
	fn a_clustered_key_and_a_clustered_scan_range_classify_the_same_way() {
		// The sink classifies the key it writes; the scan classifies a range over the same keyspace.
		// When the two disagree the rows are written to one bucket and read from another, and the
		// view comes back empty with no error raised anywhere along the way.
		let storage = StorageId::view(3);

		let mut clustered = ClusteredRowKey::storage_start(storage).as_slice().to_vec();
		clustered.extend_from_slice(&[0xAA; 8]);
		clustered.extend_from_slice(&99u64.to_be_bytes());
		let clustered = EncodedKey::new(clustered);
		let range = ClusteredRowKey::scan_range(storage, None);
		assert_eq!(classify_key(&clustered), EntryKind::Multi);
		assert_eq!(classify_range(&range).unwrap_or(EntryKind::Multi), classify_key(&clustered));

		let mut partitioned = PartitionedClusteredRowKey::storage_start(storage).as_slice().to_vec();
		partitioned.extend_from_slice(&[0xBB; 16]);
		partitioned.extend_from_slice(&[0xAA; 8]);
		partitioned.extend_from_slice(&99u64.to_be_bytes());
		let partitioned = EncodedKey::new(partitioned);
		let partitioned_range = PartitionedClusteredRowKey::scan_range(storage, None);
		assert_eq!(classify_key(&partitioned), EntryKind::Multi);
		assert_eq!(
			classify_range(&partitioned_range).unwrap_or(EntryKind::Multi),
			classify_key(&partitioned)
		);
	}

	#[test]
	fn a_clustered_scan_range_never_reaches_the_narrowed_row_bucket() {
		// The narrowed bucket keys on StorageRowKey alone, which cannot carry the sort prefix that
		// orders these rows, so routing a clustered range there loses the view's order outright.
		let storage = StorageId::view(3);
		assert_ne!(
			classify_range(&ClusteredRowKey::scan_range(storage, None)),
			Some(EntryKind::Source(storage, EntryLayout::Row))
		);
		assert_ne!(
			classify_range(&PartitionedClusteredRowKey::scan_range(storage, None)),
			Some(EntryKind::PartitionedSource(storage, EntryLayout::Row))
		);
	}

	#[test]
	fn storage_key_leaves_a_key_it_does_not_own_without_an_identity() {
		assert_eq!(
			storage_key(&RowSequenceKey::encoded(StorageId::Table(TableId(7)))),
			(EntryKind::Multi, None)
		);
	}

	#[test]
	fn classify_key_partitioned_row_is_partitioned_source() {
		let storage = StorageId::Table(TableId(7));
		let key = PartitionedRowKey::encoded(storage, part("us"), RowNumber(1));
		assert_eq!(classify_key(&key), EntryKind::PartitionedSource(storage, EntryLayout::Row));
	}

	#[test]
	fn classify_key_partitioned_view_row_is_partitioned_source() {
		// A view that owns its rows must classify to its own id, not fall through to EntryKind::Multi.
		let storage = StorageId::view(7);
		let key = PartitionedRowKey::encoded(storage, part("us"), RowNumber(1));
		assert_eq!(classify_key(&key), EntryKind::PartitionedSource(storage, EntryLayout::Row));
		assert_ne!(
			classify_key(&key),
			EntryKind::PartitionedSource(StorageId::table(7), EntryLayout::Row),
			"a view and a table sharing id 7 must not classify to the same entry"
		);
	}

	#[test]
	fn classify_key_row_is_still_source() {
		let storage = StorageId::Table(TableId(7));
		let key = RowKey::encoded(storage, RowNumber(1));
		assert_eq!(classify_key(&key), EntryKind::Source(storage, EntryLayout::Row));
	}

	#[test]
	fn classify_range_all_partition_forms_are_partitioned_source_for_table_and_view() {
		// Every range form must carry the owning variant, or a view's sweep hits the table of the same id.
		for storage in [StorageId::Table(TableId(9)), StorageId::view(9)] {
			let p = part("us");
			let last = PartitionedRowKey::encoded(storage, p, RowNumber(5));
			assert_eq!(
				classify_range(&PartitionedRowKey::partition_range(storage, p)),
				Some(EntryKind::PartitionedSource(storage, EntryLayout::Row))
			);
			assert_eq!(
				classify_range(&PartitionedRowKey::partition_scan_range(storage, p, Some(&last))),
				Some(EntryKind::PartitionedSource(storage, EntryLayout::Row))
			);
			assert_eq!(
				classify_range(&PartitionedRowKey::scan_range(storage, None)),
				Some(EntryKind::PartitionedSource(storage, EntryLayout::Row))
			);
			assert_eq!(
				classify_range(&PartitionedRowKey::full_scan(storage)),
				Some(EntryKind::PartitionedSource(storage, EntryLayout::Row))
			);
		}
	}

	#[test]
	fn classify_range_row_range_is_still_source() {
		let storage = StorageId::Table(TableId(9));
		assert_eq!(
			classify_range(&RowKey::full_scan(storage)),
			Some(EntryKind::Source(storage, EntryLayout::Row))
		);
	}

	#[test]
	fn classify_key_and_classify_range_agree_for_the_series_kinds() {
		// expired_batch picks its indexed scan by classify_range while the tier stores entries by classify_key;
		// if the two disagree the evictor scans an entry that holds none of the rows it is trying to expire.
		let series = StorageId::series(SeriesId(11));
		let view = StorageId::View(ViewId(11));

		for storage in [series, view] {
			let key = SeriesRowKey {
				storage,
				variant_tag: None,
				key: 5,
				sequence: 1,
			}
			.encode();
			assert_eq!(classify_key(&key), EntryKind::Source(storage, EntryLayout::Series));
			assert_eq!(
				classify_range(&SeriesRowKeyRange::full_scan(storage, None)),
				Some(EntryKind::Source(storage, EntryLayout::Series))
			);

			let partitioned = PartitionedSeriesRowKey::encoded(
				storage,
				Partition::of(&[Value::Utf8("us".to_string())]),
				None,
				5,
				1,
			);
			assert_eq!(
				classify_key(&partitioned),
				EntryKind::PartitionedSource(storage, EntryLayout::Series)
			);
			assert_eq!(
				classify_range(&PartitionedSeriesRowKeyRange::full_scan(storage)),
				Some(EntryKind::PartitionedSource(storage, EntryLayout::Series))
			);
		}
	}
}
