// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	mem::{replace, take},
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
	time::Duration as StdDuration,
};

use reifydb_core::{
	actors::pending::{Pending, PendingWrite},
	common::CommitVersion,
	delta::RemoveVisibility,
	interface::{
		catalog::{flow::FlowId, id::ViewId, object::ObjectId},
		change::{Change, ChangeOrigin},
	},
	key::any::TaggedKey,
	marker::{LatencyReader, Marker, MarkerEntry, MarkerSink, MarkerSweep, ViewLatency},
	return_internal_error,
	util::ioc::IocContainer,
};
use reifydb_engine::lift::{RowTarget, tagged_row_target};
use reifydb_runtime::{
	context::clock::Instant,
	sync::{mutex::Mutex, rwlock::RwLock},
};
use reifydb_value::{
	Result,
	util::hex::display as hex_display,
	value::{Value, digest::Digest, duration::Duration, value_type::ValueType},
};

use crate::commit::committer::FlowSlice;

const DIGEST_ACCURACY_PPM: u32 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WrittenView {
	pub view: ViewId,
	pub fused_member: bool,
}

#[derive(Debug, Clone)]
pub struct InputMarkers {
	by_id: BTreeMap<CommitVersion, Marker>,
	latest_parent: Instant,
}

impl InputMarkers {
	pub(crate) fn markers(&self) -> Vec<Marker> {
		self.by_id.values().cloned().collect()
	}

	fn t0s(&self) -> impl Iterator<Item = &Instant> {
		self.by_id.values().flat_map(|marker| marker.t0s.iter())
	}

	fn hop(&self, written: &WrittenView, committed_at: &Instant) -> StdDuration {
		if written.fused_member {
			return StdDuration::ZERO;
		}
		committed_at.duration_since(&self.latest_parent)
	}
}

#[derive(Default)]
struct MarkerEntries {
	by_version: BTreeMap<CommitVersion, MarkerEntry>,
	idle_through: Option<CommitVersion>,
}

#[derive(Default)]
struct MarkerMapInner {
	entries: RwLock<MarkerEntries>,
	len: AtomicUsize,
}

#[derive(Clone, Default)]
pub struct MarkerMap {
	inner: Arc<MarkerMapInner>,
}

impl MarkerMap {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn committed_at(&self, version: CommitVersion) -> Option<Instant> {
		if self.inner.len.load(Ordering::Acquire) == 0 {
			return None;
		}
		self.inner.entries.read().by_version.get(&version).map(|entry| entry.committed_at.clone())
	}

	pub fn input_markers(&self, changes: &[Change]) -> Option<InputMarkers> {
		if self.inner.len.load(Ordering::Acquire) == 0 {
			return None;
		}
		let entries = self.inner.entries.read();
		let mut input: Option<InputMarkers> = None;
		for change in changes {
			let ChangeOrigin::Object(origin) = &change.origin else {
				continue;
			};
			let Some(entry) = entries.by_version.get(&change.version.commit) else {
				continue;
			};
			let view_markers = match origin {
				ObjectId::View(view) => entry.by_view.get(view).map_or(&[][..], Vec::as_slice),
				_ => &[],
			};
			for marker in entry.writer.iter().chain(view_markers) {
				add_marker(&mut input, marker, &entry.committed_at);
			}
		}
		input
	}
}

fn add_marker(input: &mut Option<InputMarkers>, marker: &Marker, parent_committed_at: &Instant) {
	match input {
		Some(input) => {
			input.by_id.entry(marker.id).or_insert_with(|| marker.clone());
			if *parent_committed_at > input.latest_parent {
				input.latest_parent = parent_committed_at.clone();
			}
		}
		None => {
			*input = Some(InputMarkers {
				by_id: BTreeMap::from([(marker.id, marker.clone())]),
				latest_parent: parent_committed_at.clone(),
			});
		}
	}
}

impl MarkerSink for MarkerMap {
	fn insert(&self, version: CommitVersion, entry: MarkerEntry) {
		let mut entries = self.inner.entries.write();
		entries.by_version.insert(version, entry);
		self.inner.len.store(entries.by_version.len(), Ordering::Release);
	}
}

impl MarkerSweep for MarkerMap {
	fn sweep(&self, floor: Option<CommitVersion>) -> usize {
		let mut entries = self.inner.entries.write();
		let keep_from = match floor {
			Some(floor) => {
				entries.idle_through = None;
				Some(floor)
			}
			None => {
				let newest = entries.by_version.last_key_value().map(|(version, _)| *version);
				replace(&mut entries.idle_through, newest).map(|seen| CommitVersion(seen.0 + 1))
			}
		};
		let Some(keep_from) = keep_from else {
			return 0;
		};
		let kept = entries.by_version.split_off(&keep_from);
		let dropped = replace(&mut entries.by_version, kept).len();
		self.inner.len.store(entries.by_version.len(), Ordering::Release);
		dropped
	}
}

#[derive(Clone, Default)]
pub struct LatencyCollector {
	views: Arc<RwLock<BTreeMap<ViewId, Mutex<ViewLatency>>>>,
}

impl LatencyCollector {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn record(&self, view: ViewId, lags: impl Iterator<Item = StdDuration>, hop: StdDuration) {
		let views = self.views.read();
		if let Some(tally) = views.get(&view) {
			record_samples(&mut tally.lock(), lags, hop);
			return;
		}
		drop(views);
		let mut views = self.views.write();
		let tally = views.entry(view).or_insert_with(|| Mutex::new(empty_latency(view)));
		record_samples(&mut tally.lock(), lags, hop);
	}
}

impl LatencyReader for LatencyCollector {
	fn take(&self) -> Vec<ViewLatency> {
		take(&mut *self.views.write()).into_values().map(|tally| tally.lock().clone()).collect()
	}
}

fn empty_latency(view: ViewId) -> ViewLatency {
	ViewLatency {
		view,
		n: 0,
		max: Duration::zero(),
		end_to_end: empty_digest(),
		hop_max: Duration::zero(),
		hop: empty_digest(),
	}
}

fn empty_digest() -> Digest {
	match Digest::new(ValueType::Duration, DIGEST_ACCURACY_PPM) {
		Ok(digest) => digest,
		Err(e) => panic!("the latency digest accuracy of {DIGEST_ACCURACY_PPM} ppm must be valid: {e}"),
	}
}

fn record_samples(tally: &mut ViewLatency, lags: impl Iterator<Item = StdDuration>, hop: StdDuration) {
	for lag in lags {
		let lag = Duration::from_std(lag);
		add_sample(&mut tally.end_to_end, lag);
		tally.n += 1;
		tally.max = tally.max.max(lag);
	}
	let hop = Duration::from_std(hop);
	add_sample(&mut tally.hop, hop);
	tally.hop_max = tally.hop_max.max(hop);
}

fn add_sample(digest: &mut Digest, sample: Duration) {
	if let Err(e) = digest.add_value(&Value::Duration(sample)) {
		panic!("a latency digest of durations refused the duration {sample}: {e}");
	}
}

#[derive(Clone)]
pub(crate) struct MarkerPlane {
	map: MarkerMap,
	collector: LatencyCollector,
}

impl MarkerPlane {
	pub(crate) fn register(ioc: &IocContainer) -> Self {
		let plane = Self {
			map: MarkerMap::new(),
			collector: LatencyCollector::new(),
		};
		ioc.register_service::<Arc<dyn MarkerSink>>(Arc::new(plane.map.clone()));
		ioc.register_service::<Arc<dyn MarkerSweep>>(Arc::new(plane.map.clone()));
		ioc.register_service::<Arc<dyn LatencyReader>>(Arc::new(plane.collector.clone()));
		plane
	}

	pub(crate) fn map(&self) -> &MarkerMap {
		&self.map
	}

	pub(crate) fn record(&self, version: CommitVersion, slice: &FlowSlice) {
		let Some(input) = &slice.input_markers else {
			return;
		};
		if slice.written_views.is_empty() {
			return;
		}
		let Some(committed_at) = self.map.committed_at(version) else {
			return;
		};
		for written in &slice.written_views {
			let lags = input.t0s().map(|t0| committed_at.duration_since(t0));
			self.collector.record(written.view, lags, input.hop(written, &committed_at));
		}
	}
}

pub fn written_views(pending: &Pending, members: &[(FlowId, ViewId)]) -> Result<Vec<WrittenView>> {
	let head = members.first().map(|(_, view)| *view);
	let mut views = BTreeSet::new();
	for (encoded, write) in pending.rows_ordered() {
		if matches!(
			write,
			PendingWrite::Remove {
				announce: RemoveVisibility::Silent | RemoveVisibility::Unobserved,
			}
		) {
			continue;
		}
		let Some(key) = TaggedKey::decode(encoded) else {
			return_internal_error!(
				"flow pending write carries a key no typed key decodes: {}",
				hex_display(encoded.as_ref())
			)
		};
		if let Some(RowTarget {
			object: ObjectId::View(view),
			..
		}) = tagged_row_target(&key)
		{
			views.insert(view);
		}
	}
	Ok(views.into_iter()
		.map(|view| WrittenView {
			view,
			fused_member: head.is_some_and(|head| head != view),
		})
		.collect())
}
