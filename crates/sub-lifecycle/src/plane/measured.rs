// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask};
use reifydb_value::value::duration::Duration;

use crate::plane::RetentionPlane;

pub struct Measured<T: LifecycleTask> {
	inner: T,
	plane: RetentionPlane,
}

impl<T: LifecycleTask> Measured<T> {
	pub fn new(inner: T, plane: RetentionPlane) -> Self {
		Self {
			inner,
			plane,
		}
	}
}

impl<T: LifecycleTask> LifecycleTask for Measured<T> {
	fn name(&self) -> &'static str {
		self.inner.name()
	}

	fn interval(&self) -> Duration {
		self.inner.interval()
	}

	fn classes(&self) -> &'static [RetentionClass] {
		self.inner.classes()
	}

	fn run_slice(&mut self) -> Progress {
		for class in self.inner.classes() {
			self.plane.record_liveness(*class);
		}
		self.inner.run_slice()
	}
}

#[cfg(test)]
mod tests {
	use std::sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	};

	use reifydb_core::{
		common::CommitVersion,
		lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask},
	};
	use reifydb_runtime::version_epoch::VersionEpoch;
	use reifydb_value::value::duration::Duration;

	use super::Measured;
	use crate::plane::{
		RetentionPlane,
		ledger::{FloorScope, FloorSource},
	};

	struct NoFloors;

	impl FloorSource for NoFloors {
		fn query_done_until(&self) -> CommitVersion {
			CommitVersion(u64::MAX)
		}

		fn lease_min(&self) -> CommitVersion {
			CommitVersion(u64::MAX)
		}

		fn consumer_checkpoint(&self) -> CommitVersion {
			CommitVersion(u64::MAX)
		}

		fn consumer_position(&self) -> CommitVersion {
			CommitVersion(u64::MAX)
		}

		fn flush_watermark(&self, _scope: FloorScope) -> CommitVersion {
			CommitVersion(u64::MAX)
		}
	}

	fn plane() -> RetentionPlane {
		RetentionPlane::new(Arc::new(NoFloors), VersionEpoch::new())
	}

	struct TwoClassTask {
		runs: Arc<AtomicU64>,
	}

	impl LifecycleTask for TwoClassTask {
		fn name(&self) -> &'static str {
			"two-class"
		}

		fn interval(&self) -> Duration {
			Duration::from_seconds(1).unwrap()
		}

		fn classes(&self) -> &'static [RetentionClass] {
			&[RetentionClass::RowTtlSilent, RetentionClass::RowTtlAnnounced]
		}

		fn run_slice(&mut self) -> Progress {
			self.runs.fetch_add(1, Ordering::SeqCst);
			Progress::Exhausted
		}
	}

	#[test]
	fn a_slice_records_liveness_for_every_class_the_task_owns() {
		// A task may declare more than one class, and recording only the first leaves the rest reading
		// as dead in the retention report.
		let plane = plane();
		let runs = Arc::new(AtomicU64::new(0));
		let mut task = Measured::new(
			TwoClassTask {
				runs: runs.clone(),
			},
			plane.clone(),
		);

		task.run_slice();

		assert_eq!(runs.load(Ordering::SeqCst), 1, "the wrapped task must still run exactly once");
		assert_eq!(plane.snapshot(RetentionClass::RowTtlSilent).slices, 1);
		assert_eq!(plane.snapshot(RetentionClass::RowTtlAnnounced).slices, 1);
	}

	#[test]
	fn liveness_alone_never_reports_a_class_as_stuck() {
		// Liveness must not imply "reclaimed nothing", or every idle tick reports stuck.
		let plane = plane();
		let mut task = Measured::new(
			TwoClassTask {
				runs: Arc::new(AtomicU64::new(0)),
			},
			plane.clone(),
		);

		for _ in 0..5 {
			task.run_slice();
		}

		let snapshot = plane.snapshot(RetentionClass::RowTtlSilent);
		assert_eq!(snapshot.slices, 5, "every slice must be counted");
		assert_eq!(snapshot.stuck_slices, 0, "liveness must not be read as a reclamation of zero");
		assert_eq!(snapshot.work_done, 0, "the wrapper must not invent work it cannot measure");
	}

	#[test]
	fn a_task_owning_no_class_records_nothing() {
		// Non-reclaiming tasks must not appear in the retention report at all.
		struct Bystander;

		impl LifecycleTask for Bystander {
			fn name(&self) -> &'static str {
				"bystander"
			}

			fn interval(&self) -> Duration {
				Duration::from_seconds(1).unwrap()
			}

			fn classes(&self) -> &'static [RetentionClass] {
				&[]
			}

			fn run_slice(&mut self) -> Progress {
				Progress::Exhausted
			}
		}

		let plane = plane();
		let mut task = Measured::new(Bystander, plane.clone());

		task.run_slice();

		assert!(
			plane.report().iter().all(|(_, snapshot)| snapshot.slices == 0),
			"a task that owns no retention class must leave every class counter untouched"
		);
	}
}
