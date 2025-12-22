// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::BTreeMap;

use indexmap::IndexMap;
use reifydb_core::{
	CommitVersion,
	interface::{FlowId, SourceId},
};
use reifydb_flow_operator_sdk::{FlowChange, FlowDiff};

use crate::worker::{UnitOfWork, UnitsOfWork};

impl crate::engine::FlowEngine {
	/// Partition changes from multiple versions into units of work grouped by flow
	///
	/// This method handles the complete partitioning logic:
	/// 1. Processes each version's changes separately
	/// 2. Groups units by flow across all versions
	/// 3. Maintains version ordering within each flow
	///
	/// # Arguments
	/// * `changes_by_version` - Map of version to (source_id, changes) pairs
	///
	/// # Returns
	/// UnitsOfWork where each flow has its units ordered by version
	pub fn create_partition(
		&self,
		changes_by_version: BTreeMap<CommitVersion, Vec<(SourceId, Vec<FlowDiff>)>>,
	) -> UnitsOfWork {
		let mut all_units_by_flow: BTreeMap<FlowId, Vec<UnitOfWork>> = BTreeMap::new();

		// BTreeMap is already sorted by key, so we iterate in version order
		for (version, changes) in &changes_by_version {
			let version = *version;

			// Group changes by source for this version
			// Using IndexMap to preserve CDC insertion order within the version
			let mut changes_by_source: IndexMap<SourceId, Vec<FlowDiff>> = IndexMap::new();
			for (source_id, diffs) in changes {
				changes_by_source.entry(*source_id).or_insert_with(Vec::new).extend(diffs.clone());
			}

			// Partition this version's changes into units of work
			let version_units = self.partition_into_units_of_work(changes_by_source, version);

			// Merge units from this version into the overall collection
			// Each flow's units are stored in a Vec to maintain version ordering
			for flow_units in version_units.into_inner() {
				for unit in flow_units {
					all_units_by_flow.entry(unit.flow_id).or_insert_with(Vec::new).push(unit);
				}
			}
		}

		// Convert the HashMap to UnitsOfWork for the worker
		let units_vec: Vec<Vec<UnitOfWork>> = all_units_by_flow.into_iter().map(|(_, units)| units).collect();

		// INVARIANT: Validate that each flow_id appears exactly once in the output
		// and that each inner Vec contains units for only one flow
		{
			use std::collections::HashSet;
			let mut seen_flows = HashSet::new();

			for flow_units in &units_vec {
				assert!(!flow_units.is_empty(), "INVARIANT VIOLATED: Empty flow units in UnitsOfWork");

				let flow_id = flow_units[0].flow_id;
				assert!(
					!seen_flows.contains(&flow_id),
					"INVARIANT VIOLATED: flow_id {:?} appears multiple times in UnitsOfWork. \
					This means the same flow will be processed by multiple parallel tasks, \
					causing keyspace overlap.",
					flow_id
				);

				// Validate all units in this Vec are for the same flow
				for unit in flow_units {
					assert_eq!(
						unit.flow_id, flow_id,
						"INVARIANT VIOLATED: Mixed flow_ids in same Vec - expected {:?}, got {:?}. \
						All units in a Vec must belong to the same flow.",
						flow_id, unit.flow_id
					);
				}

				seen_flows.insert(flow_id);
			}
		}

		UnitsOfWork::new(units_vec)
	}

	fn partition_into_units_of_work(
		&self,
		changes_by_source: IndexMap<SourceId, Vec<FlowDiff>>,
		version: CommitVersion,
	) -> UnitsOfWork {
		// Map to collect all source changes per flow
		let mut flow_changes: BTreeMap<FlowId, Vec<FlowChange>> = BTreeMap::new();

		// Read source subscriptions and backfill versions
		let sources = self.inner.sources.read();
		let flow_creation_version = self.inner.flow_creation_versions.read();

		// For each source that changed
		for (source_id, diffs) in changes_by_source {
			// Find all flows subscribed to this source
			if let Some(subscriptions) = sources.get(&source_id) {
				for (flow_id, node_id) in subscriptions {
					// Skip CDC events that were already included in the backfill
					if let Some(&flow_creation_version) = flow_creation_version.get(flow_id) {
						if version < flow_creation_version {
							continue;
						}
					}

					// Create FlowChange scoped to the specific node in this flow
					// This ensures each flow only processes its own nodes, preventing keyspace
					// overlap
					let change = FlowChange::internal(*node_id, version, diffs.clone());
					flow_changes.entry(*flow_id).or_insert_with(Vec::new).push(change);
				}
			}
		}

		// Group all units at this version into a single UnitsOfWork
		// Since all units are at the same version, each flow gets exactly one unit
		let units: Vec<Vec<UnitOfWork>> = flow_changes
			.into_iter()
			.map(|(flow_id, source_changes)| vec![UnitOfWork::new(flow_id, version, source_changes)])
			.collect();

		UnitsOfWork::new(units)
	}
}

#[cfg(test)]
mod tests {
	use std::{
		collections::{BTreeMap, HashMap},
		sync::Arc,
	};

	use parking_lot::RwLock;
	use rand::{rng, seq::SliceRandom};
	use reifydb_core::{
		CommitVersion, Row,
		event::EventBus,
		interface::{FlowId, FlowNodeId, SourceId, TableId},
		util::CowVec,
		value::encoded::{EncodedValues, EncodedValuesNamedLayout},
	};
	use reifydb_engine::{StandardRowEvaluator, execute::Executor};
	use reifydb_flow_operator_sdk::{FlowChangeOrigin, FlowDiff};
	use reifydb_rql::flow::FlowGraphAnalyzer;
	use reifydb_type::{RowNumber, Type};

	use crate::{
		engine::{FlowEngine, FlowEngineInner},
		operator::transform::registry::TransformOperatorRegistry,
		worker::{UnitOfWork, UnitsOfWork},
	};

	/// Helper to build sources map with explicit node IDs
	/// Maps source_id to list of (flow_id, node_id) pairs
	/// The node_id encodes BOTH the source and flow: source_id*1000 + flow_id
	/// This ensures unique node IDs across all source subscriptions
	fn mk_sources(subscriptions: HashMap<SourceId, Vec<FlowId>>) -> HashMap<SourceId, Vec<(FlowId, FlowNodeId)>> {
		let mut sources_map = HashMap::new();
		for (source_id, flows) in subscriptions {
			let source_num = match source_id {
				SourceId::Table(tid) => tid.0,
				_ => panic!("Only Table sources supported in tests"),
			};
			let subscriptions_with_nodes: Vec<(FlowId, FlowNodeId)> = flows
				.into_iter()
				.map(|flow_id| {
					// Node ID = source * 1000 + flow, ensuring global uniqueness
					let node_id = FlowNodeId(source_num * 1000 + flow_id.0);
					(flow_id, node_id)
				})
				.collect();
			sources_map.insert(source_id, subscriptions_with_nodes);
		}
		sources_map
	}

	fn setup_test_engine(subscriptions: HashMap<SourceId, Vec<FlowId>>) -> FlowEngine {
		let evaluator = StandardRowEvaluator::default();
		let executor = Executor::testing();
		let registry = TransformOperatorRegistry::new();

		let sources = mk_sources(subscriptions);

		let inner = FlowEngineInner {
			evaluator,
			executor,
			registry,
			operators: RwLock::new(HashMap::new()),
			flows: RwLock::new(HashMap::new()),
			sources: RwLock::new(sources),
			sinks: RwLock::new(HashMap::new()),
			analyzer: RwLock::new(FlowGraphAnalyzer::new()),
			event_bus: EventBus::new(),
			flow_creation_versions: RwLock::new(HashMap::new()),
		};

		FlowEngine {
			inner: Arc::new(inner),
		}
	}

	/// Create a test FlowDiff with identifiable data
	fn mk_diff(label: &str) -> FlowDiff {
		let row = mk_row(label);
		FlowDiff::Insert {
			post: row,
		}
	}

	/// Create a test Row with identifiable data
	fn mk_row(label: &str) -> Row {
		Row {
			number: RowNumber(label.len() as u64),
			encoded: EncodedValues(CowVec::new(label.as_bytes().to_vec())),
			layout: EncodedValuesNamedLayout::new(std::iter::once(("test".to_string(), Type::Uint8))),
		}
	}

	/// Normalize UnitsOfWork into a sorted map for comparison
	fn normalize(units: UnitsOfWork) -> BTreeMap<FlowId, Vec<UnitOfWork>> {
		let mut map = BTreeMap::new();
		for flow_units in units.into_inner() {
			for unit in flow_units {
				map.entry(unit.flow_id).or_insert_with(Vec::new).push(unit);
			}
		}
		// Sort each flow's units by version
		for vec in map.values_mut() {
			vec.sort_by_key(|u| u.version);
		}
		map
	}

	/// Extract a snapshot of a unit: (version, source_id -> diff_count)
	/// Note: After the fix, FlowChanges use Internal origin with node_id
	/// In tests, node_id = source_id * 1000 + flow_id, so we reverse-engineer source_id
	fn snapshot_unit(unit: &UnitOfWork) -> (CommitVersion, BTreeMap<SourceId, usize>) {
		let mut sources = BTreeMap::new();

		for change in &unit.source_changes {
			match change.origin {
				FlowChangeOrigin::External(source_id) => {
					let count = change.diffs.len();
					*sources.entry(source_id).or_insert(0) += count;
				}
				FlowChangeOrigin::Internal(node_id) => {
					// In test setup, node_id = source_id * 1000 + flow_id
					// So source_id = node_id / 1000
					let source_num = node_id.0 / 1000;
					let source_id = SourceId::Table(TableId(source_num));
					let count = change.diffs.len();
					*sources.entry(source_id).or_insert(0) += count;
				}
			}
		}
		(unit.version, sources)
	}

	/// Helper to create source IDs
	fn s(id: u64) -> SourceId {
		SourceId::Table(TableId(id))
	}

	/// Helper to create flow IDs
	fn f(id: u64) -> FlowId {
		FlowId(id)
	}

	/// Helper to create commit versions
	fn v(ver: u64) -> CommitVersion {
		CommitVersion(ver)
	}

	/// Compare two normalized results by their snapshots
	fn assert_normalized_eq(
		result: &BTreeMap<FlowId, Vec<UnitOfWork>>,
		expected: &BTreeMap<FlowId, Vec<UnitOfWork>>,
	) {
		assert_eq!(result.len(), expected.len(), "Different number of flows");

		for (flow_id, result_units) in result {
			let expected_units =
				expected.get(flow_id).expect(&format!("Flow {:?} missing in expected", flow_id));
			assert_eq!(
				result_units.len(),
				expected_units.len(),
				"Flow {:?} has different unit count",
				flow_id
			);

			for (i, (result_unit, expected_unit)) in
				result_units.iter().zip(expected_units.iter()).enumerate()
			{
				let result_snapshot = snapshot_unit(result_unit);
				let expected_snapshot = snapshot_unit(expected_unit);
				assert_eq!(
					result_snapshot, expected_snapshot,
					"Flow {:?} unit {} differs: {:?} vs {:?}",
					flow_id, i, result_snapshot, expected_snapshot
				);
			}
		}
	}

	#[tokio::test]
	async fn test_empty_input() {
		let engine = setup_test_engine(HashMap::new());
		let input = BTreeMap::new();

		let result = engine.create_partition(input);

		assert!(result.is_empty(), "Empty input should produce empty output");
	}

	#[tokio::test]
	async fn test_single_version_single_source_single_flow() {
		// S1 -> F1
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1)]);
		let engine = setup_test_engine(subscriptions);

		// V10: S1[d1, d2]
		let mut input = BTreeMap::new();
		input.insert(v(10), vec![(s(1), vec![mk_diff("d1"), mk_diff("d2")])]);

		let result = engine.create_partition(input);
		let normalized = normalize(result);

		// Expect F1 has 1 unit at V10 with S1:2 diffs
		assert_eq!(normalized.len(), 1);
		let f1_units = &normalized[&f(1)];
		assert_eq!(f1_units.len(), 1);

		let (ver, sources) = snapshot_unit(&f1_units[0]);
		assert_eq!(ver, v(10));
		assert_eq!(sources.get(&s(1)), Some(&2));
	}

	#[tokio::test]
	async fn test_single_version_multi_flow_fanout() {
		// S1 -> [F1, F2, F3]
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1), f(2), f(3)]);
		let engine = setup_test_engine(subscriptions);

		// V1: S1[d1]
		let mut input = BTreeMap::new();
		input.insert(v(1), vec![(s(1), vec![mk_diff("d1")])]);

		let result = engine.create_partition(input);
		let normalized = normalize(result);

		// Expect F1, F2, F3 each have 1 unit at V1 with S1:1
		assert_eq!(normalized.len(), 3);

		for flow_id in [f(1), f(2), f(3)] {
			let units = &normalized[&flow_id];
			assert_eq!(units.len(), 1);
			let (ver, sources) = snapshot_unit(&units[0]);
			assert_eq!(ver, v(1));
			assert_eq!(sources.get(&s(1)), Some(&1));
		}
	}

	#[tokio::test]
	async fn test_single_version_multi_source_partial_overlap() {
		// S1 -> [F1, F2], S2 -> [F2, F3]
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1), f(2)]);
		subscriptions.insert(s(2), vec![f(2), f(3)]);
		let engine = setup_test_engine(subscriptions);

		// V7: S1[a], S2[b,c]
		let mut input = BTreeMap::new();
		input.insert(v(7), vec![(s(1), vec![mk_diff("a")]), (s(2), vec![mk_diff("b"), mk_diff("c")])]);

		let result = engine.create_partition(input);
		let normalized = normalize(result);

		assert_eq!(normalized.len(), 3);

		// F1 @V7: S1:1
		let f1_units = &normalized[&f(1)];
		assert_eq!(f1_units.len(), 1);
		let (ver, sources) = snapshot_unit(&f1_units[0]);
		assert_eq!(ver, v(7));
		assert_eq!(sources.len(), 1);
		assert_eq!(sources.get(&s(1)), Some(&1));

		// F2 @V7: S1:1, S2:2
		let f2_units = &normalized[&f(2)];
		assert_eq!(f2_units.len(), 1);
		let (ver, sources) = snapshot_unit(&f2_units[0]);
		assert_eq!(ver, v(7));
		assert_eq!(sources.len(), 2);
		assert_eq!(sources.get(&s(1)), Some(&1));
		assert_eq!(sources.get(&s(2)), Some(&2));

		// F3 @V7: S2:2
		let f3_units = &normalized[&f(3)];
		assert_eq!(f3_units.len(), 1);
		let (ver, sources) = snapshot_unit(&f3_units[0]);
		assert_eq!(ver, v(7));
		assert_eq!(sources.len(), 1);
		assert_eq!(sources.get(&s(2)), Some(&2));
	}

	#[tokio::test]
	async fn test_unknown_source_filtered() {
		// S1 -> [F1]
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1)]);
		let engine = setup_test_engine(subscriptions);

		// V3: S999[x] (unknown source)
		let mut input = BTreeMap::new();
		input.insert(v(3), vec![(s(999), vec![mk_diff("x")])]);

		let result = engine.create_partition(input);

		assert!(result.is_empty(), "Unknown sources should produce no units");
	}

	#[tokio::test]
	async fn test_multi_version_ordering() {
		// S1 -> [F1], S2 -> [F2]
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1)]);
		subscriptions.insert(s(2), vec![f(2)]);
		let engine = setup_test_engine(subscriptions);

		// Input versions intentionally unsorted: V20, V10, V30
		let mut input = BTreeMap::new();
		input.insert(v(20), vec![(s(1), vec![mk_diff("a")])]);
		input.insert(v(10), vec![(s(1), vec![mk_diff("b")])]);
		input.insert(v(30), vec![(s(2), vec![mk_diff("c")])]);

		let result = engine.create_partition(input);
		let normalized = normalize(result);

		// F1: units at V10 then V20 (ascending)
		let f1_units = &normalized[&f(1)];
		assert_eq!(f1_units.len(), 2);
		assert_eq!(f1_units[0].version, v(10));
		assert_eq!(f1_units[1].version, v(20));

		// F2: single unit at V30
		let f2_units = &normalized[&f(2)];
		assert_eq!(f2_units.len(), 1);
		assert_eq!(f2_units[0].version, v(30));
	}

	#[tokio::test]
	async fn test_version_gaps_preserved() {
		// S1 -> [F1]
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1)]);
		let engine = setup_test_engine(subscriptions);

		// V1, V100 (non-contiguous)
		let mut input = BTreeMap::new();
		input.insert(v(1), vec![(s(1), vec![mk_diff("a")])]);
		input.insert(v(100), vec![(s(1), vec![mk_diff("b")])]);

		let result = engine.create_partition(input);
		let normalized = normalize(result);

		// F1 has exactly 2 units at V1 and V100
		let f1_units = &normalized[&f(1)];
		assert_eq!(f1_units.len(), 2);
		assert_eq!(f1_units[0].version, v(1));
		assert_eq!(f1_units[1].version, v(100));
	}

	#[tokio::test]
	async fn test_duplicate_source_entries_merged() {
		// S1 -> [F1]
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1)]);
		let engine = setup_test_engine(subscriptions);

		// V5: S1[x,y], S1[z] (duplicate source entries)
		let mut input = BTreeMap::new();
		input.insert(v(5), vec![(s(1), vec![mk_diff("x"), mk_diff("y")]), (s(1), vec![mk_diff("z")])]);

		let result = engine.create_partition(input);
		let normalized = normalize(result);

		// F1 @V5 should have S1:3 diffs (merged)
		let f1_units = &normalized[&f(1)];
		assert_eq!(f1_units.len(), 1);
		let (ver, sources) = snapshot_unit(&f1_units[0]);
		assert_eq!(ver, v(5));
		assert_eq!(sources.get(&s(1)), Some(&3));
	}

	#[tokio::test]
	async fn test_flow_with_multiple_sources_same_version() {
		// S1 -> [F1], S2 -> [F1]
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1)]);
		subscriptions.insert(s(2), vec![f(1)]);
		let engine = setup_test_engine(subscriptions);

		// V8: S1[a], S2[b,c]
		let mut input = BTreeMap::new();
		input.insert(v(8), vec![(s(1), vec![mk_diff("a")]), (s(2), vec![mk_diff("b"), mk_diff("c")])]);

		let result = engine.create_partition(input);
		let normalized = normalize(result);

		// F1 @V8 has two sources: S1:1, S2:2
		let f1_units = &normalized[&f(1)];
		assert_eq!(f1_units.len(), 1);
		let (ver, sources) = snapshot_unit(&f1_units[0]);
		assert_eq!(ver, v(8));
		assert_eq!(sources.len(), 2);
		assert_eq!(sources.get(&s(1)), Some(&1));
		assert_eq!(sources.get(&s(2)), Some(&2));
	}

	#[tokio::test]
	async fn test_no_work_for_unaffected_flows() {
		// S1 -> [F1], S2 -> [F2]
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1)]);
		subscriptions.insert(s(2), vec![f(2)]);
		let engine = setup_test_engine(subscriptions);

		// V2: S1[a] only
		let mut input = BTreeMap::new();
		input.insert(v(2), vec![(s(1), vec![mk_diff("a")])]);

		let result = engine.create_partition(input);
		let normalized = normalize(result);

		// Only F1 should have work
		assert_eq!(normalized.len(), 1);
		assert!(normalized.contains_key(&f(1)));
		assert!(!normalized.contains_key(&f(2)));
	}

	#[tokio::test]
	async fn test_complex_multi_flow_multi_version() {
		// S1 -> [F1,F2], S2 -> [F2]
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1), f(2)]);
		subscriptions.insert(s(2), vec![f(2)]);
		let engine = setup_test_engine(subscriptions);

		// V1: S1[a1], S2[b1]
		// V2: S1[a2]
		let mut input = BTreeMap::new();
		input.insert(v(1), vec![(s(1), vec![mk_diff("a1")]), (s(2), vec![mk_diff("b1")])]);
		input.insert(v(2), vec![(s(1), vec![mk_diff("a2")])]);

		let result = engine.create_partition(input);
		let normalized = normalize(result);

		// F1: V1 {S1:1}, V2 {S1:1}
		let f1_units = &normalized[&f(1)];
		assert_eq!(f1_units.len(), 2);

		let (ver1, sources1) = snapshot_unit(&f1_units[0]);
		assert_eq!(ver1, v(1));
		assert_eq!(sources1.get(&s(1)), Some(&1));

		let (ver2, sources2) = snapshot_unit(&f1_units[1]);
		assert_eq!(ver2, v(2));
		assert_eq!(sources2.get(&s(1)), Some(&1));

		// F2: V1 {S1:1, S2:1}, V2 {S1:1}
		let f2_units = &normalized[&f(2)];
		assert_eq!(f2_units.len(), 2);

		let (ver1, sources1) = snapshot_unit(&f2_units[0]);
		assert_eq!(ver1, v(1));
		assert_eq!(sources1.get(&s(1)), Some(&1));
		assert_eq!(sources1.get(&s(2)), Some(&1));

		let (ver2, sources2) = snapshot_unit(&f2_units[1]);
		assert_eq!(ver2, v(2));
		assert_eq!(sources2.get(&s(1)), Some(&1));
	}

	#[tokio::test]
	async fn test_large_diffs_zero_subscribers() {
		let engine = setup_test_engine(HashMap::new());

		// Many diffs but no subscribers
		let diffs: Vec<FlowDiff> = (0..1000).map(|i| mk_diff(&format!("d{}", i))).collect();
		let mut input = BTreeMap::new();
		input.insert(v(1), vec![(s(1), diffs)]);

		let result = engine.create_partition(input);

		assert!(result.is_empty(), "No subscribers means no units");
	}

	#[tokio::test]
	async fn test_many_versions_sparse_changes() {
		// S1 -> [F1]
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1)]);
		let engine = setup_test_engine(subscriptions);

		// 100 versions, but flow only affected by versions 10, 50, 90
		let mut input = BTreeMap::new();
		for i in 1..=100 {
			if i == 10 || i == 50 || i == 90 {
				input.insert(v(i), vec![(s(1), vec![mk_diff(&format!("d{}", i))])]);
			} else {
				// Other sources not subscribed by F1
				input.insert(v(i), vec![(s(999), vec![mk_diff("x")])]);
			}
		}

		let result = engine.create_partition(input);
		let normalized = normalize(result);

		// F1 should only have 3 units
		let f1_units = &normalized[&f(1)];
		assert_eq!(f1_units.len(), 3);
		assert_eq!(f1_units[0].version, v(10));
		assert_eq!(f1_units[1].version, v(50));
		assert_eq!(f1_units[2].version, v(90));
	}

	#[tokio::test]
	async fn test_many_sources_selective_subscription() {
		// F1 subscribes to only S5, S15, S25, S35, S45 out of 50 sources
		let mut subscriptions = HashMap::new();
		for i in 1..=50 {
			if i % 10 == 5 {
				subscriptions.insert(s(i), vec![f(1)]);
			}
		}
		let engine = setup_test_engine(subscriptions);

		// All 50 sources change
		let mut changes = vec![];
		for i in 1..=50 {
			changes.push((s(i), vec![mk_diff(&format!("d{}", i))]));
		}
		let mut input = BTreeMap::new();
		input.insert(v(1), changes);

		let result = engine.create_partition(input);
		let normalized = normalize(result);

		// F1 should have 1 unit with exactly 5 sources
		let f1_units = &normalized[&f(1)];
		assert_eq!(f1_units.len(), 1);
		let (_, sources) = snapshot_unit(&f1_units[0]);
		assert_eq!(sources.len(), 5);
		assert!(sources.contains_key(&s(5)));
		assert!(sources.contains_key(&s(15)));
		assert!(sources.contains_key(&s(25)));
		assert!(sources.contains_key(&s(35)));
		assert!(sources.contains_key(&s(45)));
	}

	#[tokio::test]
	async fn test_input_permutation_invariance() {
		// S1 -> [F1, F2]
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1), f(2)]);
		let engine = setup_test_engine(subscriptions);

		// Create input with versions in different orders
		let test_versions =
			vec![vec![v(10), v(20), v(30)], vec![v(30), v(10), v(20)], vec![v(20), v(30), v(10)]];

		let mut results = vec![];
		for versions in test_versions {
			let mut input = BTreeMap::new();
			for ver in versions {
				input.insert(ver, vec![(s(1), vec![mk_diff(&format!("d{}", ver.0))])]);
			}
			let result = engine.create_partition(input);
			results.push(normalize(result));
		}

		// All permutations should produce the same normalized output
		for i in 1..results.len() {
			assert_normalized_eq(&results[0], &results[i]);
		}

		// Verify correct ordering
		let f1_units = &results[0][&f(1)];
		assert_eq!(f1_units.len(), 3);
		assert_eq!(f1_units[0].version, v(10));
		assert_eq!(f1_units[1].version, v(20));
		assert_eq!(f1_units[2].version, v(30));
	}

	#[tokio::test]
	async fn test_empty_diff_vec_handling() {
		// S1 -> [F1]
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1)]);
		let engine = setup_test_engine(subscriptions);

		// V1: S1 with empty diff vec
		let mut input = BTreeMap::new();
		input.insert(v(1), vec![(s(1), vec![])]);

		let result = engine.create_partition(input);
		let normalized = normalize(result);

		// Current behavior: empty diffs still create a unit with 0 count
		// This documents the actual behavior
		let f1_units = &normalized[&f(1)];
		assert_eq!(f1_units.len(), 1);
		let (ver, sources) = snapshot_unit(&f1_units[0]);
		assert_eq!(ver, v(1));
		assert_eq!(sources.get(&s(1)), Some(&0));
	}

	#[tokio::test]
	async fn test_all_sources_unknown() {
		// S1 -> [F1]
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1)]);
		let engine = setup_test_engine(subscriptions);

		// All input sources are unknown
		let mut input = BTreeMap::new();
		input.insert(v(1), vec![(s(999), vec![mk_diff("x")])]);
		input.insert(v(2), vec![(s(888), vec![mk_diff("y")])]);

		let result = engine.create_partition(input);

		assert!(result.is_empty(), "All unknown sources should produce empty output");
	}

	#[tokio::test]
	async fn test_permutation_regression_fanout() {
		// S1 -> [F1, F2, F3]
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1), f(2), f(3)]);
		let engine = setup_test_engine(subscriptions);

		// Original input
		let mut input = BTreeMap::new();
		input.insert(v(1), vec![(s(1), vec![mk_diff("d1")])]);

		let expected = normalize(engine.create_partition(input.clone()));

		// Test 5 random permutations
		for _ in 0..5 {
			let mut entries: Vec<_> = input.clone().into_iter().collect();
			entries.shuffle(&mut rand::rng());
			let shuffled: BTreeMap<_, _> = entries.into_iter().collect();

			let result = normalize(engine.create_partition(shuffled));
			assert_normalized_eq(&result, &expected);
		}
	}

	#[tokio::test]
	async fn test_permutation_regression_complex() {
		use rand::prelude::*;

		// S1 -> [F1,F2], S2 -> [F2]
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1), f(2)]);
		subscriptions.insert(s(2), vec![f(2)]);
		let engine = setup_test_engine(subscriptions);

		// Original input
		let mut input = BTreeMap::new();
		input.insert(v(1), vec![(s(1), vec![mk_diff("a1")]), (s(2), vec![mk_diff("b1")])]);
		input.insert(v(2), vec![(s(1), vec![mk_diff("a2")])]);

		let expected = normalize(engine.create_partition(input.clone()));

		// Test 5 random permutations
		for _ in 0..5 {
			let mut entries: Vec<_> = input.clone().into_iter().collect();
			entries.shuffle(&mut rand::rng());
			let shuffled: BTreeMap<_, _> = entries.into_iter().collect();

			let result = normalize(engine.create_partition(shuffled));
			assert_normalized_eq(&result, &expected);
		}
	}

	#[tokio::test]
	async fn test_large_input_smoke() {
		// 20 sources, 20 flows, sparse subscriptions
		let mut subscriptions = HashMap::new();
		for flow_i in 1..=20 {
			// Each flow subscribes to 3 sources
			for source_i in ((flow_i - 1) * 3 + 1)..=((flow_i - 1) * 3 + 3) {
				let source_id = s(source_i % 20 + 1);
				subscriptions.entry(source_id).or_insert_with(Vec::new).push(f(flow_i));
			}
		}
		let engine = setup_test_engine(subscriptions);

		// 1000 versions, each with 5 random sources changing
		let mut input = BTreeMap::new();
		for ver_i in 1..=1000 {
			let mut changes = vec![];
			for source_i in 1..=5 {
				changes.push((s((ver_i % 20) + source_i), vec![mk_diff(&format!("d{}", ver_i))]));
			}
			input.insert(v(ver_i), changes);
		}

		// Should complete without panic
		let result = engine.create_partition(input);
		let normalized = normalize(result);

		// Basic sanity checks
		assert!(!normalized.is_empty());

		// Verify all flows have ordered versions
		for (_, units) in normalized {
			for i in 1..units.len() {
				assert!(units[i - 1].version < units[i].version, "Versions not sorted");
			}
		}
	}

	#[tokio::test]
	async fn test_version_ordering_maintained_under_stress() {
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1)]);
		let engine = setup_test_engine(subscriptions);

		let mut rng = rng();
		let mut versions: Vec<u64> = (1..=100).collect();
		for _ in 0..10 {
			versions.shuffle(&mut rng);
		}

		let mut input = BTreeMap::new();
		for ver in versions {
			input.insert(v(ver), vec![(s(1), vec![mk_diff(&format!("d{}", ver))])]);
		}

		let result = engine.create_partition(input);
		let normalized = normalize(result);

		let f1_units = &normalized[&f(1)];
		assert_eq!(f1_units.len(), 100);

		for i in 0..100 {
			assert_eq!(f1_units[i].version, v((i + 1) as u64));
		}
	}

	#[tokio::test]
	async fn test_version_filtering() {
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1)]);
		let engine = setup_test_engine(subscriptions);

		engine.inner.flow_creation_versions.write().insert(f(1), v(50));

		let mut input = BTreeMap::new();
		input.insert(v(40), vec![(s(1), vec![mk_diff("d40")])]);
		input.insert(v(50), vec![(s(1), vec![mk_diff("d50")])]);
		input.insert(v(51), vec![(s(1), vec![mk_diff("d51")])]);
		input.insert(v(60), vec![(s(1), vec![mk_diff("d60")])]);
		input.insert(v(70), vec![(s(1), vec![mk_diff("d70")])]);

		let result = engine.create_partition(input);
		let normalized = normalize(result);

		let f1_units = &normalized[&f(1)];
		assert_eq!(f1_units.len(), 4);
		assert_eq!(f1_units[0].version, v(50));
		assert_eq!(f1_units[1].version, v(51));
		assert_eq!(f1_units[2].version, v(60));
		assert_eq!(f1_units[3].version, v(70));
	}

	#[tokio::test]
	async fn test_backfill_version_per_flow_isolation() {
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1), f(2)]);
		let engine = setup_test_engine(subscriptions);

		engine.inner.flow_creation_versions.write().insert(f(1), v(30));
		engine.inner.flow_creation_versions.write().insert(f(2), v(50));

		let mut input = BTreeMap::new();
		input.insert(v(20), vec![(s(1), vec![mk_diff("d20")])]);
		input.insert(v(40), vec![(s(1), vec![mk_diff("d40")])]);
		input.insert(v(60), vec![(s(1), vec![mk_diff("d60")])]);

		let result = engine.create_partition(input);
		let normalized = normalize(result);

		let f1_units = &normalized[&f(1)];
		assert_eq!(f1_units.len(), 2);
		assert_eq!(f1_units[0].version, v(40));
		assert_eq!(f1_units[1].version, v(60));

		let f2_units = &normalized[&f(2)];
		assert_eq!(f2_units.len(), 1);
		assert_eq!(f2_units[0].version, v(60));
	}

	#[tokio::test]
	async fn test_no_backfill_version_processes_all() {
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1)]);
		let engine = setup_test_engine(subscriptions);

		let mut input = BTreeMap::new();
		input.insert(v(10), vec![(s(1), vec![mk_diff("d10")])]);
		input.insert(v(20), vec![(s(1), vec![mk_diff("d20")])]);
		input.insert(v(30), vec![(s(1), vec![mk_diff("d30")])]);

		let result = engine.create_partition(input);
		let normalized = normalize(result);

		let f1_units = &normalized[&f(1)];
		assert_eq!(f1_units.len(), 3);
	}

	#[tokio::test]
	async fn test_backfill_version_exact_boundary() {
		let mut subscriptions = HashMap::new();
		subscriptions.insert(s(1), vec![f(1)]);
		let engine = setup_test_engine(subscriptions);

		engine.inner.flow_creation_versions.write().insert(f(1), v(100));

		let mut input = BTreeMap::new();
		input.insert(v(99), vec![(s(1), vec![mk_diff("d99")])]);
		input.insert(v(100), vec![(s(1), vec![mk_diff("d100")])]);
		input.insert(v(101), vec![(s(1), vec![mk_diff("d101")])]);

		let result = engine.create_partition(input);
		let normalized = normalize(result);

		let f1_units = &normalized[&f(1)];
		assert_eq!(f1_units.len(), 2);
		assert_eq!(f1_units[0].version, v(100));
		assert_eq!(f1_units[1].version, v(101));
	}
}
