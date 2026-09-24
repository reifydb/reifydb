// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, time::Duration as StdDuration};

use reifydb::{
	ConfigKey, Value, WithSubsystem, embedded,
	testing::db::{TestDb, await_value},
};
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::{WindowRequirements, WindowSizeDomain},
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	key::operator::state::{GroupId, managed_key_in, unmanaged_key_in},
	operator_with::ApplyWith,
};
use reifydb_runtime::{RuntimeConfig, fatal::FatalConfig};
use reifydb_sdk::{
	error::Result as SdkResult,
	flow::operator::{
		ManagedOperator, OperatorMetadata, UnmanagedOperator,
		column::operator::OperatorColumn,
		context::{ClassState, GuestContext, Managed, Unmanaged},
		view::{ChangeView, ColumnsView, DiffView, RowView},
	},
};
use reifydb_test_harness::assert::column_values;
use reifydb_value::{
	config::ExtensionParams,
	value::{constraint::TypeConstraint, value_type::ValueType},
};

const TIMEOUT: StdDuration = StdDuration::from_secs(20);

const KEEPER_REASON: &str = "keeps one key per group for as long as it runs";

const G_COLUMNS: &[OperatorColumn] = &[OperatorColumn {
	name: "g",
	type_constraint: TypeConstraint::unconstrained(ValueType::Int4),
	description: "group key",
}];

fn groups(change: &impl ChangeView) -> Vec<GroupId> {
	// Every post row names one group, so an owner that misses a row leaves no key to census.
	let mut out = Vec::new();
	for i in 0..change.diff_count() {
		let Some(diff) = change.diff(i) else {
			continue;
		};
		let Some(post) = diff.post() else {
			continue;
		};
		for r in 0..post.row_count() {
			let g = post.row(r).expect("row").i32("g").unwrap().expect("g");
			out.push(GroupId::of(&EncodedKey::new(g.to_be_bytes())));
		}
	}
	out
}

struct Tally;

impl OperatorMetadata for Tally {
	const NAME: &'static str = "tally";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test-only managed operator that keeps one key per group";
	const INPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl ManagedOperator for Tally {
	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
		Ok(Tally)
	}

	fn apply(&mut self, ctx: &mut impl GuestContext<Managed>, change: impl ChangeView) -> SdkResult<()> {
		for group in groups(&change) {
			ctx.state().set(&managed_key_in(group, &[]).expect("an empty id fits the keyspace"), &1i64)?;
		}
		Ok(())
	}
}

struct Keeper;

impl OperatorMetadata for Keeper {
	const NAME: &'static str = "keeper";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "test-only unmanaged operator that keeps one key per group";
	const INPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = G_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl UnmanagedOperator for Keeper {
	const UNMANAGED_BECAUSE: &'static str = KEEPER_REASON;
	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: false,
		kinds: &[],
		domain: WindowSizeDomain::Time,
		needs_pane: false,
	};

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
		Ok(Keeper)
	}

	fn apply(&mut self, ctx: &mut impl GuestContext<Unmanaged>, change: impl ChangeView) -> SdkResult<()> {
		for group in groups(&change) {
			ctx.state()
				.set(&unmanaged_key_in(group, &[]).expect("an empty id fits the keyspace"), &1i64)?;
		}
		Ok(())
	}
}

fn memory() -> TestDb {
	TestDb::from(
		embedded::memory()
			.with_runtime_config(RuntimeConfig::default().fatal(FatalConfig::disarmed()))
			.with_flow(|f| f.register_managed_operator::<Tally>().register_unmanaged_operator::<Keeper>())
			.with_config(ConfigKey::MetricsFlushInterval, Value::duration_milliseconds(10))
			.with_config(ConfigKey::MetricsSampleInterval, Value::duration_milliseconds(20))
			.build()
			.expect("build memory db with flow"),
	)
}

fn text_or_none(value: Value) -> Option<String> {
	// A none rendered as an empty string would read as an owner with no name.
	match value {
		Value::Utf8(text) => Some(text),
		Value::None {
			..
		} => None,
		other => panic!("an owner column must be text or none, found {other:?}"),
	}
}

fn owners(db: &TestDb, keyspace: Option<&str>) -> BTreeSet<(Option<String>, Option<String>)> {
	let query = match keyspace {
		Some(keyspace) => {
			format!("from system::metrics::flow::state::current filter {{ keyspace == '{keyspace}' }}")
		}
		None => "from system::metrics::flow::state::current".to_string(),
	};
	db.query(&query)
		.iter()
		.flat_map(|frame| {
			column_values(frame, "name").into_iter().zip(column_values(frame, "unmanaged_because"))
		})
		.map(|(name, reason)| (text_or_none(name), text_or_none(reason)))
		.collect()
}

#[test]
fn each_census_row_names_its_apply_owner_and_only_an_unmanaged_owner_says_why() {
	// An anonymous id hides who grows without bound; a reason on the wrong row blames the wrong owner.
	let db = memory();
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { id: int4, g: int4, ts: datetime } with { time: event(ts) }");
	db.admin("CREATE DEFERRED VIEW app::m { g: int4 } AS { FROM app::t APPLY tally{} WITH { lateness: 1h } }");
	db.admin("CREATE DEFERRED VIEW app::u { g: int4 } AS { FROM app::t APPLY keeper{} }");
	db.admin(
		"CREATE DEFERRED VIEW app::a { g: int4, n: int8 } AS { FROM app::t | aggregate { n: math::count(id) } by { g } }",
	);
	db.command(
		r#"INSERT app::t [{ id: 1, g: 1, ts: "2026-01-01T00:00:00Z" }, { id: 2, g: 2, ts: "2026-01-01T00:00:00Z" }]"#,
	);

	let keeper = (Some("keeper".to_string()), Some(KEEPER_REASON.to_string()));
	let tally = (Some("tally".to_string()), None);
	let built_in = (None, None);

	let unmanaged = BTreeSet::from([keeper.clone()]);
	assert_eq!(
		await_value(unmanaged.clone(), TIMEOUT, || owners(&db, Some("CUSTOM_UNMANAGED"))),
		unmanaged,
		"the unmanaged keyspace must name its apply owner and carry that owner's reason"
	);

	let managed = BTreeSet::from([tally.clone()]);
	assert_eq!(
		await_value(managed.clone(), TIMEOUT, || owners(&db, Some("CUSTOM_MANAGED"))),
		managed,
		"a managed owner is named but has no reason to give"
	);

	let every = BTreeSet::from([keeper, tally, built_in]);
	assert_eq!(
		await_value(every.clone(), TIMEOUT, || owners(&db, None)),
		every,
		"a built-in operator has no apply name, so its rows must carry none for both, never a guessed owner"
	);
}
