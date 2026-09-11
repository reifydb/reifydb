// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use axum::{
	Json,
	extract::{Path, State},
};
use reifydb::value::value::{datetime::DateTime, uuid::Uuid7};

use crate::{
	dto::{DailyUptimeDto, PublicStatusDto, PublicStatusMonitorDto, PublicStatusRegionDto},
	error::ApiError,
	state::AppState,
	store,
	store::MonitorRegionRow,
};

pub async fn status(State(st): State<AppState>, Path(slug): Path<String>) -> Result<Json<PublicStatusDto>, ApiError> {
	let page = store::find_status_page_by_slug(&st, &slug).await?.ok_or(ApiError::NotFound)?;
	let members = store::status_page_members(&st, page.id, page.owner).await?;
	let since = DateTime::from_nanos(st.clock.now().to_nanos().saturating_sub(store::DAY_NANOS));
	let history_since = store::history_since(st.clock.now().to_nanos());
	let mut daily = store::daily_uptime_by_owner(&st, page.owner, history_since).await?;
	let region_labels = store::region_labels(&st).await?;
	let mut region_daily = store::daily_uptime_by_owner_region(&st, page.owner, history_since).await?;

	let mut regions_by_monitor: HashMap<Uuid7, Vec<MonitorRegionRow>> = HashMap::new();
	for mr in store::monitor_regions_by_owner(&st, page.owner).await? {
		regions_by_monitor.entry(mr.monitor_id).or_default().push(mr);
	}

	let mut monitors = Vec::with_capacity(members.len());
	for monitor_id in members {
		let Some(monitor) = store::find_monitor_owned_by(&st, monitor_id, page.owner).await? else {
			continue;
		};
		let uptime_24h = store::uptime_since(&st, monitor_id, since).await?;
		let mut regions: Vec<PublicStatusRegionDto> = regions_by_monitor
			.remove(&monitor_id)
			.unwrap_or_default()
			.into_iter()
			.map(|mr| PublicStatusRegionDto {
				label: region_labels
					.get(&mr.region_id)
					.cloned()
					.unwrap_or_else(|| "Unknown".to_string()),
				status: if monitor.enabled {
					mr.status.clone()
				} else {
					"unknown".to_string()
				},
				last_checked_at: mr.last_checked_at.as_ref().map(|d| d.to_string()),
				daily: region_daily
					.remove(&(monitor_id, mr.region_id))
					.unwrap_or_default()
					.iter()
					.map(DailyUptimeDto::from_bucket)
					.collect(),
			})
			.collect();
		regions.sort_by(|a, b| a.label.cmp(&b.label));
		monitors.push(PublicStatusMonitorDto {
			name: monitor.name.clone(),
			status: if monitor.enabled {
				monitor.status.clone()
			} else {
				"unknown".to_string()
			},
			uptime_24h,
			last_checked_at: monitor.last_checked_at.as_ref().map(|d| d.to_string()),
			daily: daily
				.remove(&monitor_id)
				.unwrap_or_default()
				.iter()
				.map(DailyUptimeDto::from_bucket)
				.collect(),
			regions,
		});
	}

	Ok(Json(PublicStatusDto {
		title: page.title,
		slug: page.slug,
		monitors,
	}))
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use axum::extract::{Path, State};
	use clap::Parser;
	use reifydb::{
		Database, IdentityId, Value, WithSubsystem, server,
		value::{
			params::Params,
			value::{duration::Duration, into::IntoValue, uuid::Uuid7},
		},
	};
	use reifydb_uptime::migration_path;
	use rustls::crypto::ring::default_provider;

	use super::status;
	use crate::{cli::RunArgs, state::AppState};

	fn build() -> (Database, AppState) {
		let _ = default_provider().install_default();
		let db = server::memory()
			.with_flow(|f| f)
			.with_migrations(migration_path())
			.build()
			.expect("build memory db");
		let st = AppState::new(&db, RunArgs::parse_from(["reifydb-uptime"]));
		(db, st)
	}

	fn params(entries: &[(&str, Value)]) -> Params {
		let map: HashMap<String, Value> = entries.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect();
		Params::from(map)
	}

	fn command_as(db: &Database, id: IdentityId, rql: &str, params: Params) {
		let r = db.engine().command_as(id, rql, params);
		if let Some(e) = r.error {
			panic!("command failed for [{rql}]: {e:?}");
		}
	}

	fn new_user(db: &Database, name: &str) -> IdentityId {
		let r = db.engine().admin_as(IdentityId::root(), &format!("CREATE USER {name}"), Params::None);
		if let Some(e) = r.error {
			panic!("create user {name} failed: {e:?}");
		}
		let r = db.engine().query_as(
			IdentityId::root(),
			"from system::identities filter { name == $name } map { id }",
			params(&[("name", Value::Utf8(name.to_string()))]),
		);
		match r.frames
			.first()
			.and_then(|f| f.columns.iter().find(|c| c.name == "id"))
			.map(|c| c.data.get_value(0))
		{
			Some(Value::IdentityId(id)) => id,
			other => panic!("unexpected identity for {name}: {other:?}"),
		}
	}

	fn create_monitor(db: &Database, owner: IdentityId, name: &str) -> Uuid7 {
		let id = Uuid7::generate(db.clock(), db.engine().rng());
		command_as(
			db,
			owner,
			"CALL uptime::create_monitor($id, $name, $kind, $target, $interval, $timeout, \
			 $http_method, $expected_status, $keyword, $expected_ip, $failure_threshold, $enabled)",
			params(&[
				("id", id.into_value()),
				("name", Value::Utf8(name.to_string())),
				("kind", Value::Utf8("http".to_string())),
				("target", Value::Utf8("https://example.com".to_string())),
				("interval", Duration::from_seconds(60).unwrap().into_value()),
				("timeout", Duration::from_seconds(10).unwrap().into_value()),
				("http_method", Value::none()),
				("expected_status", Value::none()),
				("keyword", Value::none()),
				("expected_ip", Value::none()),
				("failure_threshold", Value::Int2(1)),
				("enabled", Value::Boolean(true)),
			]),
		);
		id
	}

	fn create_page(db: &Database, owner: IdentityId, slug: &str, monitor_id: Uuid7) -> Uuid7 {
		let id = Uuid7::generate(db.clock(), db.engine().rng());
		command_as(
			db,
			owner,
			"CALL uptime::create_status_page($id, $slug, $slug); \
			 CALL uptime::add_status_page_monitor($id, $monitor_id, $position)",
			params(&[
				("id", id.into_value()),
				("slug", Value::Utf8(slug.to_string())),
				("monitor_id", monitor_id.into_value()),
				("position", Value::Int2(0)),
			]),
		);
		id
	}

	fn plant_member(db: &Database, caller: IdentityId, page: Uuid7, monitor_id: Uuid7) {
		command_as(
			db,
			caller,
			"INSERT uptime::status_page_monitors [{ status_page_id: $p, owner: $owner, monitor_id: $m, position: 1 }]",
			params(&[
				("p", page.into_value()),
				("owner", caller.into_value()),
				("m", monitor_id.into_value()),
			]),
		);
	}

	fn published(st: &AppState, slug: &str) -> Vec<String> {
		let page = st.tokio.block_on(status(State(st.clone()), Path(slug.to_string()))).expect("public status");
		page.0.monitors.iter().map(|m| m.name.clone()).collect()
	}

	#[test]
	fn a_member_row_planted_on_another_owners_page_is_not_published() {
		// Any user may insert member rows it owns, so a stranger's row must never reach someone else's page.
		let (db, st) = build();
		let victim = new_user(&db, "victim");
		let attacker = new_user(&db, "attacker");
		let page = create_page(&db, victim, "status", create_monitor(&db, victim, "victim monitor"));
		plant_member(&db, attacker, page, create_monitor(&db, attacker, "planted monitor"));

		assert_eq!(published(&st, "status"), vec!["victim monitor".to_string()]);
	}

	#[test]
	fn another_owners_monitor_planted_on_a_page_is_not_published() {
		// A page owner may list any monitor id in its member rows, so a stranger's monitor must never show.
		let (db, st) = build();
		let victim = new_user(&db, "victim");
		let attacker = new_user(&db, "attacker");
		let page = create_page(&db, attacker, "mine", create_monitor(&db, attacker, "own monitor"));
		plant_member(&db, attacker, page, create_monitor(&db, victim, "victim monitor"));

		assert_eq!(published(&st, "mine"), vec!["own monitor".to_string()]);
	}
}
