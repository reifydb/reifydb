// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, HashMap, HashSet};

use reifydb::{
	FromFrame, IdentityId, Value,
	core::retry::RetryStrategy,
	engine::engine::StandardEngine,
	runtime::context::rng::Rng,
	value::{
		params,
		params::Params,
		value::{
			date::Date, datetime::DateTime, duration::Duration, frame::frame::Frame, into::IntoValue,
			uuid::Uuid7,
		},
	},
};
use reifydb_client::WsClient;
use tokio::runtime::Handle;

use crate::{checks::CheckOutcome, error::ApiError, state::AppState};

#[derive(FromFrame, Clone, Debug)]
pub struct MonitorRow {
	pub id: Uuid7,
	pub owner: IdentityId,
	pub name: String,
	pub kind: String,
	pub target: String,
	pub interval: Duration,
	pub timeout: Duration,
	#[frame(optional)]
	pub http_method: Option<String>,
	#[frame(optional)]
	pub expected_status: Option<i16>,
	#[frame(optional)]
	pub keyword: Option<String>,
	#[frame(optional)]
	pub expected_ip: Option<String>,
	pub enabled: bool,
	#[frame(optional)]
	pub last_checked_at: Option<DateTime>,
	pub status: String,
}

#[derive(FromFrame, Clone, Debug)]
pub struct JobRow {
	pub monitor_id: Uuid7,
	pub region_id: Uuid7,
}

#[derive(FromFrame, Clone, Debug)]
pub struct MonitorRegionRow {
	pub monitor_id: Uuid7,
	pub owner: IdentityId,
	pub region_id: Uuid7,
	pub status: String,
	#[frame(optional)]
	pub last_checked_at: Option<DateTime>,
}

#[derive(FromFrame, Clone, Debug)]
pub struct StatusPageRow {
	pub id: Uuid7,
	pub owner: IdentityId,
	pub slug: String,
	pub title: String,
}

#[derive(FromFrame)]
struct MemberRow {
	monitor_id: Uuid7,
}

#[derive(FromFrame)]
struct SuccessRow {
	success: bool,
}

#[derive(FromFrame)]
struct IdentityRow {
	id: IdentityId,
	name: String,
}

#[derive(FromFrame)]
struct IdentityKindRow {
	kind: String,
}

pub fn opt_value<T: IntoValue>(v: Option<T>) -> Value {
	v.map(IntoValue::into_value).unwrap_or_else(Value::none)
}

async fn exec_query(st: &AppState, rql: String, params: Params) -> Result<Vec<Frame>, ApiError> {
	let engine = st.engine.clone();
	st.tokio.spawn_blocking(move || {
		let r = engine.query_as(IdentityId::root(), &rql, params);
		match r.error {
			Some(e) => Err(e),
			None => Ok(r.frames),
		}
	})
	.await
	.map_err(|e| ApiError::internal("query task failed", e))?
	.map_err(ApiError::from)
}

async fn exec_command(st: &AppState, rql: String, params: Params) -> Result<Vec<Frame>, ApiError> {
	let engine = st.engine.clone();
	let rng = st.rng.clone();
	st.tokio.spawn_blocking(move || {
		let retry = RetryStrategy::default_conflict_retry();
		let r = retry.execute(&rng, &rql, || engine.command_as(IdentityId::root(), &rql, params.clone()));
		match r.error {
			Some(e) => Err(e),
			None => Ok(r.frames),
		}
	})
	.await
	.map_err(|e| ApiError::internal("command task failed", e))?
	.map_err(ApiError::from)
}

pub enum ProbeBackend {
	Embedded {
		engine: StandardEngine,
		rng: Rng,
		tokio: Handle,
		identity: IdentityId,
	},
	Remote {
		client: WsClient,
	},
}

impl ProbeBackend {
	pub async fn query(&self, rql: &str, params: Params) -> Result<Vec<Frame>, ApiError> {
		match self {
			ProbeBackend::Embedded {
				engine,
				identity,
				tokio,
				..
			} => {
				let engine = engine.clone();
				let identity = *identity;
				let rql = rql.to_string();
				tokio.spawn_blocking(move || {
					let r = engine.query_as(identity, &rql, params);
					match r.error {
						Some(e) => Err(e),
						None => Ok(r.frames),
					}
				})
				.await
				.map_err(|e| ApiError::internal("probe query task failed", e))?
				.map_err(ApiError::from)
			}
			ProbeBackend::Remote {
				client,
			} => client.query(rql, Some(params)).await.map_err(ApiError::from),
		}
	}

	pub async fn command(&self, rql: &str, params: Params) -> Result<Vec<Frame>, ApiError> {
		match self {
			ProbeBackend::Embedded {
				engine,
				rng,
				tokio,
				identity,
			} => {
				let engine = engine.clone();
				let rng = rng.clone();
				let identity = *identity;
				let rql = rql.to_string();
				tokio.spawn_blocking(move || {
					let retry = RetryStrategy::default_conflict_retry();
					let r = retry.execute(&rng, &rql, || {
						engine.command_as(identity, &rql, params.clone())
					});
					match r.error {
						Some(e) => Err(e),
						None => Ok(r.frames),
					}
				})
				.await
				.map_err(|e| ApiError::internal("probe command task failed", e))?
				.map_err(ApiError::from)
			}
			ProbeBackend::Remote {
				client,
			} => client.command(rql, Some(params)).await.map_err(ApiError::from),
		}
	}
}

pub async fn exec_admin(st: &AppState, rql: String, params: Params) -> Result<Vec<Frame>, ApiError> {
	let engine = st.engine.clone();
	let rng = st.rng.clone();
	st.tokio.spawn_blocking(move || {
		let retry = RetryStrategy::default_conflict_retry();
		let r = retry.execute(&rng, &rql, || engine.admin_as(IdentityId::root(), &rql, params.clone()));
		match r.error {
			Some(e) => Err(e),
			None => Ok(r.frames),
		}
	})
	.await
	.map_err(|e| ApiError::internal("admin task failed", e))?
	.map_err(ApiError::from)
}

fn rows<T: FromFrame>(frames: &[Frame]) -> Result<Vec<T>, ApiError> {
	let Some(frame) = frames.first() else {
		return Ok(Vec::new());
	};
	T::from_frame(frame).map_err(|e| ApiError::internal("failed to decode frame", e))
}

pub async fn find_monitor_owned_by(
	st: &AppState,
	id: Uuid7,
	owner: IdentityId,
) -> Result<Option<MonitorRow>, ApiError> {
	let frames = exec_query(
		st,
		"from uptime::monitors filter { id == $id and owner == $owner }".to_string(),
		params! { id: id, owner: owner },
	)
	.await?;
	Ok(rows::<MonitorRow>(&frames)?.into_iter().next())
}

pub async fn enabled_monitors(st: &AppState) -> Result<Vec<MonitorRow>, ApiError> {
	let frames =
		exec_query(st, "from uptime::monitors filter { enabled == true }".to_string(), Params::None).await?;
	rows(&frames)
}

pub async fn all_monitor_regions(st: &AppState) -> Result<Vec<MonitorRegionRow>, ApiError> {
	let frames = exec_query(
		st,
		"from uptime::monitor_regions \
		 map { monitor_id, owner, region_id, status, last_checked_at }"
			.to_string(),
		Params::None,
	)
	.await?;
	rows(&frames)
}

#[derive(FromFrame)]
struct RegionCatalogRow {
	id: Uuid7,
	label: String,
}

pub async fn region_labels(st: &AppState) -> Result<HashMap<Uuid7, String>, ApiError> {
	let frames = exec_query(st, "from uptime::regions map { id, label }".to_string(), Params::None).await?;
	Ok(rows::<RegionCatalogRow>(&frames)?.into_iter().map(|r| (r.id, r.label)).collect())
}

pub async fn monitor_regions_by_owner(st: &AppState, owner: IdentityId) -> Result<Vec<MonitorRegionRow>, ApiError> {
	let frames = exec_query(
		st,
		"from uptime::monitor_regions filter { owner == $owner } \
		 map { monitor_id, owner, region_id, status, last_checked_at }"
			.to_string(),
		params! { owner: owner },
	)
	.await?;
	rows(&frames)
}

#[allow(clippy::too_many_arguments)]
pub async fn report_result(
	backend: &ProbeBackend,
	result_id: Uuid7,
	monitor_id: Uuid7,
	owner: IdentityId,
	region_id: Uuid7,
	probe: IdentityId,
	checked_at: DateTime,
	outcome: CheckOutcome,
) -> Result<(), ApiError> {
	let response_time = outcome.response_time_ms.and_then(|ms| Duration::from_milliseconds(ms).ok());
	let mut map: HashMap<String, Value> = HashMap::new();
	map.insert("result_id".into(), result_id.into_value());
	map.insert("monitor_id".into(), monitor_id.into_value());
	map.insert("owner".into(), owner.into_value());
	map.insert("region_id".into(), region_id.into_value());
	map.insert("probe".into(), probe.into_value());
	map.insert("checked_at".into(), checked_at.into_value());
	map.insert("success".into(), outcome.success.into_value());
	map.insert("response_time".into(), opt_value(response_time));
	map.insert("status_code".into(), opt_value(outcome.status_code));
	map.insert("error".into(), opt_value(outcome.error));
	backend.command(
		"CALL uptime::report_result($result_id, $monitor_id, $owner, $region_id, $probe, \
			 $checked_at, $success, $response_time, $status_code, $error)",
		Params::from(map),
	)
	.await?;
	Ok(())
}

pub async fn register_probe(
	backend: &ProbeBackend,
	probe: IdentityId,
	name: &str,
	seen: DateTime,
) -> Result<(), ApiError> {
	backend.command(
		"CALL uptime::register_probe($probe, $name, $seen)",
		params! { probe: probe, name: name, seen: seen },
	)
	.await?;
	Ok(())
}

pub async fn probe_heartbeat(backend: &ProbeBackend, probe: IdentityId, seen: DateTime) -> Result<(), ApiError> {
	backend.command("CALL uptime::probe_heartbeat($probe, $seen)", params! { probe: probe, seen: seen }).await?;
	Ok(())
}

pub async fn probe_self(backend: &ProbeBackend) -> Result<(IdentityId, String), ApiError> {
	let frames = backend.query("map { id: $identity.id, name: $identity.name }", Params::None).await?;
	let row = rows::<IdentityRow>(&frames)?
		.into_iter()
		.next()
		.ok_or_else(|| ApiError::internal("probe self", "identity query returned no rows"))?;
	Ok((row.id, row.name))
}

pub async fn find_monitor_for_check(backend: &ProbeBackend, monitor_id: Uuid7) -> Result<Option<MonitorRow>, ApiError> {
	let frames =
		backend.command("CALL uptime::find_monitor($monitor_id)", params! { monitor_id: monitor_id }).await?;
	Ok(rows::<MonitorRow>(&frames)?.into_iter().next())
}

pub async fn enqueue_job(st: &AppState, job_id: Uuid7, monitor_id: Uuid7, region_id: Uuid7) -> Result<(), ApiError> {
	exec_command(
		st,
		"CALL uptime::enqueue_job($job_id, $monitor_id, $region_id)".to_string(),
		params! { job_id: job_id, monitor_id: monitor_id, region_id: region_id },
	)
	.await?;
	Ok(())
}

pub async fn claim_job(backend: &ProbeBackend, monitor_id: Uuid7, region: Uuid7) -> Result<Option<JobRow>, ApiError> {
	let frames = backend
		.command(
			"CALL uptime::claim_job_in_region($monitor_id, $region)",
			params! { monitor_id: monitor_id, region: region },
		)
		.await?;
	match frames.first() {
		Some(frame) if frame.column("monitor_id").is_some() && frame.row_count() > 0 => {
			Ok(rows::<JobRow>(&frames)?.into_iter().next())
		}
		_ => Ok(None),
	}
}

pub async fn pending_job_monitors(backend: &ProbeBackend, region: Uuid7) -> Result<Vec<Uuid7>, ApiError> {
	let frames = backend
		.query(
			"from uptime::jobs filter { region_id == $region } map { monitor_id }",
			params! { region: region },
		)
		.await?;
	let ids: HashSet<Uuid7> = rows::<MemberRow>(&frames)?.into_iter().map(|m| m.monitor_id).collect();
	Ok(ids.into_iter().collect())
}

pub async fn region_id_by_label(backend: &ProbeBackend, label: &str) -> Result<Option<Uuid7>, ApiError> {
	let frames = backend
		.query("from uptime::regions filter { label == $label } map { id, label }", params! { label: label })
		.await?;
	Ok(rows::<RegionCatalogRow>(&frames)?.into_iter().next().map(|r| r.id))
}

pub const UPTIME_HISTORY_DAYS: i64 = 90;

pub const DAY_NANOS: u64 = 24 * 3600 * 1_000_000_000;

pub fn history_since(now_nanos: u64) -> DateTime {
	let today = i64::from(DateTime::from_nanos(now_nanos).date().to_days_since_epoch());
	let start_day = (today - (UPTIME_HISTORY_DAYS - 1)).max(0);
	DateTime::from_nanos(start_day as u64 * DAY_NANOS)
}

#[derive(FromFrame)]
struct DayCountRow {
	monitor_id: Uuid7,
	day: Date,
	n: i64,
}

#[derive(Clone, Debug)]
pub struct DayBucket {
	pub day: Date,
	pub total: i64,
	pub up: i64,
}

pub async fn daily_uptime_by_owner(
	st: &AppState,
	owner: IdentityId,
	since: DateTime,
) -> Result<HashMap<Uuid7, Vec<DayBucket>>, ApiError> {
	let totals = exec_query(
		st,
		"from uptime::results filter { owner == $owner and checked_at >= $since } \
		 map { monitor_id, day: datetime::date(checked_at) } \
		 aggregate { n: math::count(day) } by { monitor_id, day }"
			.to_string(),
		params! { owner: owner, since: since },
	)
	.await?;
	let ups = exec_query(
		st,
		"from uptime::results filter { owner == $owner and checked_at >= $since and success == true } \
		 map { monitor_id, day: datetime::date(checked_at) } \
		 aggregate { n: math::count(day) } by { monitor_id, day }"
			.to_string(),
		params! { owner: owner, since: since },
	)
	.await?;
	let mut merged: HashMap<Uuid7, BTreeMap<Date, DayBucket>> = HashMap::new();
	for row in rows::<DayCountRow>(&totals)? {
		merged.entry(row.monitor_id).or_default().insert(
			row.day,
			DayBucket {
				day: row.day,
				total: row.n,
				up: 0,
			},
		);
	}
	for row in rows::<DayCountRow>(&ups)? {
		if let Some(days) = merged.get_mut(&row.monitor_id)
			&& let Some(bucket) = days.get_mut(&row.day)
		{
			bucket.up = row.n;
		}
	}
	Ok(merged.into_iter().map(|(id, days)| (id, days.into_values().collect())).collect())
}

#[derive(FromFrame)]
struct RegionDayCountRow {
	monitor_id: Uuid7,
	region_id: Uuid7,
	day: Date,
	n: i64,
}

pub async fn daily_uptime_by_owner_region(
	st: &AppState,
	owner: IdentityId,
	since: DateTime,
) -> Result<HashMap<(Uuid7, Uuid7), Vec<DayBucket>>, ApiError> {
	let totals = exec_query(
		st,
		"from uptime::results filter { owner == $owner and checked_at >= $since } \
		 map { monitor_id, region_id, day: datetime::date(checked_at) } \
		 aggregate { n: math::count(day) } by { monitor_id, region_id, day }"
			.to_string(),
		params! { owner: owner, since: since },
	)
	.await?;
	let ups = exec_query(
		st,
		"from uptime::results filter { owner == $owner and checked_at >= $since and success == true } \
		 map { monitor_id, region_id, day: datetime::date(checked_at) } \
		 aggregate { n: math::count(day) } by { monitor_id, region_id, day }"
			.to_string(),
		params! { owner: owner, since: since },
	)
	.await?;
	let mut merged: HashMap<(Uuid7, Uuid7), BTreeMap<Date, DayBucket>> = HashMap::new();
	for row in rows::<RegionDayCountRow>(&totals)? {
		merged.entry((row.monitor_id, row.region_id)).or_default().insert(
			row.day,
			DayBucket {
				day: row.day,
				total: row.n,
				up: 0,
			},
		);
	}
	for row in rows::<RegionDayCountRow>(&ups)? {
		if let Some(days) = merged.get_mut(&(row.monitor_id, row.region_id))
			&& let Some(bucket) = days.get_mut(&row.day)
		{
			bucket.up = row.n;
		}
	}
	Ok(merged.into_iter().map(|(k, days)| (k, days.into_values().collect())).collect())
}

pub async fn uptime_since(st: &AppState, monitor_id: Uuid7, since: DateTime) -> Result<Option<f64>, ApiError> {
	let frames = exec_query(
		st,
		"from uptime::results filter { monitor_id == $mid and checked_at >= $since } map { success }"
			.to_string(),
		params! { mid: monitor_id, since: since },
	)
	.await?;
	let results: Vec<SuccessRow> = rows(&frames)?;
	if results.is_empty() {
		return Ok(None);
	}
	let up = results.iter().filter(|r| r.success).count();
	Ok(Some(up as f64 / results.len() as f64))
}

pub async fn find_status_page_by_slug(st: &AppState, slug: &str) -> Result<Option<StatusPageRow>, ApiError> {
	let frames = exec_query(
		st,
		"from uptime::status_pages filter { slug == $slug }".to_string(),
		params! { slug: slug },
	)
	.await?;
	Ok(rows::<StatusPageRow>(&frames)?.into_iter().next())
}

pub async fn status_page_members(st: &AppState, page_id: Uuid7, owner: IdentityId) -> Result<Vec<Uuid7>, ApiError> {
	let frames = exec_query(
		st,
		"from uptime::status_page_monitors filter { status_page_id == $pid and owner == $owner } \
		 sort {position} map { monitor_id }"
			.to_string(),
		params! { pid: page_id, owner: owner },
	)
	.await?;
	Ok(rows::<MemberRow>(&frames)?.into_iter().map(|m| m.monitor_id).collect())
}

pub async fn find_identity_by_name(st: &AppState, name: &str) -> Result<Option<IdentityId>, ApiError> {
	let frames = exec_query(
		st,
		"from system::identities filter { name == $name } map { id, name }".to_string(),
		params! { name: name },
	)
	.await?;
	Ok(rows::<IdentityRow>(&frames)?.into_iter().next().map(|r| r.id))
}

pub struct IdentitySummary {
	pub kind: String,
}

pub async fn find_identity_summary(st: &AppState, id: IdentityId) -> Result<Option<IdentitySummary>, ApiError> {
	let frames = exec_query(
		st,
		"from system::identities filter { id == $id } map { kind }".to_string(),
		params! { id: id },
	)
	.await?;
	Ok(rows::<IdentityKindRow>(&frames)?.into_iter().next().map(|r| IdentitySummary {
		kind: r.kind,
	}))
}
