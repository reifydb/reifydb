// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::{
	FromFrame, IdentityId, RetryStrategy, Value,
	value::{
		params::Params,
		value::{datetime::DateTime, duration::Duration, frame::frame::Frame, into::IntoValue, uuid::Uuid7},
	},
};

use crate::{error::ApiError, state::AppState};

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
	pub failure_threshold: i16,
	pub enabled: bool,
	pub created_at: DateTime,
	#[frame(optional)]
	pub last_checked_at: Option<DateTime>,
	pub consecutive_failures: i32,
	pub status: String,
}

#[derive(FromFrame, Clone, Debug)]
pub struct CheckResultRow {
	pub checked_at: DateTime,
	pub success: bool,
	#[frame(optional)]
	pub response_time: Option<Duration>,
	#[frame(optional)]
	pub status_code: Option<i16>,
	#[frame(optional)]
	pub error: Option<String>,
}

#[derive(FromFrame, Clone, Debug)]
pub struct StatusPageRow {
	pub id: Uuid7,
	pub owner: IdentityId,
	pub slug: String,
	pub title: String,
	pub created_at: DateTime,
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

pub async fn list_monitors(st: &AppState, owner: IdentityId) -> Result<Vec<MonitorRow>, ApiError> {
	let frames = exec_query(
		st,
		"from uptime::monitors filter { owner == $owner } sort {created_at:desc}".to_string(),
		reifydb::value::params! { owner: owner },
	)
	.await?;
	rows(&frames)
}

pub async fn find_monitor(st: &AppState, owner: IdentityId, id: Uuid7) -> Result<Option<MonitorRow>, ApiError> {
	let frames = exec_query(
		st,
		"from uptime::monitors filter { id == $id and owner == $owner }".to_string(),
		reifydb::value::params! { id: id, owner: owner },
	)
	.await?;
	Ok(rows::<MonitorRow>(&frames)?.into_iter().next())
}

pub async fn find_monitor_any_owner(st: &AppState, id: Uuid7) -> Result<Option<MonitorRow>, ApiError> {
	let frames = exec_query(
		st,
		"from uptime::monitors filter { id == $id }".to_string(),
		reifydb::value::params! { id: id },
	)
	.await?;
	Ok(rows::<MonitorRow>(&frames)?.into_iter().next())
}

pub async fn enabled_monitors(st: &AppState) -> Result<Vec<MonitorRow>, ApiError> {
	let frames =
		exec_query(st, "from uptime::monitors filter { enabled == true }".to_string(), Params::None).await?;
	rows(&frames)
}

pub async fn insert_monitor(st: &AppState, row: &MonitorRow) -> Result<(), ApiError> {
	let mut map: HashMap<String, Value> = HashMap::new();
	map.insert("id".into(), row.id.into_value());
	map.insert("owner".into(), row.owner.into_value());
	map.insert("name".into(), row.name.clone().into_value());
	map.insert("kind".into(), row.kind.clone().into_value());
	map.insert("target".into(), row.target.clone().into_value());
	map.insert("interval".into(), row.interval.into_value());
	map.insert("timeout".into(), row.timeout.into_value());
	map.insert("http_method".into(), opt_value(row.http_method.clone()));
	map.insert("expected_status".into(), opt_value(row.expected_status));
	map.insert("keyword".into(), opt_value(row.keyword.clone()));
	map.insert("expected_ip".into(), opt_value(row.expected_ip.clone()));
	map.insert("failure_threshold".into(), row.failure_threshold.into_value());
	map.insert("enabled".into(), row.enabled.into_value());
	map.insert("created_at".into(), row.created_at.into_value());
	exec_command(
		st,
		"INSERT uptime::monitors [{ \
			id: $id, owner: $owner, name: $name, kind: $kind, target: $target, \
			interval: $interval, timeout: $timeout, http_method: $http_method, \
			expected_status: $expected_status, keyword: $keyword, expected_ip: $expected_ip, \
			failure_threshold: $failure_threshold, enabled: $enabled, created_at: $created_at, \
			last_checked_at: none, consecutive_failures: 0, status: \"unknown\" \
		}]"
		.to_string(),
		Params::from(map),
	)
	.await?;
	Ok(())
}

pub async fn update_monitor(st: &AppState, owner: IdentityId, row: &MonitorRow) -> Result<(), ApiError> {
	let mut map: HashMap<String, Value> = HashMap::new();
	map.insert("id".into(), row.id.into_value());
	map.insert("owner".into(), owner.into_value());
	map.insert("name".into(), row.name.clone().into_value());
	map.insert("kind".into(), row.kind.clone().into_value());
	map.insert("target".into(), row.target.clone().into_value());
	map.insert("interval".into(), row.interval.into_value());
	map.insert("timeout".into(), row.timeout.into_value());
	map.insert("http_method".into(), opt_value(row.http_method.clone()));
	map.insert("expected_status".into(), opt_value(row.expected_status));
	map.insert("keyword".into(), opt_value(row.keyword.clone()));
	map.insert("expected_ip".into(), opt_value(row.expected_ip.clone()));
	map.insert("failure_threshold".into(), row.failure_threshold.into_value());
	map.insert("enabled".into(), row.enabled.into_value());
	exec_command(
		st,
		"UPDATE uptime::monitors { \
			name: $name, kind: $kind, target: $target, interval: $interval, timeout: $timeout, \
			http_method: $http_method, expected_status: $expected_status, keyword: $keyword, \
			expected_ip: $expected_ip, failure_threshold: $failure_threshold, enabled: $enabled \
		} FILTER id == $id and owner == $owner"
			.to_string(),
		Params::from(map),
	)
	.await?;
	Ok(())
}

pub async fn delete_monitor(st: &AppState, owner: IdentityId, id: Uuid7) -> Result<(), ApiError> {
	exec_command(
		st,
		"DELETE uptime::monitors FILTER id == $id and owner == $owner;\n\
		 DELETE uptime::check_results FILTER monitor_id == $id;\n\
		 DELETE uptime::status_page_monitors FILTER monitor_id == $id"
			.to_string(),
		reifydb::value::params! { id: id, owner: owner },
	)
	.await?;
	Ok(())
}

pub async fn record_result(
	st: &AppState,
	monitor: &MonitorRow,
	checked_at: DateTime,
	success: bool,
	response_time: Option<Duration>,
	status_code: Option<i16>,
	error: Option<String>,
) -> Result<(), ApiError> {
	let failures = if success {
		0
	} else {
		monitor.consecutive_failures.saturating_add(1)
	};
	let status = if success {
		"up".to_string()
	} else if failures >= i32::from(monitor.failure_threshold) {
		"down".to_string()
	} else {
		monitor.status.clone()
	};

	let result_id = Uuid7::generate(&st.clock, &st.rng);
	let mut map: HashMap<String, Value> = HashMap::new();
	map.insert("rid".into(), result_id.into_value());
	map.insert("mid".into(), monitor.id.into_value());
	map.insert("owner".into(), monitor.owner.into_value());
	map.insert("checked_at".into(), checked_at.into_value());
	map.insert("success".into(), success.into_value());
	map.insert("response_time".into(), opt_value(response_time));
	map.insert("status_code".into(), opt_value(status_code));
	map.insert("error".into(), opt_value(error));
	map.insert("failures".into(), failures.into_value());
	map.insert("status".into(), status.into_value());
	exec_command(
		st,
		"INSERT uptime::check_results [{ \
			id: $rid, monitor_id: $mid, owner: $owner, checked_at: $checked_at, \
			success: $success, response_time: $response_time, status_code: $status_code, error: $error \
		}];\n\
		 UPDATE uptime::monitors { \
			last_checked_at: $checked_at, consecutive_failures: $failures, status: $status \
		 } FILTER id == $mid"
			.to_string(),
		Params::from(map),
	)
	.await?;
	Ok(())
}

pub async fn recent_results(st: &AppState, monitor_id: Uuid7) -> Result<Vec<CheckResultRow>, ApiError> {
	let frames = exec_query(
		st,
		"from uptime::check_results filter { monitor_id == $mid } \
		 map { checked_at, success, response_time, status_code, error } \
		 sort {checked_at:desc} take 200"
			.to_string(),
		reifydb::value::params! { mid: monitor_id },
	)
	.await?;
	rows(&frames)
}

pub async fn uptime_since(st: &AppState, monitor_id: Uuid7, since: DateTime) -> Result<Option<f64>, ApiError> {
	let frames = exec_query(
		st,
		"from uptime::check_results filter { monitor_id == $mid and checked_at >= $since } map { success }"
			.to_string(),
		reifydb::value::params! { mid: monitor_id, since: since },
	)
	.await?;
	let results: Vec<SuccessRow> = rows(&frames)?;
	if results.is_empty() {
		return Ok(None);
	}
	let up = results.iter().filter(|r| r.success).count();
	Ok(Some(up as f64 / results.len() as f64))
}

pub async fn list_status_pages(st: &AppState, owner: IdentityId) -> Result<Vec<StatusPageRow>, ApiError> {
	let frames = exec_query(
		st,
		"from uptime::status_pages filter { owner == $owner } sort {created_at:desc}".to_string(),
		reifydb::value::params! { owner: owner },
	)
	.await?;
	rows(&frames)
}

pub async fn find_status_page(st: &AppState, owner: IdentityId, id: Uuid7) -> Result<Option<StatusPageRow>, ApiError> {
	let frames = exec_query(
		st,
		"from uptime::status_pages filter { id == $id and owner == $owner }".to_string(),
		reifydb::value::params! { id: id, owner: owner },
	)
	.await?;
	Ok(rows::<StatusPageRow>(&frames)?.into_iter().next())
}

pub async fn find_status_page_by_slug(st: &AppState, slug: &str) -> Result<Option<StatusPageRow>, ApiError> {
	let frames = exec_query(
		st,
		"from uptime::status_pages filter { slug == $slug }".to_string(),
		reifydb::value::params! { slug: slug },
	)
	.await?;
	Ok(rows::<StatusPageRow>(&frames)?.into_iter().next())
}

pub async fn status_page_members(st: &AppState, page_id: Uuid7) -> Result<Vec<Uuid7>, ApiError> {
	let frames = exec_query(
		st,
		"from uptime::status_page_monitors filter { status_page_id == $pid } \
		 sort {position} map { monitor_id }"
			.to_string(),
		reifydb::value::params! { pid: page_id },
	)
	.await?;
	Ok(rows::<MemberRow>(&frames)?.into_iter().map(|m| m.monitor_id).collect())
}

fn members_insert_statement(page_param: &str, count: usize) -> String {
	let rows: Vec<String> = (0..count)
		.map(|i| format!("{{ status_page_id: ${page_param}, monitor_id: $m{i}, position: $p{i} }}"))
		.collect();
	format!("INSERT uptime::status_page_monitors [{}]", rows.join(", "))
}

pub async fn insert_status_page(st: &AppState, row: &StatusPageRow, monitor_ids: &[Uuid7]) -> Result<(), ApiError> {
	let mut map: HashMap<String, Value> = HashMap::new();
	map.insert("id".into(), row.id.into_value());
	map.insert("owner".into(), row.owner.into_value());
	map.insert("slug".into(), row.slug.clone().into_value());
	map.insert("title".into(), row.title.clone().into_value());
	map.insert("created_at".into(), row.created_at.into_value());
	for (i, mid) in monitor_ids.iter().enumerate() {
		map.insert(format!("m{i}"), mid.into_value());
		map.insert(format!("p{i}"), (i as i16).into_value());
	}
	let rql = format!(
		"INSERT uptime::status_pages [{{ id: $id, owner: $owner, slug: $slug, title: $title, created_at: $created_at }}];\n{}",
		members_insert_statement("id", monitor_ids.len())
	);
	exec_command(st, rql, Params::from(map)).await?;
	Ok(())
}

pub async fn update_status_page(
	st: &AppState,
	owner: IdentityId,
	page_id: Uuid7,
	slug: &str,
	title: &str,
	monitor_ids: &[Uuid7],
) -> Result<(), ApiError> {
	let mut map: HashMap<String, Value> = HashMap::new();
	map.insert("id".into(), page_id.into_value());
	map.insert("owner".into(), owner.into_value());
	map.insert("slug".into(), slug.to_string().into_value());
	map.insert("title".into(), title.to_string().into_value());
	for (i, mid) in monitor_ids.iter().enumerate() {
		map.insert(format!("m{i}"), mid.into_value());
		map.insert(format!("p{i}"), (i as i16).into_value());
	}
	let rql = format!(
		"UPDATE uptime::status_pages {{ slug: $slug, title: $title }} FILTER id == $id and owner == $owner;\n\
		 DELETE uptime::status_page_monitors FILTER status_page_id == $id;\n{}",
		members_insert_statement("id", monitor_ids.len())
	);
	exec_command(st, rql, Params::from(map)).await?;
	Ok(())
}

pub async fn delete_status_page(st: &AppState, owner: IdentityId, id: Uuid7) -> Result<(), ApiError> {
	exec_command(
		st,
		"DELETE uptime::status_pages FILTER id == $id and owner == $owner;\n\
		 DELETE uptime::status_page_monitors FILTER status_page_id == $id"
			.to_string(),
		reifydb::value::params! { id: id, owner: owner },
	)
	.await?;
	Ok(())
}

pub async fn find_identity_by_name(st: &AppState, name: &str) -> Result<Option<IdentityId>, ApiError> {
	let frames = exec_query(
		st,
		"from system::identities filter { name == $name } map { id, name }".to_string(),
		reifydb::value::params! { name: name },
	)
	.await?;
	Ok(rows::<IdentityRow>(&frames)?.into_iter().next().map(|r| r.id))
}

pub async fn find_identity_name(st: &AppState, id: IdentityId) -> Result<Option<String>, ApiError> {
	let frames = exec_query(
		st,
		"from system::identities filter { id == $id } map { id, name }".to_string(),
		reifydb::value::params! { id: id },
	)
	.await?;
	Ok(rows::<IdentityRow>(&frames)?.into_iter().next().map(|r| r.name))
}
