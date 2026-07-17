// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use axum::{
	Extension, Json,
	extract::{Path, State},
	http::StatusCode,
};
use reifydb::value::value::{datetime::DateTime, duration::Duration, uuid::Uuid7};
use uuid::Uuid;

use crate::{
	auth::CurrentUser,
	dto::{CheckResultDto, DailyUptimeDto, MonitorDailyDto, MonitorDto, MonitorInput},
	error::ApiError,
	state::AppState,
	store,
	store::MonitorRow,
};

fn parse_id(id: &str) -> Result<Uuid7, ApiError> {
	Uuid::parse_str(id).map(Uuid7::from).map_err(|_| ApiError::NotFound)
}

fn duration_from_ms(ms: i64, field: &str) -> Result<Duration, ApiError> {
	Duration::from_milliseconds(ms).map_err(|_| ApiError::Validation(format!("invalid {field}")))
}

pub async fn list(
	State(st): State<AppState>,
	Extension(CurrentUser(owner)): Extension<CurrentUser>,
) -> Result<Json<Vec<MonitorDto>>, ApiError> {
	let monitors = store::list_monitors(&st, owner).await?;
	Ok(Json(monitors.iter().map(MonitorDto::from_row).collect()))
}

pub async fn daily(
	State(st): State<AppState>,
	Extension(CurrentUser(owner)): Extension<CurrentUser>,
) -> Result<Json<Vec<MonitorDailyDto>>, ApiError> {
	let monitors = store::list_monitors(&st, owner).await?;
	let since = store::history_since(st.clock.now_nanos());
	let mut daily = store::daily_uptime_by_owner(&st, owner, since).await?;
	Ok(Json(monitors
		.iter()
		.map(|m| MonitorDailyDto {
			monitor_id: m.id.to_string(),
			daily: daily
				.remove(&m.id)
				.unwrap_or_default()
				.iter()
				.map(DailyUptimeDto::from_bucket)
				.collect(),
		})
		.collect()))
}

pub async fn get(
	State(st): State<AppState>,
	Extension(CurrentUser(owner)): Extension<CurrentUser>,
	Path(id): Path<String>,
) -> Result<Json<MonitorDto>, ApiError> {
	let id = parse_id(&id)?;
	let monitor = store::find_monitor(&st, owner, id).await?.ok_or(ApiError::NotFound)?;
	Ok(Json(MonitorDto::from_row(&monitor)))
}

pub async fn create(
	State(st): State<AppState>,
	Extension(CurrentUser(owner)): Extension<CurrentUser>,
	Json(input): Json<MonitorInput>,
) -> Result<(StatusCode, Json<MonitorDto>), ApiError> {
	input.validate()?;
	let row = MonitorRow {
		id: Uuid7::generate(&st.clock, &st.rng),
		owner,
		name: input.name.trim().to_string(),
		kind: input.kind.clone(),
		target: input.target.trim().to_string(),
		interval: duration_from_ms(input.interval_ms, "interval")?,
		timeout: duration_from_ms(input.timeout_ms, "timeout")?,
		http_method: input.http_method.clone(),
		expected_status: input.expected_status,
		keyword: input.keyword.clone(),
		expected_ip: input.expected_ip.clone(),
		failure_threshold: input.failure_threshold,
		enabled: input.enabled,
		created_at: DateTime::from_nanos(st.clock.now_nanos()),
		last_checked_at: None,
		consecutive_failures: 0,
		status: "unknown".to_string(),
	};
	store::insert_monitor(&st, &row).await?;
	Ok((StatusCode::CREATED, Json(MonitorDto::from_row(&row))))
}

pub async fn update(
	State(st): State<AppState>,
	Extension(CurrentUser(owner)): Extension<CurrentUser>,
	Path(id): Path<String>,
	Json(input): Json<MonitorInput>,
) -> Result<Json<MonitorDto>, ApiError> {
	let id = parse_id(&id)?;
	input.validate()?;
	let existing = store::find_monitor(&st, owner, id).await?.ok_or(ApiError::NotFound)?;
	let row = MonitorRow {
		id,
		owner,
		name: input.name.trim().to_string(),
		kind: input.kind.clone(),
		target: input.target.trim().to_string(),
		interval: duration_from_ms(input.interval_ms, "interval")?,
		timeout: duration_from_ms(input.timeout_ms, "timeout")?,
		http_method: input.http_method.clone(),
		expected_status: input.expected_status,
		keyword: input.keyword.clone(),
		expected_ip: input.expected_ip.clone(),
		failure_threshold: input.failure_threshold,
		enabled: input.enabled,
		created_at: existing.created_at,
		last_checked_at: existing.last_checked_at,
		consecutive_failures: existing.consecutive_failures,
		status: existing.status,
	};
	store::update_monitor(&st, owner, &row).await?;
	Ok(Json(MonitorDto::from_row(&row)))
}

pub async fn delete(
	State(st): State<AppState>,
	Extension(CurrentUser(owner)): Extension<CurrentUser>,
	Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
	let id = parse_id(&id)?;
	store::find_monitor(&st, owner, id).await?.ok_or(ApiError::NotFound)?;
	store::delete_monitor(&st, owner, id).await?;
	Ok(StatusCode::NO_CONTENT)
}

pub async fn results(
	State(st): State<AppState>,
	Extension(CurrentUser(owner)): Extension<CurrentUser>,
	Path(id): Path<String>,
) -> Result<Json<Vec<CheckResultDto>>, ApiError> {
	let id = parse_id(&id)?;
	store::find_monitor(&st, owner, id).await?.ok_or(ApiError::NotFound)?;
	let results = store::recent_results(&st, id).await?;
	Ok(Json(results.iter().map(CheckResultDto::from_row).collect()))
}
