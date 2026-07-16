// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use axum::{
	Json,
	extract::{Path, State},
};
use reifydb::value::value::datetime::DateTime;

use crate::{
	dto::{DailyUptimeDto, PublicStatusDto, PublicStatusMonitorDto},
	error::ApiError,
	state::AppState,
	store,
};

pub async fn status(State(st): State<AppState>, Path(slug): Path<String>) -> Result<Json<PublicStatusDto>, ApiError> {
	let page = store::find_status_page_by_slug(&st, &slug).await?.ok_or(ApiError::NotFound)?;
	let members = store::status_page_members(&st, page.id).await?;
	let since = DateTime::from_nanos(st.clock.now_nanos().saturating_sub(store::DAY_NANOS));
	let history_since = store::history_since(st.clock.now_nanos());
	let mut daily = store::daily_uptime_by_owner(&st, page.owner, history_since).await?;

	let mut monitors = Vec::with_capacity(members.len());
	for monitor_id in members {
		let Some(monitor) = store::find_monitor_any_owner(&st, monitor_id).await? else {
			continue;
		};
		let uptime_24h = store::uptime_since(&st, monitor_id, since).await?;
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
		});
	}

	Ok(Json(PublicStatusDto {
		title: page.title,
		slug: page.slug,
		monitors,
	}))
}
