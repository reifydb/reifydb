// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashSet;

use axum::{
	Extension, Json,
	extract::{Path, State},
	http::StatusCode,
};
use reifydb::{
	IdentityId,
	value::value::{datetime::DateTime, uuid::Uuid7},
};
use uuid::Uuid;

use crate::{
	auth::CurrentUser,
	dto::{StatusPageDto, StatusPageInput},
	error::ApiError,
	state::AppState,
	store,
	store::StatusPageRow,
};

fn parse_id(id: &str) -> Result<Uuid7, ApiError> {
	Uuid::parse_str(id).map(Uuid7::from).map_err(|_| ApiError::NotFound)
}

async fn validated_monitor_ids(
	st: &AppState,
	owner: IdentityId,
	input: &StatusPageInput,
) -> Result<Vec<Uuid7>, ApiError> {
	let mut ids = Vec::with_capacity(input.monitor_ids.len());
	let mut seen = HashSet::new();
	for raw in &input.monitor_ids {
		let id = Uuid::parse_str(raw)
			.map(Uuid7::from)
			.map_err(|_| ApiError::Validation(format!("invalid monitor id: {raw}")))?;
		if seen.insert(id) {
			ids.push(id);
		}
	}
	let owned: HashSet<Uuid7> = store::list_monitors(st, owner).await?.into_iter().map(|m| m.id).collect();
	for id in &ids {
		if !owned.contains(id) {
			return Err(ApiError::Validation(format!("unknown monitor id: {id}")));
		}
	}
	Ok(ids)
}

async fn page_dto(st: &AppState, page: &StatusPageRow) -> Result<StatusPageDto, ApiError> {
	let members = store::status_page_members(st, page.id).await?;
	Ok(StatusPageDto::from_row(page, members.iter().map(|m| m.to_string()).collect()))
}

pub async fn list(
	State(st): State<AppState>,
	Extension(CurrentUser(owner)): Extension<CurrentUser>,
) -> Result<Json<Vec<StatusPageDto>>, ApiError> {
	let pages = store::list_status_pages(&st, owner).await?;
	let mut dtos = Vec::with_capacity(pages.len());
	for page in &pages {
		dtos.push(page_dto(&st, page).await?);
	}
	Ok(Json(dtos))
}

pub async fn get(
	State(st): State<AppState>,
	Extension(CurrentUser(owner)): Extension<CurrentUser>,
	Path(id): Path<String>,
) -> Result<Json<StatusPageDto>, ApiError> {
	let id = parse_id(&id)?;
	let page = store::find_status_page(&st, owner, id).await?.ok_or(ApiError::NotFound)?;
	Ok(Json(page_dto(&st, &page).await?))
}

pub async fn create(
	State(st): State<AppState>,
	Extension(CurrentUser(owner)): Extension<CurrentUser>,
	Json(input): Json<StatusPageInput>,
) -> Result<(StatusCode, Json<StatusPageDto>), ApiError> {
	input.validate()?;
	if store::find_status_page_by_slug(&st, &input.slug).await?.is_some() {
		return Err(ApiError::Conflict("this slug is already taken".to_string()));
	}
	let monitor_ids = validated_monitor_ids(&st, owner, &input).await?;
	let row = StatusPageRow {
		id: Uuid7::generate(&st.clock, &st.rng),
		owner,
		slug: input.slug.clone(),
		title: input.title.trim().to_string(),
		created_at: DateTime::from_nanos(st.clock.now_nanos()),
	};
	store::insert_status_page(&st, &row, &monitor_ids).await?;
	let dto = StatusPageDto::from_row(&row, monitor_ids.iter().map(|m| m.to_string()).collect());
	Ok((StatusCode::CREATED, Json(dto)))
}

pub async fn update(
	State(st): State<AppState>,
	Extension(CurrentUser(owner)): Extension<CurrentUser>,
	Path(id): Path<String>,
	Json(input): Json<StatusPageInput>,
) -> Result<Json<StatusPageDto>, ApiError> {
	let id = parse_id(&id)?;
	input.validate()?;
	let page = store::find_status_page(&st, owner, id).await?.ok_or(ApiError::NotFound)?;
	if let Some(existing) = store::find_status_page_by_slug(&st, &input.slug).await?
		&& existing.id != id
	{
		return Err(ApiError::Conflict("this slug is already taken".to_string()));
	}
	let monitor_ids = validated_monitor_ids(&st, owner, &input).await?;
	store::update_status_page(&st, owner, id, &input.slug, input.title.trim(), &monitor_ids).await?;
	let row = StatusPageRow {
		id,
		owner,
		slug: input.slug.clone(),
		title: input.title.trim().to_string(),
		created_at: page.created_at,
	};
	let dto = StatusPageDto::from_row(&row, monitor_ids.iter().map(|m| m.to_string()).collect());
	Ok(Json(dto))
}

pub async fn delete(
	State(st): State<AppState>,
	Extension(CurrentUser(owner)): Extension<CurrentUser>,
	Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
	let id = parse_id(&id)?;
	store::find_status_page(&st, owner, id).await?.ok_or(ApiError::NotFound)?;
	store::delete_status_page(&st, owner, id).await?;
	Ok(StatusCode::NO_CONTENT)
}
