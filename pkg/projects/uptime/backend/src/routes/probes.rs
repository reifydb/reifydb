// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use axum::{Json, extract::State};

use crate::{dto::ProbeDto, error::ApiError, state::AppState, store};

pub async fn list(State(st): State<AppState>) -> Result<Json<Vec<ProbeDto>>, ApiError> {
	let probes = store::list_probes(&st).await?;
	Ok(Json(probes.iter().map(ProbeDto::from_row).collect()))
}
