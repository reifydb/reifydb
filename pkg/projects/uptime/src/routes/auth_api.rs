// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use axum::{Extension, Json, extract::State, http::StatusCode};
use reifydb::{
	Error, IdentityId,
	auth::{method::password::PasswordProvider, service::AuthResponse},
	core::interface::auth::AuthenticationProvider,
	value::params,
};

use crate::{
	auth::{CurrentUser, valid_email},
	dto::{LoginRequest, LoginResponse, MeDto, RegisterRequest},
	error::ApiError,
	state::AppState,
	store,
};

pub async fn register(
	State(st): State<AppState>,
	Json(request): Json<RegisterRequest>,
) -> Result<StatusCode, ApiError> {
	let email = request.email.trim().to_lowercase();
	if !valid_email(&email) {
		return Err(ApiError::Validation("invalid email address".to_string()));
	}
	if request.password.len() < 8 {
		return Err(ApiError::Validation("password must be at least 8 characters".to_string()));
	}
	if request.password.len() > 512 {
		return Err(ApiError::Validation("password is too long".to_string()));
	}

	if store::find_identity_by_name(&st, &email).await?.is_some() {
		return Err(ApiError::Conflict("an account with this email already exists".to_string()));
	}

	store::exec_admin(&st, format!("CREATE USER `{email}` {{ email: $email }}"), params! { email: email.clone() })
		.await
		.map_err(|_| ApiError::Conflict("an account with this email already exists".to_string()))?;

	let identity = store::find_identity_by_name(&st, &email)
		.await?
		.ok_or_else(|| ApiError::internal("register", "created user not found"))?;

	let st_blocking = st.clone();
	let password = request.password;
	st.tokio.spawn_blocking(move || -> Result<(), Error> {
		let props = PasswordProvider
			.create(&st_blocking.rng, &HashMap::from([("password".to_string(), password)]))?;
		let mut txn = st_blocking.engine.begin_admin(IdentityId::root())?;
		st_blocking.catalog.create_authentication(&mut txn, identity, "password", props)?;
		txn.commit()?;
		Ok(())
	})
	.await
	.map_err(|e| ApiError::internal("register task failed", e))??;

	Ok(StatusCode::CREATED)
}

pub async fn login(
	State(st): State<AppState>,
	Json(request): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, ApiError> {
	let auth = st.auth.clone();
	let credentials = HashMap::from([
		("identifier".to_string(), request.email.trim().to_lowercase()),
		("password".to_string(), request.password),
	]);
	let response =
		st.tokio.spawn_blocking(move || auth.authenticate("password", credentials))
			.await
			.map_err(|e| ApiError::internal("login task failed", e))?;

	match response {
		Ok(AuthResponse::Authenticated {
			identity,
			token,
		}) => Ok(Json(LoginResponse {
			token,
			identity: identity.to_string(),
		})),
		_ => Err(ApiError::Unauthorized),
	}
}

pub async fn me(
	State(st): State<AppState>,
	Extension(CurrentUser(identity)): Extension<CurrentUser>,
) -> Result<Json<MeDto>, ApiError> {
	let name = store::find_identity_name(&st, identity).await?.ok_or(ApiError::Unauthorized)?;
	Ok(Json(MeDto {
		id: identity.to_string(),
		email: name,
	}))
}
