// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use axum::{
	Extension, Json,
	extract::State,
	http::{HeaderMap, StatusCode},
};
use reifydb::{
	Error, IdentityId,
	auth::{method::password::PasswordProvider, service::AuthResponse},
	core::interface::auth::AuthenticationProvider,
	value::params,
};

use crate::{
	auth::{CurrentUser, bearer_token, identity_for_token, valid_email},
	dto::{GuestSessionResponse, LoginRequest, LoginResponse, MeDto, RegisterRequest},
	error::ApiError,
	guest::{self, PromotionError},
	state::AppState,
	store,
};

pub async fn guest_session(State(st): State<AppState>) -> Result<Json<GuestSessionResponse>, ApiError> {
	let st_blocking = st.clone();
	let session =
		st.tokio.spawn_blocking(move || -> Result<_, Error> {
			let identity = guest::create_guest(
				&st_blocking.catalog,
				&st_blocking.engine,
				&st_blocking.clock,
				&st_blocking.rng,
			)?;
			let token = st_blocking.auth.create_session(identity, Some(guest::guest_session_ttl()))?;
			Ok((identity, token))
		})
		.await
		.map_err(|e| ApiError::internal("guest session task failed", e))??;

	let (identity, token) = session;
	Ok(Json(GuestSessionResponse {
		token: token.token,
		identity: identity.to_string(),
		expires_at: token.expires_at.map(|at| (at.to_nanos() / 1_000_000_000) as i64).unwrap_or_default(),
	}))
}

pub async fn register(
	State(st): State<AppState>,
	headers: HeaderMap,
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

	if let Some(identity) = current_guest(&st, &headers).await? {
		return promote(&st, identity, email, request.password).await;
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

async fn current_guest(st: &AppState, headers: &HeaderMap) -> Result<Option<IdentityId>, ApiError> {
	let Some(token) = bearer_token(headers) else {
		return Ok(None);
	};
	let Some(identity) = identity_for_token(st, token).await? else {
		return Ok(None);
	};
	let Some(summary) = store::find_identity_summary(st, identity).await? else {
		return Ok(None);
	};
	Ok(guest::is_guest_kind(&summary.kind).then_some(identity))
}

async fn promote(st: &AppState, identity: IdentityId, email: String, password: String) -> Result<StatusCode, ApiError> {
	let st_blocking = st.clone();
	let outcome =
		st.tokio.spawn_blocking(move || {
			guest::promote_guest(
				&st_blocking.catalog,
				&st_blocking.engine,
				&st_blocking.rng,
				identity,
				&email,
				password,
			)
		})
		.await
		.map_err(|e| ApiError::internal("guest promotion task failed", e))?;

	match outcome {
		Ok(()) => Ok(StatusCode::CREATED),
		Err(PromotionError::NotAGuest) => {
			Err(ApiError::Conflict("this session already belongs to a registered account".to_string()))
		}
		Err(PromotionError::Database(err)) => Err(ApiError::from(err)),
	}
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
	let summary = store::find_identity_summary(&st, identity).await?.ok_or(ApiError::Unauthorized)?;
	let guest = guest::is_guest_kind(&summary.kind);
	Ok(Json(MeDto {
		id: identity.to_string(),
		email: (!guest).then_some(summary.name),
		guest,
	}))
}
