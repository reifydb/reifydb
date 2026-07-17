// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reqwest::Url;
use serde::{Deserialize, Serialize};

use crate::{
	error::ApiError,
	store::{CheckResultRow, DayBucket, MonitorRow, StatusPageRow},
};

#[derive(Serialize)]
pub struct MonitorDto {
	pub id: String,
	pub name: String,
	pub kind: String,
	pub target: String,
	pub interval_ms: i64,
	pub timeout_ms: i64,
	pub http_method: Option<String>,
	pub expected_status: Option<i16>,
	pub keyword: Option<String>,
	pub expected_ip: Option<String>,
	pub failure_threshold: i16,
	pub enabled: bool,
	pub status: String,
	pub created_at: String,
	pub last_checked_at: Option<String>,
	pub consecutive_failures: i32,
}

impl MonitorDto {
	pub fn from_row(row: &MonitorRow) -> Self {
		Self {
			id: row.id.to_string(),
			name: row.name.clone(),
			kind: row.kind.clone(),
			target: row.target.clone(),
			interval_ms: row.interval.milliseconds().unwrap_or(0),
			timeout_ms: row.timeout.milliseconds().unwrap_or(0),
			http_method: row.http_method.clone(),
			expected_status: row.expected_status,
			keyword: row.keyword.clone(),
			expected_ip: row.expected_ip.clone(),
			failure_threshold: row.failure_threshold,
			enabled: row.enabled,
			status: row.status.clone(),
			created_at: row.created_at.to_string(),
			last_checked_at: row.last_checked_at.as_ref().map(|d| d.to_string()),
			consecutive_failures: row.consecutive_failures,
		}
	}
}

#[derive(Deserialize)]
pub struct MonitorInput {
	pub name: String,
	pub kind: String,
	pub target: String,
	pub interval_ms: i64,
	pub timeout_ms: i64,
	pub http_method: Option<String>,
	pub expected_status: Option<i16>,
	pub keyword: Option<String>,
	pub expected_ip: Option<String>,
	pub failure_threshold: i16,
	pub enabled: bool,
}

impl MonitorInput {
	pub fn validate(&self) -> Result<(), ApiError> {
		let fail = |m: &str| Err(ApiError::Validation(m.to_string()));
		if self.name.trim().is_empty() || self.name.len() > 200 {
			return fail("name must be between 1 and 200 characters");
		}
		if self.target.trim().is_empty() || self.target.len() > 500 {
			return fail("target must be between 1 and 500 characters");
		}
		if self.interval_ms < 5_000 {
			return fail("interval must be at least 5 seconds");
		}
		if self.timeout_ms < 1_000 {
			return fail("timeout must be at least 1 second");
		}
		if self.timeout_ms > self.interval_ms {
			return fail("timeout must not exceed the interval");
		}
		if self.failure_threshold < 1 {
			return fail("failure threshold must be at least 1");
		}
		if let Some(code) = self.expected_status
			&& !(100..=599).contains(&code)
		{
			return fail("expected status must be a valid HTTP status code");
		}
		match self.kind.as_str() {
			"http" => {
				if !(self.target.starts_with("http://") || self.target.starts_with("https://")) {
					return fail("http target must start with http:// or https://");
				}
				if Url::parse(&self.target).is_err() {
					return fail("http target is not a valid URL");
				}
				if let Some(method) = &self.http_method
					&& method != "GET" && method != "HEAD"
				{
					return fail("http method must be GET or HEAD");
				}
			}
			"tcp" => {
				let Some((host, port)) = self.target.rsplit_once(':') else {
					return fail("tcp target must be host:port");
				};
				if host.is_empty() || port.parse::<u16>().is_err() {
					return fail("tcp target must be host:port with a valid port");
				}
			}
			"ping" | "dns" => {
				if self.target.contains('/') || self.target.contains(':') {
					return fail("target must be a plain hostname");
				}
			}
			_ => return fail("kind must be one of http, tcp, ping, dns"),
		}
		Ok(())
	}
}

#[derive(Serialize)]
pub struct CheckResultDto {
	pub checked_at: String,
	pub success: bool,
	pub response_time_ms: Option<i64>,
	pub status_code: Option<i16>,
	pub error: Option<String>,
}

impl CheckResultDto {
	pub fn from_row(row: &CheckResultRow) -> Self {
		Self {
			checked_at: row.checked_at.to_string(),
			success: row.success,
			response_time_ms: row.response_time.as_ref().and_then(|d| d.milliseconds().ok()),
			status_code: row.status_code,
			error: row.error.clone(),
		}
	}
}

#[derive(Serialize)]
pub struct StatusPageDto {
	pub id: String,
	pub slug: String,
	pub title: String,
	pub created_at: String,
	pub monitor_ids: Vec<String>,
}

impl StatusPageDto {
	pub fn from_row(row: &StatusPageRow, monitor_ids: Vec<String>) -> Self {
		Self {
			id: row.id.to_string(),
			slug: row.slug.clone(),
			title: row.title.clone(),
			created_at: row.created_at.to_string(),
			monitor_ids,
		}
	}
}

#[derive(Deserialize)]
pub struct StatusPageInput {
	pub slug: String,
	pub title: String,
	pub monitor_ids: Vec<String>,
}

impl StatusPageInput {
	pub fn validate(&self) -> Result<(), ApiError> {
		let fail = |m: &str| Err(ApiError::Validation(m.to_string()));
		if self.title.trim().is_empty() || self.title.len() > 200 {
			return fail("title must be between 1 and 200 characters");
		}
		if self.slug.is_empty()
			|| self.slug.len() > 64
			|| !self.slug.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
			|| self.slug.starts_with('-')
		{
			return fail("slug must contain only lowercase letters, digits, and hyphens");
		}
		if self.monitor_ids.is_empty() {
			return fail("select at least one monitor");
		}
		if self.monitor_ids.len() > 100 {
			return fail("a status page can contain at most 100 monitors");
		}
		Ok(())
	}
}

#[derive(Serialize)]
pub struct DailyUptimeDto {
	pub day: String,
	pub total: i64,
	pub up: i64,
}

impl DailyUptimeDto {
	pub fn from_bucket(b: &DayBucket) -> Self {
		Self {
			day: b.day.to_string(),
			total: b.total,
			up: b.up,
		}
	}
}

#[derive(Serialize)]
pub struct MonitorDailyDto {
	pub monitor_id: String,
	pub daily: Vec<DailyUptimeDto>,
}

#[derive(Serialize)]
pub struct PublicStatusMonitorDto {
	pub name: String,
	pub status: String,
	pub uptime_24h: Option<f64>,
	pub last_checked_at: Option<String>,
	pub daily: Vec<DailyUptimeDto>,
}

#[derive(Serialize)]
pub struct PublicStatusDto {
	pub title: String,
	pub slug: String,
	pub monitors: Vec<PublicStatusMonitorDto>,
}

#[derive(Serialize)]
pub struct MeDto {
	pub id: String,
	pub email: String,
}

#[derive(Deserialize)]
pub struct RegisterRequest {
	pub email: String,
	pub password: String,
}

#[derive(Deserialize)]
pub struct LoginRequest {
	pub email: String,
	pub password: String,
}

#[derive(Serialize)]
pub struct LoginResponse {
	pub token: String,
	pub identity: String,
}
