// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use serde::{Deserialize, Serialize};

use crate::store::DayBucket;

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
pub struct PublicStatusRegionDto {
	pub label: String,
	pub status: String,
	pub last_checked_at: Option<String>,
	pub daily: Vec<DailyUptimeDto>,
}

#[derive(Serialize)]
pub struct PublicStatusMonitorDto {
	pub name: String,
	pub status: String,
	pub uptime_24h: Option<f64>,
	pub last_checked_at: Option<String>,
	pub daily: Vec<DailyUptimeDto>,
	pub regions: Vec<PublicStatusRegionDto>,
}

#[derive(Serialize)]
pub struct PublicStatusDto {
	pub title: String,
	pub slug: String,
	pub monitors: Vec<PublicStatusMonitorDto>,
}

#[derive(Deserialize)]
pub struct RegisterRequest {
	pub email: String,
	pub password: String,
}

#[derive(Serialize)]
pub struct GuestSessionResponse {
	pub token: String,
	pub identity: String,
	pub expires_at: i64,
}
