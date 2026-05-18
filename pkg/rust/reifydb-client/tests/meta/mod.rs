// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

mod grpc;
mod http;
mod ws;

pub fn assert_duration(duration: &str) {
	assert!(
		!duration.is_empty()
			&& duration.chars().next().is_some_and(|c| c.is_ascii_digit())
			&& duration.chars().all(|c| c.is_ascii_digit() || "ymodhsmun".contains(c)),
		"unexpected duration format: {}",
		duration
	);
}
