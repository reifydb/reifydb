// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::Migration;

pub fn migrations() -> Vec<Migration> {
	vec![Migration::new(
		"0001_uptime_schema",
		vec![
			"create namespace uptime",
			"create table uptime::monitors { \
				id: uuid7, \
				owner: identity_id, \
				name: utf8, \
				kind: utf8, \
				target: utf8, \
				interval: duration, \
				timeout: duration, \
				http_method: Option(utf8), \
				expected_status: Option(int2), \
				keyword: Option(utf8), \
				expected_ip: Option(utf8), \
				failure_threshold: int2, \
				enabled: bool, \
				created_at: datetime, \
				last_checked_at: Option(datetime), \
				consecutive_failures: int4, \
				status: utf8 \
			}",
			"create table uptime::check_results { \
				id: uuid7, \
				monitor_id: uuid7, \
				owner: identity_id, \
				checked_at: datetime, \
				success: bool, \
				response_time: Option(duration), \
				status_code: Option(int2), \
				error: Option(utf8) \
			}",
			"create table uptime::status_pages { \
				id: uuid7, \
				owner: identity_id, \
				slug: utf8, \
				title: utf8, \
				created_at: datetime \
			}",
			"create table uptime::status_page_monitors { \
				status_page_id: uuid7, \
				monitor_id: uuid7, \
				position: int2 \
			}",
			"create user attribute email: utf8",
		],
	)]
}
