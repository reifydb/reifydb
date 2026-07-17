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
	),
	Migration::new(
		"0002_daily_uptime_views",
		vec![
			"create deferred view uptime::daily_totals { \
				owner: identity_id, \
				monitor_id: uuid7, \
				day: date, \
				n: int8 \
			} as { \
				from uptime::check_results \
				map { owner, monitor_id, day: datetime::date(checked_at) } \
				aggregate { n: math::count(day) } by { owner, monitor_id, day } \
			}",
			"create deferred view uptime::daily_ups { \
				owner: identity_id, \
				monitor_id: uuid7, \
				day: date, \
				n: int8 \
			} as { \
				from uptime::check_results \
				filter { success == true } \
				map { owner, monitor_id, day: datetime::date(checked_at) } \
				aggregate { n: math::count(day) } by { owner, monitor_id, day } \
			}",
			"create session policy uptime_realtime { \
				subscription: { filter { true } } \
			}",
			"create table policy uptime_monitors_owner on uptime::monitors { \
				from: { filter { owner == $identity.id } } \
			}",
			"create table policy uptime_check_results_owner on uptime::check_results { \
				from: { filter { owner == $identity.id } } \
			}",
			"create view policy uptime_daily_totals_owner on uptime::daily_totals { \
				from: { filter { owner == $identity.id } } \
			}",
			"create view policy uptime_daily_ups_owner on uptime::daily_ups { \
				from: { filter { owner == $identity.id } } \
			}",
		],
	)]
}
