// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::Migration;

pub fn migrations() -> Vec<Migration> {
	vec![
		Migration::new(
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
				"create table uptime::results { \
				id: uuid7, \
				monitor_id: uuid7, \
				owner: identity_id, \
				region_id: uuid7, \
				probe: Option(identity_id), \
				requirement_id: Option(uuid7), \
				checked_at: datetime, \
				success: bool, \
				response_time: Option(duration), \
				status_code: Option(int2), \
				error: Option(utf8) \
			}",
				"create table uptime::monitor_regions { \
				monitor_id: uuid7, \
				owner: identity_id, \
				region_id: uuid7, \
				status: utf8, \
				last_checked_at: Option(datetime), \
				consecutive_failures: int4 \
			}",
				"create table uptime::regions { \
				id: uuid7, \
				label: utf8 \
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
				"create table uptime::requirements { \
				id: uuid7, \
				monitor_id: uuid7, \
				position: int2 \
			}",
				"create table uptime::probes { \
				id: identity_id, \
				name: utf8, \
				last_seen: datetime \
			}",
				"create ringbuffer uptime::jobs { \
				id: uuid7, \
				monitor_id: uuid7, \
				region_id: uuid7 \
			} with { capacity: 100, partition: { by: { monitor_id } } }",
				"create user attribute email: utf8",
				"create procedure uptime::enqueue_job { job_id: uuid7, monitor_id: uuid7, region_id: uuid7 } \
				as { insert uptime::jobs [{ id: $job_id, monitor_id: $monitor_id, region_id: $region_id }] }",
				"create procedure uptime::claim_job { monitor_id: uuid7 } \
				as { delete uptime::jobs filter { monitor_id == $monitor_id } take 1 \
				returning { monitor_id, region_id } }",
				"create procedure uptime::register_probe { probe: identity_id, name: utf8, seen: datetime } \
				as { delete uptime::probes filter { id == $probe }; \
				insert uptime::probes [{ id: $probe, name: $name, last_seen: $seen }] }",
				"create procedure uptime::probe_heartbeat { probe: identity_id, seen: datetime } \
				as { update uptime::probes { last_seen: $seen } filter { id == $probe } }",
				"create procedure uptime::report_result { \
				result_id: uuid7, monitor_id: uuid7, owner: identity_id, region_id: uuid7, \
				probe: identity_id, checked_at: datetime, success: bool, \
				response_time: Option(duration), status_code: Option(int2), error: Option(utf8) \
			} as { \
				let $cf = from uptime::monitor_regions \
					filter { monitor_id == $monitor_id and region_id == $region_id } map { consecutive_failures }; \
				let $prev_status = from uptime::monitor_regions \
					filter { monitor_id == $monitor_id and region_id == $region_id } map { status }; \
				let $thr = from uptime::monitors filter { id == $monitor_id } map { failure_threshold }; \
				let $failures = match { $success => 0, else => $cf + 1 }; \
				let $rstatus = match { $success => \"up\", $failures >= $thr => \"down\", else => $prev_status }; \
				insert uptime::results [{ id: $result_id, monitor_id: $monitor_id, owner: $owner, \
					region_id: $region_id, probe: $probe, requirement_id: none, checked_at: $checked_at, \
					success: $success, response_time: $response_time, status_code: $status_code, error: $error }]; \
				update uptime::monitor_regions { status: $rstatus, last_checked_at: $checked_at, \
					consecutive_failures: $failures } filter { monitor_id == $monitor_id and region_id == $region_id }; \
				let $ups = from uptime::monitor_regions filter { monitor_id == $monitor_id } \
					map { f: match { status == \"up\" => 1, else => 0 } } aggregate { s: math::sum(f) }; \
				let $downs = from uptime::monitor_regions filter { monitor_id == $monitor_id } \
					map { f: match { status == \"down\" => 1, else => 0 } } aggregate { s: math::sum(f) }; \
				let $rollup = match { $ups > 0 and $downs > 0 => \"degraded\", $downs > 0 => \"down\", \
					$ups > 0 => \"up\", else => \"unknown\" }; \
				update uptime::monitors { status: $rollup, last_checked_at: $checked_at } filter { id == $monitor_id } \
			}",
				"INSERT uptime::regions [\
				{ id: uuid::v7(), label: 'US East' }, \
				{ id: uuid::v7(), label: 'EU West' }]",
				"CREATE SERVICE `probe-a`",
				"CREATE AUTHENTICATION FOR `probe-a` { method: token; token: 'probe-a-dev-token' }",
				"CREATE SERVICE `probe-b`",
				"CREATE AUTHENTICATION FOR `probe-b` { method: token; token: 'probe-b-dev-token' }",
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
				from uptime::results \
				map { owner, monitor_id, day: datetime::date(checked_at) } \
				aggregate { n: math::count(day) } by { owner, monitor_id, day } \
			}",
				"create deferred view uptime::daily_ups { \
				owner: identity_id, \
				monitor_id: uuid7, \
				day: date, \
				n: int8 \
			} as { \
				from uptime::results \
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
				"create table policy uptime_results_owner on uptime::results { \
				from: { filter { owner == $identity.id } } \
			}",
				"create table policy uptime_monitor_regions_owner on uptime::monitor_regions { \
				from: { filter { owner == $identity.id } } \
			}",
				"create table policy uptime_regions_all on uptime::regions { \
				from: { filter { true } } \
			}",
				"create table policy uptime_probes_all on uptime::probes { \
				from: { filter { true } } \
			}",
				"create view policy uptime_daily_totals_owner on uptime::daily_totals { \
				from: { filter { owner == $identity.id } } \
			}",
				"create view policy uptime_daily_ups_owner on uptime::daily_ups { \
				from: { filter { owner == $identity.id } } \
			}",
			],
		),
		Migration::new(
			"0003_probe_service_policies",
			vec![
				"create ringbuffer policy uptime_probe_jobs on uptime::jobs { \
				from: { filter { $identity.kind == \"service\" } }, \
				delete: { filter { $identity.kind == \"service\" } } \
			}",
				"create table policy uptime_probe_probes on uptime::probes { \
				insert: { filter { $identity.kind == \"service\" } }, \
				update: { filter { $identity.kind == \"service\" } }, \
				delete: { filter { $identity.kind == \"service\" } } \
			}",
				"create table policy uptime_probe_results on uptime::results { \
				insert: { filter { $identity.kind == \"service\" } } \
			}",
				"create table policy uptime_probe_monitor_regions on uptime::monitor_regions { \
				update: { filter { $identity.kind == \"service\" } } \
			}",
				"create table policy uptime_probe_monitors on uptime::monitors { \
				update: { filter { $identity.kind == \"service\" } } \
			}",
				"create procedure policy uptime_probe_call_claim_job on uptime::claim_job { \
				call: { filter { $identity.kind == \"service\" } } \
			}",
				"create procedure policy uptime_probe_call_heartbeat on uptime::probe_heartbeat { \
				call: { filter { $identity.kind == \"service\" } } \
			}",
				"create procedure policy uptime_probe_call_report on uptime::report_result { \
				call: { filter { $identity.kind == \"service\" } } \
			}",
				"create procedure policy uptime_probe_call_register on uptime::register_probe { \
				call: { filter { $identity.kind == \"service\" } } \
			}",
			],
		),
		Migration::new(
			"0004_probe_find_monitor",
			vec![
				"create procedure uptime::find_monitor { monitor_id: uuid7 } \
				as { from uptime::monitors filter { id == $monitor_id } }",
				"create procedure policy uptime_probe_call_find_monitor on uptime::find_monitor { \
				call: { filter { $identity.kind == \"service\" } } \
			}",
			],
		),
		Migration::new(
			"0005_claim_job_in_region",
			vec![
				"create procedure uptime::claim_job_in_region { monitor_id: uuid7, region_id: uuid7 } \
				as { delete uptime::jobs filter { monitor_id == $monitor_id and region_id == $region_id } take 1 \
				returning { monitor_id, region_id } }",
				"create procedure policy uptime_probe_call_claim_in_region on uptime::claim_job_in_region { \
				call: { filter { $identity.kind == \"service\" } } \
			}",
			],
		),
		Migration::new(
			"0006_probe_read_monitors",
			vec![
				"drop table policy uptime_monitors_owner",
				"create table policy uptime_monitors_owner on uptime::monitors { \
				from: { filter { owner == $identity.id or $identity.kind == \"service\" } } \
			}",
				"drop table policy uptime_monitor_regions_owner",
				"create table policy uptime_monitor_regions_owner on uptime::monitor_regions { \
				from: { filter { owner == $identity.id or $identity.kind == \"service\" } } \
			}",
			],
		),
	]
}
