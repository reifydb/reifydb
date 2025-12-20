// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Channel-based client for scheduler communication

use std::{
	collections::VecDeque,
	sync::{
		Arc, Mutex,
		atomic::{AtomicBool, AtomicU64, Ordering},
		mpsc::{self, Sender},
	},
	time::Duration,
};

use diagnostic::shutdown;
use reifydb_core::Result;
use reifydb_sub_api::{BoxedOnceTask, BoxedTask, Priority, Scheduler, TaskHandle};
use reifydb_type::{diagnostic, err, internal_err};

/// Request types for scheduler operations
pub enum SchedulerRequest {
	ScheduleEvery {
		task: BoxedTask,
		interval: Duration,
	},
	Submit {
		task: BoxedOnceTask,
		priority: Priority,
	},
	Cancel {
		handle: TaskHandle,
	},
}

/// Response types for scheduler operations
pub enum SchedulerResponse {
	TaskScheduled(TaskHandle),
	TaskSubmitted,
	TaskCancelled,
	Error(String),
}

/// Client for communicating with the worker subsystem's scheduler
pub struct SchedulerClient {
	sender: Sender<(SchedulerRequest, Sender<SchedulerResponse>)>,
	pending_requests: Arc<Mutex<VecDeque<(SchedulerRequest, Sender<SchedulerResponse>)>>>,
	next_handle: Arc<AtomicU64>,
	running: Arc<AtomicBool>,
}

impl SchedulerClient {
	pub fn new(
		sender: Sender<(SchedulerRequest, Sender<SchedulerResponse>)>,
		pending_requests: Arc<Mutex<VecDeque<(SchedulerRequest, Sender<SchedulerResponse>)>>>,
		next_handle: Arc<AtomicU64>,
		running: Arc<AtomicBool>,
	) -> Self {
		Self {
			sender,
			pending_requests,
			next_handle,
			running,
		}
	}
}

impl Scheduler for SchedulerClient {
	fn every(&self, interval: Duration, task: BoxedTask) -> reifydb_core::Result<TaskHandle> {
		if !self.running.load(Ordering::Relaxed) {
			let handle = TaskHandle::from(self.next_handle.fetch_add(1, Ordering::Relaxed));

			let (response_tx, _response_rx) = mpsc::channel();

			let request = SchedulerRequest::ScheduleEvery {
				task,
				interval,
			};

			{
				let mut pending = self.pending_requests.lock().unwrap();
				pending.push_back((request, response_tx));
			}

			return Ok(handle);
		}

		let (response_tx, response_rx) = mpsc::channel();
		let request = SchedulerRequest::ScheduleEvery {
			task,
			interval,
		};

		if self.sender.send((request, response_tx)).is_err() {
			return err!(shutdown("Scheduler"));
		}

		let response = match response_rx.recv() {
			Ok(resp) => resp,
			Err(_) => return err!(shutdown("Scheduler")),
		};

		match response {
			SchedulerResponse::TaskScheduled(handle) => Ok(handle),
			SchedulerResponse::Error(msg) => {
				internal_err!(msg)
			}
			_ => internal_err!("Unexpected response from scheduler"),
		}
	}

	fn once(&self, task: BoxedOnceTask) -> reifydb_core::Result<()> {
		let (response_tx, response_rx) = mpsc::channel();

		let priority = task.priority();
		let request = SchedulerRequest::Submit {
			task,
			priority,
		};

		if self.sender.send((request, response_tx)).is_err() {
			return err!(shutdown("Scheduler"));
		}

		let response = match response_rx.recv() {
			Ok(resp) => resp,
			Err(_) => return err!(shutdown("Scheduler")),
		};

		match response {
			SchedulerResponse::TaskSubmitted => Ok(()),
			SchedulerResponse::Error(msg) => {
				internal_err!(msg)
			}
			_ => internal_err!("Unexpected response from scheduler"),
		}
	}

	fn cancel(&self, handle: TaskHandle) -> Result<()> {
		let (response_tx, response_rx) = mpsc::channel();

		let request = SchedulerRequest::Cancel {
			handle,
		};

		if self.sender.send((request, response_tx)).is_err() {
			return err!(shutdown("Scheduler"));
		}

		let response = match response_rx.recv() {
			Ok(resp) => resp,
			Err(_) => return err!(shutdown("Scheduler")),
		};

		match response {
			SchedulerResponse::TaskCancelled => Ok(()),
			SchedulerResponse::Error(msg) => {
				internal_err!(msg)
			}
			_ => internal_err!("Unexpected response from scheduler"),
		}
	}
}
