// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Channel-based client for scheduler communication

use std::{
	collections::VecDeque,
	marker::PhantomData,
	sync::{
		Arc, Mutex,
		atomic::{AtomicBool, AtomicU64, Ordering},
		mpsc::{self, Sender},
	},
	time::Duration,
};

use reifydb_core::{Result, interface::Transaction};
use reifydb_sub_api::{BoxedTask, Scheduler, TaskHandle};

/// Request types for scheduler operations
pub enum SchedulerRequest<T: Transaction> {
	ScheduleEvery {
		task: BoxedTask<T>,
		interval: Duration,
	},
	Cancel {
		handle: TaskHandle,
	},
}

/// Response types for scheduler operations
pub enum SchedulerResponse {
	TaskScheduled(TaskHandle),
	TaskCancelled,
	Error(String),
}

/// Client for communicating with the worker subsystem's scheduler
pub struct SchedulerClient<T: Transaction> {
	sender: Sender<(SchedulerRequest<T>, Sender<SchedulerResponse>)>,
	pending_requests: Arc<Mutex<VecDeque<(SchedulerRequest<T>, Sender<SchedulerResponse>)>>>,
	next_handle: Arc<AtomicU64>,
	running: Arc<AtomicBool>,
	_phantom: PhantomData<T>,
}

impl<T: Transaction> SchedulerClient<T> {
	/// Create a new scheduler client
	pub fn new(sender: Sender<(SchedulerRequest<T>, Sender<SchedulerResponse>)>) -> Self {
		Self {
			sender,
			pending_requests: Arc::new(Mutex::new(VecDeque::new())),
			next_handle: Arc::new(AtomicU64::new(1)),
			running: Arc::new(AtomicBool::new(false)),
			_phantom: PhantomData,
		}
	}

	/// Create a scheduler client with shared queue (for use by
	/// WorkerSubsystem)
	pub fn with_queue(
		sender: Sender<(SchedulerRequest<T>, Sender<SchedulerResponse>)>,
		pending_requests: Arc<Mutex<VecDeque<(SchedulerRequest<T>, Sender<SchedulerResponse>)>>>,
		next_handle: Arc<AtomicU64>,
		running: Arc<AtomicBool>,
	) -> Self {
		Self {
			sender,
			pending_requests,
			next_handle,
			running,
			_phantom: PhantomData,
		}
	}
}

impl<T: Transaction> Scheduler<T> for SchedulerClient<T> {
	fn schedule_every(&self, task: BoxedTask<T>, interval: Duration) -> Result<TaskHandle> {
		// Check if the subsystem is running
		if !self.running.load(Ordering::Relaxed) {
			// Generate a handle for the task
			let handle = TaskHandle::from(self.next_handle.fetch_add(1, Ordering::Relaxed));

			// Create a channel for the response (we'll send the
			// response ourselves)
			let (response_tx, _response_rx) = mpsc::channel();

			// Queue the request to be processed when the subsystem
			// starts
			let request = SchedulerRequest::ScheduleEvery {
				task,
				interval,
			};

			{
				let mut pending = self.pending_requests.lock().unwrap();
				pending.push_back((request, response_tx));
			}

			// Return the pre-generated handle
			return Ok(handle);
		}

		// Normal path when subsystem is running
		let (response_tx, response_rx) = mpsc::channel();

		let request = SchedulerRequest::ScheduleEvery {
			task,
			interval,
		};

		self.sender
			.send((request, response_tx))
			.expect("Failed to send scheduler request: channel disconnected");

		// Wait for the response
		let response = response_rx.recv().expect("Failed to receive scheduler response: channel disconnected");

		match response {
			SchedulerResponse::TaskScheduled(handle) => Ok(handle),
			SchedulerResponse::Error(msg) => {
				panic!("Scheduler error: {}", msg)
			}
			_ => panic!("Unexpected response from scheduler"),
		}
	}

	fn cancel(&self, handle: TaskHandle) -> Result<()> {
		// Create a channel for the response
		let (response_tx, response_rx) = mpsc::channel();

		// Send the request
		let request = SchedulerRequest::Cancel {
			handle,
		};

		self.sender
			.send((request, response_tx))
			.expect("Failed to send scheduler request: channel disconnected");

		// Wait for the response
		let response = response_rx.recv().expect("Failed to receive scheduler response: channel disconnected");

		match response {
			SchedulerResponse::TaskCancelled => Ok(()),
			SchedulerResponse::Error(msg) => {
				panic!("Scheduler error: {}", msg)
			}
			_ => panic!("Unexpected response from scheduler"),
		}
	}
}
