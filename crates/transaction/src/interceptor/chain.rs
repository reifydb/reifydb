// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

/// Chain for a specific interceptor type
pub struct InterceptorChain<I: ?Sized> {
	pub(crate) interceptors: Vec<Arc<I>>,
}

impl<I: ?Sized> InterceptorChain<I> {
	pub fn new() -> Self {
		Self {
			interceptors: Vec::new(),
		}
	}

	pub fn add(&mut self, interceptor: Arc<I>) {
		self.interceptors.push(interceptor);
	}

	pub fn is_empty(&self) -> bool {
		self.interceptors.is_empty()
	}

	pub fn len(&self) -> usize {
		self.interceptors.len()
	}

	pub fn clear(&mut self) {
		self.interceptors.clear()
	}
}

impl<I: ?Sized> Default for InterceptorChain<I> {
	fn default() -> Self {
		Self::new()
	}
}

impl<I: ?Sized> Clone for InterceptorChain<I> {
	fn clone(&self) -> Self {
		Self {
			interceptors: self.interceptors.clone(),
		}
	}
}
