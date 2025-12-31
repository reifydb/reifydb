// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{marker::PhantomData, sync::Arc};

use crate::interface::CommandTransaction;

/// Chain for a specific interceptor type
pub struct InterceptorChain<T: CommandTransaction, I: ?Sized> {
	pub(crate) interceptors: Vec<Arc<I>>,
	_phantom: PhantomData<T>,
}

impl<T: CommandTransaction, I: ?Sized> InterceptorChain<T, I> {
	pub fn new() -> Self {
		Self {
			interceptors: Vec::new(),
			_phantom: PhantomData,
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

impl<T: CommandTransaction, I: ?Sized> Default for InterceptorChain<T, I> {
	fn default() -> Self {
		Self::new()
	}
}

impl<T: CommandTransaction, I: ?Sized> Clone for InterceptorChain<T, I> {
	fn clone(&self) -> Self {
		Self {
			interceptors: self.interceptors.clone(),
			_phantom: PhantomData,
		}
	}
}
