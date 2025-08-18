// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use crate::interface::Transaction;

/// Chain for a specific interceptor type
pub struct InterceptorChain<T: Transaction, I: ?Sized> {
	pub(crate) interceptors: Vec<Box<I>>,
	_phantom: PhantomData<T>,
}

impl<T: Transaction, I: ?Sized> InterceptorChain<T, I> {
	pub fn new() -> Self {
		Self {
			interceptors: Vec::new(),
			_phantom: PhantomData,
		}
	}

	pub fn add(&mut self, interceptor: Box<I>) {
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

impl<T: Transaction, I: ?Sized> Default for InterceptorChain<T, I> {
	fn default() -> Self {
		Self::new()
	}
}
