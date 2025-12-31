// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{IsNumber, IsTemporal, IsUuid};

use crate::value::container::{
	BlobContainer, BoolContainer, NumberContainer, RowNumberContainer, TemporalContainer, UndefinedContainer,
	Utf8Container, UuidContainer,
};

/// Trait for containers that can be created with a specific capacity
pub trait ContainerCapacity {
	fn with_capacity(capacity: usize) -> Self;
	fn clear(&mut self);
	fn capacity(&self) -> usize;
}

// Implement ContainerCapacity for all our container types
impl ContainerCapacity for BoolContainer {
	fn with_capacity(capacity: usize) -> Self {
		Self::with_capacity(capacity)
	}

	fn clear(&mut self) {
		// Clear content but preserve capacity
		let capacity = self.capacity();
		*self = Self::with_capacity(capacity);
	}

	fn capacity(&self) -> usize {
		self.capacity()
	}
}

impl<T> ContainerCapacity for NumberContainer<T>
where
	T: IsNumber + Clone + std::fmt::Debug + Default,
{
	fn with_capacity(capacity: usize) -> Self {
		Self::with_capacity(capacity)
	}

	fn clear(&mut self) {
		// Clear content but preserve capacity
		let capacity = self.capacity();
		*self = Self::with_capacity(capacity);
	}

	fn capacity(&self) -> usize {
		self.capacity()
	}
}

impl ContainerCapacity for Utf8Container {
	fn with_capacity(capacity: usize) -> Self {
		Self::with_capacity(capacity)
	}

	fn clear(&mut self) {
		// Clear content but preserve capacity
		let capacity = self.capacity();
		*self = Self::with_capacity(capacity);
	}

	fn capacity(&self) -> usize {
		self.capacity()
	}
}

impl<T> ContainerCapacity for TemporalContainer<T>
where
	T: IsTemporal + Clone + std::fmt::Debug + Default,
{
	fn with_capacity(capacity: usize) -> Self {
		Self::with_capacity(capacity)
	}

	fn clear(&mut self) {
		// Clear content but preserve capacity
		let capacity = self.capacity();
		*self = Self::with_capacity(capacity);
	}

	fn capacity(&self) -> usize {
		self.capacity()
	}
}

impl<T> ContainerCapacity for UuidContainer<T>
where
	T: IsUuid + Clone + std::fmt::Debug + Default,
{
	fn with_capacity(capacity: usize) -> Self {
		Self::with_capacity(capacity)
	}

	fn clear(&mut self) {
		// Clear content but preserve capacity
		let capacity = self.capacity();
		*self = Self::with_capacity(capacity);
	}

	fn capacity(&self) -> usize {
		self.capacity()
	}
}

impl ContainerCapacity for BlobContainer {
	fn with_capacity(capacity: usize) -> Self {
		Self::with_capacity(capacity)
	}

	fn clear(&mut self) {
		// Clear content but preserve capacity
		let capacity = self.capacity();
		*self = Self::with_capacity(capacity);
	}

	fn capacity(&self) -> usize {
		self.capacity()
	}
}

impl ContainerCapacity for RowNumberContainer {
	fn with_capacity(capacity: usize) -> Self {
		Self::with_capacity(capacity)
	}

	fn clear(&mut self) {
		// Clear content but preserve capacity
		let capacity = self.capacity();
		*self = Self::with_capacity(capacity);
	}

	fn capacity(&self) -> usize {
		self.capacity()
	}
}

impl ContainerCapacity for UndefinedContainer {
	fn with_capacity(capacity: usize) -> Self {
		Self::with_capacity(capacity)
	}

	fn clear(&mut self) {
		// Clear content but preserve capacity
		let capacity = self.capacity();
		*self = Self::with_capacity(capacity);
	}

	fn capacity(&self) -> usize {
		self.capacity()
	}
}
