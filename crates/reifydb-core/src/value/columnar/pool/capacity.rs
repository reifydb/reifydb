// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::value::{
	IsNumber, IsTemporal, IsUuid,
	container::{
		BlobContainer, BoolContainer, NumberContainer,
		RowNumberContainer, StringContainer, TemporalContainer,
		UndefinedContainer, UuidContainer,
	},
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

impl ContainerCapacity for StringContainer {
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
