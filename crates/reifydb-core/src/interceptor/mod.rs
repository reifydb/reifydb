// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod builder;
mod chain;
mod factory;
mod interceptors;
mod provider;
mod table;
mod transaction;

pub use builder::*;
pub use chain::InterceptorChain;
pub use factory::{InterceptorFactory, StandardInterceptorFactory};
pub use interceptors::Interceptors;
pub use provider::InterceptorProvider;
pub use table::*;
pub use transaction::*;

type Chain<T, I> = InterceptorChain<T, I>;

/// Generic macro to define interceptor contexts, traits, and chain execution
#[macro_export]
macro_rules! define_interceptor {
	// Version with Transaction type parameter
	(
		context: $context_name:ident<T>,
		trait: $trait_name:ident,
		fields: {
			$($field_name:ident: $field_type:ty),* $(,)?
		}
	) => {
		/// Context for interceptors
		pub struct $context_name<'a, T: $crate::interface::Transaction> {
			$(pub $field_name: $field_type,)*
		}

		impl<'a, T: $crate::interface::Transaction> $context_name<'a, T> {
			pub fn new(
				$($field_name: $field_type,)*
			) -> Self {
				Self {
					$($field_name,)*
				}
			}
		}

		pub trait $trait_name<T: $crate::interface::Transaction> {
			fn intercept(
				&self,
				ctx: &mut $context_name<T>,
			) -> $crate::Result<()>;
		}

		impl<T: $crate::interface::Transaction> $crate::interceptor::InterceptorChain<T, dyn $trait_name<T>> {
			pub fn execute(
				&self,
				mut ctx: $context_name<T>,
			) -> $crate::Result<()> {
				for interceptor in &self.interceptors {
					interceptor.intercept(&mut ctx)?;
				}
				Ok(())
			}
		}
	};

	// Version without Transaction type parameter
	(
		context: $context_name:ident,
		trait: $trait_name:ident<T>,
		fields: {
			$($field_name:ident: $field_type:ty),* $(,)?
		}
	) => {
		/// Context for interceptors
		pub struct $context_name {
			$(pub $field_name: $field_type,)*
		}

		impl $context_name {
			pub fn new(
				$($field_name: $field_type,)*
			) -> Self {
				Self {
					$($field_name,)*
				}
			}
		}

		pub trait $trait_name<T: $crate::interface::Transaction> {
			fn intercept(
				&self,
				ctx: &mut $context_name,
			) -> $crate::Result<()>;
		}

		impl<T: $crate::interface::Transaction> $crate::interceptor::InterceptorChain<T, dyn $trait_name<T>> {
			pub fn execute(
				&self,
				mut ctx: $context_name,
			) -> $crate::Result<()> {
				for interceptor in &self.interceptors {
					interceptor.intercept(&mut ctx)?;
				}
				Ok(())
			}
		}
	};
}

/// Macro to generate closure interceptor wrapper types
#[macro_export]
macro_rules! define_closure_interceptor {
	// Version with Transaction type parameter
	(
		$wrapper_name:ident,
		$trait_name:ident,
		$context_type:ident,
		with_transaction
	) => {
		pub struct $wrapper_name<T: Transaction, F>
		where
			F: Fn(&mut $context_type<T>) -> crate::Result<()>,
		{
			closure: F,
			_phantom: PhantomData<T>,
		}

		impl<T: Transaction, F> $wrapper_name<T, F>
		where
			F: Fn(&mut $context_type<T>) -> crate::Result<()>,
		{
			pub fn new(closure: F) -> Self {
				Self {
					closure,
					_phantom: PhantomData,
				}
			}
		}

		impl<T: Transaction, F> Clone for $wrapper_name<T, F>
		where
			F: Fn(&mut $context_type<T>) -> crate::Result<()>
				+ Clone,
		{
			fn clone(&self) -> Self {
				Self {
					closure: self.closure.clone(),
					_phantom: PhantomData,
				}
			}
		}

		impl<T: Transaction, F> $trait_name<T> for $wrapper_name<T, F>
		where
			F: Fn(&mut $context_type<T>) -> crate::Result<()>,
		{
			fn intercept(
				&self,
				ctx: &mut $context_type<T>,
			) -> crate::Result<()> {
				(self.closure)(ctx)
			}
		}
	};

	// Version without Transaction type parameter in struct (but still
	// implements trait with T)
	(
		$wrapper_name:ident,
		$trait_name:ident,
		$context_type:ident,
		no_transaction_param
	) => {
		pub struct $wrapper_name<F>
		where
			F: Fn(&mut $context_type) -> crate::Result<()>,
		{
			closure: F,
		}

		impl<F> $wrapper_name<F>
		where
			F: Fn(&mut $context_type) -> crate::Result<()>,
		{
			pub fn new(closure: F) -> Self {
				Self {
					closure,
				}
			}
		}

		impl<F> Clone for $wrapper_name<F>
		where
			F: Fn(&mut $context_type) -> crate::Result<()> + Clone,
		{
			fn clone(&self) -> Self {
				Self {
					closure: self.closure.clone(),
				}
			}
		}

		impl<T: Transaction, F> $trait_name<T> for $wrapper_name<F>
		where
			F: Fn(&mut $context_type) -> crate::Result<()>,
		{
			fn intercept(
				&self,
				ctx: &mut $context_type,
			) -> crate::Result<()> {
				(self.closure)(ctx)
			}
		}
	};
}

/// Macro to implement AddToBuilder for closure interceptor types
#[macro_export]
macro_rules! impl_add_to_builder {
	// Version with Transaction type parameter in closure type
	(
		$closure_type:ident<T, F>,
		$context_type:ident<T>,
		$builder_method:ident
	) => {
		impl<T: Transaction, F> $crate::interceptor::AddToBuilder<T>
			for $closure_type<T, F>
		where
			F: Fn(&mut $context_type<T>) -> crate::Result<()>
				+ Send
				+ Sync
				+ Clone
				+ 'static,
		{
			fn add_to_builder(
				self,
				builder: $crate::interceptor::StandardInterceptorBuilder<T>,
			) -> $crate::interceptor::StandardInterceptorBuilder<T>
			{
				use std::sync::Arc;
				// Create a factory function that creates new
				// instances
				builder.$builder_method(move || {
					Arc::new(self.clone())
				})
			}
		}
	};

	// Version without Transaction type parameter in closure type
	(
		$closure_type:ident<F>,
		$context_type:ident,
		$builder_method:ident
	) => {
		impl<T: Transaction, F> $crate::interceptor::AddToBuilder<T>
			for $closure_type<F>
		where
			F: Fn(&mut $context_type) -> crate::Result<()>
				+ Send
				+ Sync
				+ Clone
				+ 'static,
		{
			fn add_to_builder(
				self,
				builder: $crate::interceptor::StandardInterceptorBuilder<T>,
			) -> $crate::interceptor::StandardInterceptorBuilder<T>
			{
				use std::sync::Arc;
				// Create a factory function that creates new
				// instances
				builder.$builder_method(move || {
					Arc::new(self.clone())
				})
			}
		}
	};
}

/// Macro to create helper functions that create closure interceptors
#[macro_export]
macro_rules! define_helper_function {
	// Version with Transaction type parameter
	(
		$fn_name:ident,
		$closure_type:ident<T, F>,
		$context_type:ident<T>
	) => {
		pub fn $fn_name<T: Transaction, F>(f: F) -> $closure_type<T, F>
		where
			F: Fn(&mut $context_type<T>) -> crate::Result<()>
				+ Send
				+ Sync
				+ Clone
				+ 'static,
		{
			$closure_type::new(f)
		}
	};

	// Version without Transaction type parameter
	(
		$fn_name:ident,
		$closure_type:ident<F>,
		$context_type:ident
	) => {
		pub fn $fn_name<F>(f: F) -> $closure_type<F>
		where
			F: Fn(&mut $context_type) -> crate::Result<()>
				+ Send
				+ Sync
				+ Clone
				+ 'static,
		{
			$closure_type::new(f)
		}
	};
}
