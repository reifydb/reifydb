// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod builder;
mod chain;
mod factory;
mod filter;
mod filtered;
mod interceptor;
mod interceptors;
mod namespace_def;
mod ring_buffer;
mod ring_buffer_def;
mod table;
mod table_def;
mod transaction;
mod view;
mod view_def;

pub use builder::*;
pub use chain::InterceptorChain;
pub use factory::{InterceptorFactory, StandardInterceptorFactory};
pub use filter::InterceptFilter;
pub use filtered::*;
pub use interceptors::Interceptors;
pub use namespace_def::*;
pub use ring_buffer::*;
pub use ring_buffer_def::*;
pub use table::*;
pub use table_def::*;
pub use transaction::*;
pub use view::*;
pub use view_def::*;

pub type Chain<T, I> = InterceptorChain<T, I>;

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
		pub struct $context_name<'a, T: $crate::interface::CommandTransaction> {
			$(pub $field_name: $field_type,)*
		}

		impl<'a, T: $crate::interface::CommandTransaction> $context_name<'a, T> {
			pub fn new(
				$($field_name: $field_type,)*
			) -> Self {
				Self {
					$($field_name,)*
				}
			}
		}

		pub trait $trait_name<T: $crate::interface::CommandTransaction> {
			fn intercept(
				&self,
				ctx: &mut $context_name<T>,
			) -> $crate::Result<()>;
		}

		impl<T: $crate::interface::CommandTransaction> $crate::interceptor::InterceptorChain<T, dyn $trait_name<T>> {
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

		pub trait $trait_name<T: $crate::interface::CommandTransaction> {
			fn intercept(
				&self,
				ctx: &mut $context_name,
			) -> $crate::Result<()>;
		}

		impl<T: $crate::interface::CommandTransaction> $crate::interceptor::InterceptorChain<T, dyn $trait_name<T>> {
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
		pub struct $wrapper_name<T: crate::interface::CommandTransaction, F>
		where
			F: Fn(&mut $context_type<T>) -> crate::Result<()>,
		{
			closure: F,
			_phantom: PhantomData<T>,
		}

		impl<T: crate::interface::CommandTransaction, F> $wrapper_name<T, F>
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

		impl<T: crate::interface::CommandTransaction, F> Clone for $wrapper_name<T, F>
		where
			F: Fn(&mut $context_type<T>) -> crate::Result<()> + Clone,
		{
			fn clone(&self) -> Self {
				Self {
					closure: self.closure.clone(),
					_phantom: PhantomData,
				}
			}
		}

		impl<T: crate::interface::CommandTransaction, F> $trait_name<T> for $wrapper_name<T, F>
		where
			F: Fn(&mut $context_type<T>) -> crate::Result<()>,
		{
			fn intercept(&self, ctx: &mut $context_type<T>) -> crate::Result<()> {
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

		impl<T: crate::interface::CommandTransaction, F> $trait_name<T> for $wrapper_name<F>
		where
			F: Fn(&mut $context_type) -> crate::Result<()>,
		{
			fn intercept(&self, ctx: &mut $context_type) -> crate::Result<()> {
				(self.closure)(ctx)
			}
		}
	};
}

/// Macro to create helper functions that create closure interceptors
#[macro_export]
macro_rules! define_api_function {
	// Version with Transaction type parameter
	(
		$fn_name:ident,
		$closure_type:ident<T, F>,
		$context_type:ident<T>
	) => {
		pub fn $fn_name<T: crate::interface::CommandTransaction, F>(f: F) -> $closure_type<T, F>
		where
			F: Fn(&mut $context_type<T>) -> crate::Result<()> + Send + Sync + Clone + 'static,
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
			F: Fn(&mut $context_type) -> crate::Result<()> + Send + Sync + Clone + 'static,
		{
			$closure_type::new(f)
		}
	};
}

/// Trait for self-registering interceptors
/// This allows interceptors that implement multiple interceptor traits
/// to register themselves in all appropriate chains with a single Rc instance
pub trait RegisterInterceptor<T: crate::interface::CommandTransaction> {
	fn register(self: std::rc::Rc<Self>, interceptors: &mut Interceptors<T>);
}

#[macro_export]
macro_rules! impl_register_interceptor {
	(
		$closure_type:ident<T, F>,
		$context_type:ident<T>,
		$trait_type:ident,
		$field:ident
	) => {
		impl<T, F> $crate::interceptor::RegisterInterceptor<T> for $closure_type<T, F>
		where
			T: $crate::interface::CommandTransaction + 'static,
			F: Fn(&mut $context_type<T>) -> $crate::Result<()> + 'static,
		{
			fn register(self: std::rc::Rc<Self>, interceptors: &mut $crate::interceptor::Interceptors<T>) {
				interceptors.$field.add(self as std::rc::Rc<dyn $trait_type<T>>);
			}
		}
	};
	(
		$closure_type:ident<F>,
		$context_type:ident,
		$trait_type:ident<T>,
		$field:ident
	) => {
		impl<T, F> $crate::interceptor::RegisterInterceptor<T> for $closure_type<F>
		where
			T: $crate::interface::CommandTransaction,
			F: Fn(&mut $context_type) -> $crate::Result<()> + 'static,
		{
			fn register(self: std::rc::Rc<Self>, interceptors: &mut $crate::interceptor::Interceptors<T>) {
				interceptors.$field.add(self as std::rc::Rc<dyn $trait_type<T>>);
			}
		}
	};
}
