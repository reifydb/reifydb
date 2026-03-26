// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Submodule declarations
pub mod authentication;
pub mod builder;
pub mod chain;
pub mod dictionary;
pub mod dictionary_row;
pub mod factory;
pub mod filter;
pub mod filtered;
pub mod granted_role;
pub mod identity;
pub mod interceptors;
pub mod namespace;
pub mod ringbuffer;
pub mod ringbuffer_row;
pub mod role;
pub mod series;
pub mod series_row;
pub mod table;
pub mod table_row;
pub mod transaction;
pub mod view;
pub mod view_row;

// Re-import types for use in WithInterceptors trait
use authentication::{AuthenticationPostCreateInterceptor, AuthenticationPreDeleteInterceptor};
use chain::InterceptorChain;
use dictionary::{
	DictionaryPostCreateInterceptor, DictionaryPostUpdateInterceptor, DictionaryPreDeleteInterceptor,
	DictionaryPreUpdateInterceptor,
};
use dictionary_row::{
	DictionaryRowPostDeleteInterceptor, DictionaryRowPostInsertInterceptor, DictionaryRowPostUpdateInterceptor,
	DictionaryRowPreDeleteInterceptor, DictionaryRowPreInsertInterceptor, DictionaryRowPreUpdateInterceptor,
};
use granted_role::{GrantedRolePostCreateInterceptor, GrantedRolePreDeleteInterceptor};
use identity::{
	IdentityPostCreateInterceptor, IdentityPostUpdateInterceptor, IdentityPreDeleteInterceptor,
	IdentityPreUpdateInterceptor,
};
use namespace::{
	NamespacePostCreateInterceptor, NamespacePostUpdateInterceptor, NamespacePreDeleteInterceptor,
	NamespacePreUpdateInterceptor,
};
use ringbuffer::{
	RingBufferPostCreateInterceptor, RingBufferPostUpdateInterceptor, RingBufferPreDeleteInterceptor,
	RingBufferPreUpdateInterceptor,
};
use ringbuffer_row::{
	RingBufferRowPostDeleteInterceptor, RingBufferRowPostInsertInterceptor, RingBufferRowPostUpdateInterceptor,
	RingBufferRowPreDeleteInterceptor, RingBufferRowPreInsertInterceptor, RingBufferRowPreUpdateInterceptor,
};
use role::{RolePostCreateInterceptor, RolePostUpdateInterceptor, RolePreDeleteInterceptor, RolePreUpdateInterceptor};
use series::{
	SeriesPostCreateInterceptor, SeriesPostUpdateInterceptor, SeriesPreDeleteInterceptor,
	SeriesPreUpdateInterceptor,
};
use series_row::{
	SeriesRowPostDeleteInterceptor, SeriesRowPostInsertInterceptor, SeriesRowPostUpdateInterceptor,
	SeriesRowPreDeleteInterceptor, SeriesRowPreInsertInterceptor, SeriesRowPreUpdateInterceptor,
};
use table::{
	TablePostCreateInterceptor, TablePostUpdateInterceptor, TablePreDeleteInterceptor, TablePreUpdateInterceptor,
};
use table_row::{
	TableRowPostDeleteInterceptor, TableRowPostInsertInterceptor, TableRowPostUpdateInterceptor,
	TableRowPreDeleteInterceptor, TableRowPreInsertInterceptor, TableRowPreUpdateInterceptor,
};
use transaction::{PostCommitInterceptor, PreCommitInterceptor};
use view::{ViewPostCreateInterceptor, ViewPostUpdateInterceptor, ViewPreDeleteInterceptor, ViewPreUpdateInterceptor};
use view_row::{
	ViewRowPostDeleteInterceptor, ViewRowPostInsertInterceptor, ViewRowPostUpdateInterceptor,
	ViewRowPreDeleteInterceptor, ViewRowPreInsertInterceptor, ViewRowPreUpdateInterceptor,
};

pub type Chain<I> = InterceptorChain<I>;

/// Trait for accessing interceptor chains from transaction types
pub trait WithInterceptors {
	// Table row (DML) interceptors
	fn table_row_pre_insert_interceptors(&mut self) -> &mut Chain<dyn TableRowPreInsertInterceptor + Send + Sync>;
	fn table_row_post_insert_interceptors(&mut self)
	-> &mut Chain<dyn TableRowPostInsertInterceptor + Send + Sync>;
	fn table_row_pre_update_interceptors(&mut self) -> &mut Chain<dyn TableRowPreUpdateInterceptor + Send + Sync>;
	fn table_row_post_update_interceptors(&mut self)
	-> &mut Chain<dyn TableRowPostUpdateInterceptor + Send + Sync>;
	fn table_row_pre_delete_interceptors(&mut self) -> &mut Chain<dyn TableRowPreDeleteInterceptor + Send + Sync>;
	fn table_row_post_delete_interceptors(&mut self)
	-> &mut Chain<dyn TableRowPostDeleteInterceptor + Send + Sync>;

	// Ring buffer row (DML) interceptors
	fn ringbuffer_row_pre_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferRowPreInsertInterceptor + Send + Sync>;
	fn ringbuffer_row_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferRowPostInsertInterceptor + Send + Sync>;
	fn ringbuffer_row_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferRowPreUpdateInterceptor + Send + Sync>;
	fn ringbuffer_row_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferRowPostUpdateInterceptor + Send + Sync>;
	fn ringbuffer_row_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferRowPreDeleteInterceptor + Send + Sync>;
	fn ringbuffer_row_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferRowPostDeleteInterceptor + Send + Sync>;

	// Transaction interceptors
	fn pre_commit_interceptors(&mut self) -> &mut Chain<dyn PreCommitInterceptor + Send + Sync>;
	fn post_commit_interceptors(&mut self) -> &mut Chain<dyn PostCommitInterceptor + Send + Sync>;

	// Namespace (DDL) interceptors
	fn namespace_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn NamespacePostCreateInterceptor + Send + Sync>;
	fn namespace_pre_update_interceptors(&mut self) -> &mut Chain<dyn NamespacePreUpdateInterceptor + Send + Sync>;
	fn namespace_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn NamespacePostUpdateInterceptor + Send + Sync>;
	fn namespace_pre_delete_interceptors(&mut self) -> &mut Chain<dyn NamespacePreDeleteInterceptor + Send + Sync>;

	// Table (DDL) interceptors
	fn table_post_create_interceptors(&mut self) -> &mut Chain<dyn TablePostCreateInterceptor + Send + Sync>;
	fn table_pre_update_interceptors(&mut self) -> &mut Chain<dyn TablePreUpdateInterceptor + Send + Sync>;
	fn table_post_update_interceptors(&mut self) -> &mut Chain<dyn TablePostUpdateInterceptor + Send + Sync>;
	fn table_pre_delete_interceptors(&mut self) -> &mut Chain<dyn TablePreDeleteInterceptor + Send + Sync>;

	// View row (DML) interceptors
	fn view_row_pre_insert_interceptors(&mut self) -> &mut Chain<dyn ViewRowPreInsertInterceptor + Send + Sync>;
	fn view_row_post_insert_interceptors(&mut self) -> &mut Chain<dyn ViewRowPostInsertInterceptor + Send + Sync>;
	fn view_row_pre_update_interceptors(&mut self) -> &mut Chain<dyn ViewRowPreUpdateInterceptor + Send + Sync>;
	fn view_row_post_update_interceptors(&mut self) -> &mut Chain<dyn ViewRowPostUpdateInterceptor + Send + Sync>;
	fn view_row_pre_delete_interceptors(&mut self) -> &mut Chain<dyn ViewRowPreDeleteInterceptor + Send + Sync>;
	fn view_row_post_delete_interceptors(&mut self) -> &mut Chain<dyn ViewRowPostDeleteInterceptor + Send + Sync>;

	// View (DDL) interceptors
	fn view_post_create_interceptors(&mut self) -> &mut Chain<dyn ViewPostCreateInterceptor + Send + Sync>;
	fn view_pre_update_interceptors(&mut self) -> &mut Chain<dyn ViewPreUpdateInterceptor + Send + Sync>;
	fn view_post_update_interceptors(&mut self) -> &mut Chain<dyn ViewPostUpdateInterceptor + Send + Sync>;
	fn view_pre_delete_interceptors(&mut self) -> &mut Chain<dyn ViewPreDeleteInterceptor + Send + Sync>;

	// Ring buffer (DDL) interceptors
	fn ringbuffer_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPostCreateInterceptor + Send + Sync>;
	fn ringbuffer_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPreUpdateInterceptor + Send + Sync>;
	fn ringbuffer_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPostUpdateInterceptor + Send + Sync>;
	fn ringbuffer_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPreDeleteInterceptor + Send + Sync>;

	// Dictionary row (DML) interceptors
	fn dictionary_row_pre_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryRowPreInsertInterceptor + Send + Sync>;
	fn dictionary_row_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryRowPostInsertInterceptor + Send + Sync>;
	fn dictionary_row_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryRowPreUpdateInterceptor + Send + Sync>;
	fn dictionary_row_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryRowPostUpdateInterceptor + Send + Sync>;
	fn dictionary_row_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryRowPreDeleteInterceptor + Send + Sync>;
	fn dictionary_row_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryRowPostDeleteInterceptor + Send + Sync>;

	// Dictionary (DDL) interceptors
	fn dictionary_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryPostCreateInterceptor + Send + Sync>;
	fn dictionary_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryPreUpdateInterceptor + Send + Sync>;
	fn dictionary_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryPostUpdateInterceptor + Send + Sync>;
	fn dictionary_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryPreDeleteInterceptor + Send + Sync>;

	// Series row (DML) interceptors
	fn series_row_pre_insert_interceptors(&mut self)
	-> &mut Chain<dyn SeriesRowPreInsertInterceptor + Send + Sync>;
	fn series_row_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn SeriesRowPostInsertInterceptor + Send + Sync>;
	fn series_row_pre_update_interceptors(&mut self)
	-> &mut Chain<dyn SeriesRowPreUpdateInterceptor + Send + Sync>;
	fn series_row_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn SeriesRowPostUpdateInterceptor + Send + Sync>;
	fn series_row_pre_delete_interceptors(&mut self)
	-> &mut Chain<dyn SeriesRowPreDeleteInterceptor + Send + Sync>;
	fn series_row_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn SeriesRowPostDeleteInterceptor + Send + Sync>;

	// Series (DDL) interceptors
	fn series_post_create_interceptors(&mut self) -> &mut Chain<dyn SeriesPostCreateInterceptor + Send + Sync>;
	fn series_pre_update_interceptors(&mut self) -> &mut Chain<dyn SeriesPreUpdateInterceptor + Send + Sync>;
	fn series_post_update_interceptors(&mut self) -> &mut Chain<dyn SeriesPostUpdateInterceptor + Send + Sync>;
	fn series_pre_delete_interceptors(&mut self) -> &mut Chain<dyn SeriesPreDeleteInterceptor + Send + Sync>;

	// Identity (DDL) interceptors
	fn identity_post_create_interceptors(&mut self) -> &mut Chain<dyn IdentityPostCreateInterceptor + Send + Sync>;
	fn identity_pre_update_interceptors(&mut self) -> &mut Chain<dyn IdentityPreUpdateInterceptor + Send + Sync>;
	fn identity_post_update_interceptors(&mut self) -> &mut Chain<dyn IdentityPostUpdateInterceptor + Send + Sync>;
	fn identity_pre_delete_interceptors(&mut self) -> &mut Chain<dyn IdentityPreDeleteInterceptor + Send + Sync>;

	// Role (DDL) interceptors
	fn role_post_create_interceptors(&mut self) -> &mut Chain<dyn RolePostCreateInterceptor + Send + Sync>;
	fn role_pre_update_interceptors(&mut self) -> &mut Chain<dyn RolePreUpdateInterceptor + Send + Sync>;
	fn role_post_update_interceptors(&mut self) -> &mut Chain<dyn RolePostUpdateInterceptor + Send + Sync>;
	fn role_pre_delete_interceptors(&mut self) -> &mut Chain<dyn RolePreDeleteInterceptor + Send + Sync>;

	// Granted role (DDL) interceptors
	fn granted_role_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn GrantedRolePostCreateInterceptor + Send + Sync>;
	fn granted_role_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn GrantedRolePreDeleteInterceptor + Send + Sync>;

	// Authentication (DDL) interceptors
	fn authentication_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn AuthenticationPostCreateInterceptor + Send + Sync>;
	fn authentication_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn AuthenticationPreDeleteInterceptor + Send + Sync>;
}
