// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DML plan nodes (Insert, Update, Delete).

use crate::{
	plan::{CatalogColumn, Dictionary, Plan, RingBuffer, Table},
	token::Span,
};

/// Insert into table.
#[derive(Debug, Clone, Copy)]
pub struct InsertNode<'bump> {
	pub target: InsertTarget<'bump>,
	pub input: &'bump Plan<'bump>,
	pub columns: Option<&'bump [&'bump CatalogColumn<'bump>]>,
	pub span: Span,
}

/// Insert target.
#[derive(Debug, Clone, Copy)]
pub enum InsertTarget<'bump> {
	Table(&'bump Table<'bump>),
	RingBuffer(&'bump RingBuffer<'bump>),
	Dictionary(&'bump Dictionary<'bump>),
}

/// Update rows.
#[derive(Debug, Clone, Copy)]
pub struct UpdateNode<'bump> {
	pub target: UpdateTarget<'bump>,
	pub input: Option<&'bump Plan<'bump>>,
	pub span: Span,
}

/// Update target.
#[derive(Debug, Clone, Copy)]
pub enum UpdateTarget<'bump> {
	Table(&'bump Table<'bump>),
	RingBuffer(&'bump RingBuffer<'bump>),
}

/// Delete rows.
#[derive(Debug, Clone, Copy)]
pub struct DeleteNode<'bump> {
	pub target: DeleteTarget<'bump>,
	pub input: Option<&'bump Plan<'bump>>,
	pub span: Span,
}

/// Delete target.
#[derive(Debug, Clone, Copy)]
pub enum DeleteTarget<'bump> {
	Table(&'bump Table<'bump>),
	RingBuffer(&'bump RingBuffer<'bump>),
}
