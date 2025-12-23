// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	impl_event,
	interface::{DictionaryDef, NamespaceDef, RingBufferDef, TableDef, ViewDef},
	row::Row,
};

#[derive(Clone)]
pub struct TableInsertedEvent {
	pub table: TableDef,
	pub row: Row,
}

impl_event!(TableInsertedEvent);

#[derive(Clone)]
pub struct NamespaceCreatedEvent {
	pub namespace: NamespaceDef,
}

impl_event!(NamespaceCreatedEvent);

#[derive(Clone)]
pub struct TableCreatedEvent {
	pub table: TableDef,
}

impl_event!(TableCreatedEvent);

#[derive(Clone)]
pub struct ViewCreatedEvent {
	pub view: ViewDef,
}

impl_event!(ViewCreatedEvent);

#[derive(Clone)]
pub struct RingBufferCreatedEvent {
	pub ringbuffer: RingBufferDef,
}

impl_event!(RingBufferCreatedEvent);

#[derive(Clone)]
pub struct DictionaryCreatedEvent {
	pub dictionary: DictionaryDef,
}

impl_event!(DictionaryCreatedEvent);
