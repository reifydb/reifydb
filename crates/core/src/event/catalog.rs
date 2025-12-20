// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	impl_event,
	interface::{DictionaryDef, NamespaceDef, RingBufferDef, TableDef, ViewDef},
	row::Row,
};

pub struct TableInsertedEvent {
	pub table: TableDef,
	pub row: Row,
}

impl_event!(TableInsertedEvent);

pub struct NamespaceCreatedEvent {
	pub namespace: NamespaceDef,
}

impl_event!(NamespaceCreatedEvent);

pub struct TableCreatedEvent {
	pub table: TableDef,
}

impl_event!(TableCreatedEvent);

pub struct ViewCreatedEvent {
	pub view: ViewDef,
}

impl_event!(ViewCreatedEvent);

pub struct RingBufferCreatedEvent {
	pub ringbuffer: RingBufferDef,
}

impl_event!(RingBufferCreatedEvent);

pub struct DictionaryCreatedEvent {
	pub dictionary: DictionaryDef,
}

impl_event!(DictionaryCreatedEvent);
