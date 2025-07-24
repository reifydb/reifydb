// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::hook::Hook;
use crate::interface::{Engine, Transaction, UnversionedStorage, VersionedStorage};
use std::any::Any;

pub struct OnStartHook<VS, US, T, E>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    E: Engine<VS, US, T>,
{
    pub engine: E,
    pub _phantom: std::marker::PhantomData<(VS, US, T)>,
}

impl<VS, US, T, E> Hook for OnStartHook<VS, US, T, E>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    E: Engine<VS, US, T>,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct OnCreateHook<VS, US, T, E>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    E: Engine<VS, US, T>,
{
    pub engine: E,
    pub _phantom: std::marker::PhantomData<(VS, US, T)>,
}

impl<VS, US, T, E> Hook for OnCreateHook<VS, US, T, E>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
    E: Engine<VS, US, T>,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
}
