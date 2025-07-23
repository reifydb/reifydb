use crate::interface::{Engine, Transaction, UnversionedStorage, VersionedStorage};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

pub struct HookContext<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    engine: Arc<dyn Engine<VS, US, T>>,
    extensions: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl<VS, US, T> HookContext<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn engine(&self) -> &dyn Engine<VS, US, T> {
        self.engine.as_ref()
    }

    pub fn get<X: 'static>(&self) -> Option<&X> {
        self.extensions.get(&TypeId::of::<X>()).and_then(|s| s.downcast_ref::<X>())
    }
}

impl<VS, US, T> Engine<VS, US, T> for HookContext<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn begin_tx(&self) -> crate::Result<T::Tx> {
        self.engine.begin_tx()
    }

    fn begin_rx(&self) -> crate::Result<T::Rx> {
        self.engine.begin_rx()
    }
}
