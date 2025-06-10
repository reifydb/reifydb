// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::sync::{Arc, RwLock};

pub struct Registry<T: Send + Sync + 'static + ?Sized> {
    hooks: RwLock<Vec<Arc<T>>>,
}

impl<T: Send + Sync + 'static + ?Sized> Default for Registry<T> {
    fn default() -> Self {
        Self { hooks: RwLock::new(Vec::new()) }
    }
}

impl<T: Send + Sync + 'static + ?Sized> Registry<T> {
    pub fn register(&self, hook: Arc<T>) {
        self.hooks.write().unwrap().push(hook);
    }

    pub fn for_each<F: Fn(&Arc<T>)>(&self, f: F) {
        for hook in self.hooks.read().unwrap().iter() {
            f(hook);
        }
    }
}
