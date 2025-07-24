// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub mod lifecycle;
pub mod transaction;

pub type BoxedHookIter = Box<dyn Iterator<Item = Box<dyn Hook>>>;

pub trait Hook: Any + Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;
}

pub trait Callback<H>: Send + Sync + 'static
where
    H: Hook,
{
    fn on(&self, hook: &H) -> Result<BoxedHookIter, crate::Error>;
}

trait CallbackList: Any + Send + Sync {
    fn on_any(&self, hook: &dyn Any) -> Result<Vec<Box<dyn Hook>>, crate::Error>;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

struct CallbackListImpl<T> {
    handlers: RwLock<Vec<Box<dyn Callback<T>>>>,
}

impl<H> CallbackListImpl<H>
where
    H: Hook,
{
    fn new() -> Self {
        Self { handlers: RwLock::new(Vec::new()) }
    }

    fn add(&mut self, obs: Box<dyn Callback<H>>) {
        self.handlers.write().unwrap().push(obs);
    }
}

impl<H> CallbackList for CallbackListImpl<H>
where
    H: Hook,
{
    fn on_any(&self, hook: &dyn Any) -> Result<Vec<Box<dyn Hook>>, crate::Error> {
        if let Some(hook) = hook.downcast_ref::<H>() {
            let mut result = Vec::new();
            for handler in self.handlers.read().unwrap().iter() {
                result.extend(handler.on(hook)?);
            }
            Ok(result)
        } else {
            Ok(vec![])
        }
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Default, Clone)]
pub struct Hooks {
    callbacks: Arc<RwLock<HashMap<TypeId, Box<dyn CallbackList>>>>,
}

impl Hooks {
    pub fn new() -> Self {
        Self { callbacks: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub fn register<H, C>(&self, callback: C)
    where
        H: Hook,
        C: Callback<H>,
    {
        let type_id = TypeId::of::<H>();

        self.callbacks
            .write()
            .unwrap()
            .entry(type_id)
            .or_insert_with(|| Box::new(CallbackListImpl::<H>::new()))
            .as_any_mut()
            .downcast_mut::<CallbackListImpl<H>>()
            .unwrap()
            .add(Box::new(callback));
    }

    pub fn trigger<H>(&self, hook: H) -> crate::Result<()>
    where
        H: Hook,
    {
        let mut queue: Vec<Box<dyn Hook>> = vec![Box::new(hook)];
        while let Some(hook) = queue.pop() {
            let type_id = (*hook).type_id();
            let callbacks = self.callbacks.read().unwrap();

            if let Some(handler_list) = callbacks.get(&type_id) {
                let new_hooks = handler_list.on_any(hook.as_any())?;
                queue.extend(new_hooks);
            }
        }
        Ok(())
    }
}

#[macro_export]
macro_rules! impl_hook {
    ($ty:ty) => {
        impl $crate::hook::Hook for $ty {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
        }
    };
}

#[macro_export]
macro_rules! return_hooks {
    () => {
        Ok(Box::new(std::iter::empty()) as $crate::hook::BoxedHookIter)
    };
    ($single:expr) => {
        Ok(Box::new(std::iter::once(Box::new($single) as Box<dyn $crate::hook::Hook>)) as $crate::hook::BoxedHookIter)
    };
    ($($hook:expr),+ $(,)?) => {
        Ok(Box::new(vec![$(Box::new($hook) as Box<dyn $crate::hook::Hook>),+].into_iter()) as $crate::hook::BoxedHookIter)
    };
}

#[cfg(test)]
mod tests {
    use crate::hook::{BoxedHookIter, Callback, Hooks};
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    pub struct TestHook {}

    impl_hook!(TestHook);

    #[derive(Debug)]
    pub struct AnotherHook {}

    impl_hook!(AnotherHook);

    #[derive(Default, Debug, Clone)]
    pub struct TestCallback(Arc<TestHandlerInner>);

    #[derive(Default, Debug)]
    pub struct TestHandlerInner {
        pub counter: Arc<Mutex<i32>>,
    }

    impl Callback<TestHook> for TestCallback {
        fn on(&self, _hook: &TestHook) -> crate::Result<BoxedHookIter> {
            let mut x = self.0.counter.lock().unwrap();
            *x += 1;
            return_hooks!(AnotherHook {})
        }
    }

    impl Callback<AnotherHook> for TestCallback {
        fn on(&self, _hook: &AnotherHook) -> crate::Result<BoxedHookIter> {
            let mut x = self.0.counter.lock().unwrap();
            *x *= 2;
            return_hooks!()
        }
    }

    #[test]
    fn test_subscribe_to_many_hooks() {
        let test_instance = Hooks::default();
        let test_callback = TestCallback::default();

        test_instance.register::<TestHook, TestCallback>(test_callback.clone());
        test_instance.register::<AnotherHook, TestCallback>(test_callback.clone());

        test_instance.trigger(TestHook {}).unwrap();
        assert_eq!(*test_callback.0.counter.lock().unwrap(), 2);

        test_instance.trigger(TestHook {}).unwrap();
        assert_eq!(*test_callback.0.counter.lock().unwrap(), 6);

        test_instance.trigger(AnotherHook {}).unwrap();
        assert_eq!(*test_callback.0.counter.lock().unwrap(), 12);
    }
}
