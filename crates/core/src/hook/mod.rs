// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

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

struct HookPool {
    pool: Mutex<Vec<Vec<Box<dyn Hook>>>>,
}

impl HookPool {
    fn new() -> Self {
        Self { pool: Mutex::new(Vec::new()) }
    }

    fn get_queue(&self) -> Vec<Box<dyn Hook>> {
        self.pool.lock().unwrap().pop().unwrap_or_default()
    }

    fn return_queue(&self, mut queue: Vec<Box<dyn Hook>>) {
        queue.clear();
        if queue.capacity() <= 64 {
            self.pool.lock().unwrap().push(queue);
        }
    }
}

impl Default for HookPool {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct Hooks {
    callbacks: Arc<RwLock<HashMap<TypeId, Box<dyn CallbackList>>>>,
    pool: Arc<HookPool>,
}

impl Default for Hooks {
    fn default() -> Self {
        Self::new()
    }
}

impl Hooks {
    pub fn new() -> Self {
        Self { callbacks: Arc::new(RwLock::new(HashMap::new())), pool: Arc::new(HookPool::new()) }
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
        let mut queue = self.pool.get_queue();
        queue.push(Box::new(hook));

        while let Some(hook) = queue.pop() {
            let type_id = (*hook).type_id();
            let callbacks = self.callbacks.read().unwrap();

            if let Some(handler_list) = callbacks.get(&type_id) {
                let new_hooks = handler_list.on_any(hook.as_any())?;
                queue.extend(new_hooks);
            }
        }

        self.pool.return_queue(queue);
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
    use crate::error::diagnostic::Diagnostic;
    use crate::hook::{BoxedHookIter, Callback, Hook, Hooks};
    use crate::return_error;
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
    fn test_hooks_new() {
        let hooks = Hooks::new();
        let result = hooks.trigger(TestHook {});
        assert!(result.is_ok());
    }

    #[test]
    fn test_hooks_default() {
        let hooks = Hooks::default();
        let result = hooks.trigger(TestHook {});
        assert!(result.is_ok());
    }

    #[test]
    fn test_register_single_callback() {
        let hooks = Hooks::new();
        let callback = TestCallback::default();

        hooks.register::<TestHook, TestCallback>(callback.clone());
        hooks.trigger(TestHook {}).unwrap();
        assert_eq!(*callback.0.counter.lock().unwrap(), 1);
    }

    #[test]
    fn test_trigger_unregistered_hook() {
        let hooks = Hooks::new();
        let result = hooks.trigger(TestHook {});
        assert!(result.is_ok());
    }

    #[derive(Debug)]
    pub struct ErrorHook {}

    impl_hook!(ErrorHook);

    #[derive(Default, Debug, Clone)]
    pub struct ErrorCallback;

    impl Callback<ErrorHook> for ErrorCallback {
        fn on(&self, _hook: &ErrorHook) -> crate::Result<BoxedHookIter> {
            return_error!(Diagnostic {
                code: "".to_string(),
                statement: None,
                message: "".to_string(),
                column: None,
                span: None,
                label: None,
                help: None,
                notes: vec![],
                cause: None,
            })
        }
    }

    #[test]
    fn test_callback_error_propagation() {
        let hooks = Hooks::new();
        let callback = ErrorCallback::default();

        hooks.register::<ErrorHook, ErrorCallback>(callback);
        let result = hooks.trigger(ErrorHook {});
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_callbacks_same_hook() {
        let hooks = Hooks::new();
        let callback1 = TestCallback::default();
        let callback2 = TestCallback::default();

        hooks.register::<TestHook, TestCallback>(callback1.clone());
        hooks.register::<TestHook, TestCallback>(callback2.clone());

        hooks.trigger(TestHook {}).unwrap();
        assert_eq!(*callback1.0.counter.lock().unwrap(), 1);
        assert_eq!(*callback2.0.counter.lock().unwrap(), 1);
    }

    #[test]
    fn test_hooks_clone() {
        let hooks1 = Hooks::new();
        let callback = TestCallback::default();
        hooks1.register::<TestHook, TestCallback>(callback.clone());

        let hooks2 = hooks1.clone();
        hooks2.trigger(TestHook {}).unwrap();
        assert_eq!(*callback.0.counter.lock().unwrap(), 1);
    }

    #[test]
    fn test_concurrent_registration() {
        use std::thread;

        let hooks = Arc::new(Hooks::new());
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let hooks = hooks.clone();
                thread::spawn(move || {
                    let callback = TestCallback::default();
                    hooks.register::<TestHook, TestCallback>(callback);
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let result = hooks.trigger(TestHook {});
        assert!(result.is_ok());
    }

    #[test]
    fn test_concurrent_triggering() {
        use std::thread;

        let hooks = Arc::new(Hooks::new());
        let callback = TestCallback::default();
        hooks.register::<TestHook, TestCallback>(callback.clone());

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let hooks = hooks.clone();
                thread::spawn(move || {
                    hooks.trigger(TestHook {}).unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert!(*callback.0.counter.lock().unwrap() >= 10);
    }

    #[derive(Debug)]
    pub struct MacroTestHook {
        pub value: i32,
    }

    impl_hook!(MacroTestHook);

    #[test]
    fn test_impl_hook_macro() {
        let hook = MacroTestHook { value: 42 };
        let any_ref = hook.as_any();
        assert!(any_ref.downcast_ref::<MacroTestHook>().is_some());
        assert_eq!(any_ref.downcast_ref::<MacroTestHook>().unwrap().value, 42);
    }

    #[test]
    fn test_return_hooks_macro_empty() {
        let result: crate::Result<BoxedHookIter> = return_hooks!();
        let mut iter = result.unwrap();
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_return_hooks_macro_single() {
        let result: crate::Result<BoxedHookIter> = return_hooks!(TestHook {});
        let mut iter = result.unwrap();
        assert!(iter.next().is_some());
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_return_hooks_macro_multiple() {
        let result: crate::Result<BoxedHookIter> = return_hooks!(TestHook {}, AnotherHook {});
        let mut iter = result.unwrap();
        assert!(iter.next().is_some());
        assert!(iter.next().is_some());
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_hook_pool_reuse() {
        use crate::hook::HookPool;

        let pool = HookPool::new();
        let mut queue1 = pool.get_queue();
        let capacity1 = queue1.capacity();

        queue1.push(Box::new(TestHook {}));
        pool.return_queue(queue1);

        let queue2 = pool.get_queue();
        let capacity2 = queue2.capacity();

        assert!(capacity2 > 0);
        assert_eq!(queue2.len(), 0);
    }

    #[test]
    fn test_hook_pool_capacity_limit() {
        use crate::hook::HookPool;

        let pool = HookPool::new();
        let mut large_queue = Vec::with_capacity(100);
        for _ in 0..100 {
            large_queue.push(Box::new(TestHook {}) as Box<dyn Hook>);
        }

        pool.return_queue(large_queue);

        let reused_queue = pool.get_queue();
        assert_eq!(reused_queue.capacity(), 0);
    }

    #[test]
    fn test_hook_pool_concurrent_access() {
        use crate::hook::HookPool;
        use std::thread;

        let pool = Arc::new(HookPool::new());
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let pool = pool.clone();
                thread::spawn(move || {
                    let mut queue = pool.get_queue();
                    queue.push(Box::new(TestHook {}) as Box<dyn Hook>);
                    pool.return_queue(queue);
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let final_queue = pool.get_queue();
        assert_eq!(final_queue.len(), 0);
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
