use std::sync::{Arc, Condvar, Mutex};

/// Quick and dirty Read-Write lock which does not require lifetime, might not be correct / safe
pub struct RwLock<T> {
    inner: Arc<Inner<T>>,
}

struct Inner<T> {
    state: Mutex<LockState<T>>,
    cvar: Condvar,
}

struct LockState<T> {
    data: Box<T>,
    readers: usize,
    writer: bool,
}

pub struct ReadGuard<T> {
    inner: Arc<Inner<T>>,
    ptr: *const T,
}

pub struct WriteGuard<T> {
    inner: Arc<Inner<T>>,
    ptr: *mut T,
}

impl<T> RwLock<T> {
    pub fn new(data: T) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(LockState { data: Box::new(data), readers: 0, writer: false }),
                cvar: Condvar::new(),
            }),
        }
    }

    pub fn read(&self) -> ReadGuard<T> {
        let mut lock = self.inner.state.lock().unwrap();
        while lock.writer {
            lock = self.inner.cvar.wait(lock).unwrap();
        }
        lock.readers += 1;

        let ptr = &*lock.data as *const T;
        drop(lock);
        ReadGuard { inner: Arc::clone(&self.inner), ptr: ptr }
    }

    pub fn write(&self) -> WriteGuard<T> {
        let mut lock = self.inner.state.lock().unwrap();
        while lock.writer || lock.readers > 0 {
            lock = self.inner.cvar.wait(lock).unwrap();
        }
        lock.writer = true;

        let ptr = &mut *lock.data as *mut T;

        drop(lock);
        WriteGuard { inner: Arc::clone(&self.inner), ptr: ptr }
    }
}

impl<T> std::ops::Deref for ReadGuard<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        // SAFETY: Safe because constant and not null
        unsafe { &*self.ptr }
    }
}

impl<T> std::ops::Deref for WriteGuard<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        // SAFETY: Safe because constant and not null
        unsafe { &*self.ptr }
    }
}

impl<T> std::ops::DerefMut for WriteGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: Safe because constant and not null
        unsafe { &mut *self.ptr }
    }
}

impl<T> Drop for ReadGuard<T> {
    fn drop(&mut self) {
        let mut lock = self.inner.state.lock().unwrap();
        lock.readers -= 1;
        if lock.readers == 0 {
            self.inner.cvar.notify_all();
        }
    }
}

impl<T> Drop for WriteGuard<T> {
    fn drop(&mut self) {
        let mut lock = self.inner.state.lock().unwrap();
        lock.writer = false;
        self.inner.cvar.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn test_basic_read_write() {
        let lock = RwLock::new(10);

        {
            let read = lock.read();
            assert_eq!(*read, 10);
        }

        {
            let mut write = lock.write();
            *write += 5;
        }

        {
            let read = lock.read();
            assert_eq!(*read, 15);
        }
    }

    #[test]
    fn test_multiple_readers() {
        let start = Instant::now();
        let lock = RwLock::new(9924);
        let r1 = lock.read();
        let r2 = lock.read();
        let r3 = lock.read();

        assert_eq!(*r1, 9924);
        assert_eq!(*r2, 9924);
        assert_eq!(*r3, 9924);
    }

    #[test]
    fn test_read_write_exclusion() {
        let lock = Arc::new(RwLock::new(1));
        let lock2 = Arc::clone(&lock);

        let reader = thread::spawn(move || {
            let r = lock2.read();
            thread::sleep(Duration::from_millis(20));
            assert_eq!(*r, 1);
        });

        // Give reader time to acquire lock
        thread::sleep(Duration::from_millis(10));

        // This should block until reader releases
        let writer_lock = lock.clone();
        let writer = thread::spawn(move || {
            let mut w = writer_lock.write();
            *w = 42;
        });

        reader.join().unwrap();
        writer.join().unwrap();

        assert_eq!(*lock.read(), 42);
    }

    #[test]
    fn test_write_exclusion() {
        let lock1 = Arc::new(RwLock::new(5));
        let lock2 = Arc::clone(&lock1);

        let writer_lock = lock1.clone();
        let writer1 = thread::spawn(move || {
            let mut w = writer_lock.write();
            *w += 1;
            thread::sleep(Duration::from_millis(10));
        });

        // let writer1 lock first
        thread::sleep(Duration::from_millis(10));

        let writer2 = thread::spawn(move || {
            let mut w = lock2.write();
            *w *= 10;
        });

        writer1.join().unwrap();
        writer2.join().unwrap();

        let final_val = *lock1.read();
        assert_eq!(final_val, (5 + 1) * 10);
    }

    #[test]
    fn test_concurrent_reads_then_write() {
        let lock = Arc::new(RwLock::new(100));
        let mut handles = vec![];

        for _ in 0..4 {
            let l = Arc::clone(&lock);
            handles.push(thread::spawn(move || {
                let r = l.read();
                assert_eq!(*r, 100);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        {
            let mut w = lock.write();
            *w = 200;
        }

        assert_eq!(*lock.read(), 200);
    }
}
