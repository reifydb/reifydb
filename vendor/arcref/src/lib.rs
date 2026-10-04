//! `ArcRef<T>` is a smart pointer type that can hold either a `&'static T` or an `Arc<T>`.
//!
//! It's especially helpful for composible plugin systems where some of the implementations are
//! ZSTs or packaged with the core library, and the others are dynamically generated.
//!
//! ## Example usage:
//!
//! ```rust
//! use arcref::ArcRef;
//! use std::sync::Arc;
//!
//! #[derive(Debug, Copy, Clone, PartialOrd, PartialEq)]
//! pub enum Level { INFO, WARN, ERROR }
//!
//! pub trait Logger {
//!   fn log(&self, level: Level, line: &str) -> String;
//! }
//!
//! pub struct ConsoleLogger;
//!
//! impl Logger for ConsoleLogger {
//!   fn log(&self, level: Level, line: &str) -> String {
//!     format!("{level:?}: {line}")
//!   }
//! }
//!
//! pub struct FilteredLogger {
//!   min_level: Level,
//!   delegate: ArcRef<dyn Logger>,
//! }
//!
//! impl Logger for FilteredLogger {
//!   fn log(&self, level: Level, line: &str) -> String {
//!     if level >= self.min_level {
//!       self.delegate.log(level, line)
//!     } else {
//!       String::new()
//!     }
//!   }
//! }
//!
//! let logger = ArcRef::new_arc(Arc::new(FilteredLogger {
//!   min_level: Level::WARN,
//!   delegate: ArcRef::new_ref(&ConsoleLogger),
//! }));
//!
//! assert_eq!(logger.log(Level::INFO, "not printed"), String::new());
//! assert_eq!(logger.log(Level::WARN, "printed"), "WARN: printed");
//! ```
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;

/// Either a reference-counted `Arc` or a static reference to a value.
pub struct ArcRef<T: ?Sized + 'static>(Inner<T>);

enum Inner<T: ?Sized + 'static> {
    Arc(Arc<T>),
    Ref(&'static T),
}

impl<T: ?Sized> ArcRef<T> {
    pub fn new_arc(t: Arc<T>) -> Self
    where
        T: 'static,
    {
        ArcRef(Inner::Arc(t))
    }

    pub const fn new_ref(t: &'static T) -> Self {
        ArcRef(Inner::Ref(t))
    }
}

impl<T: ?Sized> Clone for ArcRef<T> {
    fn clone(&self) -> Self {
        match &self.0 {
            Inner::Arc(arc) => ArcRef(Inner::Arc(Arc::clone(arc))),
            Inner::Ref(r) => ArcRef(Inner::Ref(*r)),
        }
    }
}

impl<T: ?Sized> From<&'static T> for ArcRef<T> {
    fn from(r: &'static T) -> Self {
        ArcRef(Inner::Ref(r))
    }
}

impl<T: 'static> From<T> for ArcRef<T> {
    fn from(t: T) -> Self {
        ArcRef(Inner::Arc(Arc::new(t)))
    }
}

impl<T: ?Sized + 'static> From<Arc<T>> for ArcRef<T> {
    fn from(arc: Arc<T>) -> Self {
        ArcRef(Inner::Arc(arc))
    }
}

impl<T: ?Sized> Deref for ArcRef<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match &self.0 {
            Inner::Arc(arc) => arc,
            Inner::Ref(r) => r,
        }
    }
}

impl<S, T> PartialEq<ArcRef<S>> for ArcRef<T>
where
    S: ?Sized + 'static,
    T: ?Sized + 'static + PartialEq<S>,
{
    fn eq(&self, other: &ArcRef<S>) -> bool {
        self.deref() == other.deref()
    }
}

impl<T> Eq for ArcRef<T> where T: ?Sized + 'static + Eq {}

impl<S, T> PartialOrd<ArcRef<S>> for ArcRef<T>
where
    S: ?Sized + 'static,
    T: ?Sized + 'static + PartialOrd<S>,
{
    fn partial_cmp(&self, other: &ArcRef<S>) -> Option<Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}

impl<T> Ord for ArcRef<T>
where
    T: ?Sized + 'static + Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.deref().cmp(other.deref())
    }
}

impl<T> Hash for ArcRef<T>
where
    T: ?Sized + 'static + Hash,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.deref().hash(state)
    }
}

impl<T> Debug for ArcRef<T>
where
    T: ?Sized + 'static + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

impl<T> Display for ArcRef<T>
where
    T: ?Sized + 'static + Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

impl<T: ?Sized + 'static> AsRef<T> for ArcRef<T> {
    fn as_ref(&self) -> &T {
        self
    }
}
