use arcref::ArcRef;
use std::sync::Arc;

/// Logging levels
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq)]
pub enum Level {
    INFO,
    WARN,
    ERROR,
}

/// Generic interface for logging
pub trait Logger {
    fn log(&self, level: Level, line: &str);
}

pub trait LoggerFactory {
    fn new_logger(&self) -> ArcRef<dyn Logger>;
}

// Builtin loggers
pub struct StdoutLogger;

impl Logger for StdoutLogger {
    fn log(&self, level: Level, line: &str) {
        println!("{level:?}: {line}");
    }
}

// Default console logger factory
pub struct ConsoleFactory;

impl LoggerFactory for ConsoleFactory {
    fn new_logger(&self) -> ArcRef<dyn Logger> {
        // StdoutLogger is a ZST, so we can create a &'static dyn Logger to it
        ArcRef::new_ref(&StdoutLogger)
    }
}

/// Only log at the configured level or higher
pub struct FilteredLogger(Level, ArcRef<dyn Logger>);

impl Logger for FilteredLogger {
    fn log(&self, level: Level, line: &str) {
        if level >= self.0 {
            self.1.log(level, line);
        }
    }
}

pub struct FilteredLoggerFactory {
    min_level: Level,
    delegate: ArcRef<dyn Logger>,
}

impl LoggerFactory for FilteredLoggerFactory {
    fn new_logger(&self) -> ArcRef<dyn Logger> {
        ArcRef::new_arc(Arc::new(FilteredLogger(
            self.min_level,
            self.delegate.clone(),
        )))
    }
}

fn main() {
    let console = ConsoleFactory.new_logger();
    let filtered = FilteredLoggerFactory {
        min_level: Level::WARN,
        delegate: console,
    };

    let logger = filtered.new_logger();
    logger.log(Level::INFO, "this will not print");
    logger.log(Level::WARN, "but this will");
    logger.log(Level::ERROR, "and so will this");
}
