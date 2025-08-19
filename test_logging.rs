use reifydb_logging::{LoggingBuilder, info, warn, error};

fn main() {
    // Create and start the logging subsystem
    let logging = LoggingBuilder::new()
        .with_console()
        .build();
    
    // Initialize the global logger
    reifydb_logging::init_logger(logging.get_sender());
    
    // Start the logging subsystem
    logging.start().expect("Failed to start logging");
    
    // Log some messages
    info!("This is an info message");
    warn!("This is a warning message");
    error!("This is an error message");
    
    // Give the logging thread time to process
    std::thread::sleep(std::time::Duration::from_millis(200));
    
    // Stop the logging subsystem
    logging.stop().expect("Failed to stop logging");
    
    println!("Test completed");
}