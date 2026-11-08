use std::time::Duration;
use common::log_system::LogEvent;

pub struct BatchConfig {
    pub max_batch_size: usize,
    pub max_flush_interval: Duration
}

pub struct LogBuffer {
    config: BatchConfig,
    events: Vec<LogEvent>
}

impl LogBuffer {
    pub fn new(config: BatchConfig) -> Self {
        let vec_capacity = config.max_batch_size;
        Self {
            config,
            events: Vec::with_capacity(vec_capacity)
        }
    }

    /// pushes an event into the buffer and return ths batch for flushing if capacity is reached.
    pub fn push(&mut self, event: LogEvent) -> Option<Vec<LogEvent>> {
        self.events.push(event);

        if self.events.len() >= self.config.max_batch_size {
            return Some(self.drain())
        }

        None
    }

    /// flushes and returns all accumulated events currently in the buffer.
    pub fn drain(&mut self) -> Vec<LogEvent> {
        // Clears the buffer vector and returns the owned elements in $O(1)$ time without reallocating underlying heap memory.
        std::mem::take(&mut self.events)
    }
}