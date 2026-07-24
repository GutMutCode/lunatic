use std::io::{self, BufWriter, Write};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};

use crate::protocol::{encode_event_line, EventEnvelope, EventMessage, RequestId};

#[derive(Clone)]
pub struct EventSink {
    inner: Arc<Mutex<EventWriter>>,
}

struct EventWriter {
    next_sequence: u64,
    output: BufWriter<io::Stdout>,
}

impl EventSink {
    pub fn stdout() -> Self {
        Self {
            inner: Arc::new(Mutex::new(EventWriter {
                next_sequence: 1,
                output: BufWriter::new(io::stdout()),
            })),
        }
    }

    pub fn emit(&self, request_id: RequestId, message: EventMessage) -> Result<()> {
        let mut writer = self
            .inner
            .lock()
            .map_err(|_| anyhow!("event writer lock poisoned"))?;
        let sequence = writer.next_sequence;
        let envelope = EventEnvelope::new(sequence, request_id, message);
        let line = encode_event_line(&envelope)?;
        writer.output.write_all(line.as_bytes())?;
        writer.output.write_all(b"\n")?;
        writer.output.flush()?;
        writer.next_sequence = sequence
            .checked_add(1)
            .ok_or_else(|| anyhow!("event sequence overflow"))?;
        Ok(())
    }
}
