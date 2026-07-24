//! PublicationImage: the driver-side receive state for one network publication's stream.
//!
//! Mirrors `io.aeron.driver.PublicationImage.java` (data path only). The Receiver hands each
//! incoming DATA frame here; it is rebuilt into the shared image log via [`TermRebuilder`], and
//! the subscriber polls the reconstructed stream from that same log.

use agrona::AtomicBuffer;

use aeron_client::log_buffers::LogBuffers;
use aeron_client::logbuffer::{TermRebuilder, index_by_term_count};

/// Reassembles a single session's data frames into a shared image log.
pub struct PublicationImage {
    session_id: i32,
    stream_id: i32,
    initial_term_id: i32,
    term_length: usize,
    log_buffers: LogBuffers,
    log_file_name: String,
}

impl PublicationImage {
    pub fn new(
        session_id: i32,
        stream_id: i32,
        initial_term_id: i32,
        log_buffers: LogBuffers,
        log_file_name: String,
    ) -> Self {
        let term_length = log_buffers.term_length();
        Self {
            session_id,
            stream_id,
            initial_term_id,
            term_length,
            log_buffers,
            log_file_name,
        }
    }

    pub fn session_id(&self) -> i32 {
        self.session_id
    }

    pub fn stream_id(&self) -> i32 {
        self.stream_id
    }

    pub fn log_file_name(&self) -> &str {
        &self.log_file_name
    }

    /// Rebuild one wire data frame into the image log at the position it carries.
    pub fn on_data_frame(
        &self,
        term_id: i32,
        term_offset: usize,
        frame: &AtomicBuffer,
        length: usize,
    ) {
        let index = index_by_term_count((term_id - self.initial_term_id) as i64) as usize;
        let term_buffers = self.log_buffers.duplicate_term_buffers();
        if let Some(term_buffer) = term_buffers.get(index) {
            debug_assert!(term_offset + length <= self.term_length);
            TermRebuilder::insert(term_buffer, term_offset, frame, length);
        }
    }
}
