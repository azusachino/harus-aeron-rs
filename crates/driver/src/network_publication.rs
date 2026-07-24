//! NetworkPublication: the driver-side send state for a UDP publication.
//!
//! Mirrors `io.aeron.driver.NetworkPublication.java` (data path only). The publisher client
//! writes framed data frames into the shared log; the [`Sender`](crate::sender::Sender) scans
//! the active term and transmits SETUP + DATA + heartbeat frames over the channel endpoint.

use std::net::SocketAddr;

use agrona::AtomicBuffer;

use aeron_client::log_buffers::LogBuffers;
use aeron_client::logbuffer::{
    TermScanner, compute_position, index_by_position, position_bits_to_shift,
};
use aeron_client::protocol::{
    CURRENT_VERSION, DATA_HEADER_LENGTH, DataHeaderFlyweight, HDR_TYPE_DATA, HDR_TYPE_SETUP,
    SETUP_HEADER_LENGTH, SetupFlyweight,
};

use crate::error::Result;
use crate::media::send_channel_endpoint::SendChannelEndpoint;

/// Interval between SETUP re-sends and idle heartbeats (100 ms), matching upstream defaults.
const PUBLICATION_HEARTBEAT_NS: i64 = 100_000_000;

/// A network publication's outbound data path over a single channel endpoint.
pub struct NetworkPublication {
    registration_id: i64,
    log_buffers: LogBuffers,
    endpoint: SendChannelEndpoint,
    destination: SocketAddr,
    session_id: i32,
    stream_id: i32,
    initial_term_id: i32,
    term_length: usize,
    mtu_length: usize,
    position_bits_to_shift: usize,
    /// Stream position (bytes) already transmitted.
    sender_position: i64,
    setup_sent: bool,
    last_send_ns: i64,
    receiver_limit: i64,
    status_received: bool,
}

impl NetworkPublication {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        registration_id: i64,
        log_buffers: LogBuffers,
        endpoint: SendChannelEndpoint,
        destination: SocketAddr,
        session_id: i32,
        stream_id: i32,
        initial_term_id: i32,
        mtu_length: usize,
    ) -> Self {
        let term_length = log_buffers.term_length();
        Self {
            registration_id,
            log_buffers,
            endpoint,
            destination,
            session_id,
            stream_id,
            initial_term_id,
            term_length,
            mtu_length,
            position_bits_to_shift: position_bits_to_shift(term_length),
            sender_position: 0,
            setup_sent: false,
            last_send_ns: i64::MIN,
            receiver_limit: 0,
            status_received: false,
        }
    }

    pub fn registration_id(&self) -> i64 {
        self.registration_id
    }

    pub fn sender_position(&self) -> i64 {
        self.sender_position
    }

    pub fn receiver_limit(&self) -> i64 {
        self.receiver_limit
    }

    /// Drain SM/NAK control frames and apply them to this publication.
    pub fn poll_control(&mut self) -> Result<usize> {
        let mut work = self.endpoint.poll_control()?;
        for status in self.endpoint.take_status_messages() {
            if status.session_id == self.session_id && status.stream_id == self.stream_id {
                let consumption_position = compute_position(
                    status.term_id,
                    status.term_offset,
                    self.position_bits_to_shift as i32,
                    self.initial_term_id,
                );
                self.receiver_limit = consumption_position
                    .saturating_add(status.receiver_window_length.max(0) as i64);
                self.status_received = true;
                work += 1;
            }
        }
        for nak in self.endpoint.take_nak_messages() {
            if nak.session_id == self.session_id && nak.stream_id == self.stream_id {
                work +=
                    self.retransmit(nak.term_id, nak.term_offset, nak.length.max(0) as usize)?;
            }
        }
        Ok(work)
    }

    /// One send duty cycle: (re)announce with SETUP, flush any new data, else heartbeat.
    pub fn do_work(&mut self, now_ns: i64) -> Result<usize> {
        let mut work = 0;

        // Announce the stream with SETUP on start-up and periodically while idle so a late
        // receiver can join. Real connection gating on Status Messages lands with flow control.
        if !self.setup_sent || now_ns.saturating_sub(self.last_send_ns) >= PUBLICATION_HEARTBEAT_NS
        {
            self.send_setup()?;
            self.setup_sent = true;
            self.last_send_ns = now_ns;
            work += 1;
        }

        work += self.send_data(now_ns)?;
        Ok(work)
    }

    fn send_data(&mut self, now_ns: i64) -> Result<usize> {
        if !self.status_received || self.sender_position >= self.receiver_limit {
            return Ok(0);
        }
        let index = index_by_position(self.sender_position, self.position_bits_to_shift);
        let term_buffers = self.log_buffers.duplicate_term_buffers();
        let term_buffer = term_buffers[index];
        let term_offset = (self.sender_position as usize) & (self.term_length - 1);

        // Whole data frames available from the sender position, bounded by the MTU.
        let scan = TermScanner::scan_for_availability(&term_buffer, term_offset, self.mtu_length);
        let available = TermScanner::available(scan);
        let padding = TermScanner::padding(scan);

        if available > 0 && self.sender_position + available as i64 <= self.receiver_limit {
            let mut bytes = vec![0u8; available as usize];
            term_buffer.get_bytes(term_offset, &mut bytes);
            self.endpoint.send_data(&bytes, self.destination)?;
            self.sender_position += (available + padding) as i64;
            self.last_send_ns = now_ns;
            return Ok(1);
        }

        // Idle: emit a zero-length heartbeat data frame so the receiver keeps the image alive.
        if now_ns.saturating_sub(self.last_send_ns) >= PUBLICATION_HEARTBEAT_NS {
            self.send_heartbeat()?;
            self.last_send_ns = now_ns;
            return Ok(1);
        }
        Ok(0)
    }

    fn retransmit(&self, term_id: i32, term_offset: i32, length: usize) -> Result<usize> {
        if length == 0 || term_offset < 0 {
            return Ok(0);
        }
        let term_count = term_id.wrapping_sub(self.initial_term_id);
        if term_count < 0 || term_count as usize >= 3 {
            return Ok(0);
        }

        let term_buffers = self.log_buffers.duplicate_term_buffers();
        let term_buffer = term_buffers[term_count as usize];
        let mut offset = term_offset as usize;
        let end = offset.saturating_add(length).min(self.term_length);
        let mut work = 0;
        while offset < end {
            let scan = TermScanner::scan_for_availability(&term_buffer, offset, self.mtu_length);
            let available = TermScanner::available(scan);
            if available <= 0 {
                break;
            }
            let available = available as usize;
            let mut bytes = vec![0u8; available];
            term_buffer.get_bytes(offset, &mut bytes);
            self.endpoint.send_data(&bytes, self.destination)?;
            offset += available + TermScanner::padding(scan) as usize;
            work += 1;
        }
        Ok(work)
    }

    fn send_setup(&self) -> Result<usize> {
        let mut bytes = [0u8; SETUP_HEADER_LENGTH];
        let setup = SetupFlyweight::new(AtomicBuffer::new(&mut bytes));
        let header = setup.header();
        header.set_frame_length(SETUP_HEADER_LENGTH as i32);
        header.set_version(CURRENT_VERSION);
        header.set_flags(0);
        header.set_header_type(HDR_TYPE_SETUP);
        setup.set_term_offset(0);
        setup.set_session_id(self.session_id);
        setup.set_stream_id(self.stream_id);
        setup.set_initial_term_id(self.initial_term_id);
        setup.set_active_term_id(self.initial_term_id);
        setup.set_term_length(self.term_length as i32);
        setup.set_mtu_length(self.mtu_length as i32);
        setup.set_ttl(0);
        self.endpoint.send_data(&bytes, self.destination)?;
        Ok(1)
    }

    fn send_heartbeat(&self) -> Result<usize> {
        let index = index_by_position(self.sender_position, self.position_bits_to_shift);
        let term_id = self.initial_term_id + index as i32;
        let term_offset = (self.sender_position as usize) & (self.term_length - 1);

        let mut bytes = [0u8; DATA_HEADER_LENGTH];
        let data = DataHeaderFlyweight::new(AtomicBuffer::new(&mut bytes));
        data.set_frame_length(0);
        data.set_version(CURRENT_VERSION);
        data.set_header_type(HDR_TYPE_DATA);
        data.set_term_offset(term_offset as i32);
        data.set_session_id(self.session_id);
        data.set_stream_id(self.stream_id);
        data.set_term_id(term_id);
        self.endpoint.send_data(&bytes, self.destination)?;
        Ok(1)
    }
}
