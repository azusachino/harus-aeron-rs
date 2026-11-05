//! NetworkPublication: the driver-side send state for a UDP publication.
//!
//! Mirrors `io.aeron.driver.NetworkPublication.java` (data path only). The publisher client
//! writes framed data frames into the shared log; the [`Sender`](crate::sender::Sender) scans
//! the active term and transmits SETUP + DATA + heartbeat frames over the channel endpoint.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use agrona::AtomicBuffer;

use aeron_client::log_buffers::LogBuffers;
use aeron_client::logbuffer::{
    TermScanner, index_by_position, position_bits_to_shift, set_is_connected,
};
use aeron_client::protocol::{
    CURRENT_VERSION, DATA_HEADER_LENGTH, DataHeaderFlyweight, END_OF_STREAM_FLAG, HDR_TYPE_DATA,
    HDR_TYPE_SETUP, HDR_TYPE_SM, SETUP_HEADER_LENGTH, STATUS_MESSAGE_HEADER_LENGTH, SetupFlyweight,
    StatusMessageFlyweight,
};

use crate::error::Result;
use crate::flow_control::calculate_retransmission_length;
use crate::loss_detector::StaticDelayGenerator;
use crate::media::send_channel_endpoint::SendChannelEndpoint;
use crate::multicast_flow_control::NetworkFlowControl;
use crate::receiver_liveness_tracker::ReceiverLivenessTracker;
use crate::retransmit_handler::{RetransmitFlowControl, RetransmitHandler};

/// Interval between SETUP re-sends and idle heartbeats (100 ms), matching upstream defaults.
const PUBLICATION_HEARTBEAT_NS: i64 = 100_000_000;
const RETRANSMIT_UNICAST_LINGER_NS: i64 = 10_000_000;

struct PublicationRetransmitFlowControl {
    retransmit_receiver_window_multiple: i32,
}

impl RetransmitFlowControl for PublicationRetransmitFlowControl {
    fn max_retransmission_length(
        &self,
        term_offset: i32,
        resend_length: i32,
        term_length: i32,
        _mtu_length: i32,
    ) -> i32 {
        calculate_retransmission_length(
            resend_length,
            term_length,
            term_offset,
            self.retransmit_receiver_window_multiple,
        )
    }
}

fn term_id_and_offset(
    position: i64,
    initial_term_id: i32,
    position_bits_to_shift: usize,
    term_length: usize,
) -> (i32, i32) {
    let term_count = position >> position_bits_to_shift;
    let term_offset = (position as usize) & (term_length - 1);
    (
        initial_term_id.wrapping_add(term_count as i32),
        term_offset as i32,
    )
}

/// A network publication's outbound data path over a single channel endpoint.
pub struct NetworkPublication {
    registration_id: i64,
    log_buffers: LogBuffers,
    endpoint: SendChannelEndpoint,
    destinations: Vec<SocketAddr>,
    session_id: i32,
    stream_id: i32,
    initial_term_id: i32,
    term_length: usize,
    mtu_length: usize,
    position_bits_to_shift: usize,
    /// Stream position (bytes) already transmitted.
    sender_position: i64,
    /// Position through which old term data has been cleared; trails the sender by one term.
    clean_position: i64,
    setup_sent: bool,
    last_send_ns: i64,
    receiver_limit: i64,
    status_received: bool,
    publication_limit: Arc<AtomicI64>,
    retransmit_receiver_window_multiple: i32,
    retransmit_handler: RetransmitHandler,
    /// The channel's selected `fc=` strategy (max/min/tagged), live per-receiver state included.
    flow_control: NetworkFlowControl,
    liveness_tracker: ReceiverLivenessTracker,
    connection_timeout_ns: i64,
    is_connected: bool,
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
        publication_limit: Arc<AtomicI64>,
        retransmit_receiver_window_multiple: i32,
    ) -> Result<Self> {
        Self::new_with_position(
            registration_id,
            log_buffers,
            endpoint,
            destination,
            session_id,
            stream_id,
            initial_term_id,
            mtu_length,
            0,
            publication_limit,
            retransmit_receiver_window_multiple,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_position(
        registration_id: i64,
        log_buffers: LogBuffers,
        endpoint: SendChannelEndpoint,
        destination: SocketAddr,
        session_id: i32,
        stream_id: i32,
        initial_term_id: i32,
        mtu_length: usize,
        initial_position: i64,
        publication_limit: Arc<AtomicI64>,
        retransmit_receiver_window_multiple: i32,
    ) -> Result<Self> {
        let term_length = log_buffers.term_length();
        let has_group_semantics = endpoint.udp_channel().has_group_semantics();
        let flow_control = NetworkFlowControl::new_for_channel(
            endpoint.udp_channel(),
            stream_id,
            registration_id,
        )?;
        let connection_timeout_ns = flow_control.receiver_timeout_ns();
        let destinations = if destination.port() == 0 {
            Vec::new()
        } else {
            vec![destination]
        };
        Ok(Self {
            registration_id,
            log_buffers,
            endpoint,
            destinations,
            session_id,
            stream_id,
            initial_term_id,
            term_length,
            mtu_length,
            position_bits_to_shift: position_bits_to_shift(term_length),
            sender_position: initial_position,
            clean_position: initial_position,
            setup_sent: false,
            last_send_ns: i64::MIN,
            receiver_limit: 0,
            status_received: false,
            publication_limit,
            retransmit_receiver_window_multiple,
            retransmit_handler: RetransmitHandler::new(
                Arc::new(AtomicI64::new(0)),
                StaticDelayGenerator::new(0),
                StaticDelayGenerator::new(RETRANSMIT_UNICAST_LINGER_NS),
                has_group_semantics,
                16,
                Arc::new(AtomicI64::new(0)),
            ),
            flow_control,
            liveness_tracker: ReceiverLivenessTracker::new(),
            connection_timeout_ns,
            is_connected: false,
        })
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

    pub fn publication_limit(&self) -> i64 {
        self.publication_limit.load(Ordering::Acquire)
    }

    pub fn clean_position(&self) -> i64 {
        self.clean_position
    }

    pub fn is_connected(&self) -> bool {
        self.is_connected
    }

    fn has_subscribers(&self) -> bool {
        self.liveness_tracker.has_receivers() && self.flow_control.has_required_receivers()
    }

    fn update_connected_state(&mut self) {
        let new_connected_state = self.has_subscribers();
        if self.is_connected != new_connected_state {
            set_is_connected(&self.log_buffers.meta_data_buffer(), new_connected_state);
            self.is_connected = new_connected_state;
        }
    }

    /// Check if the Status Message position is valid given the current sender position.
    ///
    /// Mirrors `NetworkPublication.isValidStatusMessage` in upstream Aeron 1.54:
    /// an SM is valid if its consumption position is between half a term behind
    /// and 1.5 terms ahead of the current `sender_position`.
    pub fn is_valid_status_message(&self, term_id: i32, term_offset: i32) -> bool {
        let sm_pos = aeron_client::logbuffer::compute_position(
            term_id,
            term_offset,
            self.position_bits_to_shift as i32,
            self.initial_term_id,
        );
        let snd_pos = self.sender_position;
        let half_term = (self.term_length >> 1) as i64;
        sm_pos >= (snd_pos - half_term) && sm_pos <= (snd_pos + self.term_length as i64 + half_term)
    }

    pub fn add_destination(&mut self, destination: SocketAddr) -> bool {
        if self.destinations.contains(&destination) {
            false
        } else {
            self.destinations.push(destination);
            self.setup_sent = false;
            true
        }
    }

    pub fn remove_destination(&mut self, destination: SocketAddr) -> bool {
        let Some(index) = self
            .destinations
            .iter()
            .position(|item| *item == destination)
        else {
            return false;
        };
        self.destinations.remove(index);
        true
    }

    pub fn destinations(&self) -> &[SocketAddr] {
        &self.destinations
    }

    /// Drain SM/NAK control frames and apply them to this publication.
    pub fn poll_control(&mut self, now_ns: i64) -> Result<usize> {
        let mut work = self.endpoint.poll_control()?;
        for status in self.endpoint.take_status_messages() {
            if status.session_id == self.session_id && status.stream_id == self.stream_id {
                let term_length = self.term_length as i32;
                if status.term_offset < 0
                    || status.term_offset >= term_length
                    || status.term_offset % 32 != 0
                    || status.receiver_window_length < 0
                    || status.receiver_window_length > term_length / 2
                {
                    continue;
                }
                if status.flags & aeron_client::protocol::SEND_SETUP_FLAG != 0 {
                    self.setup_sent = false;
                    work += 1;
                    continue;
                }
                if !self.is_valid_status_message(status.term_id, status.term_offset) {
                    continue;
                }
                // The channel endpoint already decoded the wire frame into plain fields; rebuild
                // a flyweight view over them so the selected flow-control strategy (which mirrors
                // vendor's flyweight-based API) can read it exactly as it would off the wire.
                let mut scratch = [0u8; STATUS_MESSAGE_HEADER_LENGTH + 8];
                let length = if status.group_tag.is_some() {
                    scratch.len()
                } else {
                    STATUS_MESSAGE_HEADER_LENGTH
                };
                let sm = StatusMessageFlyweight::new(AtomicBuffer::new(&mut scratch[..length]));
                sm.header()
                    .set_frame_length(length as i32)
                    .set_version(CURRENT_VERSION)
                    .set_flags(status.flags)
                    .set_header_type(HDR_TYPE_SM);
                sm.set_session_id(status.session_id)
                    .set_stream_id(status.stream_id)
                    .set_consumption_term_id(status.term_id)
                    .set_consumption_term_offset(status.term_offset)
                    .set_receiver_window_length(status.receiver_window_length)
                    .set_receiver_id(status.receiver_id);
                if let Some(group_tag) = status.group_tag {
                    sm.set_group_tag(Some(group_tag));
                }

                if status.flags & END_OF_STREAM_FLAG != 0 {
                    self.liveness_tracker.on_remote_close(status.receiver_id);
                } else {
                    self.liveness_tracker
                        .on_status_message(status.receiver_id, now_ns);
                }

                // SETUP is periodically re-sent and can elicit an older offset-0 SM after a
                // newer data-progress SM. A reliable publication must not move its send limit
                // backwards because of that stale control frame -- every strategy's
                // on_status_message is itself monotonic against the sender_limit it's handed.
                self.receiver_limit = self.flow_control.on_status_message(
                    sm,
                    self.receiver_limit,
                    self.initial_term_id,
                    self.position_bits_to_shift as i32,
                    now_ns,
                );
                self.status_received = true;
                self.update_publication_limit();
                self.update_connected_state();
                work += 1;
            }
        }
        for nak in self.endpoint.take_nak_messages() {
            if nak.session_id == self.session_id && nak.stream_id == self.stream_id {
                if nak.term_offset < 0
                    || nak.term_offset >= self.term_length as i32
                    || nak.term_offset % 32 != 0
                    || nak.length < 0
                    || nak.term_offset as i64 + nak.length as i64 > self.term_length as i64
                {
                    continue;
                }
                let flow_control = PublicationRetransmitFlowControl {
                    retransmit_receiver_window_multiple: self.retransmit_receiver_window_multiple,
                };
                let mut retransmit_work = 0;
                let mut retransmit_error = None;
                let log_buffers = &self.log_buffers;
                let endpoint = &self.endpoint;
                let destinations = self.destinations.clone();
                let initial_term_id = self.initial_term_id;
                let term_length = self.term_length;
                let mtu_length = self.mtu_length;
                let rrwm = self.retransmit_receiver_window_multiple;
                self.retransmit_handler.on_nak(
                    nak.term_id,
                    nak.term_offset,
                    nak.length,
                    term_length.min(i32::MAX as usize) as i32,
                    mtu_length.min(i32::MAX as usize) as i32,
                    now_ns,
                    &flow_control,
                    &mut |term_id, term_offset, length| match Self::retransmit_range(
                        log_buffers,
                        endpoint,
                        destinations.clone(),
                        initial_term_id,
                        term_length,
                        mtu_length,
                        rrwm,
                        term_id,
                        term_offset,
                        length,
                    ) {
                        Ok(count) => retransmit_work += count,
                        Err(error) => retransmit_error = Some(error),
                    },
                );
                if let Some(error) = retransmit_error {
                    return Err(error);
                }
                work += retransmit_work;
            }
        }
        let log_buffers = &self.log_buffers;
        let endpoint = &self.endpoint;
        let destinations = self.destinations.clone();
        let initial_term_id = self.initial_term_id;
        let term_length = self.term_length;
        let mtu_length = self.mtu_length;
        let rrwm = self.retransmit_receiver_window_multiple;
        let mut retransmit_work = 0;
        let mut retransmit_error = None;
        self.retransmit_handler
            .process_timeouts(now_ns, &mut |term_id, term_offset, length| {
                match Self::retransmit_range(
                    log_buffers,
                    endpoint,
                    destinations.clone(),
                    initial_term_id,
                    term_length,
                    mtu_length,
                    rrwm,
                    term_id,
                    term_offset,
                    length,
                ) {
                    Ok(count) => retransmit_work += count,
                    Err(error) => retransmit_error = Some(error),
                }
            });
        if let Some(error) = retransmit_error {
            return Err(error);
        }
        work += retransmit_work;
        // Expire receivers min/tagged flow control has not heard from within their timeout,
        // independent of whether an SM arrived this cycle -- a receiver that stops sending
        // status messages entirely (crash, network partition) must eventually stop constraining
        // the sender's window, not hold it hostage forever.
        let previous_limit = self.receiver_limit;
        self.receiver_limit = self.flow_control.on_idle(now_ns, self.receiver_limit);
        if self.receiver_limit != previous_limit {
            self.update_publication_limit();
        }
        self.liveness_tracker
            .on_idle(now_ns, self.connection_timeout_ns);
        self.update_connected_state();
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
        let term_buffer = term_buffers[index].clone();
        let term_offset = (self.sender_position as usize) & (self.term_length - 1);

        // Whole data frames available from the sender position, bounded by the MTU.
        let scan = TermScanner::scan_for_availability(&term_buffer, term_offset, self.mtu_length);
        let available = TermScanner::available(scan);
        let padding = TermScanner::padding(scan);

        if available > 0 && self.sender_position + available as i64 <= self.receiver_limit {
            let mut bytes = vec![0u8; available as usize];
            term_buffer.get_bytes(term_offset, &mut bytes);
            for destination in &self.destinations {
                self.endpoint.send_data(&bytes, *destination)?;
            }
            self.sender_position += (available + padding) as i64;
            self.clean_log_behind_sender();
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

    fn update_publication_limit(&self) {
        if !self.status_received {
            self.publication_limit.store(0, Ordering::Release);
            return;
        }

        let current_limit = self.publication_limit.load(Ordering::Acquire);
        if self.receiver_limit <= current_limit {
            return;
        }

        // This is the same dirty-term guard as NetworkPublication.updatePublisherPositionAndLimit
        // in vendor/aeron/aeron-driver/src/main/java/io/aeron/driver/NetworkPublication.java:
        // the producer may advance through the current term, but must not advance two terms
        // beyond the term that has been cleaned.
        let clean_term = self.clean_position >> self.position_bits_to_shift;
        let active_term = self.receiver_limit >> self.position_bits_to_shift;
        let term_gap = active_term - clean_term;
        let clean_term_is_partial = (self.clean_position & (self.term_length as i64 - 1)) != 0;
        if term_gap < 2 || (term_gap == 2 && clean_term_is_partial) {
            self.publication_limit
                .store(self.receiver_limit, Ordering::Release);
        }
    }

    /// Clear data which is at least one complete term behind the sender.
    fn clean_log_behind_sender(&mut self) {
        let target = self.sender_position.saturating_sub(self.term_length as i64);
        if target <= self.clean_position {
            return;
        }

        let term_buffers = self.log_buffers.duplicate_term_buffers();
        let mut position = self.clean_position;
        while position < target {
            let index = index_by_position(position, self.position_bits_to_shift);
            let offset = (position as usize) & (self.term_length - 1);
            let length = (target - position).min((self.term_length - offset) as i64) as usize;
            if let Some(term_buffer) = term_buffers.get(index) {
                term_buffer.put_bytes(offset, &vec![0; length]);
            }
            position += length as i64;
        }
        self.clean_position = target;
        self.update_publication_limit();
    }

    #[allow(clippy::too_many_arguments)]
    fn retransmit_range(
        log_buffers: &LogBuffers,
        endpoint: &SendChannelEndpoint,
        destinations: Vec<SocketAddr>,
        initial_term_id: i32,
        term_length: usize,
        mtu_length: usize,
        retransmit_receiver_window_multiple: i32,
        term_id: i32,
        term_offset: i32,
        length: i32,
    ) -> Result<usize> {
        if length <= 0 || term_offset < 0 {
            return Ok(0);
        }
        let term_count = term_id.wrapping_sub(initial_term_id);
        if term_count < 0 || term_count as usize >= 3 {
            return Ok(0);
        }

        let term_buffers = log_buffers.duplicate_term_buffers();
        let term_buffer = term_buffers[term_count as usize].clone();
        let mut offset = term_offset as usize;
        let length = calculate_retransmission_length(
            length,
            term_length.min(i32::MAX as usize) as i32,
            term_offset,
            retransmit_receiver_window_multiple,
        )
        .max(0) as usize;
        let end = offset.saturating_add(length).min(term_length);
        let mut work = 0;
        while offset < end {
            let scan = TermScanner::scan_for_availability(&term_buffer, offset, mtu_length);
            let available = TermScanner::available(scan);
            if available <= 0 {
                break;
            }
            let available = available as usize;
            let mut bytes = vec![0u8; available];
            term_buffer.get_bytes(offset, &mut bytes);
            for destination in &destinations {
                endpoint.send_data(&bytes, *destination)?;
            }
            offset += available + TermScanner::padding(scan) as usize;
            work += 1;
        }
        Ok(work)
    }

    fn send_setup(&self) -> Result<usize> {
        let mut bytes = [0u8; SETUP_HEADER_LENGTH];
        let setup = SetupFlyweight::new(AtomicBuffer::new(&mut bytes));
        let (active_term_id, term_offset) = term_id_and_offset(
            self.sender_position,
            self.initial_term_id,
            self.position_bits_to_shift,
            self.term_length,
        );
        let header = setup.header();
        header.set_frame_length(SETUP_HEADER_LENGTH as i32);
        header.set_version(CURRENT_VERSION);
        header.set_flags(0);
        header.set_header_type(HDR_TYPE_SETUP);
        setup.set_term_offset(term_offset);
        setup.set_session_id(self.session_id);
        setup.set_stream_id(self.stream_id);
        setup.set_initial_term_id(self.initial_term_id);
        setup.set_active_term_id(active_term_id);
        setup.set_term_length(self.term_length as i32);
        setup.set_mtu_length(self.mtu_length as i32);
        setup.set_ttl(0);
        for destination in &self.destinations {
            self.endpoint.send_data(&bytes, *destination)?;
        }
        Ok(1)
    }

    fn send_heartbeat(&self) -> Result<usize> {
        let (term_id, term_offset) = term_id_and_offset(
            self.sender_position,
            self.initial_term_id,
            self.position_bits_to_shift,
            self.term_length,
        );

        let mut bytes = [0u8; DATA_HEADER_LENGTH];
        let data = DataHeaderFlyweight::new(AtomicBuffer::new(&mut bytes));
        data.set_frame_length(0);
        data.set_version(CURRENT_VERSION);
        data.set_header_type(HDR_TYPE_DATA);
        data.set_term_offset(term_offset);
        data.set_session_id(self.session_id);
        data.set_stream_id(self.stream_id);
        data.set_term_id(term_id);
        for destination in &self.destinations {
            self.endpoint.send_data(&bytes, *destination)?;
        }
        Ok(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::media::send_channel_endpoint::SendChannelEndpoint;
    use crate::media::udp_channel::UdpChannel;
    use crate::media::udp_channel_transport::UdpChannelTransport;
    use aeron_client::cnc_file::DEFAULT_FILE_PAGE_SIZE;
    use aeron_client::log_buffers::LogBuffers;
    use aeron_client::logbuffer::TERM_MIN_LENGTH;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn should_derive_active_term_from_absolute_position_across_ring_rotation() {
        let (term_id, term_offset) = term_id_and_offset(
            3 * 64 * 1024 + 96,
            99,
            position_bits_to_shift(64 * 1024),
            64 * 1024,
        );
        assert_eq!(term_id, 102);
        assert_eq!(term_offset, 96);
    }

    #[test]
    fn should_add_and_remove_dynamic_destinations() {
        let channel = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let transport = UdpChannelTransport::bind(channel.clone(), bind_addr, None, 0, 0).unwrap();
        let endpoint = SendChannelEndpoint::new(channel, transport);
        let path =
            std::env::temp_dir().join(format!("aeron-destination-test-{}", std::process::id()));
        let buffers = LogBuffers::map_new(&path, TERM_MIN_LENGTH, 4096).unwrap();
        let first = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 21001);
        let second = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 21002);
        let mut publication = NetworkPublication::new(
            42,
            buffers,
            endpoint,
            first,
            7,
            11,
            3,
            1408,
            Arc::new(AtomicI64::new(0)),
            16,
        )
        .unwrap();

        assert!(publication.add_destination(second));
        assert!(!publication.add_destination(second));
        assert_eq!(publication.destinations(), &[first, second]);
        assert!(publication.remove_destination(second));
        assert!(!publication.remove_destination(second));
        assert_eq!(publication.destinations(), &[first]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn should_initialize_empty_destinations_when_port_is_zero() {
        let channel = UdpChannel::parse("aeron:udp?control-mode=manual").unwrap();
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let transport = UdpChannelTransport::bind(channel.clone(), bind_addr, None, 0, 0).unwrap();
        let endpoint = SendChannelEndpoint::new(channel, transport);
        let path =
            std::env::temp_dir().join(format!("aeron-empty-dest-test-{}", std::process::id()));
        let buffers = LogBuffers::map_new(&path, TERM_MIN_LENGTH, 4096).unwrap();
        let zero_dest = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0);
        let first = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 40123);

        let mut publication = NetworkPublication::new(
            1,
            buffers,
            endpoint,
            zero_dest,
            7,
            11,
            3,
            1408,
            Arc::new(AtomicI64::new(0)),
            16,
        )
        .unwrap();

        assert!(publication.destinations().is_empty());
        assert!(publication.add_destination(first));
        assert_eq!(publication.destinations(), &[first]);
        assert!(publication.remove_destination(first));
        assert!(publication.destinations().is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn should_wrap_term_id_like_aeron_term_counters() {
        let (term_id, term_offset) = term_id_and_offset(
            64 * 1024,
            i32::MAX,
            position_bits_to_shift(64 * 1024),
            64 * 1024,
        );
        assert_eq!(term_id, i32::MIN);
        assert_eq!(term_offset, 0);
    }

    #[test]
    fn should_clean_one_term_behind_sender_and_keep_limit_inside_clean_data() {
        let path = std::env::temp_dir().join(format!(
            "aeron-network-publication-clean-test-{}",
            std::process::id()
        ));
        let buffers = LogBuffers::map_new(&path, TERM_MIN_LENGTH, DEFAULT_FILE_PAGE_SIZE).unwrap();
        let channel = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let transport = UdpChannelTransport::bind(channel.clone(), bind, None, 0, 0).unwrap();
        let endpoint = SendChannelEndpoint::new(channel, transport);
        let publication_limit = Arc::new(AtomicI64::new(0));
        let mut publication = NetworkPublication::new(
            1,
            buffers,
            endpoint,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9),
            7,
            9,
            1,
            1408,
            publication_limit.clone(),
            16,
        )
        .unwrap();
        publication.status_received = true;
        publication.receiver_limit = TERM_MIN_LENGTH as i64 + 128;
        let term_buffers = publication.log_buffers.duplicate_term_buffers();
        term_buffers[0].put_i32(0, 111);
        term_buffers[0].put_i32(256, 222);
        publication.sender_position = TERM_MIN_LENGTH as i64 + 128;

        publication.clean_log_behind_sender();

        assert_eq!(publication.clean_position(), 128);
        assert_eq!(term_buffers[0].get_i32(0), 0);
        assert_eq!(term_buffers[0].get_i32(256), 222);
        assert_eq!(
            publication_limit.load(Ordering::Acquire),
            TERM_MIN_LENGTH as i64 + 128
        );

        drop(publication);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn should_validate_status_message_bounds_relative_to_sender_position() {
        let path = std::env::temp_dir().join(format!(
            "aeron-network-publication-sm-bounds-test-{}",
            std::process::id()
        ));
        let buffers = LogBuffers::map_new(&path, TERM_MIN_LENGTH, DEFAULT_FILE_PAGE_SIZE).unwrap();
        let channel = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let transport = UdpChannelTransport::bind(channel.clone(), bind, None, 0, 0).unwrap();
        let endpoint = SendChannelEndpoint::new(channel, transport);
        let mut publication = NetworkPublication::new(
            1,
            buffers,
            endpoint,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9),
            7,
            9,
            10, // initial_term_id = 10
            1408,
            Arc::new(AtomicI64::new(0)),
            16,
        )
        .unwrap();

        // Sender position at start of term 11 (65536 bytes transmitted)
        publication.sender_position = 65536;
        let half_term = TERM_MIN_LENGTH / 2; // 32768

        // Lower bound: sender_position - half_term = 32768 (term 10, offset 32768)
        assert!(publication.is_valid_status_message(10, half_term));
        // Just below lower bound: 32768 - 32 = 32736 (rejected: > 0.5 term behind)
        assert!(!publication.is_valid_status_message(10, half_term - 32));
        // Way below lower bound: term 10, offset 0 (rejected: 1.0 term behind)
        assert!(!publication.is_valid_status_message(10, 0));

        // Upper bound: sender_position + term_length + half_term = 65536 + 65536 + 32768 = 163840
        // which corresponds to term 12, offset 32768
        assert!(publication.is_valid_status_message(12, half_term));
        // Just above upper bound: 163840 + 32 (rejected: > 1.5 terms ahead)
        assert!(!publication.is_valid_status_message(12, half_term + 32));
        // Way above upper bound: term 13, offset 0 (rejected: 2.0 terms ahead)
        assert!(!publication.is_valid_status_message(13, 0));

        drop(publication);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn should_handle_term_id_wrap_in_status_message_validation() {
        let path = std::env::temp_dir().join(format!(
            "aeron-network-publication-wrap-test-{}",
            std::process::id()
        ));
        let buffers = LogBuffers::map_new(&path, TERM_MIN_LENGTH, DEFAULT_FILE_PAGE_SIZE).unwrap();
        let channel = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let transport = UdpChannelTransport::bind(channel.clone(), bind, None, 0, 0).unwrap();
        let endpoint = SendChannelEndpoint::new(channel, transport);
        let mut publication = NetworkPublication::new(
            1,
            buffers,
            endpoint,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9),
            7,
            9,
            i32::MAX, // initial_term_id at i32::MAX
            1408,
            Arc::new(AtomicI64::new(0)),
            16,
        )
        .unwrap();

        publication.sender_position = 0;
        let half_term = TERM_MIN_LENGTH / 2;

        // Current term is i32::MAX. In-bounds:
        assert!(publication.is_valid_status_message(i32::MAX, 0));
        assert!(publication.is_valid_status_message(i32::MAX, half_term));

        // Next term wraps to i32::MIN:
        // Position is 1 term ahead (65536), which is <= 1.5 terms ahead (98304)
        assert!(publication.is_valid_status_message(i32::MIN, 0));
        assert!(publication.is_valid_status_message(i32::MIN, half_term));
        // Just beyond 1.5 terms ahead:
        assert!(!publication.is_valid_status_message(i32::MIN, half_term + 32));

        // Term before i32::MAX is i32::MAX - 1 (position -65536, < -32768):
        assert!(!publication.is_valid_status_message(i32::MAX - 1, 0));

        drop(publication);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn should_reject_out_of_bounds_status_message_in_poll_control_and_protect_limit() {
        let path = std::env::temp_dir().join(format!(
            "aeron-network-publication-control-test-{}",
            std::process::id()
        ));
        let buffers = LogBuffers::map_new(&path, TERM_MIN_LENGTH, DEFAULT_FILE_PAGE_SIZE).unwrap();
        let channel = UdpChannel::parse("aeron:udp?endpoint=localhost:0").unwrap();
        let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let transport = UdpChannelTransport::bind(channel.clone(), bind, None, 0, 0).unwrap();
        let endpoint = SendChannelEndpoint::new(channel, transport);
        let publication_limit = Arc::new(AtomicI64::new(0));
        let mut publication = NetworkPublication::new(
            1,
            buffers,
            endpoint,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9),
            7, // session_id = 7
            9, // stream_id = 9
            1, // initial_term_id = 1
            1408,
            publication_limit.clone(),
            16,
        )
        .unwrap();

        publication.sender_position = 0;

        // 1. Inject an out-of-bounds SM (5 terms ahead):
        publication.endpoint.on_status_message(7, 9, 6, 0, 4096);
        publication.poll_control(0).unwrap();
        // Limit must NOT move:
        assert_eq!(publication.receiver_limit(), 0);
        assert_eq!(publication_limit.load(Ordering::Acquire), 0);

        // 2. Inject an in-bounds valid SM (term 1, offset 0, window 4096):
        publication.endpoint.on_status_message(7, 9, 1, 0, 4096);
        publication.poll_control(0).unwrap();
        // Limit should advance:
        assert_eq!(publication.receiver_limit(), 4096);
        assert_eq!(publication_limit.load(Ordering::Acquire), 4096);

        // 3. Inject a stale but in-bounds legal SM (term 1, offset 0, window 2048):
        publication.endpoint.on_status_message(7, 9, 1, 0, 2048);
        publication.poll_control(0).unwrap();
        // Limit must NOT retreat:
        assert_eq!(publication.receiver_limit(), 4096);

        // 4. Inject an out-of-bounds SM with SEND_SETUP_FLAG:
        publication.setup_sent = true;
        publication.endpoint.on_status_message_full(
            7,
            9,
            99, // out of bounds
            0,
            4096,
            0,
            aeron_client::protocol::SEND_SETUP_FLAG,
            None,
        );
        publication.poll_control(0).unwrap();
        // setup_sent must be reset to trigger sending setup frame:
        assert!(!publication.setup_sent);
        // Limit must not be affected by out-of-bounds setup request:
        assert_eq!(publication.receiver_limit(), 4096);

        drop(publication);
        let _ = std::fs::remove_file(path);
    }
}
