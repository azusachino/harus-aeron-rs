//! PublicationImage: the driver-side receive state for one network publication's stream.
//!
//! Mirrors `io.aeron.driver.PublicationImage.java` (data path only). The Receiver hands each
//! incoming DATA frame here; it is rebuilt into the shared image log via [`TermRebuilder`], and
//! the subscriber polls the reconstructed stream from that same log.

use agrona::AtomicBuffer;

use aeron_client::log_buffers::LogBuffers;
use aeron_client::logbuffer::{
    TermGapScanner, TermRebuilder, TermScanner, compute_position, index_by_position,
    index_by_term_count, position_bits_to_shift,
};

use std::net::SocketAddr;

/// The lifecycle states used by upstream `PublicationImage`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationImageState {
    Init,
    Active,
    Draining,
    Linger,
    Done,
}

/// A contiguous region not yet rebuilt because an earlier packet is missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LossRange {
    pub term_id: i32,
    pub term_offset: usize,
    pub length: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ImageLink {
    subscription_registration_id: i64,
    correlation_id: i64,
}

/// Reassembles a single session's data frames into a shared image log.
pub struct PublicationImage {
    session_id: i32,
    stream_id: i32,
    initial_term_id: i32,
    term_length: usize,
    position_bits_to_shift: usize,
    log_buffers: LogBuffers,
    log_file_name: String,
    source_addr: SocketAddr,
    image_links: Vec<ImageLink>,
    hwm_position: i64,
    rebuild_position: i64,
    /// Last receiver Status Message position and the upstream overrun threshold derived from it.
    last_status_position: i64,
    last_overrun_threshold: i64,
    pending_loss: Option<LossRange>,
    state: PublicationImageState,
    last_packet_ns: i64,
}

impl PublicationImage {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session_id: i32,
        stream_id: i32,
        initial_term_id: i32,
        log_buffers: LogBuffers,
        log_file_name: String,
        source_addr: SocketAddr,
        subscription_registration_id: i64,
        correlation_id: i64,
    ) -> Self {
        Self::new_with_active_position(
            session_id,
            stream_id,
            initial_term_id,
            initial_term_id,
            0,
            log_buffers,
            log_file_name,
            source_addr,
            subscription_registration_id,
            correlation_id,
        )
    }

    /// Construct an image at the active term position advertised by SETUP.
    ///
    /// Upstream `PublicationImage` initializes both HWM and rebuild positions from
    /// `activeTermId`/`termOffset`; starting at zero would make a non-zero initial offset look
    /// like a permanent loss gap.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_active_position(
        session_id: i32,
        stream_id: i32,
        initial_term_id: i32,
        active_term_id: i32,
        active_term_offset: i32,
        log_buffers: LogBuffers,
        log_file_name: String,
        source_addr: SocketAddr,
        subscription_registration_id: i64,
        correlation_id: i64,
    ) -> Self {
        let term_length = log_buffers.term_length();
        let position_bits_to_shift = position_bits_to_shift(term_length);
        let active_position = compute_position(
            active_term_id,
            active_term_offset,
            position_bits_to_shift as i32,
            initial_term_id,
        );
        Self {
            session_id,
            stream_id,
            initial_term_id,
            term_length,
            position_bits_to_shift,
            log_buffers,
            log_file_name,
            source_addr,
            image_links: vec![ImageLink {
                subscription_registration_id,
                correlation_id,
            }],
            hwm_position: active_position,
            rebuild_position: active_position,
            last_status_position: active_position,
            last_overrun_threshold: active_position + (term_length as i64 / 2),
            pending_loss: None,
            state: PublicationImageState::Init,
            last_packet_ns: 0,
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

    pub fn source_addr(&self) -> SocketAddr {
        self.source_addr
    }

    pub fn subscription_registration_id(&self) -> i64 {
        self.image_links
            .first()
            .map_or(0, |link| link.subscription_registration_id)
    }

    pub fn correlation_id(&self) -> i64 {
        self.image_links
            .first()
            .map_or(0, |link| link.correlation_id)
    }

    pub fn image_links(&self) -> impl Iterator<Item = (i64, i64)> + '_ {
        self.image_links
            .iter()
            .map(|link| (link.subscription_registration_id, link.correlation_id))
    }

    pub fn has_subscription(&self, subscription_registration_id: i64) -> bool {
        self.image_links
            .iter()
            .any(|link| link.subscription_registration_id == subscription_registration_id)
    }

    pub fn add_subscription_link(
        &mut self,
        subscription_registration_id: i64,
        correlation_id: i64,
    ) {
        if !self.has_subscription(subscription_registration_id) {
            self.image_links.push(ImageLink {
                subscription_registration_id,
                correlation_id,
            });
        }
    }

    pub fn remove_subscription_link(&mut self, subscription_registration_id: i64) {
        self.image_links
            .retain(|link| link.subscription_registration_id != subscription_registration_id);
    }

    pub fn has_no_subscriptions(&self) -> bool {
        self.image_links.is_empty()
    }

    pub fn touch(&mut self, now_ns: i64) {
        self.last_packet_ns = now_ns;
    }

    pub fn state(&self) -> PublicationImageState {
        self.state
    }

    pub fn activate(&mut self) {
        self.state = PublicationImageState::Active;
    }

    pub fn deactivate(&mut self) {
        if self.state == PublicationImageState::Active {
            self.state = PublicationImageState::Draining;
        }
    }

    pub fn hwm_position(&self) -> i64 {
        self.hwm_position
    }

    pub fn rebuild_position(&self) -> i64 {
        self.rebuild_position
    }

    pub fn pending_loss(&self) -> Option<LossRange> {
        self.pending_loss
    }

    /// Apply the position advertised by the receiver's latest Status Message.
    ///
    /// Upstream `PublicationImage.sendPendingStatusMessage` resets the overrun threshold to half
    /// a term beyond each new status position. The receiver agent supplies the wire term id and
    /// offset here so malformed or stale packets are rejected before rebuilding the log.
    pub fn update_flow_control(
        &mut self,
        consumption_term_id: i32,
        consumption_term_offset: i32,
        _receiver_window_length: i32,
    ) {
        if consumption_term_offset < 0 || consumption_term_offset as usize > self.term_length {
            return;
        }
        let term_count = consumption_term_id.wrapping_sub(self.initial_term_id) as i64;
        if !(0..3).contains(&term_count) {
            return;
        }
        let position = compute_position(
            consumption_term_id,
            consumption_term_offset,
            self.position_bits_to_shift as i32,
            self.initial_term_id,
        );
        if position >= self.last_status_position {
            self.last_status_position = position;
            self.last_overrun_threshold = position + (self.term_length as i64 / 2);
        }
    }

    pub fn is_connected(&self, now_ns: i64, liveness_timeout_ns: i64) -> bool {
        self.last_packet_ns == 0
            || now_ns.saturating_sub(self.last_packet_ns) <= liveness_timeout_ns
    }

    /// Rebuild one wire data frame into the image log at the position it carries.
    pub fn on_data_frame(
        &mut self,
        term_id: i32,
        term_offset: usize,
        frame: &AtomicBuffer,
        length: usize,
        now_ns: i64,
    ) {
        let term_count = term_id.wrapping_sub(self.initial_term_id) as i64;
        if !(0..3).contains(&term_count) || term_offset >= self.term_length {
            return;
        }
        let packet_position = compute_position(
            term_id,
            term_offset as i32,
            self.position_bits_to_shift as i32,
            self.initial_term_id,
        );
        let proposed_position = packet_position.saturating_add(length as i64);
        if packet_position < self.last_status_position
            || proposed_position > self.last_overrun_threshold
        {
            return;
        }
        let index = index_by_term_count(term_count) as usize;
        let term_buffers = self.log_buffers.duplicate_term_buffers();
        if let Some(term_buffer) = term_buffers.get(index) {
            if term_offset.saturating_add(length) > self.term_length {
                return;
            }
            TermRebuilder::insert(term_buffer, term_offset, frame, length);
            self.hwm_position = self
                .hwm_position
                .max(packet_position.saturating_add(length as i64));
            self.last_packet_ns = now_ns;
            if self.state == PublicationImageState::Init {
                self.state = PublicationImageState::Active;
            }
            self.advance_rebuild_position();
        }
    }

    /// Record a heartbeat's position without writing over an existing frame in the image log.
    pub fn on_heartbeat(&mut self, term_id: i32, term_offset: usize, now_ns: i64) {
        let term_count = term_id.wrapping_sub(self.initial_term_id) as i64;
        if !(0..3).contains(&term_count) || term_offset > self.term_length {
            return;
        }
        let position = compute_position(
            term_id,
            term_offset as i32,
            self.position_bits_to_shift as i32,
            self.initial_term_id,
        );
        if position < self.last_status_position || position > self.last_overrun_threshold {
            return;
        }
        self.hwm_position = self.hwm_position.max(position);
        self.last_packet_ns = now_ns;
        if self.state == PublicationImageState::Init {
            self.state = PublicationImageState::Active;
        }
        self.advance_rebuild_position();
    }

    fn advance_rebuild_position(&mut self) {
        let term_buffers = self.log_buffers.duplicate_term_buffers();
        while self.rebuild_position < self.hwm_position {
            let index = index_by_position(self.rebuild_position, self.position_bits_to_shift);
            let term_offset = (self.rebuild_position as usize) & (self.term_length - 1);
            let Some(term_buffer) = term_buffers.get(index) else {
                break;
            };
            let scan = TermScanner::scan_for_availability(
                term_buffer,
                term_offset,
                self.term_length.saturating_sub(term_offset),
            );
            let available = TermScanner::available(scan);
            if available <= 0 {
                break;
            }
            self.rebuild_position = self
                .rebuild_position
                .saturating_add((available + TermScanner::padding(scan)) as i64);
        }

        self.pending_loss = self.find_first_gap();
    }

    fn find_first_gap(&self) -> Option<LossRange> {
        if self.rebuild_position >= self.hwm_position {
            return None;
        }

        let index = index_by_position(self.rebuild_position, self.position_bits_to_shift);
        let term_offset = (self.rebuild_position as usize) & (self.term_length - 1);
        let term_id = self
            .initial_term_id
            .wrapping_add((self.rebuild_position >> self.position_bits_to_shift) as i32);
        let hwm_term_offset = (self.hwm_position as usize) & (self.term_length - 1);
        let hwm_term_id = self
            .initial_term_id
            .wrapping_add((self.hwm_position >> self.position_bits_to_shift) as i32);
        // A HWM in a later term means the entire remainder of the current term is in scope.
        // Using the later term's wrapped offset here can hide a gap at the end of the current
        // term (the exact boundary exercised by upstream LossDetector/PublicationImage tests).
        let limit = if hwm_term_id == term_id {
            hwm_term_offset.max(term_offset)
        } else {
            self.term_length
        };
        let term_buffers = self.log_buffers.duplicate_term_buffers();
        let term_buffer = term_buffers.get(index)?;
        let mut gap = None;
        TermGapScanner::scan_for_gap(
            term_buffer,
            term_id,
            term_offset,
            limit,
            |found_term_id, gap_offset, gap_length| {
                gap = Some(LossRange {
                    term_id: found_term_id,
                    term_offset: gap_offset,
                    length: gap_length,
                });
            },
        );
        gap
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aeron_client::cnc_file::DEFAULT_FILE_PAGE_SIZE;
    use aeron_client::log_buffers::LogBuffers;
    use aeron_client::logbuffer::TERM_MIN_LENGTH;
    use aeron_client::protocol::{DATA_HEADER_LENGTH, DataHeaderFlyweight};
    use std::net::{IpAddr, Ipv4Addr};

    // Behavioral cases are cross-checked against:
    // vendor/aeron/aeron-driver/src/test/java/io/aeron/driver/PublicationImageTest.java

    fn data_frame(term_id: i32, term_offset: i32, value: u8) -> Vec<u8> {
        let mut bytes = vec![0u8; 64];
        let header = DataHeaderFlyweight::new(AtomicBuffer::new(&mut bytes));
        header
            .set_frame_length(40)
            .set_term_offset(term_offset)
            .set_session_id(7)
            .set_stream_id(9)
            .set_term_id(term_id);
        bytes[DATA_HEADER_LENGTH..40].fill(value);
        bytes
    }

    #[test]
    fn should_track_hwm_rebuild_and_loss_for_out_of_order_packets() {
        let path = std::env::temp_dir().join(format!(
            "aeron-publication-image-test-{}-{}",
            std::process::id(),
            1
        ));
        let buffers = LogBuffers::map_new(&path, TERM_MIN_LENGTH, DEFAULT_FILE_PAGE_SIZE).unwrap();
        let source = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000);
        let mut image = PublicationImage::new(
            7,
            9,
            1,
            buffers,
            path.to_string_lossy().into_owned(),
            source,
            11,
            12,
        );
        assert_eq!(image.image_links().collect::<Vec<_>>(), vec![(11, 12)]);
        image.add_subscription_link(13, 14);
        assert!(image.has_subscription(13));
        image.remove_subscription_link(13);
        assert!(!image.has_subscription(13));

        let mut second = data_frame(1, 64, 0x22);
        let second_buffer = AtomicBuffer::new(&mut second);
        image.on_data_frame(1, 64, &second_buffer, 64, 10);
        assert_eq!(image.state(), PublicationImageState::Active);
        assert_eq!(image.rebuild_position(), 0);
        assert_eq!(image.hwm_position(), 128);
        assert_eq!(
            image.pending_loss(),
            Some(LossRange {
                term_id: 1,
                term_offset: 0,
                length: 64,
            })
        );

        let mut first = data_frame(1, 0, 0x11);
        let first_buffer = AtomicBuffer::new(&mut first);
        image.on_data_frame(1, 0, &first_buffer, 64, 20);
        assert_eq!(image.rebuild_position(), 128);
        assert_eq!(image.pending_loss(), None);
        assert_eq!(image.hwm_position(), 128);
        assert_eq!(image.source_addr(), source);
        assert!(image.is_connected(20, 1));

        image.on_heartbeat(1, 256, 30);
        assert_eq!(image.hwm_position(), 256);
        let term_buffer = image.log_buffers.duplicate_term_buffers()[0];
        assert_eq!(term_buffer.get_i32(64), 40);

        drop(image);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn should_start_positions_at_setup_active_term_and_offset() {
        let path = std::env::temp_dir().join(format!(
            "aeron-publication-image-position-test-{}",
            std::process::id()
        ));
        let buffers = LogBuffers::map_new(&path, TERM_MIN_LENGTH, DEFAULT_FILE_PAGE_SIZE).unwrap();
        let image = PublicationImage::new_with_active_position(
            7,
            9,
            1,
            1,
            1024,
            buffers,
            path.to_string_lossy().into_owned(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000),
            11,
            12,
        );

        assert_eq!(image.hwm_position(), 1024);
        assert_eq!(image.rebuild_position(), 1024);
        assert_eq!(image.pending_loss(), None);

        drop(image);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn heartbeat_does_not_overwrite_data_and_data_overwrites_heartbeat() {
        let path = std::env::temp_dir().join(format!(
            "aeron-publication-image-heartbeat-test-{}",
            std::process::id()
        ));
        let buffers = LogBuffers::map_new(&path, TERM_MIN_LENGTH, DEFAULT_FILE_PAGE_SIZE).unwrap();
        let source = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000);
        let mut image = PublicationImage::new(
            7,
            9,
            1,
            buffers,
            path.to_string_lossy().into_owned(),
            source,
            11,
            12,
        );

        image.on_heartbeat(1, 0, 10);
        let term_buffer = image.log_buffers.duplicate_term_buffers()[0];
        assert_eq!(term_buffer.get_i32(0), 0);

        let mut data = data_frame(1, 0, 0x11);
        image.on_data_frame(1, 0, &AtomicBuffer::new(&mut data), 64, 20);
        assert_eq!(term_buffer.get_i32(0), 40);

        image.on_heartbeat(1, 0, 30);
        assert_eq!(term_buffer.get_i32(0), 40);

        drop(image);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn accepts_term_id_rollover_when_selecting_the_next_partition() {
        let path = std::env::temp_dir().join(format!(
            "aeron-publication-image-term-rollover-{}",
            std::process::id()
        ));
        let buffers = LogBuffers::map_new(&path, TERM_MIN_LENGTH, DEFAULT_FILE_PAGE_SIZE).unwrap();
        let mut image = PublicationImage::new_with_active_position(
            7,
            9,
            i32::MAX,
            i32::MIN,
            0,
            buffers,
            path.to_string_lossy().into_owned(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000),
            11,
            12,
        );

        let mut data = data_frame(i32::MIN, 0, 0x55);
        image.on_data_frame(i32::MIN, 0, &AtomicBuffer::new(&mut data), 64, 10);

        assert_eq!(image.hwm_position(), TERM_MIN_LENGTH as i64 + 64);
        assert_eq!(image.log_buffers.duplicate_term_buffers()[1].get_i32(0), 40);

        drop(image);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn rejects_a_packet_outside_the_three_term_log_window() {
        let path = std::env::temp_dir().join(format!(
            "aeron-publication-image-term-window-{}",
            std::process::id()
        ));
        let buffers = LogBuffers::map_new(&path, TERM_MIN_LENGTH, DEFAULT_FILE_PAGE_SIZE).unwrap();
        let mut image = PublicationImage::new(
            7,
            9,
            10,
            buffers,
            path.to_string_lossy().into_owned(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000),
            11,
            12,
        );

        let mut data = data_frame(13, 0, 0x66);
        image.on_data_frame(13, 0, &AtomicBuffer::new(&mut data), 64, 10);

        assert_eq!(image.hwm_position(), 0);
        assert_eq!(image.pending_loss(), None);

        drop(image);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn enforces_status_position_under_run_and_half_term_overrun_threshold() {
        let path = std::env::temp_dir().join(format!(
            "aeron-publication-image-flow-control-{}",
            std::process::id()
        ));
        let buffers = LogBuffers::map_new(&path, TERM_MIN_LENGTH, DEFAULT_FILE_PAGE_SIZE).unwrap();
        let mut image = PublicationImage::new(
            7,
            9,
            1,
            buffers,
            path.to_string_lossy().into_owned(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000),
            11,
            12,
        );
        image.update_flow_control(1, 128, TERM_MIN_LENGTH / 2);

        let mut under_run = data_frame(1, 64, 0x11);
        image.on_data_frame(1, 64, &AtomicBuffer::new(&mut under_run), 64, 10);
        assert_eq!(image.hwm_position(), 0);

        let mut in_window = data_frame(1, 128, 0x22);
        image.on_data_frame(1, 128, &AtomicBuffer::new(&mut in_window), 64, 20);
        assert_eq!(image.hwm_position(), 192);

        let overrun_offset = TERM_MIN_LENGTH / 2 + 128;
        let mut over_run = data_frame(1, overrun_offset, 0x33);
        image.on_data_frame(
            1,
            overrun_offset as usize,
            &AtomicBuffer::new(&mut over_run),
            64,
            30,
        );
        assert_eq!(image.hwm_position(), 192);

        drop(image);
        let _ = std::fs::remove_file(path);
    }
}
