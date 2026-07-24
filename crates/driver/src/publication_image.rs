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
        let term_length = log_buffers.term_length();
        Self {
            session_id,
            stream_id,
            initial_term_id,
            term_length,
            position_bits_to_shift: position_bits_to_shift(term_length),
            log_buffers,
            log_file_name,
            source_addr,
            image_links: vec![ImageLink {
                subscription_registration_id,
                correlation_id,
            }],
            hwm_position: 0,
            rebuild_position: 0,
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
        if term_id < self.initial_term_id || term_offset >= self.term_length {
            return;
        }
        let index = index_by_term_count((term_id - self.initial_term_id) as i64);
        if !(0..3).contains(&index) {
            return;
        }
        let index = index as usize;
        let term_buffers = self.log_buffers.duplicate_term_buffers();
        if let Some(term_buffer) = term_buffers.get(index) {
            if term_offset.saturating_add(length) > self.term_length {
                return;
            }
            TermRebuilder::insert(term_buffer, term_offset, frame, length);
            let packet_position = compute_position(
                term_id,
                term_offset as i32,
                self.position_bits_to_shift as i32,
                self.initial_term_id,
            );
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
        if term_id < self.initial_term_id || term_offset > self.term_length {
            return;
        }
        let position = compute_position(
            term_id,
            term_offset as i32,
            self.position_bits_to_shift as i32,
            self.initial_term_id,
        );
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
        let term_id =
            self.initial_term_id + (self.rebuild_position >> self.position_bits_to_shift) as i32;
        let limit = ((self.hwm_position as usize) & (self.term_length - 1)).max(term_offset);
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
}
