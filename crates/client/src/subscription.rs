//! Subscription: Aeron subscriber API for receiving reconstructed Image streams over a channel/streamId.

use crate::image::Image;
use crate::logbuffer::{ControlledPollAction, Header};
use agrona::AtomicBuffer;
use std::sync::Arc;

pub type AvailableImageHandler = Arc<dyn Fn(&Image) + Send + Sync>;
pub type UnavailableImageHandler = Arc<dyn Fn(&Image) + Send + Sync>;

/// Subscriber for receiving messages over an Aeron channel and stream.
pub struct Subscription {
    registration_id: i64,
    stream_id: i32,
    channel: String,
    images: Vec<Image>,
    round_robin_index: usize,
    channel_status_counter_id: Option<i32>,
    available_image_handler: Option<AvailableImageHandler>,
    unavailable_image_handler: Option<UnavailableImageHandler>,
    is_closed: bool,
}

impl Subscription {
    pub fn new(registration_id: i64, stream_id: i32, channel: String) -> Self {
        Self {
            registration_id,
            stream_id,
            channel,
            images: Vec::new(),
            round_robin_index: 0,
            channel_status_counter_id: None,
            available_image_handler: None,
            unavailable_image_handler: None,
            is_closed: false,
        }
    }

    pub fn registration_id(&self) -> i64 {
        self.registration_id
    }

    pub fn stream_id(&self) -> i32 {
        self.stream_id
    }

    pub fn channel(&self) -> &str {
        &self.channel
    }

    pub fn channel_status_counter_id(&self) -> Option<i32> {
        self.channel_status_counter_id
    }

    pub fn set_channel_status_counter_id(&mut self, counter_id: i32) {
        self.channel_status_counter_id = Some(counter_id);
    }

    pub fn set_available_image_handler(&mut self, handler: AvailableImageHandler) {
        self.available_image_handler = Some(handler);
    }

    pub fn set_unavailable_image_handler(&mut self, handler: UnavailableImageHandler) {
        self.unavailable_image_handler = Some(handler);
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    pub fn close(&mut self) {
        if !self.is_closed {
            self.is_closed = true;
            for image in &mut self.images {
                image.close();
            }
            self.images.clear();
        }
    }

    pub fn is_connected(&self) -> bool {
        !self.is_closed && self.images.iter().any(|image| !image.is_closed())
    }

    pub fn has_no_images(&self) -> bool {
        self.images.is_empty()
    }

    pub fn image_count(&self) -> usize {
        self.images.len()
    }

    pub fn images(&self) -> &[Image] {
        &self.images
    }

    pub fn image_at_index(&self, index: usize) -> Option<&Image> {
        self.images.get(index)
    }

    pub fn image_by_session_id(&self, session_id: i32) -> Option<&Image> {
        self.images
            .iter()
            .find(|image| image.session_id() == session_id)
    }

    pub fn for_each_image<F>(&self, mut consumer: F)
    where
        F: FnMut(&Image),
    {
        for image in &self.images {
            consumer(image);
        }
    }

    pub fn add_image(&mut self, image: Image) {
        self.images.push(image);
        if let Some(handler) = &self.available_image_handler {
            if let Some(image) = self.images.last() {
                handler(image);
            }
        }
    }

    pub fn remove_image(&mut self, correlation_id: i64) -> Option<Image> {
        if let Some(pos) = self
            .images
            .iter()
            .position(|img| img.correlation_id() == correlation_id)
        {
            let mut img = self.images.remove(pos);
            img.close();
            if let Some(handler) = &self.unavailable_image_handler {
                handler(&img);
            }
            Some(img)
        } else {
            None
        }
    }

    pub fn image_by_correlation_id(&self, correlation_id: i64) -> Option<&Image> {
        self.images
            .iter()
            .find(|img| img.correlation_id() == correlation_id)
    }

    pub fn poll<F>(&mut self, mut fragment_handler: F, fragment_limit: usize) -> usize
    where
        F: FnMut(&AtomicBuffer, usize, usize, &Header),
    {
        if self.is_closed || self.images.is_empty() {
            return 0;
        }

        let num_images = self.images.len();
        let starting_index = self.round_robin_index % num_images;
        let mut fragments_read = 0;

        for i in 0..num_images {
            let index = (starting_index + i) % num_images;
            let limit = fragment_limit.saturating_sub(fragments_read);
            if limit == 0 {
                break;
            }

            let read = self.images[index].poll(&mut fragment_handler, limit);
            fragments_read += read;
        }

        self.round_robin_index = (starting_index + 1) % num_images;
        fragments_read
    }

    /// Round-robin [`Image::controlled_poll`] across every image, same fragment-budget sharing
    /// as [`Subscription::poll`]. A handler's [`ControlledPollAction::Abort`]/`Break` only ends
    /// that image's own scan for this call; the round-robin moves on to the next image as usual.
    pub fn controlled_poll<F>(&mut self, mut fragment_handler: F, fragment_limit: usize) -> usize
    where
        F: FnMut(&AtomicBuffer, usize, usize, &Header) -> ControlledPollAction,
    {
        if self.is_closed || self.images.is_empty() {
            return 0;
        }

        let num_images = self.images.len();
        let starting_index = self.round_robin_index % num_images;
        let mut fragments_read = 0;

        for i in 0..num_images {
            let index = (starting_index + i) % num_images;
            let limit = fragment_limit.saturating_sub(fragments_read);
            if limit == 0 {
                break;
            }

            let read = self.images[index].controlled_poll(&mut fragment_handler, limit);
            fragments_read += read;
        }

        self.round_robin_index = (starting_index + 1) % num_images;
        fragments_read
    }

    /// Poll one image without consuming fragments from any sibling image.
    pub fn poll_image<F>(
        &mut self,
        correlation_id: i64,
        fragment_handler: F,
        fragment_limit: usize,
    ) -> usize
    where
        F: FnMut(&AtomicBuffer, usize, usize, &Header),
    {
        self.images
            .iter_mut()
            .find(|image| image.correlation_id() == correlation_id)
            .map_or(0, |image| image.poll(fragment_handler, fragment_limit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_buffers::LogBuffers;
    use crate::logbuffer::{FRAME_ALIGNMENT, HEADER_LENGTH, PAGE_MIN_SIZE, TERM_MIN_LENGTH, align};
    use crate::protocol::DataHeaderFlyweight;
    use std::sync::Arc;
    use std::sync::atomic::AtomicI64;

    const REGISTRATION_ID: i64 = 100;
    const STREAM_ID: i32 = 1002;
    const CHANNEL: &str = "aeron:udp?endpoint=localhost:40124";
    const INITIAL_TERM_ID: i32 = 123;

    fn create_test_log_buffers() -> LogBuffers {
        let term_len = TERM_MIN_LENGTH as usize;
        let total_len = term_len * 3 + 4096;
        let mut contents = vec![0u8; total_len];
        let meta_offset = total_len - 4096;
        let mut meta_slice = contents[meta_offset..].to_vec();
        let meta_buf = AtomicBuffer::new(&mut meta_slice);
        crate::logbuffer::set_term_length(&meta_buf, TERM_MIN_LENGTH);
        crate::logbuffer::set_page_size(&meta_buf, PAGE_MIN_SIZE);
        crate::logbuffer::set_end_of_stream_position(
            &meta_buf,
            crate::logbuffer::NO_END_OF_STREAM_POSITION,
        );
        contents[meta_offset..].copy_from_slice(&meta_slice);
        LogBuffers::from_bytes(contents).unwrap()
    }

    fn create_test_image_with_messages(
        session_id: i32,
        correlation_id: i64,
        num_messages: usize,
    ) -> Image {
        let subscriber_pos = Arc::new(AtomicI64::new(0));
        let log_buffers = create_test_log_buffers();
        let terms = log_buffers.duplicate_term_buffers();
        let mut term_offset = 0;
        for _ in 0..num_messages {
            let data = [1u8; 32];
            let msg_len = HEADER_LENGTH as usize + data.len();
            let aligned_frame_len = align(msg_len as i32, FRAME_ALIGNMENT) as usize;
            let mut frame_bytes = vec![0u8; aligned_frame_len];
            let frame_buf = AtomicBuffer::new(&mut frame_bytes);
            let data_header = DataHeaderFlyweight::new(frame_buf);
            data_header.set_header_type(crate::protocol::HDR_TYPE_DATA);
            data_header.set_frame_length(msg_len as i32);
            data_header.set_session_id(session_id);
            data_header.set_term_id(INITIAL_TERM_ID);
            data_header.set_term_offset(term_offset as i32);
            terms[0].put_bytes(term_offset, &frame_bytes);
            term_offset += aligned_frame_len;
        }
        Image::new(
            1,
            session_id,
            INITIAL_TERM_ID,
            0,
            subscriber_pos,
            log_buffers,
            "test".to_string(),
            correlation_id,
        )
    }

    #[test]
    fn should_round_robin_starting_image_when_polling_with_exhausted_fragment_limit() {
        let mut subscription = Subscription::new(REGISTRATION_ID, STREAM_ID, CHANNEL.to_string());
        let img1 = create_test_image_with_messages(101, 1, 3);
        let img2 = create_test_image_with_messages(202, 2, 3);
        subscription.add_image(img1);
        subscription.add_image(img2);

        let mut poll_session_ids = Vec::new();
        for _ in 0..6 {
            let read = subscription.poll(
                |_buf, _offset, _len, header| {
                    poll_session_ids.push(header.session_id());
                },
                1,
            );
            assert_eq!(read, 1);
        }

        assert_eq!(poll_session_ids, vec![101, 202, 101, 202, 101, 202]);
    }

    #[test]
    fn should_round_robin_starting_image_when_controlled_polling_with_exhausted_fragment_limit() {
        let mut subscription = Subscription::new(REGISTRATION_ID, STREAM_ID, CHANNEL.to_string());
        let img1 = create_test_image_with_messages(101, 1, 3);
        let img2 = create_test_image_with_messages(202, 2, 3);
        subscription.add_image(img1);
        subscription.add_image(img2);

        let mut poll_session_ids = Vec::new();
        for _ in 0..6 {
            let read = subscription.controlled_poll(
                |_buf, _offset, _len, header| {
                    poll_session_ids.push(header.session_id());
                    ControlledPollAction::Continue
                },
                1,
            );
            assert_eq!(read, 1);
        }

        assert_eq!(poll_session_ids, vec![101, 202, 101, 202, 101, 202]);
    }

    #[test]
    fn should_ensure_subscription_is_open_when_polling() {
        let mut subscription = Subscription::new(REGISTRATION_ID, STREAM_ID, CHANNEL.to_string());
        assert!(!subscription.is_closed());
        assert_eq!(subscription.image_count(), 0);
        assert!(!subscription.is_connected());

        subscription.close();
        assert!(subscription.is_closed());
        assert_eq!(subscription.poll(|_, _, _, _| {}, 10), 0);
        assert_eq!(
            subscription.controlled_poll(|_, _, _, _| ControlledPollAction::Continue, 10),
            0
        );
    }

    #[test]
    fn should_report_no_images_and_no_connection_before_an_image_is_added() {
        let subscription = Subscription::new(REGISTRATION_ID, STREAM_ID, CHANNEL.to_string());

        assert!(subscription.has_no_images());
        assert!(!subscription.is_connected());
        assert!(subscription.image_at_index(0).is_none());
        assert!(subscription.image_by_session_id(7).is_none());
    }

    #[test]
    fn should_close_images_and_make_close_idempotent() {
        let mut subscription = Subscription::new(REGISTRATION_ID, STREAM_ID, CHANNEL.to_string());
        subscription.close();
        subscription.close();

        assert!(subscription.is_closed());
        assert!(subscription.has_no_images());
        assert!(!subscription.is_connected());
    }
}
