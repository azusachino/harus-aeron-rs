//! Subscription: Aeron subscriber API for receiving reconstructed Image streams over a channel/streamId.

use crate::image::Image;
use crate::logbuffer::Header;
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

    const REGISTRATION_ID: i64 = 100;
    const STREAM_ID: i32 = 1002;
    const CHANNEL: &str = "aeron:udp?endpoint=localhost:40124";

    #[test]
    fn should_ensure_subscription_is_open_when_polling() {
        let mut subscription = Subscription::new(REGISTRATION_ID, STREAM_ID, CHANNEL.to_string());
        assert!(!subscription.is_closed());
        assert_eq!(subscription.image_count(), 0);
        assert!(!subscription.is_connected());

        subscription.close();
        assert!(subscription.is_closed());
        assert_eq!(subscription.poll(|_, _, _, _| {}, 10), 0);
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
