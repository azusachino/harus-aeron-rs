//! Client-side resource lifecycle coordinator.
//!
//! Commands are submitted through [`DriverProxy`], while driver responses are consumed from an
//! Agrona broadcast buffer and reconciled into ready publications, subscriptions, and images.

use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};
use std::time::{Duration, Instant};

use agrona::AtomicBuffer;
use agrona::counters::AtomicCounter;

use crate::cnc_file::{
    COUNTER_METADATA_LENGTH, COUNTER_RECORD_ALLOCATED, COUNTER_VALUE_LENGTH, CncFile,
};
use crate::driver_events::{DriverEvent, DriverEventsAdapter};
use crate::driver_proxy::DriverProxy;
use crate::error::{AeronError, Result};
use crate::image::Image;
use crate::log_buffers::LogBuffers;
use crate::logbuffer::initial_term_id;
use crate::publication::ConcurrentPublication;
use crate::subscription::Subscription;

struct PendingPublication {
    channel: String,
    exclusive: bool,
}

struct PendingSubscription {
    channel: String,
    stream_id: i32,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum RegistrationState {
    Pending,
    Ready,
    Closed,
}

/// Coordinates the asynchronous command and response lifecycle of an Aeron client.
pub struct ClientConductor<'a> {
    driver_proxy: DriverProxy<'a>,
    event_adapter: Option<DriverEventsAdapter<'a>>,
    pending_publications: HashMap<i64, PendingPublication>,
    pending_subscriptions: HashMap<i64, PendingSubscription>,
    publications: HashMap<i64, ConcurrentPublication>,
    subscriptions: HashMap<i64, Subscription>,
    counter_values: HashMap<i32, Arc<AtomicI64>>,
    /// CnC counter slots mirrored into the stable Arc handles used by publications and images.
    cnc_counter_offsets: HashMap<i32, usize>,
    cnc_file: Option<CncFile>,
    log_buffers: HashMap<String, LogBuffers>,
    is_closed: bool,
    timed_out: bool,
    registration_states: HashMap<i64, RegistrationState>,
    pending_since: HashMap<i64, Instant>,
    driver_timeout: Duration,
    last_keepalive: Instant,
    pre_touch_mapped_memory: bool,
    control_protocol_version: i32,
    next_session_id: Option<i32>,
}

impl<'a> ClientConductor<'a> {
    /// Construct a conductor without a response buffer. `on_*` methods can still be used by a
    /// driver integration or test harness to deliver decoded events directly.
    pub fn new(driver_proxy: DriverProxy<'a>) -> Self {
        Self {
            driver_proxy,
            event_adapter: None,
            pending_publications: HashMap::new(),
            pending_subscriptions: HashMap::new(),
            publications: HashMap::new(),
            subscriptions: HashMap::new(),
            counter_values: HashMap::new(),
            cnc_counter_offsets: HashMap::new(),
            cnc_file: None,
            log_buffers: HashMap::new(),
            is_closed: false,
            timed_out: false,
            registration_states: HashMap::new(),
            pending_since: HashMap::new(),
            driver_timeout: Duration::from_millis(crate::context::DEFAULT_DRIVER_TIMEOUT_MS),
            last_keepalive: Instant::now(),
            pre_touch_mapped_memory: false,
            // Mirrors upstream ClientConductor's default of 0 until a real system counter
            // confirms the driver's actual control-protocol version; get_next_available_session_id
            // relies on this staying unsupported (< CONTROL_PROTOCOL_SEMANTIC_VERSION) by default.
            control_protocol_version: 0,
            next_session_id: None,
        }
    }

    /// Construct a conductor connected to the media driver's broadcast event buffer.
    pub fn with_event_buffer(
        driver_proxy: DriverProxy<'a>,
        to_client_buffer: AtomicBuffer<'a>,
    ) -> Result<Self> {
        let mut conductor = Self::new(driver_proxy);
        conductor.event_adapter = Some(DriverEventsAdapter::new(to_client_buffer)?);
        Ok(conductor)
    }

    /// Construct a client conductor directly over a mapped CnC file.
    ///
    /// This is the cross-process path used by a real Aeron client: counter IDs in driver events
    /// refer to Agrona slots in `cnc.dat`, rather than to Rust-owned `Arc<AtomicI64>` values.
    /// Layout authority is the vendored `CncFileDescriptor.java` plus Agrona counters source.
    pub fn with_cnc_file(driver_proxy: DriverProxy<'a>, cnc: CncFile) -> Result<Self> {
        let mut conductor = Self::with_event_buffer(driver_proxy, cnc.to_clients_buffer())?;
        conductor.cnc_file = Some(cnc);
        Ok(conductor)
    }

    pub fn set_driver_timeout(&mut self, timeout: Duration) {
        self.driver_timeout = timeout;
    }
    pub fn set_pre_touch_mapped_memory(&mut self, enabled: bool) {
        self.pre_touch_mapped_memory = enabled;
    }
    pub fn pre_touch_mapped_memory(&self) -> bool {
        self.pre_touch_mapped_memory
    }
    pub fn set_control_protocol_version(&mut self, version: i32) {
        self.control_protocol_version = version;
    }
    pub fn control_protocol_version(&self) -> i32 {
        self.control_protocol_version
    }
    pub fn next_session_id(&self) -> Option<i32> {
        self.next_session_id
    }
    pub fn registration_state(&self, correlation_id: i64) -> Option<RegistrationState> {
        self.registration_states.get(&correlation_id).copied()
    }

    pub fn driver_proxy(&self) -> &DriverProxy<'a> {
        &self.driver_proxy
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    pub fn timed_out(&self) -> bool {
        self.timed_out
    }

    /// Register a shared counter value buffer entry referenced by a driver response.
    pub fn register_counter(&mut self, counter_id: i32, value: Arc<AtomicI64>) {
        if counter_id == crate::counters::SYSTEM_COUNTER_ID_CONTROL_PROTOCOL_VERSION {
            self.control_protocol_version = value.load(std::sync::atomic::Ordering::Acquire) as i32;
        }
        self.counter_values.insert(counter_id, value);
    }

    fn bind_cnc_counter(&mut self, counter_id: i32) {
        let Some(cnc) = &self.cnc_file else {
            return;
        };
        if counter_id < 0 || self.counter_values.contains_key(&counter_id) {
            return;
        }
        let id = counter_id as usize;
        let metadata_offset = id.saturating_mul(COUNTER_METADATA_LENGTH);
        let value_offset = id.saturating_mul(COUNTER_VALUE_LENGTH);
        if metadata_offset + 4 > cnc.counters_metadata_length()
            || value_offset + 8 > cnc.counters_values_length()
        {
            return;
        }
        let metadata = cnc.counters_metadata_buffer();
        if metadata.get_i32_volatile(metadata_offset) != COUNTER_RECORD_ALLOCATED {
            return;
        }
        let values = cnc.counters_values_buffer();
        self.counter_values.insert(
            counter_id,
            Arc::new(AtomicI64::new(values.get_i64_volatile(value_offset))),
        );
        self.cnc_counter_offsets.insert(counter_id, value_offset);
    }

    fn sync_cnc_counters(&mut self) {
        let Some(cnc) = &self.cnc_file else {
            return;
        };
        let values = cnc.counters_values_buffer();
        for (&counter_id, &offset) in &self.cnc_counter_offsets {
            if let Some(value) = self.counter_values.get(&counter_id) {
                value.store(
                    values.get_i64_volatile(offset),
                    std::sync::atomic::Ordering::Release,
                );
            }
        }
    }

    fn cnc_counter(&self, counter_id: i32) -> Option<AtomicCounter<'static>> {
        if !self.cnc_counter_offsets.contains_key(&counter_id) {
            return None;
        }
        self.cnc_file
            .as_ref()
            .map(|cnc| AtomicCounter::new(cnc.counters_values_buffer(), counter_id))
    }

    /// Register an already-open log buffer by the name used in a driver event. This is useful
    /// for embedded drivers; normal clients fall back to opening the event's file path.
    pub fn register_log_buffers(&mut self, name: impl Into<String>, buffers: LogBuffers) {
        self.log_buffers.insert(name.into(), buffers);
    }

    /// Drain all currently available driver events.
    pub fn service(&mut self) -> Result<usize> {
        self.sync_cnc_counters();
        let mut events = Vec::new();
        if let Some(adapter) = &mut self.event_adapter {
            events = adapter.receive()?;
        }

        let count = events.len();
        for event in events {
            self.on_event(event)?;
        }
        Ok(count)
    }

    /// Run one conductor duty cycle: consume responses, send keepalive, and enforce the
    /// inter-service response deadline for pending registrations.
    pub fn do_work(&mut self) -> Result<usize> {
        self.do_work_at(Instant::now())
    }

    pub fn do_work_at(&mut self, now: Instant) -> Result<usize> {
        let work = self.service()?;
        if now.duration_since(self.last_keepalive) >= self.driver_timeout / 3 {
            self.driver_proxy.client_keepalive()?;
            self.last_keepalive = now;
        }
        if let Some((correlation_id, _started)) = self
            .pending_since
            .iter()
            .find(|(_, started)| now.duration_since(**started) > self.driver_timeout)
        {
            let correlation_id = *correlation_id;
            self.pending_publications.remove(&correlation_id);
            self.pending_subscriptions.remove(&correlation_id);
            self.pending_since.remove(&correlation_id);
            self.registration_states
                .insert(correlation_id, RegistrationState::Closed);
            return Err(AeronError::DriverTimeout(
                self.driver_timeout.as_millis() as u64
            ));
        }
        Ok(work)
    }

    pub fn on_event(&mut self, event: DriverEvent) -> Result<()> {
        match event {
            DriverEvent::PublicationReady {
                exclusive,
                correlation_id,
                registration_id,
                session_id,
                stream_id,
                publication_limit_counter_id,
                channel_status_counter_id,
                log_file_name,
            } => self.on_new_publication_with_mode(
                correlation_id,
                registration_id,
                session_id,
                stream_id,
                publication_limit_counter_id,
                channel_status_counter_id,
                &log_file_name,
                exclusive,
            ),
            DriverEvent::SubscriptionReady {
                correlation_id,
                channel_status_counter_id,
            } => self.on_new_subscription(correlation_id, channel_status_counter_id),
            DriverEvent::AvailableImage {
                correlation_id,
                session_id,
                stream_id,
                subscription_registration_id,
                subscriber_position_id,
                log_file_name,
                source_identity,
            } => self.on_available_image(
                correlation_id,
                session_id,
                stream_id,
                subscription_registration_id,
                subscriber_position_id,
                &log_file_name,
                &source_identity,
            ),
            DriverEvent::UnavailableImage {
                correlation_id,
                subscription_registration_id,
            } => self.on_unavailable_image(correlation_id, subscription_registration_id),
            DriverEvent::OperationSucceeded { correlation_id } => {
                self.pending_since.remove(&correlation_id);
                Ok(())
            }
            DriverEvent::Error {
                correlation_id,
                code,
                message,
            } => self.on_error(correlation_id, code, message),
            DriverEvent::ClientTimeout { client_id } => self.on_client_timeout(client_id),
            DriverEvent::NextAvailableSessionId {
                correlation_id,
                session_id,
            } => {
                self.next_session_id = Some(session_id);
                self.pending_since.remove(&correlation_id);
                Ok(())
            }
            DriverEvent::CounterReady { correlation_id, .. }
            | DriverEvent::CounterUnavailable { correlation_id, .. } => {
                self.pending_since.remove(&correlation_id);
                Ok(())
            }
        }
    }

    pub fn add_publication(&mut self, channel: &str, stream_id: i32) -> Result<i64> {
        self.ensure_open()?;
        let correlation_id = self.driver_proxy.add_publication(channel, stream_id)?;
        self.pending_publications.insert(
            correlation_id,
            PendingPublication {
                channel: channel.into(),
                exclusive: false,
            },
        );
        self.registration_states
            .insert(correlation_id, RegistrationState::Pending);
        self.pending_since.insert(correlation_id, Instant::now());
        Ok(correlation_id)
    }

    pub fn add_exclusive_publication(&mut self, channel: &str, stream_id: i32) -> Result<i64> {
        self.ensure_open()?;
        let correlation_id = self
            .driver_proxy
            .add_exclusive_publication(channel, stream_id)?;
        self.pending_publications.insert(
            correlation_id,
            PendingPublication {
                channel: channel.into(),
                exclusive: true,
            },
        );
        self.registration_states
            .insert(correlation_id, RegistrationState::Pending);
        self.pending_since.insert(correlation_id, Instant::now());
        Ok(correlation_id)
    }

    pub fn add_subscription(&mut self, channel: &str, stream_id: i32) -> Result<i64> {
        self.ensure_open()?;
        let correlation_id = self.driver_proxy.add_subscription(channel, stream_id)?;
        self.pending_subscriptions.insert(
            correlation_id,
            PendingSubscription {
                channel: channel.into(),
                stream_id,
            },
        );
        self.registration_states
            .insert(correlation_id, RegistrationState::Pending);
        self.pending_since.insert(correlation_id, Instant::now());
        Ok(correlation_id)
    }

    pub fn get_next_available_session_id(&mut self, stream_id: i32) -> Result<i64> {
        self.ensure_open()?;
        if self.control_protocol_version < crate::driver_events::CONTROL_PROTOCOL_SEMANTIC_VERSION {
            return Err(AeronError::IllegalState(
                "driver does not support next session id".into(),
            ));
        }
        let correlation_id = self.driver_proxy.get_next_available_session_id(stream_id)?;
        self.pending_since.insert(correlation_id, Instant::now());
        Ok(correlation_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn on_new_publication(
        &mut self,
        correlation_id: i64,
        registration_id: i64,
        session_id: i32,
        stream_id: i32,
        publication_limit_counter_id: i32,
        _channel_status_counter_id: i32,
        log_file_name: &str,
    ) -> Result<()> {
        self.on_new_publication_with_mode(
            correlation_id,
            registration_id,
            session_id,
            stream_id,
            publication_limit_counter_id,
            _channel_status_counter_id,
            log_file_name,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn on_new_publication_with_mode(
        &mut self,
        correlation_id: i64,
        registration_id: i64,
        session_id: i32,
        stream_id: i32,
        publication_limit_counter_id: i32,
        _channel_status_counter_id: i32,
        log_file_name: &str,
        exclusive: bool,
    ) -> Result<()> {
        // The to-clients broadcast is shared by every client; ignore a publication-ready
        // event for a correlation this client never registered (it belongs to another client).
        let pending = match self.pending_publications.remove(&correlation_id) {
            Some(pending) => pending,
            None => return Ok(()),
        };
        if pending.exclusive != exclusive {
            return Err(AeronError::IllegalState(format!(
                "publication mode mismatch for correlation {correlation_id}"
            )));
        }
        if exclusive && correlation_id != registration_id {
            return Err(AeronError::IllegalState(format!(
                "exclusive publication correlation {correlation_id} != registration {registration_id}"
            )));
        }
        let buffers = self.load_log_buffers(log_file_name)?;
        let initial_term_id = initial_term_id(&buffers.meta_data_buffer());
        self.bind_cnc_counter(publication_limit_counter_id);
        let publication_limit = self
            .counter_values
            .get(&publication_limit_counter_id)
            .cloned()
            .ok_or_else(|| {
                AeronError::IllegalState(format!(
                    "missing publication limit counter {publication_limit_counter_id}"
                ))
            })?;
        let publication = ConcurrentPublication::new(
            pending.channel,
            stream_id,
            session_id,
            initial_term_id,
            registration_id,
            correlation_id,
            publication_limit,
            buffers,
        );
        self.publications.insert(registration_id, publication);
        self.registration_states
            .insert(correlation_id, RegistrationState::Ready);
        self.pending_since.remove(&correlation_id);
        Ok(())
    }

    pub fn on_new_subscription(
        &mut self,
        correlation_id: i64,
        channel_status_counter_id: i32,
    ) -> Result<()> {
        // Shared broadcast: ignore a subscription-ready event for another client's correlation.
        let pending = match self.pending_subscriptions.remove(&correlation_id) {
            Some(pending) => pending,
            None => return Ok(()),
        };
        let mut subscription =
            Subscription::new(correlation_id, pending.stream_id, pending.channel);
        subscription.set_channel_status_counter_id(channel_status_counter_id);
        self.subscriptions.insert(correlation_id, subscription);
        self.registration_states
            .insert(correlation_id, RegistrationState::Ready);
        self.pending_since.remove(&correlation_id);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn on_available_image(
        &mut self,
        correlation_id: i64,
        session_id: i32,
        stream_id: i32,
        subscription_registration_id: i64,
        subscriber_position_id: i32,
        log_file_name: &str,
        source_identity: &str,
    ) -> Result<()> {
        if !self
            .subscriptions
            .contains_key(&subscription_registration_id)
        {
            return Ok(());
        }
        let buffers = self.load_log_buffers(log_file_name)?;
        self.bind_cnc_counter(subscriber_position_id);
        let position = self
            .counter_values
            .get(&subscriber_position_id)
            .cloned()
            .ok_or_else(|| {
                AeronError::IllegalState(format!(
                    "missing subscriber position counter {subscriber_position_id}"
                ))
            })?;
        let initial_term_id = initial_term_id(&buffers.meta_data_buffer());
        // The driver already seeded this counter with the image's real starting position (0 for
        // a fresh network rebuild buffer, the publication's current tail for a subscriber
        // joining an already-active IPC/spy publication) -- read it rather than assuming 0.
        let initial_position = position.load(Ordering::Acquire);
        let cnc_counter = self.cnc_counter(subscriber_position_id);
        let mut image = Image::new(
            subscription_registration_id,
            session_id,
            initial_term_id,
            initial_position,
            position,
            buffers,
            source_identity.into(),
            correlation_id,
        );
        image.set_subscriber_position_id(subscriber_position_id);
        if let Some(counter) = cnc_counter {
            image.set_cnc_subscriber_position(counter);
        }
        if let Some(subscription) = self.subscriptions.get_mut(&subscription_registration_id) {
            if subscription.stream_id() != stream_id {
                return Err(AeronError::IllegalState(format!(
                    "image stream {stream_id} does not match subscription stream {}",
                    subscription.stream_id()
                )));
            }
            subscription.add_image(image);
        }
        Ok(())
    }

    pub fn on_unavailable_image(
        &mut self,
        correlation_id: i64,
        subscription_registration_id: i64,
    ) -> Result<()> {
        if let Some(subscription) = self.subscriptions.get_mut(&subscription_registration_id) {
            subscription.remove_image(correlation_id);
        }
        Ok(())
    }

    pub fn on_error(&mut self, correlation_id: i64, code: i32, message: String) -> Result<()> {
        self.pending_publications.remove(&correlation_id);
        self.pending_subscriptions.remove(&correlation_id);
        self.pending_since.remove(&correlation_id);
        self.registration_states
            .insert(correlation_id, RegistrationState::Closed);
        if let Some(mut subscription) = self.subscriptions.remove(&correlation_id) {
            subscription.close();
        }
        let publication_id = self
            .publications
            .iter()
            .find(|(_, publication)| publication.original_registration_id() == correlation_id)
            .map(|(registration_id, _)| *registration_id);
        if let Some(publication_id) = publication_id {
            if let Some(mut publication) = self.publications.remove(&publication_id) {
                publication.close();
            }
        }
        Err(AeronError::DriverResponse {
            correlation_id,
            code,
            message,
        })
    }

    pub fn on_client_timeout(&mut self, client_id: i64) -> Result<()> {
        if client_id != self.driver_proxy.client_id() {
            return Ok(());
        }
        self.timed_out = true;
        self.close_resources();
        self.is_closed = true;
        Err(AeronError::ClientTimeout(client_id))
    }

    pub fn publication(&self, registration_id: i64) -> Option<&ConcurrentPublication> {
        self.publications.get(&registration_id)
    }

    pub fn subscription(&self, registration_id: i64) -> Option<&Subscription> {
        self.subscriptions.get(&registration_id)
    }

    pub fn subscription_mut(&mut self, registration_id: i64) -> Option<&mut Subscription> {
        self.subscriptions.get_mut(&registration_id)
    }

    pub fn remove_publication(&mut self, registration_id: i64) -> Result<bool> {
        self.ensure_open()?;
        if self.subscriptions.contains_key(&registration_id)
            || self.pending_subscriptions.contains_key(&registration_id)
        {
            return Err(AeronError::WrongResourceType {
                registration_id,
                expected: "publication",
                actual: "subscription",
            });
        }
        // `publications` is keyed by the driver-assigned registration_id, which can differ
        // from the original correlation_id add_publication() returned to the caller (e.g. a
        // shared/duplicate publication resolves to another client's existing registration_id).
        // Fall back to matching on original_registration_id, mirroring on_error's lookup, so
        // callers can still remove a publication using the id they were originally given.
        let actual_key = if self.publications.contains_key(&registration_id) {
            Some(registration_id)
        } else {
            self.publications
                .iter()
                .find(|(_, publication)| publication.original_registration_id() == registration_id)
                .map(|(key, _)| *key)
        };
        if let Some(actual_key) = actual_key {
            if let Some(mut publication) = self.publications.remove(&actual_key) {
                publication.close();
                self.driver_proxy.remove_publication(actual_key, false)?;
                self.registration_states
                    .insert(registration_id, RegistrationState::Closed);
                return Ok(true);
            }
        }
        if self.pending_publications.remove(&registration_id).is_some() {
            self.driver_proxy
                .remove_publication(registration_id, false)?;
            self.pending_since.remove(&registration_id);
            self.registration_states
                .insert(registration_id, RegistrationState::Closed);
            return Ok(true);
        }
        Ok(false)
    }

    pub fn remove_subscription(&mut self, registration_id: i64) -> Result<bool> {
        self.ensure_open()?;
        if self.publications.contains_key(&registration_id)
            || self.pending_publications.contains_key(&registration_id)
        {
            return Err(AeronError::WrongResourceType {
                registration_id,
                expected: "subscription",
                actual: "publication",
            });
        }
        if let Some(mut subscription) = self.subscriptions.remove(&registration_id) {
            subscription.close();
            self.driver_proxy.remove_subscription(registration_id)?;
            self.registration_states
                .insert(registration_id, RegistrationState::Closed);
            return Ok(true);
        }
        if self
            .pending_subscriptions
            .remove(&registration_id)
            .is_some()
        {
            self.driver_proxy.remove_subscription(registration_id)?;
            self.pending_since.remove(&registration_id);
            self.registration_states
                .insert(registration_id, RegistrationState::Closed);
            return Ok(true);
        }
        Ok(false)
    }

    pub fn publication_count(&self) -> usize {
        self.publications.len() + self.pending_publications.len()
    }

    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len() + self.pending_subscriptions.len()
    }

    pub fn close(&mut self) -> Result<()> {
        if self.is_closed {
            return Ok(());
        }
        for registration_id in self
            .publications
            .keys()
            .copied()
            .chain(self.pending_publications.keys().copied())
            .collect::<Vec<_>>()
        {
            self.driver_proxy
                .remove_publication(registration_id, false)?;
        }
        for registration_id in self
            .subscriptions
            .keys()
            .copied()
            .chain(self.pending_subscriptions.keys().copied())
            .collect::<Vec<_>>()
        {
            self.driver_proxy.remove_subscription(registration_id)?;
        }
        self.close_resources();
        for registration_id in self.registration_states.keys().copied().collect::<Vec<_>>() {
            self.registration_states
                .insert(registration_id, RegistrationState::Closed);
        }
        self.driver_proxy.client_close()?;
        self.is_closed = true;
        Ok(())
    }

    fn close_resources(&mut self) {
        for publication in self.publications.values_mut() {
            publication.close();
        }
        for subscription in self.subscriptions.values_mut() {
            subscription.close();
        }
        self.publications.clear();
        self.subscriptions.clear();
        self.pending_publications.clear();
        self.pending_subscriptions.clear();
        self.pending_since.clear();
    }

    fn load_log_buffers(&self, name: &str) -> Result<LogBuffers> {
        // A registered buffer (unit tests / in-memory scenarios) wins; otherwise map the
        // driver-named log file `MAP_SHARED` so publisher writes are visible to this
        // subscriber's reads (real cross-process/IPC log sharing, not a private copy).
        let buffers = self
            .log_buffers
            .get(name)
            .cloned()
            .map(Ok)
            .unwrap_or_else(|| LogBuffers::map_existing(name))?;
        if self.pre_touch_mapped_memory {
            buffers.pre_touch();
        }
        Ok(buffers)
    }

    fn ensure_open(&self) -> Result<()> {
        if self.is_closed {
            Err(AeronError::IllegalState("ClientConductor is closed".into()))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cnc_file::{
        DEFAULT_COUNTER_METADATA_LENGTH, DEFAULT_COUNTER_VALUES_LENGTH, DEFAULT_ERROR_LOG_LENGTH,
        DEFAULT_TO_CLIENTS_LENGTH, DEFAULT_TO_DRIVER_LENGTH,
    };
    use crate::driver_proxy::{
        ADD_PUBLICATION, ADD_SUBSCRIPTION, CLIENT_CLOSE, REMOVE_PUBLICATION, REMOVE_SUBSCRIPTION,
    };
    use crate::logbuffer::{
        LOG_META_DATA_LENGTH, PAGE_MIN_SIZE, TERM_MIN_LENGTH, set_initial_term_id, set_mtu_length,
        set_page_size, set_term_length,
    };
    use agrona::ring_buffer::{TRAILER_LENGTH, mpsc::ManyToOneRingBuffer};

    fn conductor<'a>(bytes: &'a mut [u8]) -> (ClientConductor<'a>, ManyToOneRingBuffer<'a>) {
        let buffer = ManyToOneRingBuffer::new(AtomicBuffer::new(bytes)).unwrap();
        (
            ClientConductor::new(DriverProxy::new(buffer.clone(), 42)),
            buffer,
        )
    }

    #[test]
    fn maps_and_refreshes_counters_from_cnc_file() {
        let path = std::env::temp_dir().join(format!(
            "aeron-client-cnc-counter-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&path);
        let cnc = CncFile::create_new_with_regions(
            &path,
            DEFAULT_TO_DRIVER_LENGTH,
            DEFAULT_TO_CLIENTS_LENGTH,
            DEFAULT_COUNTER_METADATA_LENGTH,
            DEFAULT_COUNTER_VALUES_LENGTH,
            DEFAULT_ERROR_LOG_LENGTH,
            10_000_000_000,
        )
        .unwrap();
        cnc.write_counter_metadata(5, 1, 5, 123, "test counter")
            .unwrap();

        let ring = ManyToOneRingBuffer::new(cnc.to_driver_buffer()).unwrap();
        let mut conductor =
            ClientConductor::with_cnc_file(DriverProxy::new(ring, 42), cnc.clone()).unwrap();
        conductor.bind_cnc_counter(5);
        assert_eq!(
            conductor.counter_values[&5].load(std::sync::atomic::Ordering::Acquire),
            123
        );

        cnc.set_counter_value(5, 456).unwrap();
        conductor.sync_cnc_counters();
        assert_eq!(
            conductor.counter_values[&5].load(std::sync::atomic::Ordering::Acquire),
            456
        );

        drop(conductor);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn tracks_pending_commands_and_closes_with_driver_notification() {
        let mut bytes = vec![0u8; TRAILER_LENGTH + 4096];
        let (mut conductor, reader) = conductor(&mut bytes);
        let publication = conductor.add_publication("aeron:ipc", 7).unwrap();
        let subscription = conductor.add_subscription("aeron:ipc", 8).unwrap();
        assert_eq!(conductor.publication_count(), 1);
        assert_eq!(conductor.subscription_count(), 1);
        conductor.close().unwrap();
        let mut types = Vec::new();
        reader.read(|message_type, _, _, _| types.push(message_type));
        assert_eq!(
            types,
            vec![
                ADD_PUBLICATION,
                ADD_SUBSCRIPTION,
                REMOVE_PUBLICATION,
                REMOVE_SUBSCRIPTION,
                CLIENT_CLOSE
            ]
        );
        assert!(publication > 0 && subscription > 0);
    }

    fn log_buffers() -> LogBuffers {
        let mut bytes = vec![0u8; TERM_MIN_LENGTH as usize * 3 + LOG_META_DATA_LENGTH];
        let metadata = AtomicBuffer::new(&mut bytes[TERM_MIN_LENGTH as usize * 3..]);
        set_term_length(&metadata, TERM_MIN_LENGTH);
        set_page_size(&metadata, PAGE_MIN_SIZE);
        set_mtu_length(&metadata, 1408);
        set_initial_term_id(&metadata, 11);
        LogBuffers::from_bytes(bytes).unwrap()
    }

    #[test]
    fn resolves_publication_subscription_and_image_events() {
        let mut bytes = vec![0u8; TRAILER_LENGTH + 4096];
        let (mut conductor, _) = conductor(&mut bytes);
        let buffers = log_buffers();
        conductor.register_log_buffers("pub.log", buffers.clone());
        conductor.register_log_buffers("image.log", buffers);
        let publication_limit = Arc::new(AtomicI64::new(4096));
        let subscriber_position = Arc::new(AtomicI64::new(0));
        conductor.register_counter(5, publication_limit);
        conductor.register_counter(6, subscriber_position);

        let publication_correlation = conductor.add_publication("aeron:ipc", 7).unwrap();
        assert_eq!(
            conductor.registration_state(publication_correlation),
            Some(RegistrationState::Pending)
        );
        conductor
            .on_new_publication(publication_correlation, 100, 33, 7, 5, 9, "pub.log")
            .unwrap();
        assert_eq!(
            conductor.registration_state(publication_correlation),
            Some(RegistrationState::Ready)
        );
        assert_eq!(conductor.publication(100).unwrap().session_id(), 33);
        assert_eq!(conductor.publication(100).unwrap().initial_term_id(), 11);

        let subscription_correlation = conductor.add_subscription("aeron:ipc", 7).unwrap();
        conductor
            .on_new_subscription(subscription_correlation, 10)
            .unwrap();
        conductor
            .on_available_image(
                77,
                33,
                7,
                subscription_correlation,
                6,
                "image.log",
                "udp://source",
            )
            .unwrap();
        let subscription = conductor.subscription(subscription_correlation).unwrap();
        assert_eq!(subscription.channel_status_counter_id(), Some(10));
        assert_eq!(subscription.image_count(), 1);
        assert_eq!(
            subscription
                .image_by_session_id(33)
                .unwrap()
                .source_identity(),
            "udp://source"
        );

        conductor
            .on_unavailable_image(77, subscription_correlation)
            .unwrap();
        assert_eq!(
            conductor
                .subscription(subscription_correlation)
                .unwrap()
                .image_count(),
            0
        );
        assert_eq!(
            conductor.registration_state(subscription_correlation),
            Some(RegistrationState::Ready)
        );
    }

    #[test]
    fn driver_error_and_timeout_clear_client_state() {
        let mut bytes = vec![0u8; TRAILER_LENGTH + 4096];
        let (mut conductor, _) = conductor(&mut bytes);
        let correlation = conductor.add_subscription("aeron:ipc", 7).unwrap();
        assert!(matches!(
            conductor.on_error(correlation, 13, "bad channel".into()),
            Err(AeronError::DriverResponse { .. })
        ));
        assert_eq!(conductor.subscription_count(), 0);
        let _ = conductor.add_publication("aeron:ipc", 8).unwrap();
        assert!(matches!(
            conductor.on_client_timeout(42),
            Err(AeronError::ClientTimeout(42))
        ));
        assert!(conductor.timed_out());
        assert!(conductor.is_closed());
        assert_eq!(conductor.publication_count(), 0);
    }

    #[test]
    fn supports_exclusive_publication_and_wrong_resource_errors() {
        let mut bytes = vec![0u8; TRAILER_LENGTH + 4096];
        let (mut conductor, reader) = conductor(&mut bytes);
        let subscription = conductor.add_subscription("aeron:ipc", 7).unwrap();
        let publication = conductor.add_exclusive_publication("aeron:ipc", 8).unwrap();
        assert!(matches!(
            conductor.remove_publication(subscription),
            Err(AeronError::WrongResourceType { .. })
        ));
        assert!(matches!(
            conductor.remove_subscription(publication),
            Err(AeronError::WrongResourceType { .. })
        ));
        let mut types = Vec::new();
        reader.read(|message_type, _, _, _| types.push(message_type));
        assert!(types.contains(&crate::driver_proxy::ADD_EXCLUSIVE_PUBLICATION));
    }

    #[test]
    fn do_work_sends_keepalive_and_times_out_pending_registration() {
        let mut bytes = vec![0u8; TRAILER_LENGTH + 4096];
        let (mut conductor, reader) = conductor(&mut bytes);
        conductor.set_driver_timeout(Duration::from_millis(10));
        conductor.add_subscription("aeron:ipc", 7).unwrap();
        let now = Instant::now() + Duration::from_millis(20);
        assert!(matches!(
            conductor.do_work_at(now),
            Err(AeronError::DriverTimeout(10))
        ));
        let mut types = Vec::new();
        reader.read(|message_type, _, _, _| types.push(message_type));
        assert!(types.contains(&crate::driver_proxy::CLIENT_KEEPALIVE));
        assert_eq!(conductor.subscription_count(), 0);
    }

    #[test]
    fn control_protocol_version_defaults_to_zero_and_gates_next_session_id() {
        let mut bytes = vec![0u8; TRAILER_LENGTH + 4096];
        let (mut conductor, _reader) = conductor(&mut bytes);

        // Matches upstream: controlProtocolVersion defaults to 0 until a real system counter
        // confirms the driver's version, so feature-gated commands must be rejected by default.
        assert_eq!(conductor.control_protocol_version(), 0);
        assert!(conductor.get_next_available_session_id(7).is_err());

        conductor.register_counter(
            crate::counters::SYSTEM_COUNTER_ID_CONTROL_PROTOCOL_VERSION,
            Arc::new(AtomicI64::new(
                crate::driver_events::CONTROL_PROTOCOL_SEMANTIC_VERSION as i64,
            )),
        );
        assert_eq!(
            conductor.control_protocol_version(),
            crate::driver_events::CONTROL_PROTOCOL_SEMANTIC_VERSION
        );
        assert!(conductor.get_next_available_session_id(7).is_ok());
    }

    #[test]
    fn remove_publication_falls_back_to_original_registration_id() {
        // Simulates a shared/duplicate publication, where the driver-assigned registration_id
        // can differ from the correlation_id add_publication() returned to the caller. Removal
        // must still work when the caller only knows the original correlation_id.
        let mut bytes = vec![0u8; TRAILER_LENGTH + 4096];
        let (mut conductor, reader) = conductor(&mut bytes);
        conductor.register_log_buffers("shared.log", log_buffers());
        conductor.register_counter(5, Arc::new(AtomicI64::new(4096)));

        let correlation_id = conductor.add_publication("aeron:ipc", 7).unwrap();
        let driver_registration_id = correlation_id + 1000;
        conductor
            .on_new_publication(
                correlation_id,
                driver_registration_id,
                33,
                7,
                5,
                9,
                "shared.log",
            )
            .unwrap();

        assert!(conductor.publication(driver_registration_id).is_some());
        assert!(conductor.remove_publication(correlation_id).unwrap());
        assert!(conductor.publication(driver_registration_id).is_none());

        let mut types = Vec::new();
        reader.read(|message_type, _, _, _| types.push(message_type));
        assert!(types.contains(&crate::driver_proxy::REMOVE_PUBLICATION));
    }
}
