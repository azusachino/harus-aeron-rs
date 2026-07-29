//! Managed Archive service attached to a running Aeron Media Driver.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::JoinHandle;
use std::time::Duration;

use aeron_client::cnc_file::{CNC_FILE, CncFile};
use aeron_client::conductor::ClientConductor;
use aeron_client::driver_proxy::DriverProxy;
use aeron_client::error::AeronError;
use agrona::AtomicBuffer;
use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;

use crate::codecs::{
    ControlResponse, ControlResponseCode, PROTOCOL_SEMANTIC_VERSION, StartRecordingRequest,
};
use crate::control_session_adapter::{ControlAdapterError, ControlRequest, ControlSessionAdapter};
use crate::runtime::{ArchiveRuntime, ArchiveRuntimeError};

pub const DEFAULT_CONTROL_CHANNEL: &str = "aeron:ipc";
pub const DEFAULT_CONTROL_STREAM_ID: i32 = 10;
pub const DEFAULT_ARCHIVE_CLIENT_ID: i64 = 7_001;

#[derive(Debug, Clone)]
pub struct ArchiveContext {
    pub aeron_directory: PathBuf,
    pub archive_directory: PathBuf,
    pub control_channel: String,
    pub control_stream_id: i32,
    pub client_id: i64,
    pub idle_duration: Duration,
}

impl ArchiveContext {
    pub fn new(aeron_directory: impl Into<PathBuf>, archive_directory: impl Into<PathBuf>) -> Self {
        Self {
            aeron_directory: aeron_directory.into(),
            archive_directory: archive_directory.into(),
            control_channel: DEFAULT_CONTROL_CHANNEL.into(),
            control_stream_id: DEFAULT_CONTROL_STREAM_ID,
            client_id: DEFAULT_ARCHIVE_CLIENT_ID,
            idle_duration: Duration::from_millis(1),
        }
    }

    fn validate(&self) -> Result<(), ArchiveServiceError> {
        if self.control_channel.is_empty() {
            return Err(ArchiveServiceError::InvalidContext(
                "control channel must not be empty".into(),
            ));
        }
        if self.aeron_directory == self.archive_directory {
            return Err(ArchiveServiceError::InvalidContext(
                "Aeron and Archive directories must differ".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ArchiveServiceError {
    #[error(transparent)]
    Aeron(#[from] AeronError),
    #[error(transparent)]
    Runtime(#[from] ArchiveRuntimeError),
    #[error(transparent)]
    Adapter(#[from] ControlAdapterError),
    #[error("invalid Archive context: {0}")]
    InvalidContext(String),
    #[error("Archive service thread panicked")]
    ThreadPanicked,
}

struct PendingConnect {
    publication_id: i64,
    correlation_id: i64,
    control_session_id: i64,
}

struct PendingStart {
    registration_id: i64,
    request: StartRecordingRequest,
}

struct PendingReplay {
    publication_id: i64,
    control_session_id: i64,
    correlation_id: i64,
    recording_id: i64,
}

struct ActiveRecording {
    subscription_id: i64,
    recording_id: i64,
}

struct QueuedResponse {
    publication_id: i64,
    bytes: Vec<u8>,
}

pub struct ArchiveService {
    context: ArchiveContext,
    client: ClientConductor<'static>,
    runtime: ArchiveRuntime,
    control_subscription_id: i64,
    next_control_session_id: i64,
    sessions: HashMap<i64, i64>,
    pending_connects: Vec<PendingConnect>,
    pending_starts: Vec<PendingStart>,
    pending_replays: Vec<PendingReplay>,
    active_recordings: HashMap<(String, i32), ActiveRecording>,
    responses: VecDeque<QueuedResponse>,
    observed_requests: HashSet<(u16, i64, i64)>,
}

impl ArchiveService {
    pub fn launch(context: ArchiveContext) -> Result<Self, ArchiveServiceError> {
        context.validate()?;
        let mut client = connect_client(&context.aeron_directory, context.client_id)?;
        let control_subscription_id =
            client.add_subscription(&context.control_channel, context.control_stream_id)?;
        let runtime = ArchiveRuntime::open(&context.archive_directory)?;
        Ok(Self {
            context,
            client,
            runtime,
            control_subscription_id,
            next_control_session_id: 1,
            sessions: HashMap::new(),
            pending_connects: Vec::new(),
            pending_starts: Vec::new(),
            pending_replays: Vec::new(),
            active_recordings: HashMap::new(),
            responses: VecDeque::new(),
            observed_requests: HashSet::new(),
        })
    }

    pub fn context(&self) -> &ArchiveContext {
        &self.context
    }

    pub fn runtime(&self) -> &ArchiveRuntime {
        &self.runtime
    }

    pub fn do_work(&mut self) -> Result<usize, ArchiveServiceError> {
        let mut work = self.client.do_work()?;
        work += self.poll_control_requests()?;
        work += self.finish_connects()?;
        work += self.finish_starts()?;
        work += self.poll_recordings()?;
        work += self.finish_replays()?;
        work += self.flush_responses()?;
        Ok(work)
    }

    fn poll_control_requests(&mut self) -> Result<usize, ArchiveServiceError> {
        let mut frames = Vec::new();
        if let Some(subscription) = self.client.subscription_mut(self.control_subscription_id) {
            subscription.poll(
                |buffer, offset, length, _| {
                    let mut frame = vec![0; length];
                    buffer.get_bytes(offset, &mut frame);
                    frames.push(frame);
                },
                20,
            );
        }
        let count = frames.len();
        for frame in frames {
            let request = ControlSessionAdapter::decode(&frame)?;
            if self.observed_requests.insert(request_key(&request)) {
                self.on_request(request)?;
            }
        }
        Ok(count)
    }

    fn on_request(&mut self, request: ControlRequest) -> Result<(), ArchiveServiceError> {
        match request {
            ControlRequest::Connect(request) => {
                let control_session_id = self.next_control_session_id;
                self.next_control_session_id += 1;
                let publication_id = self.client.add_exclusive_publication(
                    &request.response_channel,
                    request.response_stream_id,
                )?;
                self.pending_connects.push(PendingConnect {
                    publication_id,
                    correlation_id: request.correlation_id,
                    control_session_id,
                });
            }
            ControlRequest::CloseSession(request) => {
                if let Some(publication_id) = self.sessions.remove(&request.control_session_id) {
                    let _ = self.client.remove_publication(publication_id)?;
                }
            }
            ControlRequest::StartRecording(request) => {
                if !self.sessions.contains_key(&request.control_session_id) {
                    return Ok(());
                }
                let registration_id = self
                    .client
                    .add_subscription(&request.channel, request.stream_id)?;
                self.pending_starts.push(PendingStart {
                    registration_id,
                    request,
                });
            }
            ControlRequest::StopRecording(request) => {
                let key = (request.channel, request.stream_id);
                let result = self.active_recordings.remove(&key);
                let response = if let Some(active) = result {
                    let stop_position = self.runtime.stop_recording(active.recording_id)?;
                    let _ = self.client.remove_subscription(active.subscription_id)?;
                    ok(
                        request.control_session_id,
                        request.correlation_id,
                        stop_position,
                    )
                } else {
                    error(
                        request.control_session_id,
                        request.correlation_id,
                        ControlResponseCode::SubscriptionUnknown,
                        "recording subscription unknown",
                    )
                };
                self.queue_session_response(request.control_session_id, response)?;
            }
            ControlRequest::Replay(request) => {
                if !self.sessions.contains_key(&request.control_session_id) {
                    return Ok(());
                }
                let publication_id = self
                    .client
                    .add_exclusive_publication(&request.replay_channel, request.replay_stream_id)?;
                self.pending_replays.push(PendingReplay {
                    publication_id,
                    control_session_id: request.control_session_id,
                    correlation_id: request.correlation_id,
                    recording_id: request.recording_id,
                });
            }
            ControlRequest::StopReplay(request) => {
                self.queue_session_response(
                    request.control_session_id,
                    error(
                        request.control_session_id,
                        request.correlation_id,
                        ControlResponseCode::SubscriptionUnknown,
                        "replay is not active",
                    ),
                )?;
            }
            ControlRequest::ListRecordings(request) => {
                let count = self
                    .runtime
                    .catalog()
                    .recordings()
                    .iter()
                    .filter(|descriptor| descriptor.recording_id >= request.from_recording_id)
                    .take(request.record_count.max(0) as usize)
                    .count() as i64;
                self.queue_session_response(
                    request.control_session_id,
                    ok(request.control_session_id, request.correlation_id, count),
                )?;
            }
            ControlRequest::ListRecordingsForUri(request) => {
                let count = self
                    .runtime
                    .catalog()
                    .recordings()
                    .iter()
                    .filter(|descriptor| {
                        descriptor.recording_id >= request.from_recording_id
                            && descriptor.stream_id == request.stream_id
                            && descriptor.original_channel.contains(&request.channel)
                    })
                    .take(request.record_count.max(0) as usize)
                    .count() as i64;
                self.queue_session_response(
                    request.control_session_id,
                    ok(request.control_session_id, request.correlation_id, count),
                )?;
            }
            ControlRequest::ListRecording(request) => {
                let response = if self
                    .runtime
                    .catalog()
                    .get_recording(request.recording_id)
                    .is_some()
                {
                    ok(
                        request.control_session_id,
                        request.correlation_id,
                        request.recording_id,
                    )
                } else {
                    error(
                        request.control_session_id,
                        request.correlation_id,
                        ControlResponseCode::RecordingUnknown,
                        "recording unknown",
                    )
                };
                self.queue_session_response(request.control_session_id, response)?;
            }
        }
        Ok(())
    }

    fn finish_connects(&mut self) -> Result<usize, ArchiveServiceError> {
        let mut ready = Vec::new();
        for (index, pending) in self.pending_connects.iter().enumerate() {
            if self.client.publication(pending.publication_id).is_some() {
                ready.push(index);
            }
        }
        let count = ready.len();
        for index in ready.into_iter().rev() {
            let pending = self.pending_connects.swap_remove(index);
            self.sessions
                .insert(pending.control_session_id, pending.publication_id);
            self.responses.push_back(QueuedResponse {
                publication_id: pending.publication_id,
                bytes: ok(
                    pending.control_session_id,
                    pending.correlation_id,
                    pending.control_session_id,
                )
                .encode()
                .map_err(ControlAdapterError::from)?,
            });
        }
        Ok(count)
    }

    fn finish_starts(&mut self) -> Result<usize, ArchiveServiceError> {
        let mut ready = Vec::new();
        for (index, pending) in self.pending_starts.iter().enumerate() {
            if self
                .client
                .subscription(pending.registration_id)
                .is_some_and(|subscription| !subscription.images().is_empty())
            {
                ready.push(index);
            }
        }
        let count = ready.len();
        for index in ready.into_iter().rev() {
            let pending = self.pending_starts.swap_remove(index);
            let recording_id = {
                let subscription = self
                    .client
                    .subscription(pending.registration_id)
                    .ok_or_else(|| {
                        ArchiveServiceError::InvalidContext("subscription vanished".into())
                    })?;
                self.runtime.start_recording(subscription)?
            };
            self.active_recordings.insert(
                (pending.request.channel, pending.request.stream_id),
                ActiveRecording {
                    subscription_id: pending.registration_id,
                    recording_id,
                },
            );
            self.queue_session_response(
                pending.request.control_session_id,
                ok(
                    pending.request.control_session_id,
                    pending.request.correlation_id,
                    recording_id,
                ),
            )?;
        }
        Ok(count)
    }

    fn poll_recordings(&mut self) -> Result<usize, ArchiveServiceError> {
        let recordings = self
            .active_recordings
            .values()
            .map(|active| (active.subscription_id, active.recording_id))
            .collect::<Vec<_>>();
        let mut work = 0;
        for (subscription_id, recording_id) in recordings {
            if let Some(subscription) = self.client.subscription_mut(subscription_id) {
                work += self
                    .runtime
                    .poll_recording(recording_id, subscription, 20)?;
            }
        }
        Ok(work)
    }

    fn finish_replays(&mut self) -> Result<usize, ArchiveServiceError> {
        let mut ready = Vec::new();
        for (index, pending) in self.pending_replays.iter().enumerate() {
            if self.client.publication(pending.publication_id).is_some() {
                ready.push(index);
            }
        }
        let count = ready.len();
        for index in ready.into_iter().rev() {
            let pending = self.pending_replays.swap_remove(index);
            let result = {
                let publication =
                    self.client
                        .publication(pending.publication_id)
                        .ok_or_else(|| {
                            ArchiveServiceError::InvalidContext(
                                "replay publication vanished".into(),
                            )
                        })?;
                self.runtime.replay(pending.recording_id, publication)
            };
            let response = match result {
                Ok(_) => ok(
                    pending.control_session_id,
                    pending.correlation_id,
                    pending.publication_id,
                ),
                Err(ArchiveRuntimeError::UnknownRecording(_)) => error(
                    pending.control_session_id,
                    pending.correlation_id,
                    ControlResponseCode::RecordingUnknown,
                    "recording unknown",
                ),
                Err(error) => return Err(error.into()),
            };
            self.queue_session_response(pending.control_session_id, response)?;
        }
        Ok(count)
    }

    fn queue_session_response(
        &mut self,
        control_session_id: i64,
        response: ControlResponse,
    ) -> Result<(), ArchiveServiceError> {
        if let Some(&publication_id) = self.sessions.get(&control_session_id) {
            self.responses.push_back(QueuedResponse {
                publication_id,
                bytes: response.encode().map_err(ControlAdapterError::from)?,
            });
        }
        Ok(())
    }

    fn flush_responses(&mut self) -> Result<usize, ArchiveServiceError> {
        let mut sent = 0;
        let queued = self.responses.len();
        for _ in 0..queued {
            let Some(mut response) = self.responses.pop_front() else {
                break;
            };
            let Some(publication) = self.client.publication(response.publication_id) else {
                self.responses.push_back(response);
                continue;
            };
            let length = response.bytes.len();
            let buffer = AtomicBuffer::new(&mut response.bytes);
            if publication.offer(&buffer, 0, length)? > 0 {
                sent += 1;
            } else {
                self.responses.push_back(response);
            }
        }
        Ok(sent)
    }
}

pub struct ManagedArchive {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<Result<(), ArchiveServiceError>>>,
}

impl ManagedArchive {
    pub fn start(context: ArchiveContext) -> Result<Self, ArchiveServiceError> {
        let idle_duration = context.idle_duration;
        let mut service = ArchiveService::launch(context)?;
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                if service.do_work()? == 0 {
                    std::thread::park_timeout(idle_duration);
                }
            }
            Ok(())
        });
        Ok(Self {
            stop,
            thread: Some(thread),
        })
    }

    pub fn stop(mut self) -> Result<(), ArchiveServiceError> {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| ArchiveServiceError::ThreadPanicked)??;
        }
        Ok(())
    }
}

impl Drop for ManagedArchive {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn connect_client(
    directory: &Path,
    client_id: i64,
) -> Result<ClientConductor<'static>, ArchiveServiceError> {
    let cnc = CncFile::map_existing(directory.join(CNC_FILE))?;
    let ring = ManyToOneRingBuffer::new(cnc.to_driver_buffer())
        .map_err(|error| AeronError::IllegalState(error.to_string()))?;
    Ok(ClientConductor::with_cnc_file(
        DriverProxy::new(ring, client_id),
        cnc,
    )?)
}

fn ok(control_session_id: i64, correlation_id: i64, relevant_id: i64) -> ControlResponse {
    ControlResponse {
        control_session_id,
        correlation_id,
        relevant_id,
        code: ControlResponseCode::Ok,
        version: PROTOCOL_SEMANTIC_VERSION,
        error_message: String::new(),
    }
}

fn error(
    control_session_id: i64,
    correlation_id: i64,
    code: ControlResponseCode,
    message: &str,
) -> ControlResponse {
    ControlResponse {
        control_session_id,
        correlation_id,
        relevant_id: -1,
        code,
        version: PROTOCOL_SEMANTIC_VERSION,
        error_message: message.into(),
    }
}

fn request_key(request: &ControlRequest) -> (u16, i64, i64) {
    match request {
        ControlRequest::Connect(request) => (2, 0, request.correlation_id),
        ControlRequest::CloseSession(request) => (3, request.control_session_id, -1),
        ControlRequest::StartRecording(request) => {
            (4, request.control_session_id, request.correlation_id)
        }
        ControlRequest::StopRecording(request) => {
            (5, request.control_session_id, request.correlation_id)
        }
        ControlRequest::Replay(request) => (6, request.control_session_id, request.correlation_id),
        ControlRequest::StopReplay(request) => {
            (7, request.control_session_id, request.correlation_id)
        }
        ControlRequest::ListRecordings(request) => {
            (8, request.control_session_id, request.correlation_id)
        }
        ControlRequest::ListRecordingsForUri(request) => {
            (9, request.control_session_id, request.correlation_id)
        }
        ControlRequest::ListRecording(request) => {
            (10, request.control_session_id, request.correlation_id)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_rejects_shared_data_directory() {
        let context = ArchiveContext::new("/tmp/aeron", "/tmp/aeron");
        assert!(matches!(
            context.validate(),
            Err(ArchiveServiceError::InvalidContext(_))
        ));
    }
}
