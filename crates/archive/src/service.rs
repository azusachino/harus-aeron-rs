//! Managed Archive service attached to a running Aeron Media Driver.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use aeron_client::cnc_file::{
    CNC_FILE, COUNTER_METADATA_LENGTH, COUNTER_RECORD_ALLOCATED, COUNTER_VALUE_LENGTH, CncFile,
};
use aeron_client::conductor::ClientConductor;
use aeron_client::driver_proxy::DriverProxy;
use aeron_client::error::AeronError;
use agrona::AtomicBuffer;
use agrona::ring_buffer::mpsc::ManyToOneRingBuffer;

use crate::archive_mark_file::{
    ArchiveMarkFile, ArchiveMarkFileConfig, ArchiveMarkFileError, FILENAME, LINK_FILENAME,
};
use crate::codecs::{
    ControlResponse, ControlResponseCode, ExtendRecordingRequest,
    ListRecordingSubscriptionsRequest, PROTOCOL_SEMANTIC_VERSION, StartRecordingRequest,
};
use crate::control_session_adapter::{ControlAdapterError, ControlRequest, ControlSessionAdapter};
use crate::recording_descriptor::RecordingDescriptor;
use crate::recording_subscription_descriptor::RecordingSubscriptionDescriptor;
use crate::runtime::{ArchiveRuntime, ArchiveRuntimeError, ReplayCursor};

pub const DEFAULT_CONTROL_CHANNEL: &str = "aeron:ipc";
pub const DEFAULT_CONTROL_STREAM_ID: i32 = 10;
pub const DEFAULT_ARCHIVE_CLIENT_ID: i64 = 7_001;
const REPLAY_LINGER_DURATION: Duration = Duration::from_millis(100);

#[derive(Debug, Clone)]
pub struct ArchiveContext {
    pub aeron_directory: PathBuf,
    pub archive_directory: PathBuf,
    pub mark_file_directory: Option<PathBuf>,
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
            mark_file_directory: None,
            control_channel: DEFAULT_CONTROL_CHANNEL.into(),
            control_stream_id: DEFAULT_CONTROL_STREAM_ID,
            client_id: DEFAULT_ARCHIVE_CLIENT_ID,
            idle_duration: Duration::from_millis(1),
        }
    }

    pub fn mark_file_directory(mut self, directory: impl Into<PathBuf>) -> Self {
        self.mark_file_directory = Some(directory.into());
        self
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
    #[error(transparent)]
    MarkFile(#[from] ArchiveMarkFileError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
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
    recording_id: Option<i64>,
    auto_stop: bool,
}

struct PendingReplay {
    publication_id: i64,
    control_session_id: i64,
    correlation_id: i64,
    recording_id: i64,
    position: i64,
    length: i64,
    limit_counter_id: Option<i32>,
}

struct ActiveRecording {
    subscription_id: i64,
    recordings: HashMap<i64, i64>,
    auto_stop: bool,
    follow_images: bool,
    subscription_ref_count: usize,
}

struct ActiveReplay {
    publication_id: i64,
    cursor: ReplayCursor,
    limit_counter_id: Option<i32>,
}

struct LingeringReplayPublication {
    publication_id: i64,
    deadline: Instant,
}

struct QueuedResponse {
    publication_id: i64,
    bytes: Vec<u8>,
}

pub struct ArchiveService {
    context: ArchiveContext,
    mark_file: ArchiveMarkFile,
    cnc: CncFile,
    client: ClientConductor<'static>,
    runtime: ArchiveRuntime,
    control_subscription_id: i64,
    next_control_session_id: i64,
    next_replay_session_id: i64,
    sessions: HashMap<i64, i64>,
    pending_connects: Vec<PendingConnect>,
    pending_starts: Vec<PendingStart>,
    pending_replays: Vec<PendingReplay>,
    active_recordings: HashMap<(String, i32), ActiveRecording>,
    active_replays: HashMap<i64, ActiveReplay>,
    lingering_replay_publications: Vec<LingeringReplayPublication>,
    responses: VecDeque<QueuedResponse>,
    observed_requests: HashSet<(u16, i64, i64)>,
}

impl ArchiveService {
    pub fn launch(context: ArchiveContext) -> Result<Self, ArchiveServiceError> {
        context.validate()?;
        let mut client = connect_client(&context.aeron_directory, context.client_id)?;
        let cnc = CncFile::map_existing(context.aeron_directory.join(CNC_FILE))?;
        let control_subscription_id =
            client.add_subscription(&context.control_channel, context.control_stream_id)?;
        let runtime = ArchiveRuntime::open(&context.archive_directory)?;
        let mark_file = create_mark_file(&context)?;
        Ok(Self {
            context,
            mark_file,
            cnc,
            client,
            runtime,
            control_subscription_id,
            next_control_session_id: 1,
            next_replay_session_id: 1,
            sessions: HashMap::new(),
            pending_connects: Vec::new(),
            pending_starts: Vec::new(),
            pending_replays: Vec::new(),
            active_recordings: HashMap::new(),
            active_replays: HashMap::new(),
            lingering_replay_publications: Vec::new(),
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
        self.mark_file
            .update_activity_timestamp(current_time_millis());
        let mut work = self.client.do_work()?;
        work += self.poll_control_requests()?;
        work += self.finish_connects()?;
        work += self.finish_starts()?;
        work += self.poll_recordings()?;
        work += self.finish_replays()?;
        work += self.flush_responses()?;
        work += self.poll_replays()?;
        work += self.poll_replay_linger()?;
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
                self.start_recording(request, false)?;
            }
            ControlRequest::StartRecordingV2(request) => {
                let auto_stop = request.auto_stop;
                self.start_recording(
                    StartRecordingRequest {
                        control_session_id: request.control_session_id,
                        correlation_id: request.correlation_id,
                        stream_id: request.stream_id,
                        source_location: request.source_location,
                        channel: request.channel,
                    },
                    auto_stop,
                )?;
            }
            ControlRequest::ExtendRecording(request) => {
                self.extend_recording(request, false)?;
            }
            ControlRequest::ExtendRecordingV2(request) => {
                let auto_stop = request.auto_stop;
                self.extend_recording(
                    ExtendRecordingRequest {
                        control_session_id: request.control_session_id,
                        correlation_id: request.correlation_id,
                        recording_id: request.recording_id,
                        stream_id: request.stream_id,
                        source_location: request.source_location,
                        channel: request.channel,
                    },
                    auto_stop,
                )?;
            }
            ControlRequest::TruncateRecording(request) => {
                let response = if self
                    .active_replays
                    .values()
                    .any(|replay| replay.cursor.recording_id() == request.recording_id)
                {
                    error(
                        request.control_session_id,
                        request.correlation_id,
                        ControlResponseCode::Error,
                        "cannot truncate recording with active replay",
                    )
                } else {
                    mutation_response(
                        request.control_session_id,
                        request.correlation_id,
                        request.recording_id,
                        self.runtime
                            .truncate_recording(request.recording_id, request.position),
                    )
                };
                self.queue_session_response(request.control_session_id, response)?;
            }
            ControlRequest::PurgeRecording(request) => {
                let response = if self
                    .active_replays
                    .values()
                    .any(|replay| replay.cursor.recording_id() == request.recording_id)
                {
                    error(
                        request.control_session_id,
                        request.correlation_id,
                        ControlResponseCode::Error,
                        "cannot purge recording with active replay",
                    )
                } else {
                    mutation_response(
                        request.control_session_id,
                        request.correlation_id,
                        request.recording_id,
                        self.runtime.purge_recording(request.recording_id),
                    )
                };
                self.queue_session_response(request.control_session_id, response)?;
            }
            ControlRequest::StopRecording(request) => {
                let key = (request.channel, request.stream_id);
                let response = if self.stop_recording_by_key(&key)? {
                    ok(request.control_session_id, request.correlation_id, 0)
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
            ControlRequest::StopRecordingSubscription(request) => {
                let response = if self.stop_recording_subscription(request.subscription_id)? {
                    ok(request.control_session_id, request.correlation_id, 0)
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
                    position: request.position,
                    length: request.length,
                    limit_counter_id: None,
                });
            }
            ControlRequest::BoundedReplay(request) => {
                if !self.sessions.contains_key(&request.control_session_id) {
                    return Ok(());
                }
                if self.counter_value(request.limit_counter_id).is_none() {
                    self.queue_session_response(
                        request.control_session_id,
                        error(
                            request.control_session_id,
                            request.correlation_id,
                            ControlResponseCode::Error,
                            "replay limit counter is unavailable",
                        ),
                    )?;
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
                    position: request.position,
                    length: request.length,
                    limit_counter_id: Some(request.limit_counter_id),
                });
            }
            ControlRequest::Replicate(request) => {
                self.queue_session_response(
                    request.control_session_id,
                    error(
                        request.control_session_id,
                        request.correlation_id,
                        ControlResponseCode::Error,
                        "replication is not implemented",
                    ),
                )?;
            }
            ControlRequest::ReplayToken(request) => {
                self.queue_session_response(
                    request.control_session_id,
                    error(
                        request.control_session_id,
                        request.correlation_id,
                        ControlResponseCode::Error,
                        "replay tokens are not implemented",
                    ),
                )?;
            }
            ControlRequest::StopReplay(request) => {
                if let Some(active) = self.active_replays.remove(&request.replay_session_id) {
                    let _ = self.client.remove_publication(active.publication_id)?;
                }
                self.queue_session_response(
                    request.control_session_id,
                    ok(request.control_session_id, request.correlation_id, 0),
                )?;
            }
            ControlRequest::ListRecordings(request) => {
                let descriptors = self
                    .runtime
                    .catalog()
                    .recordings()
                    .iter()
                    .filter(|descriptor| descriptor.recording_id >= request.from_recording_id)
                    .take(request.record_count.max(0) as usize)
                    .cloned()
                    .collect::<Vec<_>>();
                self.queue_descriptors(
                    request.control_session_id,
                    request.correlation_id,
                    &descriptors,
                )?;
                if descriptors.len() < request.record_count.max(0) as usize {
                    let next_recording_id = self
                        .runtime
                        .catalog()
                        .recordings()
                        .last()
                        .map_or(request.from_recording_id, |descriptor| {
                            descriptor.recording_id + 1
                        });
                    self.queue_session_response(
                        request.control_session_id,
                        recording_unknown(
                            request.control_session_id,
                            request.correlation_id,
                            next_recording_id,
                        ),
                    )?;
                }
            }
            ControlRequest::ListRecordingsForUri(request) => {
                let descriptors = self
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
                    .cloned()
                    .collect::<Vec<_>>();
                self.queue_descriptors(
                    request.control_session_id,
                    request.correlation_id,
                    &descriptors,
                )?;
                if descriptors.len() < request.record_count.max(0) as usize {
                    let next_recording_id = self
                        .runtime
                        .catalog()
                        .recordings()
                        .last()
                        .map_or(request.from_recording_id, |descriptor| {
                            descriptor.recording_id + 1
                        });
                    self.queue_session_response(
                        request.control_session_id,
                        recording_unknown(
                            request.control_session_id,
                            request.correlation_id,
                            next_recording_id,
                        ),
                    )?;
                }
            }
            ControlRequest::ListRecording(request) => {
                let descriptor = self
                    .runtime
                    .catalog()
                    .get_recording(request.recording_id)
                    .cloned();
                if let Some(descriptor) = descriptor {
                    self.queue_descriptor(
                        request.control_session_id,
                        request.correlation_id,
                        &descriptor,
                    )?;
                } else {
                    self.queue_session_response(
                        request.control_session_id,
                        recording_unknown(
                            request.control_session_id,
                            request.correlation_id,
                            request.recording_id,
                        ),
                    )?;
                }
            }
            ControlRequest::ListRecordingSubscriptions(request) => {
                self.list_recording_subscriptions(request)?;
            }
            ControlRequest::StartPosition(request) => {
                let response = self
                    .runtime
                    .start_position(request.recording_id)
                    .map_or_else(
                        || {
                            recording_unknown(
                                request.control_session_id,
                                request.correlation_id,
                                request.recording_id,
                            )
                        },
                        |position| ok(request.control_session_id, request.correlation_id, position),
                    );
                self.queue_session_response(request.control_session_id, response)?;
            }
            ControlRequest::RecordingPosition(request) => {
                let response = self
                    .runtime
                    .recording_position(request.recording_id)
                    .map_or_else(
                        || {
                            recording_unknown(
                                request.control_session_id,
                                request.correlation_id,
                                request.recording_id,
                            )
                        },
                        |position| ok(request.control_session_id, request.correlation_id, position),
                    );
                self.queue_session_response(request.control_session_id, response)?;
            }
            ControlRequest::StopPosition(request) => {
                let response = self
                    .runtime
                    .stop_position(request.recording_id)
                    .map_or_else(
                        || {
                            recording_unknown(
                                request.control_session_id,
                                request.correlation_id,
                                request.recording_id,
                            )
                        },
                        |position| ok(request.control_session_id, request.correlation_id, position),
                    );
                self.queue_session_response(request.control_session_id, response)?;
            }
            ControlRequest::MaxRecordedPosition(request) => {
                let response = self
                    .runtime
                    .max_recorded_position(request.recording_id)
                    .map_or_else(
                        || {
                            recording_unknown(
                                request.control_session_id,
                                request.correlation_id,
                                request.recording_id,
                            )
                        },
                        |position| ok(request.control_session_id, request.correlation_id, position),
                    );
                self.queue_session_response(request.control_session_id, response)?;
            }
            ControlRequest::FindLastMatchingRecording(request) => {
                let response = if request.min_recording_id < 0 {
                    error(
                        request.control_session_id,
                        request.correlation_id,
                        ControlResponseCode::RecordingUnknown,
                        "min recording id must be non-negative",
                    )
                } else {
                    ok(
                        request.control_session_id,
                        request.correlation_id,
                        self.runtime
                            .find_last_matching_recording(
                                request.min_recording_id,
                                request.session_id,
                                request.stream_id,
                                &request.channel,
                            )
                            .unwrap_or(-1),
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

    fn start_recording(
        &mut self,
        request: StartRecordingRequest,
        auto_stop: bool,
    ) -> Result<(), ArchiveServiceError> {
        if !self.sessions.contains_key(&request.control_session_id) {
            return Ok(());
        }
        let key = (request.channel.clone(), request.stream_id);
        if self.active_recordings.contains_key(&key)
            || self.pending_starts.iter().any(|pending| {
                pending.request.channel == request.channel
                    && pending.request.stream_id == request.stream_id
            })
        {
            self.queue_session_response(
                request.control_session_id,
                error(
                    request.control_session_id,
                    request.correlation_id,
                    ControlResponseCode::Error,
                    "recording subscription is already active",
                ),
            )?;
            return Ok(());
        }
        let registration_id = self
            .client
            .add_subscription(&request.channel, request.stream_id)?;
        let control_session_id = request.control_session_id;
        let correlation_id = request.correlation_id;
        self.pending_starts.push(PendingStart {
            registration_id,
            request,
            recording_id: None,
            auto_stop,
        });
        self.queue_session_response(
            control_session_id,
            ok(control_session_id, correlation_id, registration_id),
        )
    }

    fn extend_recording(
        &mut self,
        request: ExtendRecordingRequest,
        auto_stop: bool,
    ) -> Result<(), ArchiveServiceError> {
        if !self.sessions.contains_key(&request.control_session_id) {
            return Ok(());
        }
        let descriptor = self.runtime.catalog().get_recording(request.recording_id);
        if descriptor.is_none_or(|descriptor| descriptor.stream_id != request.stream_id) {
            self.queue_session_response(
                request.control_session_id,
                recording_unknown(
                    request.control_session_id,
                    request.correlation_id,
                    request.recording_id,
                ),
            )?;
            return Ok(());
        }
        if self
            .runtime
            .recording_position(request.recording_id)
            .is_some_and(|position| position != crate::runtime::NULL_POSITION)
        {
            self.queue_session_response(
                request.control_session_id,
                error(
                    request.control_session_id,
                    request.correlation_id,
                    ControlResponseCode::Error,
                    "cannot extend active recording",
                ),
            )?;
            return Ok(());
        }
        let key = (request.channel.clone(), request.stream_id);
        if self.active_recordings.contains_key(&key)
            || self.pending_starts.iter().any(|pending| {
                pending.request.channel == request.channel
                    && pending.request.stream_id == request.stream_id
            })
        {
            self.queue_session_response(
                request.control_session_id,
                error(
                    request.control_session_id,
                    request.correlation_id,
                    ControlResponseCode::Error,
                    "recording subscription is already active",
                ),
            )?;
            return Ok(());
        }
        let registration_id = self
            .client
            .add_subscription(&request.channel, request.stream_id)?;
        self.pending_starts.push(PendingStart {
            registration_id,
            request: StartRecordingRequest {
                control_session_id: request.control_session_id,
                correlation_id: request.correlation_id,
                stream_id: request.stream_id,
                source_location: request.source_location,
                channel: request.channel,
            },
            recording_id: Some(request.recording_id),
            auto_stop,
        });
        self.queue_session_response(
            request.control_session_id,
            ok(
                request.control_session_id,
                request.correlation_id,
                registration_id,
            ),
        )
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
            let image_correlations = self
                .client
                .subscription(pending.registration_id)
                .ok_or_else(|| ArchiveServiceError::InvalidContext("subscription vanished".into()))?
                .images()
                .iter()
                .map(|image| image.correlation_id())
                .collect::<Vec<_>>();
            let image_correlations = if pending.recording_id.is_some() {
                image_correlations.into_iter().take(1).collect()
            } else {
                image_correlations
            };
            let mut recordings = HashMap::new();
            for image_correlation_id in image_correlations {
                let result: Result<i64, ArchiveServiceError> = {
                    let subscription = self
                        .client
                        .subscription(pending.registration_id)
                        .ok_or_else(|| {
                            ArchiveServiceError::InvalidContext("subscription vanished".into())
                        })?;
                    if let Some(recording_id) = pending.recording_id {
                        self.runtime.extend_recording(
                            recording_id,
                            pending.request.control_session_id,
                            pending.request.correlation_id,
                            subscription,
                            image_correlation_id,
                        )?;
                        Ok(recording_id)
                    } else {
                        Ok(self
                            .runtime
                            .start_recording(subscription, image_correlation_id)?)
                    }
                };
                match result {
                    Ok(recording_id) => {
                        recordings.insert(image_correlation_id, recording_id);
                    }
                    Err(runtime_error) => {
                        for recording_id in recordings.values().copied() {
                            let _ = self.runtime.stop_recording(recording_id);
                        }
                        let _ = self.client.remove_subscription(pending.registration_id)?;
                        self.queue_session_response(
                            pending.request.control_session_id,
                            error(
                                pending.request.control_session_id,
                                pending.request.correlation_id,
                                ControlResponseCode::Error,
                                &runtime_error.to_string(),
                            ),
                        )?;
                        recordings.clear();
                        break;
                    }
                }
            }
            if recordings.is_empty() {
                continue;
            }
            self.active_recordings.insert(
                (pending.request.channel, pending.request.stream_id),
                ActiveRecording {
                    subscription_id: pending.registration_id,
                    subscription_ref_count: recordings.len() + 1,
                    recordings,
                    auto_stop: pending.auto_stop,
                    follow_images: pending.recording_id.is_none(),
                },
            );
        }
        Ok(count)
    }

    fn stop_recording_by_key(&mut self, key: &(String, i32)) -> Result<bool, ArchiveServiceError> {
        if let Some(index) = self.pending_starts.iter().position(|pending| {
            pending.request.channel == key.0 && pending.request.stream_id == key.1
        }) {
            let pending = self.pending_starts.swap_remove(index);
            let _ = self.client.remove_subscription(pending.registration_id)?;
            return Ok(true);
        }
        if let Some(active) = self.active_recordings.remove(key) {
            for recording_id in active.recordings.values().copied() {
                self.runtime.stop_recording(recording_id)?;
            }
            let _ = self.client.remove_subscription(active.subscription_id)?;
            return Ok(true);
        }
        Ok(false)
    }

    fn stop_recording_subscription(
        &mut self,
        subscription_id: i64,
    ) -> Result<bool, ArchiveServiceError> {
        if let Some(index) = self
            .pending_starts
            .iter()
            .position(|pending| pending.registration_id == subscription_id)
        {
            let pending = self.pending_starts.swap_remove(index);
            let _ = self.client.remove_subscription(pending.registration_id)?;
            return Ok(true);
        }
        let key = self.active_recordings.iter().find_map(|(key, active)| {
            (active.subscription_id == subscription_id).then(|| key.clone())
        });
        if let Some(key) = key {
            return self.stop_recording_by_key(&key);
        }
        Ok(false)
    }

    fn poll_recordings(&mut self) -> Result<usize, ArchiveServiceError> {
        let active = self
            .active_recordings
            .iter()
            .map(|(key, active)| (key.clone(), active.subscription_id))
            .collect::<Vec<_>>();
        let mut work = 0;
        for (key, subscription_id) in &active {
            let Some((follow_images, known_images)) =
                self.active_recordings.get(key).map(|recording| {
                    (
                        recording.follow_images,
                        recording.recordings.keys().copied().collect::<HashSet<_>>(),
                    )
                })
            else {
                continue;
            };
            if follow_images {
                let image_correlations = self
                    .client
                    .subscription(*subscription_id)
                    .map(|subscription| {
                        subscription
                            .images()
                            .iter()
                            .map(|image| image.correlation_id())
                            .filter(|image_correlation_id| {
                                !known_images.contains(image_correlation_id)
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                for image_correlation_id in image_correlations {
                    let recording_id = {
                        let subscription =
                            self.client.subscription(*subscription_id).ok_or_else(|| {
                                ArchiveServiceError::InvalidContext("subscription vanished".into())
                            })?;
                        self.runtime
                            .start_recording(subscription, image_correlation_id)?
                    };
                    if let Some(active) = self.active_recordings.get_mut(key) {
                        active.recordings.insert(image_correlation_id, recording_id);
                        active.subscription_ref_count += 1;
                    }
                }
            }
        }

        let recordings = self
            .active_recordings
            .iter()
            .flat_map(|(key, active)| {
                active
                    .recordings
                    .iter()
                    .map(|(image_correlation_id, recording_id)| {
                        (
                            key.clone(),
                            active.subscription_id,
                            *image_correlation_id,
                            *recording_id,
                            active.auto_stop,
                        )
                    })
            })
            .collect::<Vec<_>>();
        let mut stopped_sessions = Vec::new();
        for (key, subscription_id, image_correlation_id, recording_id, auto_stop) in recordings {
            if let Some(subscription) = self.client.subscription_mut(subscription_id) {
                work += self.runtime.poll_recording(
                    recording_id,
                    subscription,
                    image_correlation_id,
                    20,
                )?;
                if subscription
                    .image_by_correlation_id(image_correlation_id)
                    .is_none_or(|image| image.is_end_of_stream() || image.is_closed())
                {
                    stopped_sessions.push((key, image_correlation_id, recording_id, auto_stop));
                }
            }
        }
        let mut stopped_keys = HashSet::new();
        for (key, image_correlation_id, recording_id, auto_stop) in stopped_sessions {
            self.runtime.stop_recording(recording_id)?;
            if let Some(active) = self.active_recordings.get_mut(&key) {
                active.recordings.remove(&image_correlation_id);
                active.subscription_ref_count = active.subscription_ref_count.saturating_sub(1);
                if auto_stop {
                    let all_images_stopped = self
                        .client
                        .subscription(active.subscription_id)
                        .is_none_or(|subscription| {
                            subscription.images().is_empty()
                                || subscription
                                    .images()
                                    .iter()
                                    .all(|image| image.is_end_of_stream() || image.is_closed())
                        });
                    if all_images_stopped && active.subscription_ref_count == 1 {
                        stopped_keys.insert(key.clone());
                    }
                }
            }
        }
        for key in stopped_keys {
            work += usize::from(self.stop_recording_by_key(&key)?);
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
            if let Some(limit_counter_id) = pending.limit_counter_id {
                if self.counter_value(limit_counter_id).is_none() {
                    self.queue_session_response(
                        pending.control_session_id,
                        error(
                            pending.control_session_id,
                            pending.correlation_id,
                            ControlResponseCode::Error,
                            "replay limit counter is unavailable",
                        ),
                    )?;
                    continue;
                }
            }
            let result =
                self.runtime
                    .start_replay(pending.recording_id, pending.position, pending.length);
            let response = match result {
                Ok(mut cursor) => {
                    if let Some(limit_counter_id) = pending.limit_counter_id {
                        if let Some(limit) = self.counter_value(limit_counter_id) {
                            cursor.set_counter_limit(limit);
                        }
                    }
                    let replay_session_id = self.next_replay_session_id;
                    self.next_replay_session_id += 1;
                    self.active_replays.insert(
                        replay_session_id,
                        ActiveReplay {
                            publication_id: pending.publication_id,
                            cursor,
                            limit_counter_id: pending.limit_counter_id,
                        },
                    );
                    ok(
                        pending.control_session_id,
                        pending.correlation_id,
                        replay_session_id,
                    )
                }
                Err(ArchiveRuntimeError::UnknownRecording(_)) => error(
                    pending.control_session_id,
                    pending.correlation_id,
                    ControlResponseCode::RecordingUnknown,
                    "recording unknown",
                ),
                Err(runtime_error) => error(
                    pending.control_session_id,
                    pending.correlation_id,
                    ControlResponseCode::Error,
                    &runtime_error.to_string(),
                ),
            };
            self.queue_session_response(pending.control_session_id, response)?;
        }
        Ok(count)
    }

    fn poll_replays(&mut self) -> Result<usize, ArchiveServiceError> {
        let replay_session_ids = self.active_replays.keys().copied().collect::<Vec<_>>();
        let mut work = 0;
        let mut completed = Vec::new();
        for replay_session_id in replay_session_ids {
            let limit = self
                .active_replays
                .get(&replay_session_id)
                .and_then(|active| active.limit_counter_id)
                .and_then(|counter_id| self.counter_value(counter_id));
            let Some(active) = self.active_replays.get_mut(&replay_session_id) else {
                continue;
            };
            let Some(publication) = self.client.publication(active.publication_id) else {
                completed.push(replay_session_id);
                continue;
            };
            self.runtime.refresh_replay_limit(&mut active.cursor);
            if let Some(limit) = limit {
                active.cursor.refresh_counter_limit(limit);
            }
            match active.cursor.poll(publication, 1) {
                Ok(count) => work += count,
                Err(ArchiveRuntimeError::ReplayOffer { .. }) => {
                    completed.push(replay_session_id);
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
            if active.cursor.is_complete() {
                completed.push(replay_session_id);
            }
        }
        for replay_session_id in completed {
            if let Some(active) = self.active_replays.remove(&replay_session_id) {
                self.lingering_replay_publications
                    .push(LingeringReplayPublication {
                        publication_id: active.publication_id,
                        deadline: Instant::now() + REPLAY_LINGER_DURATION,
                    });
            }
        }
        Ok(work)
    }

    fn poll_replay_linger(&mut self) -> Result<usize, ArchiveServiceError> {
        let now = Instant::now();
        let expired = self
            .lingering_replay_publications
            .iter()
            .filter(|publication| publication.deadline <= now)
            .map(|publication| publication.publication_id)
            .collect::<Vec<_>>();
        self.lingering_replay_publications
            .retain(|publication| publication.deadline > now);
        for publication_id in &expired {
            let _ = self.client.remove_publication(*publication_id)?;
        }
        Ok(expired.len())
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

    fn counter_value(&self, counter_id: i32) -> Option<i64> {
        if counter_id < 0 {
            return None;
        }
        let counter_id = counter_id as usize;
        let metadata_offset = counter_id.checked_mul(COUNTER_METADATA_LENGTH)?;
        let value_offset = counter_id.checked_mul(COUNTER_VALUE_LENGTH)?;
        if metadata_offset + 4 > self.cnc.counters_metadata_length()
            || value_offset + 8 > self.cnc.counters_values_length()
        {
            return None;
        }
        let metadata = self.cnc.counters_metadata_buffer();
        if metadata.get_i32_volatile(metadata_offset) != COUNTER_RECORD_ALLOCATED {
            return None;
        }
        Some(
            self.cnc
                .counters_values_buffer()
                .get_i64_volatile(value_offset),
        )
    }

    fn queue_descriptors(
        &mut self,
        control_session_id: i64,
        correlation_id: i64,
        descriptors: &[RecordingDescriptor],
    ) -> Result<(), ArchiveServiceError> {
        for descriptor in descriptors {
            self.queue_descriptor(control_session_id, correlation_id, descriptor)?;
        }
        Ok(())
    }

    fn list_recording_subscriptions(
        &mut self,
        request: ListRecordingSubscriptionsRequest,
    ) -> Result<(), ArchiveServiceError> {
        let mut subscriptions = self
            .pending_starts
            .iter()
            .map(|pending| {
                (
                    pending.registration_id,
                    pending.request.stream_id,
                    pending.request.channel.clone(),
                )
            })
            .chain(
                self.active_recordings
                    .iter()
                    .map(|(key, active)| (active.subscription_id, key.1, key.0.clone())),
            )
            .collect::<Vec<_>>();
        subscriptions.sort_by_key(|(subscription_id, _, _)| *subscription_id);
        if request.pseudo_index < 0
            || request.pseudo_index as usize >= subscriptions.len()
            || request.subscription_count <= 0
        {
            self.queue_session_response(
                request.control_session_id,
                error(
                    request.control_session_id,
                    request.correlation_id,
                    ControlResponseCode::SubscriptionUnknown,
                    "recording subscription unknown",
                ),
            )?;
            return Ok(());
        }

        let descriptors = subscriptions
            .into_iter()
            .skip(request.pseudo_index as usize)
            .filter(|(_, stream_id, channel)| {
                (!request.apply_stream_id || *stream_id == request.stream_id)
                    && channel.contains(&request.channel)
            })
            .take(request.subscription_count as usize)
            .map(
                |(subscription_id, stream_id, stripped_channel)| RecordingSubscriptionDescriptor {
                    control_session_id: request.control_session_id,
                    correlation_id: request.correlation_id,
                    subscription_id,
                    stream_id,
                    stripped_channel,
                },
            )
            .collect::<Vec<_>>();
        for descriptor in &descriptors {
            self.queue_recording_subscription_descriptor(descriptor)?;
        }
        if descriptors.len() < request.subscription_count as usize {
            self.queue_session_response(
                request.control_session_id,
                error(
                    request.control_session_id,
                    request.correlation_id,
                    ControlResponseCode::SubscriptionUnknown,
                    "recording subscription unknown",
                ),
            )?;
        }
        Ok(())
    }

    fn queue_recording_subscription_descriptor(
        &mut self,
        descriptor: &RecordingSubscriptionDescriptor,
    ) -> Result<(), ArchiveServiceError> {
        if let Some(&publication_id) = self.sessions.get(&descriptor.control_session_id) {
            self.responses.push_back(QueuedResponse {
                publication_id,
                bytes: descriptor.encode().map_err(ControlAdapterError::from)?,
            });
        }
        Ok(())
    }

    fn queue_descriptor(
        &mut self,
        control_session_id: i64,
        correlation_id: i64,
        descriptor: &RecordingDescriptor,
    ) -> Result<(), ArchiveServiceError> {
        if let Some(&publication_id) = self.sessions.get(&control_session_id) {
            let mut descriptor = descriptor.clone();
            descriptor.control_session_id = control_session_id;
            descriptor.correlation_id = correlation_id;
            self.responses.push_back(QueuedResponse {
                publication_id,
                bytes: descriptor.encode().map_err(ControlAdapterError::from)?,
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
            service.mark_file.signal_terminated()?;
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

impl Drop for ArchiveService {
    fn drop(&mut self) {
        let _ = self.mark_file.signal_terminated();
    }
}

fn create_mark_file(context: &ArchiveContext) -> Result<ArchiveMarkFile, ArchiveServiceError> {
    let mark_directory = context
        .mark_file_directory
        .as_deref()
        .unwrap_or(&context.archive_directory);
    std::fs::create_dir_all(mark_directory)?;

    if mark_directory == context.archive_directory {
        let _ = std::fs::remove_file(context.archive_directory.join(LINK_FILENAME));
    } else {
        std::fs::create_dir_all(&context.archive_directory)?;
        std::fs::write(
            context.archive_directory.join(LINK_FILENAME),
            mark_directory.to_string_lossy().as_bytes(),
        )?;
    }

    let config = ArchiveMarkFileConfig {
        start_timestamp: current_time_millis(),
        control_stream_id: context.control_stream_id,
        archive_id: 0,
        control_channel: context.control_channel.clone(),
        aeron_directory: context.aeron_directory.to_string_lossy().into_owned(),
        ..ArchiveMarkFileConfig::default()
    };
    let mut mark_file = ArchiveMarkFile::create(mark_directory.join(FILENAME), &config)?;
    mark_file.signal_ready(config.start_timestamp)?;
    Ok(mark_file)
}

fn current_time_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(i64::MAX as u128) as i64
        })
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

fn recording_unknown(
    control_session_id: i64,
    correlation_id: i64,
    recording_id: i64,
) -> ControlResponse {
    ControlResponse {
        control_session_id,
        correlation_id,
        relevant_id: recording_id,
        code: ControlResponseCode::RecordingUnknown,
        version: PROTOCOL_SEMANTIC_VERSION,
        error_message: "recording unknown".into(),
    }
}

fn mutation_response(
    control_session_id: i64,
    correlation_id: i64,
    recording_id: i64,
    result: Result<(), ArchiveRuntimeError>,
) -> ControlResponse {
    match result {
        Ok(()) => ok(control_session_id, correlation_id, 0),
        Err(ArchiveRuntimeError::UnknownRecording(_)) => {
            recording_unknown(control_session_id, correlation_id, recording_id)
        }
        Err(archive_error) => error(
            control_session_id,
            correlation_id,
            ControlResponseCode::Error,
            &archive_error.to_string(),
        ),
    }
}

fn request_key(request: &ControlRequest) -> (u16, i64, i64) {
    match request {
        ControlRequest::Connect(request) => (2, 0, request.correlation_id),
        ControlRequest::CloseSession(request) => (3, request.control_session_id, -1),
        ControlRequest::StartRecording(request) => {
            (4, request.control_session_id, request.correlation_id)
        }
        ControlRequest::StartRecordingV2(request) => {
            (63, request.control_session_id, request.correlation_id)
        }
        ControlRequest::ExtendRecording(request) => {
            (11, request.control_session_id, request.correlation_id)
        }
        ControlRequest::ExtendRecordingV2(request) => {
            (64, request.control_session_id, request.correlation_id)
        }
        ControlRequest::TruncateRecording(request) => {
            (13, request.control_session_id, request.correlation_id)
        }
        ControlRequest::PurgeRecording(request) => {
            (104, request.control_session_id, request.correlation_id)
        }
        ControlRequest::StopRecording(request) => {
            (5, request.control_session_id, request.correlation_id)
        }
        ControlRequest::StopRecordingSubscription(request) => {
            (14, request.control_session_id, request.correlation_id)
        }
        ControlRequest::Replay(request) => (6, request.control_session_id, request.correlation_id),
        ControlRequest::BoundedReplay(request) => {
            (18, request.control_session_id, request.correlation_id)
        }
        ControlRequest::Replicate(request) => {
            (66, request.control_session_id, request.correlation_id)
        }
        ControlRequest::ReplayToken(request) => {
            (105, request.control_session_id, request.correlation_id)
        }
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
        ControlRequest::ListRecordingSubscriptions(request) => {
            (17, request.control_session_id, request.correlation_id)
        }
        ControlRequest::StartPosition(request) => {
            (52, request.control_session_id, request.correlation_id)
        }
        ControlRequest::RecordingPosition(request) => {
            (12, request.control_session_id, request.correlation_id)
        }
        ControlRequest::StopPosition(request) => {
            (15, request.control_session_id, request.correlation_id)
        }
        ControlRequest::MaxRecordedPosition(request) => {
            (67, request.control_session_id, request.correlation_id)
        }
        ControlRequest::FindLastMatchingRecording(request) => {
            (16, request.control_session_id, request.correlation_id)
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

    #[test]
    fn context_uses_managed_service_defaults() {
        let context = ArchiveContext::new("/tmp/aeron", "/tmp/archive");

        assert_eq!(context.control_channel, DEFAULT_CONTROL_CHANNEL);
        assert_eq!(context.control_stream_id, DEFAULT_CONTROL_STREAM_ID);
        assert_eq!(context.client_id, DEFAULT_ARCHIVE_CLIENT_ID);
        assert_eq!(context.idle_duration, Duration::from_millis(1));
        assert!(context.validate().is_ok());
    }

    #[test]
    fn context_rejects_an_empty_control_channel() {
        let mut context = ArchiveContext::new("/tmp/aeron", "/tmp/archive");
        context.control_channel.clear();

        assert!(matches!(
            context.validate(),
            Err(ArchiveServiceError::InvalidContext(message))
                if message == "control channel must not be empty"
        ));
    }

    #[test]
    fn context_creates_a_link_for_a_separate_mark_file_directory() {
        let root = std::env::temp_dir().join(format!(
            "aeron-archive-mark-link-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let archive_directory = root.join("archive");
        let mark_file_directory = root.join("mark");
        let context = ArchiveContext::new("/tmp/aeron", &archive_directory)
            .mark_file_directory(&mark_file_directory);

        let mark_file = create_mark_file(&context).expect("create separate mark file");
        assert_eq!(mark_file.path(), mark_file_directory.join(FILENAME));
        assert_eq!(
            std::fs::read_to_string(archive_directory.join(LINK_FILENAME))
                .expect("read mark-file link"),
            mark_file_directory.to_string_lossy()
        );
        drop(mark_file);
        let _ = std::fs::remove_dir_all(root);
    }
}
