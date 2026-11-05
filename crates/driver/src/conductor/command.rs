use agrona::AtomicBuffer;

use aeron_client::command::{
    CounterMessageFlyweight, DestinationMessageFlyweight, PublicationMessageFlyweight,
    RemoveMessageFlyweight, SubscriptionMessageFlyweight, TerminateDriverFlyweight,
};
use aeron_client::driver_proxy::{
    ADD_COUNTER, ADD_DESTINATION, ADD_EXCLUSIVE_PUBLICATION, ADD_PUBLICATION, ADD_RCV_DESTINATION,
    ADD_SUBSCRIPTION, CLIENT_CLOSE, CLIENT_KEEPALIVE, GET_NEXT_AVAILABLE_SESSION_ID,
    REMOVE_COUNTER, REMOVE_DESTINATION, REMOVE_PUBLICATION, REMOVE_RCV_DESTINATION,
    REMOVE_SUBSCRIPTION, TERMINATE_DRIVER,
};

use crate::error::{DriverError, Result};
use crate::media::control_mode::ControlMode;
use crate::media::udp_channel::UdpChannel;
use crate::publication_params::RESPONSE_CORRELATION_ID_PARAM_NAME;

/// A decoded client command, extracted from a ring-buffer record so it can be processed
/// after the (immutable) ring-buffer read borrow is released.
#[derive(Debug)]
pub enum Command {
    AddPublication {
        client_id: i64,
        correlation_id: i64,
        stream_id: i32,
        channel: String,
        exclusive: bool,
    },
    AddSubscription {
        client_id: i64,
        correlation_id: i64,
        stream_id: i32,
        channel: String,
    },
    AddCounter {
        client_id: i64,
        correlation_id: i64,
        type_id: i32,
        key: Vec<u8>,
        label: String,
    },
    RemovePublication {
        correlation_id: i64,
        registration_id: i64,
    },
    RemoveSubscription {
        correlation_id: i64,
        registration_id: i64,
    },
    RemoveCounter {
        correlation_id: i64,
        registration_id: i64,
    },
    GetNextAvailableSessionId {
        client_id: i64,
        correlation_id: i64,
        stream_id: i32,
    },
    Keepalive {
        client_id: i64,
    },
    ClientClose {
        client_id: i64,
    },
    TerminateDriver {
        client_id: i64,
        correlation_id: i64,
    },
    AddDestination {
        client_id: i64,
        correlation_id: i64,
        registration_id: i64,
        channel: String,
    },
    RemoveDestination {
        client_id: i64,
        correlation_id: i64,
        registration_id: i64,
        channel: String,
    },
    AddReceiveDestination {
        client_id: i64,
        correlation_id: i64,
        registration_id: i64,
        channel: String,
    },
    RemoveReceiveDestination {
        client_id: i64,
        correlation_id: i64,
        registration_id: i64,
        channel: String,
    },
    Unknown {
        correlation_id: i64,
        message_type: i32,
    },
}

impl Command {
    pub fn correlation_id(&self) -> Option<i64> {
        match self {
            Self::AddPublication { correlation_id, .. }
            | Self::AddSubscription { correlation_id, .. }
            | Self::AddCounter { correlation_id, .. }
            | Self::RemovePublication { correlation_id, .. }
            | Self::RemoveSubscription { correlation_id, .. }
            | Self::RemoveCounter { correlation_id, .. }
            | Self::GetNextAvailableSessionId { correlation_id, .. }
            | Self::TerminateDriver { correlation_id, .. }
            | Self::AddDestination { correlation_id, .. }
            | Self::RemoveDestination { correlation_id, .. }
            | Self::AddReceiveDestination { correlation_id, .. }
            | Self::RemoveReceiveDestination { correlation_id, .. }
            | Self::Unknown { correlation_id, .. } => Some(*correlation_id),
            Self::Keepalive { .. } | Self::ClientClose { .. } => None,
        }
    }
}

pub fn decode_command(
    msg_type: i32,
    buffer: &AtomicBuffer,
    offset: usize,
    length: usize,
) -> Option<Command> {
    const CORRELATED_HEADER: usize = aeron_client::command::CORRELATED_MESSAGE_HEADER_LENGTH;

    // Client commands arrive through a shared ring buffer. Treat their declared record length
    // as untrusted: the flyweights deliberately provide zero-copy access and therefore do not
    // perform bounds checks for every field read.
    let malformed = || {
        (length >= CORRELATED_HEADER).then(|| {
            let fw = aeron_client::command::CorrelatedMessageFlyweight::new(buffer.clone(), offset);
            Command::Unknown {
                correlation_id: fw.correlation_id(),
                message_type: msg_type,
            }
        })
    };

    match msg_type {
        ADD_PUBLICATION | ADD_EXCLUSIVE_PUBLICATION => {
            if length < aeron_client::command::PUB_CHANNEL_OFFSET {
                return malformed();
            }
            let channel_length =
                buffer.get_i32(offset + aeron_client::command::PUB_CHANNEL_LENGTH_OFFSET);
            if channel_length < 0
                || aeron_client::command::PUB_CHANNEL_OFFSET
                    .checked_add(channel_length as usize)
                    .is_none_or(|end| end > length)
            {
                return malformed();
            }
            let fw = PublicationMessageFlyweight::new(buffer.clone(), offset);
            Some(Command::AddPublication {
                client_id: fw.client_id(),
                correlation_id: fw.correlation_id(),
                stream_id: fw.stream_id(),
                channel: fw.channel(),
                exclusive: msg_type == ADD_EXCLUSIVE_PUBLICATION,
            })
        }
        ADD_SUBSCRIPTION => {
            if length < aeron_client::command::SUB_CHANNEL_OFFSET {
                return malformed();
            }
            let channel_length =
                buffer.get_i32(offset + aeron_client::command::SUB_CHANNEL_LENGTH_OFFSET);
            if channel_length < 0
                || aeron_client::command::SUB_CHANNEL_OFFSET
                    .checked_add(channel_length as usize)
                    .is_none_or(|end| end > length)
            {
                return malformed();
            }
            let fw = SubscriptionMessageFlyweight::new(buffer.clone(), offset);
            Some(Command::AddSubscription {
                client_id: fw.client_id(),
                correlation_id: fw.correlation_id(),
                stream_id: fw.stream_id(),
                channel: fw.channel(),
            })
        }
        ADD_COUNTER if length >= aeron_client::command::KEY_BUFFER_OFFSET => {
            let fw = CounterMessageFlyweight::new(buffer.clone(), offset);
            let key_length_raw = buffer.get_i32(offset + aeron_client::command::KEY_LENGTH_OFFSET);
            if key_length_raw < 0 {
                return malformed();
            }
            let key_length = key_length_raw as usize;
            let label_length_offset = key_length
                .checked_add(3)
                .map(|value| value & !3)
                .and_then(|value| aeron_client::command::KEY_BUFFER_OFFSET.checked_add(value));
            let Some(label_length_offset) = label_length_offset else {
                return malformed();
            };
            if label_length_offset
                .checked_add(4)
                .is_none_or(|end| end > length)
            {
                return malformed();
            }
            let label_length_raw = buffer.get_i32(offset + label_length_offset);
            if label_length_raw < 0 {
                return malformed();
            }
            let label_length = label_length_raw as usize;
            if label_length_offset
                .checked_add(4)
                .and_then(|end| end.checked_add(label_length))
                .is_none_or(|end| end > length)
            {
                return malformed();
            }
            let mut key = vec![0u8; key_length];
            buffer.get_bytes(offset + fw.key_buffer_offset(), &mut key);
            let mut label_bytes = vec![0u8; label_length];
            buffer.get_bytes(offset + label_length_offset + 4, &mut label_bytes);
            Some(Command::AddCounter {
                client_id: fw.client_id(),
                correlation_id: fw.correlation_id(),
                type_id: fw.type_id(),
                key,
                label: String::from_utf8_lossy(&label_bytes).into_owned(),
            })
        }
        REMOVE_PUBLICATION
            if length
                >= aeron_client::command::RemoveMessageFlyweight::remove_publication_length() =>
        {
            let fw = RemoveMessageFlyweight::new(buffer.clone(), offset);
            Some(Command::RemovePublication {
                correlation_id: fw.correlation_id(),
                registration_id: fw.registration_id(),
            })
        }
        REMOVE_SUBSCRIPTION | REMOVE_COUNTER
            if length >= aeron_client::command::RemoveMessageFlyweight::length() =>
        {
            let fw = RemoveMessageFlyweight::new(buffer.clone(), offset);
            if msg_type == REMOVE_SUBSCRIPTION {
                Some(Command::RemoveSubscription {
                    correlation_id: fw.correlation_id(),
                    registration_id: fw.registration_id(),
                })
            } else {
                Some(Command::RemoveCounter {
                    correlation_id: fw.correlation_id(),
                    registration_id: fw.registration_id(),
                })
            }
        }
        GET_NEXT_AVAILABLE_SESSION_ID
            if length >= aeron_client::command::CORRELATED_MESSAGE_HEADER_LENGTH + 4 =>
        {
            let fw = aeron_client::command::CorrelatedMessageFlyweight::new(buffer.clone(), offset);
            Some(Command::GetNextAvailableSessionId {
                client_id: fw.client_id(),
                correlation_id: fw.correlation_id(),
                stream_id: buffer
                    .get_i32(offset + aeron_client::command::CORRELATED_MESSAGE_HEADER_LENGTH),
            })
        }
        CLIENT_KEEPALIVE | CLIENT_CLOSE if length >= CORRELATED_HEADER => {
            let fw = aeron_client::command::CorrelatedMessageFlyweight::new(buffer.clone(), offset);
            if msg_type == CLIENT_KEEPALIVE {
                Some(Command::Keepalive {
                    client_id: fw.client_id(),
                })
            } else {
                Some(Command::ClientClose {
                    client_id: fw.client_id(),
                })
            }
        }
        TERMINATE_DRIVER if length >= CORRELATED_HEADER + 4 => {
            let token_length = buffer.get_i32(offset + CORRELATED_HEADER);
            if token_length < 0
                || CORRELATED_HEADER
                    .checked_add(4)
                    .and_then(|start| start.checked_add(token_length as usize))
                    .is_none_or(|end| end > length)
            {
                return malformed();
            }
            let fw = TerminateDriverFlyweight::new(buffer.clone(), offset);
            Some(Command::TerminateDriver {
                client_id: fw.client_id(),
                correlation_id: fw.correlation_id(),
            })
        }
        ADD_DESTINATION | REMOVE_DESTINATION
            if length >= aeron_client::command::DEST_CHANNEL_LENGTH_OFFSET + 4 =>
        {
            let channel_length =
                buffer.get_i32(offset + aeron_client::command::DEST_CHANNEL_LENGTH_OFFSET);
            let channel_offset = aeron_client::command::DEST_CHANNEL_OFFSET;
            if channel_length < 0
                || channel_offset
                    .checked_add(channel_length as usize)
                    .is_none_or(|end| end > length)
            {
                return malformed();
            }
            let fw = DestinationMessageFlyweight::new(buffer.clone(), offset);
            let command = if msg_type == ADD_DESTINATION {
                Command::AddDestination {
                    client_id: fw.client_id(),
                    correlation_id: fw.correlation_id(),
                    registration_id: fw.registration_correlation_id(),
                    channel: fw.channel(),
                }
            } else {
                Command::RemoveDestination {
                    client_id: fw.client_id(),
                    correlation_id: fw.correlation_id(),
                    registration_id: fw.registration_correlation_id(),
                    channel: fw.channel(),
                }
            };
            Some(command)
        }
        ADD_RCV_DESTINATION | REMOVE_RCV_DESTINATION
            if length >= aeron_client::command::DEST_CHANNEL_LENGTH_OFFSET + 4 =>
        {
            let channel_length =
                buffer.get_i32(offset + aeron_client::command::DEST_CHANNEL_LENGTH_OFFSET);
            let channel_offset = aeron_client::command::DEST_CHANNEL_OFFSET;
            if channel_length < 0
                || channel_offset
                    .checked_add(channel_length as usize)
                    .is_none_or(|end| end > length)
            {
                return malformed();
            }
            let fw = DestinationMessageFlyweight::new(buffer.clone(), offset);
            let command = if msg_type == ADD_RCV_DESTINATION {
                Command::AddReceiveDestination {
                    client_id: fw.client_id(),
                    correlation_id: fw.correlation_id(),
                    registration_id: fw.registration_correlation_id(),
                    channel: fw.channel(),
                }
            } else {
                Command::RemoveReceiveDestination {
                    client_id: fw.client_id(),
                    correlation_id: fw.correlation_id(),
                    registration_id: fw.registration_correlation_id(),
                    channel: fw.channel(),
                }
            };
            Some(command)
        }
        _ if length >= aeron_client::command::CORRELATED_MESSAGE_HEADER_LENGTH => {
            let fw = aeron_client::command::CorrelatedMessageFlyweight::new(buffer.clone(), offset);
            Some(Command::Unknown {
                correlation_id: fw.correlation_id(),
                message_type: msg_type,
            })
        }
        _ => None,
    }
}

pub fn parse_destination(channel: &str) -> Result<UdpChannel> {
    let udp_channel = UdpChannel::parse_with_resolver(
        channel,
        &crate::media::socket_address_parser::DefaultNameResolver,
        true,
    )?;
    if udp_channel.control_mode == ControlMode::Response {
        return Err(DriverError::InvalidChannel(
            "destinations may not specify control-mode=response".into(),
        ));
    }
    if udp_channel
        .channel_uri
        .get(RESPONSE_CORRELATION_ID_PARAM_NAME)
        .is_some()
    {
        return Err(DriverError::InvalidChannel(
            "destinations must not contain response-correlation-id".into(),
        ));
    }
    if !udp_channel.has_explicit_endpoint {
        return Err(DriverError::InvalidChannel(
            "destination must specify endpoint".into(),
        ));
    }
    Ok(udp_channel)
}

pub fn error_code(error: &DriverError) -> i32 {
    match error {
        DriverError::InvalidChannel(_) => 1,
        DriverError::IllegalState(message) | DriverError::IllegalArgument(message)
            if message.contains("No space left on device")
                || message.contains("There is not enough space on the disk") =>
        {
            12
        }
        DriverError::StorageSpace(_) => 12,
        DriverError::IllegalState(_) | DriverError::IllegalArgument(_) => 11,
    }
}
