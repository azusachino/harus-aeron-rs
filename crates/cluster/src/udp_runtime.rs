//! Minimal Tokio UDP runtime used for the issue #20 transport comparison.
//!
//! This is deliberately a comparison harness, not Aeron wire or Archive
//! compatibility. It exercises real process boundaries, UDP sockets, leader
//! heartbeats, quorum voting, and client load so the Rust side can be compared
//! with the sibling Zig host reproduction.

use std::collections::{HashMap, HashSet};
use std::env;
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::time::{self, MissedTickBehavior};

const MAGIC: u32 = 0x5255_5354;
const HEADER_SIZE: usize = 5;
const HEARTBEAT: u8 = 1;
const VOTE_REQUEST: u8 = 2;
const VOTE_RESPONSE: u8 = 3;
const LEADER_ANNOUNCEMENT: u8 = 4;
const ORDER: u8 = 5;
const ORDER_ACK: u8 = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WireMessage {
    Heartbeat {
        source: i32,
        term: i64,
        log_position: i64,
    },
    VoteRequest {
        source: i32,
        term: i64,
        log_position: i64,
    },
    VoteResponse {
        source: i32,
        term: i64,
        granted: bool,
    },
    LeaderAnnouncement {
        source: i32,
        term: i64,
        log_position: i64,
    },
    Order {
        sequence: u64,
    },
    OrderAck {
        source: i32,
        term: i64,
        sequence: u64,
        accepted: bool,
    },
}

impl WireMessage {
    fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(32);
        bytes.extend_from_slice(&MAGIC.to_le_bytes());
        match self {
            Self::Heartbeat {
                source,
                term,
                log_position,
            } => {
                bytes.push(HEARTBEAT);
                put_i32(&mut bytes, source);
                put_i64(&mut bytes, term);
                put_i64(&mut bytes, log_position);
            }
            Self::VoteRequest {
                source,
                term,
                log_position,
            } => {
                bytes.push(VOTE_REQUEST);
                put_i32(&mut bytes, source);
                put_i64(&mut bytes, term);
                put_i64(&mut bytes, log_position);
            }
            Self::VoteResponse {
                source,
                term,
                granted,
            } => {
                bytes.push(VOTE_RESPONSE);
                put_i32(&mut bytes, source);
                put_i64(&mut bytes, term);
                bytes.push(u8::from(granted));
            }
            Self::LeaderAnnouncement {
                source,
                term,
                log_position,
            } => {
                bytes.push(LEADER_ANNOUNCEMENT);
                put_i32(&mut bytes, source);
                put_i64(&mut bytes, term);
                put_i64(&mut bytes, log_position);
            }
            Self::Order { sequence } => {
                bytes.push(ORDER);
                put_u64(&mut bytes, sequence);
            }
            Self::OrderAck {
                source,
                term,
                sequence,
                accepted,
            } => {
                bytes.push(ORDER_ACK);
                put_i32(&mut bytes, source);
                put_i64(&mut bytes, term);
                put_u64(&mut bytes, sequence);
                bytes.push(u8::from(accepted));
            }
        }
        bytes
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < HEADER_SIZE || read_u32(bytes, 0)? != MAGIC {
            return Err(invalid_data("invalid comparison message"));
        }
        match bytes[4] {
            HEARTBEAT => Ok(Self::Heartbeat {
                source: read_i32(bytes, 5)?,
                term: read_i64(bytes, 9)?,
                log_position: read_i64(bytes, 17)?,
            }),
            VOTE_REQUEST => Ok(Self::VoteRequest {
                source: read_i32(bytes, 5)?,
                term: read_i64(bytes, 9)?,
                log_position: read_i64(bytes, 17)?,
            }),
            VOTE_RESPONSE => Ok(Self::VoteResponse {
                source: read_i32(bytes, 5)?,
                term: read_i64(bytes, 9)?,
                granted: read_byte(bytes, 17)? != 0,
            }),
            LEADER_ANNOUNCEMENT => Ok(Self::LeaderAnnouncement {
                source: read_i32(bytes, 5)?,
                term: read_i64(bytes, 9)?,
                log_position: read_i64(bytes, 17)?,
            }),
            ORDER => Ok(Self::Order {
                sequence: read_u64(bytes, 5)?,
            }),
            ORDER_ACK => Ok(Self::OrderAck {
                source: read_i32(bytes, 5)?,
                term: read_i64(bytes, 9)?,
                sequence: read_u64(bytes, 17)?,
                accepted: read_byte(bytes, 25)? != 0,
            }),
            _ => Err(invalid_data("unknown comparison message")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub member_id: i32,
    pub initial_leader_id: i32,
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub endpoints: HashMap<i32, SocketAddr>,
}

impl NodeConfig {
    pub fn from_env() -> io::Result<Self> {
        let member_id = env_i32("CLUSTER_MEMBER_ID", 0)?;
        let initial_leader_id = env_i32("CLUSTER_LEADER_MEMBER_ID", 0)?;
        let election_timeout = Duration::from_millis(env_u64("ELECTION_TIMEOUT_MS", 5_000)?);
        let heartbeat_interval = Duration::from_millis(env_u64("HEARTBEAT_INTERVAL_MS", 100)?);
        let endpoint_text = env::var("CLUSTER_INTERNAL_ENDPOINTS")
            .or_else(|_| env::var("CLUSTER_ENDPOINTS"))
            .map_err(|_| invalid_input("CLUSTER_ENDPOINTS is required"))?;
        let endpoints = parse_endpoints(&endpoint_text)?;
        if !endpoints.contains_key(&member_id) {
            return Err(invalid_input("member id has no configured endpoint"));
        }
        Ok(Self {
            member_id,
            initial_leader_id,
            election_timeout,
            heartbeat_interval,
            endpoints,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub endpoints: Vec<SocketAddr>,
    pub order_count: usize,
    pub start_delay: Duration,
    pub order_interval: Duration,
    pub offer_timeout: Duration,
    pub response_timeout: Duration,
}

impl ClientConfig {
    pub fn from_env() -> io::Result<Self> {
        let endpoint_text = env::var("INGRESS_ENDPOINTS")
            .or_else(|_| env::var("CLUSTER_ENDPOINTS"))
            .map_err(|_| invalid_input("INGRESS_ENDPOINTS is required"))?;
        let endpoints = endpoint_text
            .split(',')
            .map(|endpoint| {
                endpoint
                    .parse()
                    .map_err(|_| invalid_input("invalid ingress endpoint"))
            })
            .collect::<io::Result<Vec<SocketAddr>>>()?;
        if endpoints.is_empty() {
            return Err(invalid_input("at least one ingress endpoint is required"));
        }
        let order_count = env_usize("ORDER_COUNT", 3)?;
        if order_count == 0 {
            return Err(invalid_input("ORDER_COUNT must be positive"));
        }
        Ok(Self {
            endpoints,
            order_count,
            start_delay: Duration::from_millis(env_u64("START_DELAY_MS", 0)?),
            order_interval: Duration::from_millis(env_u64("ORDER_INTERVAL_MS", 0)?),
            offer_timeout: Duration::from_millis(env_u64("OFFER_TIMEOUT_MS", 60_000)?),
            response_timeout: Duration::from_millis(env_u64("RESPONSE_TIMEOUT_MS", 1_000)?),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

pub async fn run_node(config: NodeConfig) -> io::Result<()> {
    let bind_addr = config
        .endpoints
        .get(&config.member_id)
        .copied()
        .ok_or_else(|| invalid_input("member id has no bind endpoint"))?;
    let socket = UdpSocket::bind(bind_addr).await?;
    let now = Instant::now();
    let role = if config.member_id == config.initial_leader_id {
        Role::Leader
    } else {
        Role::Follower
    };
    let leader_id = if role == Role::Leader {
        config.member_id
    } else {
        config.initial_leader_id
    };
    let mut node = NodeRuntime {
        config,
        socket,
        role,
        term: 1,
        leader_id: Some(leader_id),
        log_position: 0,
        voted_for: None,
        votes: HashSet::new(),
        last_heartbeat: now,
        election_deadline: now + Duration::from_millis(1),
        last_heartbeat_sent: now - Duration::from_secs(1),
    };
    node.reset_election_deadline(now);
    println!(
        "RUST_CLUSTER_NODE_READY member={} endpoint={} role={:?} leader={} term=1",
        node.config.member_id,
        bind_addr,
        node.role,
        node.leader_id.unwrap_or(-1)
    );

    let mut receive_buffer = [0_u8; 1024];
    let mut ticker = time::interval(Duration::from_millis(10));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            result = node.socket.recv_from(&mut receive_buffer) => {
                let (length, sender) = result?;
                if let Ok(message) = WireMessage::decode(&receive_buffer[..length]) {
                    node.handle(message, sender).await?;
                }
            }
            _ = ticker.tick() => node.tick().await?,
            _ = tokio::signal::ctrl_c() => return Ok(()),
        }
    }
}

pub async fn run_client(config: ClientConfig) -> io::Result<usize> {
    time::sleep(config.start_delay).await;
    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    let mut accepted = 0;
    for sequence in 0..config.order_count as u64 {
        let deadline = Instant::now() + config.offer_timeout;
        let mut endpoint_index = sequence as usize % config.endpoints.len();
        let mut acknowledged = false;
        while Instant::now() < deadline {
            socket
                .send_to(
                    &WireMessage::Order { sequence }.encode(),
                    config.endpoints[endpoint_index],
                )
                .await?;
            endpoint_index = (endpoint_index + 1) % config.endpoints.len();
            let remaining = deadline.saturating_duration_since(Instant::now());
            let wait = remaining.min(config.response_timeout);
            let mut buffer = [0_u8; 256];
            if let Ok(Ok((length, _))) = time::timeout(wait, socket.recv_from(&mut buffer)).await {
                if let Ok(WireMessage::OrderAck {
                    sequence: response_sequence,
                    accepted: response_accepted,
                    ..
                }) = WireMessage::decode(&buffer[..length])
                    && response_sequence == sequence
                {
                    if response_accepted {
                        acknowledged = true;
                        break;
                    }
                    continue;
                }
            }
        }
        if !acknowledged {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("order {sequence} was not acknowledged"),
            ));
        }
        accepted += 1;
        if !config.order_interval.is_zero() {
            time::sleep(config.order_interval).await;
        }
    }
    println!(
        "RUST_CLUSTER_CLIENT_RESULT accepted={} total={} local_endpoint={}",
        accepted,
        config.order_count,
        socket.local_addr()?
    );
    Ok(accepted)
}

struct NodeRuntime {
    config: NodeConfig,
    socket: UdpSocket,
    role: Role,
    term: i64,
    leader_id: Option<i32>,
    log_position: i64,
    voted_for: Option<i32>,
    votes: HashSet<i32>,
    last_heartbeat: Instant,
    election_deadline: Instant,
    last_heartbeat_sent: Instant,
}

impl NodeRuntime {
    async fn handle(&mut self, message: WireMessage, sender: SocketAddr) -> io::Result<()> {
        match message {
            WireMessage::Order { sequence } => {
                let accepted = self.role == Role::Leader;
                if accepted {
                    self.log_position += 1;
                }
                self.send(
                    sender,
                    WireMessage::OrderAck {
                        source: self.config.member_id,
                        term: self.term,
                        sequence,
                        accepted,
                    },
                )
                .await?;
            }
            WireMessage::Heartbeat {
                source,
                term,
                log_position,
            }
            | WireMessage::LeaderAnnouncement {
                source,
                term,
                log_position,
            } => {
                if self.config.endpoints.contains_key(&source)
                    && term >= self.term
                    && source != self.config.member_id
                {
                    self.become_follower(source, term, log_position);
                }
            }
            WireMessage::VoteRequest {
                source,
                term,
                log_position,
            } => {
                let granted = self.grant_vote(source, term, log_position);
                self.send(
                    sender,
                    WireMessage::VoteResponse {
                        source: self.config.member_id,
                        term,
                        granted,
                    },
                )
                .await?;
            }
            WireMessage::VoteResponse {
                source,
                term,
                granted,
            } => {
                if self.role == Role::Candidate
                    && self.config.endpoints.contains_key(&source)
                    && term == self.term
                    && granted
                {
                    self.votes.insert(source);
                    if self.votes.len() >= self.quorum() {
                        self.become_leader().await?;
                    }
                }
            }
            WireMessage::OrderAck { .. } => {}
        }
        Ok(())
    }

    async fn tick(&mut self) -> io::Result<()> {
        let now = Instant::now();
        match self.role {
            Role::Leader
                if now.duration_since(self.last_heartbeat_sent)
                    >= self.config.heartbeat_interval =>
            {
                self.last_heartbeat_sent = now;
                self.broadcast(WireMessage::Heartbeat {
                    source: self.config.member_id,
                    term: self.term,
                    log_position: self.log_position,
                })
                .await?;
            }
            Role::Follower | Role::Candidate if now >= self.election_deadline => {
                self.start_election().await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn start_election(&mut self) -> io::Result<()> {
        self.role = Role::Candidate;
        self.term += 1;
        self.leader_id = None;
        self.voted_for = Some(self.config.member_id);
        self.votes.clear();
        self.votes.insert(self.config.member_id);
        self.reset_election_deadline(Instant::now());
        println!(
            "RUST_CLUSTER_ELECTION member={} term={}",
            self.config.member_id, self.term
        );
        if self.votes.len() >= self.quorum() {
            self.become_leader().await?;
        } else {
            self.broadcast(WireMessage::VoteRequest {
                source: self.config.member_id,
                term: self.term,
                log_position: self.log_position,
            })
            .await?;
        }
        Ok(())
    }

    async fn become_leader(&mut self) -> io::Result<()> {
        self.role = Role::Leader;
        self.leader_id = Some(self.config.member_id);
        self.voted_for = None;
        self.last_heartbeat = Instant::now();
        self.last_heartbeat_sent = Instant::now() - self.config.heartbeat_interval;
        println!(
            "RUST_CLUSTER_LEADER_CHANGE member={} leader={} term={} source=election",
            self.config.member_id, self.config.member_id, self.term
        );
        self.broadcast(WireMessage::LeaderAnnouncement {
            source: self.config.member_id,
            term: self.term,
            log_position: self.log_position,
        })
        .await
    }

    fn become_follower(&mut self, leader_id: i32, term: i64, log_position: i64) {
        let changed = self.leader_id != Some(leader_id) || self.term != term;
        self.role = Role::Follower;
        self.term = term;
        self.leader_id = Some(leader_id);
        self.log_position = self.log_position.max(log_position);
        self.votes.clear();
        self.voted_for = None;
        self.last_heartbeat = Instant::now();
        self.reset_election_deadline(self.last_heartbeat);
        if changed {
            println!(
                "RUST_CLUSTER_LEADER_CHANGE member={} leader={} term={} source=heartbeat",
                self.config.member_id, leader_id, term
            );
        }
    }

    fn grant_vote(&mut self, candidate_id: i32, term: i64, log_position: i64) -> bool {
        if !self.config.endpoints.contains_key(&candidate_id)
            || term < self.term
            || log_position < self.log_position
        {
            return false;
        }
        if term > self.term {
            self.become_follower(candidate_id, term, log_position);
        }
        if self.voted_for.is_none() || self.voted_for == Some(candidate_id) {
            self.voted_for = Some(candidate_id);
            self.reset_election_deadline(Instant::now());
            true
        } else {
            false
        }
    }

    async fn broadcast(&self, message: WireMessage) -> io::Result<()> {
        for (&member_id, &endpoint) in &self.config.endpoints {
            if member_id != self.config.member_id {
                self.send(endpoint, message).await?;
            }
        }
        Ok(())
    }

    async fn send(&self, endpoint: SocketAddr, message: WireMessage) -> io::Result<()> {
        self.socket.send_to(&message.encode(), endpoint).await?;
        Ok(())
    }

    fn quorum(&self) -> usize {
        (self.config.endpoints.len() / 2) + 1
    }

    fn reset_election_deadline(&mut self, now: Instant) {
        let jitter = Duration::from_millis(self.config.member_id.max(0) as u64 * 25);
        self.election_deadline = now + self.config.election_timeout + jitter;
    }
}

pub fn parse_endpoints(raw: &str) -> io::Result<HashMap<i32, SocketAddr>> {
    raw.split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            let (member, endpoint) = entry
                .split_once('=')
                .ok_or_else(|| invalid_input("endpoint must use member=address"))?;
            let member = member
                .parse()
                .map_err(|_| invalid_input("invalid endpoint member id"))?;
            let endpoint = endpoint
                .parse()
                .map_err(|_| invalid_input("invalid endpoint address"))?;
            Ok((member, endpoint))
        })
        .collect()
}

fn env_i32(name: &str, default: i32) -> io::Result<i32> {
    env::var(name).ok().map_or(Ok(default), |value| {
        value.parse().map_err(|_| invalid_input(name))
    })
}

fn env_u64(name: &str, default: u64) -> io::Result<u64> {
    env::var(name).ok().map_or(Ok(default), |value| {
        value.parse().map_err(|_| invalid_input(name))
    })
}

fn env_usize(name: &str, default: usize) -> io::Result<usize> {
    env::var(name).ok().map_or(Ok(default), |value| {
        value.parse().map_err(|_| invalid_input(name))
    })
}

fn invalid_input(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_data(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn put_i32(bytes: &mut Vec<u8>, value: i32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_i64(bytes: &mut Vec<u8>, value: i64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn read_byte(bytes: &[u8], offset: usize) -> io::Result<u8> {
    bytes
        .get(offset)
        .copied()
        .ok_or_else(|| invalid_data("truncated comparison message"))
}

fn read_u32(bytes: &[u8], offset: usize) -> io::Result<u32> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| invalid_data("truncated comparison message"))?;
    Ok(u32::from_le_bytes(
        value.try_into().expect("slice length checked"),
    ))
}

fn read_i32(bytes: &[u8], offset: usize) -> io::Result<i32> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| invalid_data("truncated comparison message"))?;
    Ok(i32::from_le_bytes(
        value.try_into().expect("slice length checked"),
    ))
}

fn read_i64(bytes: &[u8], offset: usize) -> io::Result<i64> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| invalid_data("truncated comparison message"))?;
    Ok(i64::from_le_bytes(
        value.try_into().expect("slice length checked"),
    ))
}

fn read_u64(bytes: &[u8], offset: usize) -> io::Result<u64> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| invalid_data("truncated comparison message"))?;
    Ok(u64::from_le_bytes(
        value.try_into().expect("slice length checked"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_roundtrip_every_comparison_message() {
        let messages = [
            WireMessage::Heartbeat {
                source: 1,
                term: 2,
                log_position: 3,
            },
            WireMessage::VoteRequest {
                source: 1,
                term: 2,
                log_position: 3,
            },
            WireMessage::VoteResponse {
                source: 1,
                term: 2,
                granted: true,
            },
            WireMessage::LeaderAnnouncement {
                source: 1,
                term: 2,
                log_position: 3,
            },
            WireMessage::Order { sequence: 4 },
            WireMessage::OrderAck {
                source: 1,
                term: 2,
                sequence: 4,
                accepted: false,
            },
        ];
        for message in messages {
            assert_eq!(WireMessage::decode(&message.encode()).unwrap(), message);
        }
    }

    #[test]
    fn should_parse_member_endpoints() {
        let endpoints = parse_endpoints("0=127.0.0.1:19010,1=127.0.0.1:19011").unwrap();
        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[&1].port(), 19011);
    }

    #[test]
    fn should_reject_malformed_comparison_messages() {
        assert!(WireMessage::decode(&[]).is_err());
        assert!(WireMessage::decode(&MAGIC.to_le_bytes()).is_err());
        assert!(WireMessage::decode(&[0, 0, 0, 0, ORDER]).is_err());
    }
}
