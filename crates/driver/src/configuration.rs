//! Media driver configuration defaults and property names.
//!
//! Mirrors `io.aeron.driver.Configuration.java`.

use std::env;

use aeron_client::cnc_file::{
    DEFAULT_COUNTER_METADATA_LENGTH, DEFAULT_COUNTER_VALUES_LENGTH, DEFAULT_ERROR_LOG_LENGTH,
    DEFAULT_FILE_PAGE_SIZE, DEFAULT_TO_CLIENTS_LENGTH, DEFAULT_TO_DRIVER_LENGTH,
};
use aeron_client::logbuffer::{TERM_MAX_LENGTH, TERM_MIN_LENGTH, check_page_size};
use agrona::broadcast::TRAILER_LENGTH as BROADCAST_TRAILER_LENGTH;
use agrona::ring_buffer::TRAILER_LENGTH as RING_TRAILER_LENGTH;

use crate::error::{DriverError, Result};

pub const TERM_BUFFER_SPARSE_FILE_PROP_NAME: &str = "aeron.term.buffer.sparse.file";
pub const TERM_BUFFER_LENGTH_PROP_NAME: &str = "aeron.term.buffer.length";
pub const TERM_BUFFER_LENGTH_DEFAULT: usize = 16 * 1024 * 1024;

pub const IPC_TERM_BUFFER_LENGTH_PROP_NAME: &str = "aeron.ipc.term.buffer.length";
pub const TERM_BUFFER_IPC_LENGTH_DEFAULT: usize = 64 * 1024 * 1024;

pub const MTU_LENGTH_PROP_NAME: &str = "aeron.mtu.length";
pub const MTU_LENGTH_DEFAULT: usize = 1408;

pub const IPC_MTU_LENGTH_PROP_NAME: &str = "aeron.ipc.mtu.length";
pub const IPC_MTU_LENGTH_DEFAULT: usize = MTU_LENGTH_DEFAULT;

pub const PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME: &str = "aeron.publication.term.window.length";
pub const IPC_PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME: &str =
    "aeron.ipc.publication.term.window.length";

pub const PUBLICATION_LINGER_PROP_NAME: &str = "aeron.publication.linger.timeout";
pub const PUBLICATION_LINGER_DEFAULT_NS: u64 = 5 * 1000 * 1000 * 1000; // 5s in ns

pub const CLIENT_LIVENESS_TIMEOUT_PROP_NAME: &str = "aeron.client.liveness.timeout";
pub const CLIENT_LIVENESS_TIMEOUT_DEFAULT_NS: i64 = 10 * 1000 * 1000 * 1000;

pub const SPIES_SIMULATE_CONNECTION_PROP_NAME: &str = "aeron.spies.simulate.connection";

pub const UNTETHERED_WINDOW_LIMIT_TIMEOUT_PROP_NAME: &str = "aeron.untethered.window.limit.timeout";
pub const UNTETHERED_WINDOW_LIMIT_TIMEOUT_DEFAULT_NS: u64 = 5 * 1000 * 1000 * 1000;

pub const UNTETHERED_RESTING_TIMEOUT_PROP_NAME: &str = "aeron.untethered.resting.timeout";
pub const UNTETHERED_RESTING_TIMEOUT_DEFAULT_NS: u64 = 10 * 1000 * 1000 * 1000;

pub const UNTETHERED_LINGER_TIMEOUT_PROP_NAME: &str = "aeron.untethered.linger.timeout";
pub const UNTETHERED_LINGER_TIMEOUT_DEFAULT_NS: i64 = -1; // Aeron.NULL_VALUE

pub const MAX_RESEND_PROP_NAME: &str = "aeron.max.resend";
pub const MAX_RESEND_DEFAULT: i32 = 16;
pub const MAX_RESEND_MAX: i32 = 256;

pub const STREAM_SESSION_LIMIT_PROP_NAME: &str = "aeron.driver.stream.session.limit";
pub const STREAM_SESSION_LIMIT_DEFAULT: usize = i32::MAX as usize;

/// Rust adaptation of Aeron's termination-validator boundary. The upstream default validator
/// denies requests; applications may explicitly opt in until a pluggable validator API lands.
pub const TERMINATION_ALLOW_PROP_NAME: &str = "aeron.driver.termination.allow";
pub const PERFORM_STORAGE_CHECKS_PROP_NAME: &str = "aeron.perform.storage.checks";
pub const LOW_FILE_STORE_WARNING_THRESHOLD_PROP_NAME: &str =
    "aeron.low.file.store.warning.threshold";
pub const LOW_FILE_STORE_WARNING_THRESHOLD_DEFAULT: u64 = TERM_BUFFER_LENGTH_DEFAULT as u64 * 10;
pub const CONDUCTOR_BUFFER_LENGTH_PROP_NAME: &str = "aeron.conductor.buffer.length";
pub const TO_CLIENTS_BUFFER_LENGTH_PROP_NAME: &str = "aeron.clients.buffer.length";
pub const COUNTERS_VALUES_BUFFER_LENGTH_PROP_NAME: &str = "aeron.counters.buffer.length";
pub const ERROR_BUFFER_LENGTH_PROP_NAME: &str = "aeron.error.buffer.length";
pub const FILE_PAGE_SIZE_PROP_NAME: &str = "aeron.file.page.size";
pub const NAK_MULTICAST_MAX_BACKOFF_PROP_NAME: &str = "aeron.nak.multicast.max.backoff";
pub const NAK_UNICAST_DELAY_PROP_NAME: &str = "aeron.nak.unicast.delay";
pub const NAK_UNICAST_RETRY_DELAY_RATIO_PROP_NAME: &str = "aeron.nak.unicast.retry.delay.ratio";
pub const NAK_MULTICAST_MAX_BACKOFF_DEFAULT_NS: i64 = 10_000_000;
pub const NAK_UNICAST_DELAY_DEFAULT_NS: i64 = 1_000;
pub const NAK_UNICAST_DELAY_MIN_NS: i64 = 1_000;
pub const NAK_UNICAST_RETRY_DELAY_RATIO_DEFAULT: i64 = 100;

/// Driver configuration settings, populated from environment variables or defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Configuration {
    pub conductor_buffer_length: usize,
    pub to_clients_buffer_length: usize,
    pub counters_metadata_buffer_length: usize,
    pub counters_values_buffer_length: usize,
    pub error_buffer_length: usize,
    pub file_page_size: i32,
    pub term_buffer_sparse_file: bool,
    pub publication_term_buffer_length: usize,
    pub ipc_term_buffer_length: usize,
    pub mtu_length: usize,
    pub ipc_mtu_length: usize,
    pub publication_term_window_length: usize,
    pub ipc_publication_term_window_length: usize,
    pub publication_linger_timeout_ns: u64,
    pub client_liveness_timeout_ns: i64,
    pub spies_simulate_connection: bool,
    pub untethered_window_limit_timeout_ns: u64,
    pub untethered_resting_timeout_ns: u64,
    pub untethered_linger_timeout_ns: i64,
    pub max_resend: i32,
    /// Maximum number of active receive sessions allowed for one stream.
    /// Mirrors `Configuration.streamSessionLimit()` in vendored Aeron.
    pub stream_session_limit: usize,
    /// Whether TERMINATE_DRIVER control messages are accepted. Defaults to false, matching
    /// Aeron's DefaultDenyTerminationValidator.
    pub allow_termination: bool,
    /// Whether log allocation checks available file-store space before mapping a log.
    pub perform_storage_checks: bool,
    /// Warn when usable file-store space falls at or below this threshold.
    pub low_file_store_warning_threshold: u64,
    /// Maximum multicast NAK feedback backoff in nanoseconds.
    pub nak_multicast_max_backoff_ns: i64,
    /// Initial unicast NAK delay in nanoseconds.
    pub nak_unicast_delay_ns: i64,
    /// Multiplier applied to the unicast NAK delay for retries.
    pub nak_unicast_retry_delay_ratio: i64,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            conductor_buffer_length: DEFAULT_TO_DRIVER_LENGTH,
            to_clients_buffer_length: DEFAULT_TO_CLIENTS_LENGTH,
            counters_metadata_buffer_length: DEFAULT_COUNTER_METADATA_LENGTH,
            counters_values_buffer_length: DEFAULT_COUNTER_VALUES_LENGTH,
            error_buffer_length: DEFAULT_ERROR_LOG_LENGTH,
            file_page_size: DEFAULT_FILE_PAGE_SIZE,
            term_buffer_sparse_file: true,
            publication_term_buffer_length: TERM_BUFFER_LENGTH_DEFAULT,
            ipc_term_buffer_length: TERM_BUFFER_IPC_LENGTH_DEFAULT,
            mtu_length: MTU_LENGTH_DEFAULT,
            ipc_mtu_length: IPC_MTU_LENGTH_DEFAULT,
            publication_term_window_length: 0,
            ipc_publication_term_window_length: 0,
            publication_linger_timeout_ns: PUBLICATION_LINGER_DEFAULT_NS,
            client_liveness_timeout_ns: CLIENT_LIVENESS_TIMEOUT_DEFAULT_NS,
            spies_simulate_connection: false,
            untethered_window_limit_timeout_ns: UNTETHERED_WINDOW_LIMIT_TIMEOUT_DEFAULT_NS,
            untethered_resting_timeout_ns: UNTETHERED_RESTING_TIMEOUT_DEFAULT_NS,
            untethered_linger_timeout_ns: UNTETHERED_LINGER_TIMEOUT_DEFAULT_NS,
            max_resend: MAX_RESEND_DEFAULT,
            stream_session_limit: STREAM_SESSION_LIMIT_DEFAULT,
            allow_termination: false,
            perform_storage_checks: true,
            low_file_store_warning_threshold: LOW_FILE_STORE_WARNING_THRESHOLD_DEFAULT,
            nak_multicast_max_backoff_ns: NAK_MULTICAST_MAX_BACKOFF_DEFAULT_NS,
            nak_unicast_delay_ns: NAK_UNICAST_DELAY_DEFAULT_NS,
            nak_unicast_retry_delay_ratio: NAK_UNICAST_RETRY_DELAY_RATIO_DEFAULT,
        }
    }
}

impl Configuration {
    /// Validate the settings that define log-buffer geometry and publication flow control.
    ///
    /// Keeping this check at driver construction makes invalid MediaDriver contexts fail before
    /// a CnC file or transport endpoint is created.
    pub fn validate(&self) -> Result<()> {
        if !valid_ring_buffer_length(self.conductor_buffer_length, RING_TRAILER_LENGTH) {
            return Err(DriverError::IllegalArgument(format!(
                "conductor-buffer-length must contain a power-of-two data region: value={}",
                self.conductor_buffer_length
            )));
        }
        if !valid_ring_buffer_length(self.to_clients_buffer_length, BROADCAST_TRAILER_LENGTH) {
            return Err(DriverError::IllegalArgument(format!(
                "to-clients-buffer-length must contain a power-of-two data region: value={}",
                self.to_clients_buffer_length
            )));
        }
        if self.counters_metadata_buffer_length == 0
            || self.counters_values_buffer_length == 0
            || self.error_buffer_length == 0
        {
            return Err(DriverError::IllegalArgument(
                "counter and error buffer lengths must be positive".to_string(),
            ));
        }
        check_page_size(self.file_page_size)
            .map_err(|message| DriverError::IllegalArgument(message.to_string()))?;

        validate_term_length(
            "publication-term-buffer-length",
            self.publication_term_buffer_length,
        )?;
        validate_term_length("ipc-term-buffer-length", self.ipc_term_buffer_length)?;
        validate_mtu("mtu-length", self.mtu_length)?;
        validate_mtu("ipc-mtu-length", self.ipc_mtu_length)?;
        validate_mtu_against_term(
            "mtu-length",
            self.mtu_length,
            self.publication_term_buffer_length,
        )?;
        validate_mtu_against_term(
            "ipc-mtu-length",
            self.ipc_mtu_length,
            self.ipc_term_buffer_length,
        )?;
        validate_window(
            "publication-term-window-length",
            self.publication_term_window_length,
            self.mtu_length,
            self.publication_term_buffer_length,
        )?;
        validate_window(
            "ipc-publication-term-window-length",
            self.ipc_publication_term_window_length,
            self.ipc_mtu_length,
            self.ipc_term_buffer_length,
        )?;
        if !(0..=MAX_RESEND_MAX).contains(&self.max_resend) {
            return Err(DriverError::IllegalArgument(format!(
                "max-resend must be between 0 and {MAX_RESEND_MAX}: value={}",
                self.max_resend
            )));
        }
        if self.client_liveness_timeout_ns <= 0 {
            return Err(DriverError::IllegalArgument(format!(
                "client-liveness-timeout must be positive: value={}",
                self.client_liveness_timeout_ns
            )));
        }
        if self.untethered_linger_timeout_ns < -1 {
            return Err(DriverError::IllegalArgument(format!(
                "untethered-linger-timeout must be -1 or non-negative: value={}",
                self.untethered_linger_timeout_ns
            )));
        }
        if self.stream_session_limit == 0 {
            return Err(DriverError::IllegalArgument(
                "stream-session-limit must be positive".to_string(),
            ));
        }
        if self.nak_multicast_max_backoff_ns < 0 {
            return Err(DriverError::IllegalArgument(format!(
                "nak-multicast-max-backoff must be non-negative: value={}",
                self.nak_multicast_max_backoff_ns
            )));
        }
        if self.nak_unicast_delay_ns < NAK_UNICAST_DELAY_MIN_NS {
            return Err(DriverError::IllegalArgument(format!(
                "nak-unicast-delay must be at least {NAK_UNICAST_DELAY_MIN_NS}: value={}",
                self.nak_unicast_delay_ns
            )));
        }
        if self.nak_unicast_retry_delay_ratio < 1 {
            return Err(DriverError::IllegalArgument(format!(
                "nak-unicast-retry-delay-ratio must be positive: value={}",
                self.nak_unicast_retry_delay_ratio
            )));
        }
        Ok(())
    }

    /// Create a new Configuration populated with values from environment variables or defaults.
    pub fn load() -> Self {
        Self::load_from_getter(|prop| env::var(prop).ok())
    }

    /// Load configuration using a custom property getter closure (e.g. for isolated testing).
    pub fn load_from_getter<F>(get_prop: F) -> Self
    where
        F: Fn(&str) -> Option<String>,
    {
        let mut config = Self::default();

        if let Some(val) = get_prop(TERM_BUFFER_SPARSE_FILE_PROP_NAME) {
            config.term_buffer_sparse_file = val.parse::<bool>().unwrap_or(true);
        }
        if let Some(val) = get_prop(CONDUCTOR_BUFFER_LENGTH_PROP_NAME) {
            if let Ok(v) = val.parse::<usize>() {
                config.conductor_buffer_length = v;
            }
        }
        if let Some(val) = get_prop(TO_CLIENTS_BUFFER_LENGTH_PROP_NAME) {
            if let Ok(v) = val.parse::<usize>() {
                config.to_clients_buffer_length = v;
            }
        }
        if let Some(val) = get_prop(COUNTERS_VALUES_BUFFER_LENGTH_PROP_NAME) {
            if let Ok(v) = val.parse::<usize>() {
                config.counters_values_buffer_length = v;
            }
        }
        if let Some(val) = get_prop(ERROR_BUFFER_LENGTH_PROP_NAME) {
            if let Ok(v) = val.parse::<usize>() {
                config.error_buffer_length = v;
            }
        }
        if let Some(val) = get_prop(FILE_PAGE_SIZE_PROP_NAME) {
            if let Ok(v) = val.parse::<i32>() {
                config.file_page_size = v;
            }
        }
        if let Some(val) = get_prop(TERM_BUFFER_LENGTH_PROP_NAME) {
            if let Ok(v) = val.parse::<usize>() {
                config.publication_term_buffer_length = v;
            }
        }
        if let Some(val) = get_prop(IPC_TERM_BUFFER_LENGTH_PROP_NAME) {
            if let Ok(v) = val.parse::<usize>() {
                config.ipc_term_buffer_length = v;
            }
        }
        if let Some(val) = get_prop(MTU_LENGTH_PROP_NAME) {
            if let Ok(v) = val.parse::<usize>() {
                config.mtu_length = v;
            }
        }
        if let Some(val) = get_prop(IPC_MTU_LENGTH_PROP_NAME) {
            if let Ok(v) = val.parse::<usize>() {
                config.ipc_mtu_length = v;
            }
        }
        if let Some(val) = get_prop(PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME) {
            if let Ok(v) = val.parse::<usize>() {
                config.publication_term_window_length = v;
            }
        }
        if let Some(val) = get_prop(IPC_PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME) {
            if let Ok(v) = val.parse::<usize>() {
                config.ipc_publication_term_window_length = v;
            }
        }
        if let Some(val) = get_prop(PUBLICATION_LINGER_PROP_NAME) {
            if let Ok(v) = val.parse::<u64>() {
                config.publication_linger_timeout_ns = v;
            }
        }
        if let Some(val) = get_prop(CLIENT_LIVENESS_TIMEOUT_PROP_NAME) {
            if let Ok(v) = val.parse::<i64>() {
                config.client_liveness_timeout_ns = v;
            }
        }
        if let Some(val) = get_prop(SPIES_SIMULATE_CONNECTION_PROP_NAME) {
            config.spies_simulate_connection = val.parse::<bool>().unwrap_or(false);
        }
        if let Some(val) = get_prop(UNTETHERED_WINDOW_LIMIT_TIMEOUT_PROP_NAME) {
            if let Ok(v) = val.parse::<u64>() {
                config.untethered_window_limit_timeout_ns = v;
            }
        }
        if let Some(val) = get_prop(UNTETHERED_RESTING_TIMEOUT_PROP_NAME) {
            if let Ok(v) = val.parse::<u64>() {
                config.untethered_resting_timeout_ns = v;
            }
        }
        if let Some(val) = get_prop(UNTETHERED_LINGER_TIMEOUT_PROP_NAME) {
            if let Ok(v) = val.parse::<i64>() {
                config.untethered_linger_timeout_ns = v;
            }
        }
        if let Some(val) = get_prop(MAX_RESEND_PROP_NAME) {
            if let Ok(v) = val.parse::<i32>() {
                config.max_resend = v;
            }
        }
        if let Some(val) = get_prop(STREAM_SESSION_LIMIT_PROP_NAME) {
            if let Ok(v) = val.parse::<usize>() {
                config.stream_session_limit = v;
            }
        }
        if let Some(val) = get_prop(TERMINATION_ALLOW_PROP_NAME) {
            config.allow_termination = val.parse::<bool>().unwrap_or(false);
        }
        if let Some(val) = get_prop(PERFORM_STORAGE_CHECKS_PROP_NAME) {
            config.perform_storage_checks = val.parse::<bool>().unwrap_or(true);
        }
        if let Some(val) = get_prop(LOW_FILE_STORE_WARNING_THRESHOLD_PROP_NAME) {
            if let Ok(v) = val.parse::<u64>() {
                config.low_file_store_warning_threshold = v;
            }
        }
        if let Some(val) = get_prop(NAK_MULTICAST_MAX_BACKOFF_PROP_NAME) {
            if let Some(v) = parse_duration_ns(&val) {
                config.nak_multicast_max_backoff_ns = v;
            }
        }
        if let Some(val) = get_prop(NAK_UNICAST_DELAY_PROP_NAME) {
            if let Some(v) = parse_duration_ns(&val) {
                config.nak_unicast_delay_ns = v;
            }
        }
        if let Some(val) = get_prop(NAK_UNICAST_RETRY_DELAY_RATIO_PROP_NAME) {
            if let Ok(v) = val.parse::<i64>() {
                config.nak_unicast_retry_delay_ratio = v;
            }
        }

        config
    }

    /// Check if term buffer sparse files are enabled for a given property lookup closure.
    pub fn term_buffer_sparse_file_from<F>(get_prop: F) -> bool
    where
        F: Fn(&str) -> Option<String>,
    {
        get_prop(TERM_BUFFER_SPARSE_FILE_PROP_NAME)
            .map(|v| v.parse::<bool>().unwrap_or(true))
            .unwrap_or(true)
    }

    /// Helper for standalone `term_buffer_sparse_file()` reading environment variable.
    pub fn term_buffer_sparse_file() -> bool {
        Self::term_buffer_sparse_file_from(|prop| env::var(prop).ok())
    }
}

fn parse_duration_ns(value: &str) -> Option<i64> {
    let value = value.trim();
    let (number, multiplier) = if let Some(value) = value.strip_suffix("ns") {
        (value, 1i64)
    } else if let Some(value) = value.strip_suffix("us") {
        (value, 1_000i64)
    } else if let Some(value) = value.strip_suffix("ms") {
        (value, 1_000_000i64)
    } else if let Some(value) = value.strip_suffix('s') {
        (value, 1_000_000_000i64)
    } else {
        (value, 1i64)
    };
    number.parse::<i64>().ok()?.checked_mul(multiplier)
}

fn validate_term_length(name: &str, value: usize) -> Result<()> {
    let value_i32 = i32::try_from(value)
        .map_err(|_| DriverError::IllegalArgument(format!("{name} is too large: value={value}")))?;
    if !(TERM_MIN_LENGTH..=TERM_MAX_LENGTH).contains(&value_i32)
        || !(value_i32 as u32).is_power_of_two()
    {
        return Err(DriverError::IllegalArgument(format!(
            "{name} must be a power of two between {TERM_MIN_LENGTH} and {TERM_MAX_LENGTH}: value={value}"
        )));
    }
    Ok(())
}

fn valid_ring_buffer_length(length: usize, trailer_length: usize) -> bool {
    length >= trailer_length && (length - trailer_length).is_power_of_two()
}

fn validate_mtu(name: &str, value: usize) -> Result<()> {
    if !(32..=65_504).contains(&value) || value % 32 != 0 {
        return Err(DriverError::IllegalArgument(format!(
            "{name} must be a multiple of 32 between 32 and 65504: value={value}"
        )));
    }
    Ok(())
}

fn validate_mtu_against_term(name: &str, mtu: usize, term_length: usize) -> Result<()> {
    let max_message_length = (term_length / 8).min(16 * 1024 * 1024);
    if mtu > max_message_length {
        return Err(DriverError::IllegalArgument(format!(
            "{name} exceeds max message length {max_message_length}: value={mtu}"
        )));
    }
    Ok(())
}

fn validate_window(name: &str, window: usize, mtu: usize, term_length: usize) -> Result<()> {
    if window != 0 && (window < mtu || window > term_length / 2) {
        return Err(DriverError::IllegalArgument(format!(
            "{name} must be zero or between MTU {mtu} and half term length {}: value={window}",
            term_length / 2
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn should_use_sparse_files_by_default() {
        let env_map = HashMap::<String, String>::new();
        assert!(Configuration::term_buffer_sparse_file_from(|k| env_map
            .get(k)
            .cloned()));
    }

    #[test]
    fn should_use_sparse_files_when_configured() {
        for &use_sparse_files in &[true, false] {
            let mut env_map = HashMap::<String, String>::new();
            env_map.insert(
                TERM_BUFFER_SPARSE_FILE_PROP_NAME.to_string(),
                use_sparse_files.to_string(),
            );
            assert_eq!(
                use_sparse_files,
                Configuration::term_buffer_sparse_file_from(|k| env_map.get(k).cloned())
            );
        }
    }

    #[test]
    fn should_load_default_values() {
        let config = Configuration::default();
        assert_eq!(config.conductor_buffer_length, DEFAULT_TO_DRIVER_LENGTH);
        assert_eq!(config.to_clients_buffer_length, DEFAULT_TO_CLIENTS_LENGTH);
        assert_eq!(
            config.counters_metadata_buffer_length,
            DEFAULT_COUNTER_METADATA_LENGTH
        );
        assert_eq!(
            config.counters_values_buffer_length,
            DEFAULT_COUNTER_VALUES_LENGTH
        );
        assert_eq!(config.error_buffer_length, DEFAULT_ERROR_LOG_LENGTH);
        assert_eq!(config.file_page_size, DEFAULT_FILE_PAGE_SIZE);
        assert!(config.term_buffer_sparse_file);
        assert_eq!(
            config.publication_term_buffer_length,
            TERM_BUFFER_LENGTH_DEFAULT
        );
        assert_eq!(
            config.ipc_term_buffer_length,
            TERM_BUFFER_IPC_LENGTH_DEFAULT
        );
        assert_eq!(config.mtu_length, MTU_LENGTH_DEFAULT);
        assert_eq!(config.ipc_mtu_length, IPC_MTU_LENGTH_DEFAULT);
        assert_eq!(config.publication_term_window_length, 0);
        assert_eq!(config.ipc_publication_term_window_length, 0);
        assert_eq!(
            config.publication_linger_timeout_ns,
            PUBLICATION_LINGER_DEFAULT_NS
        );
        assert_eq!(
            config.client_liveness_timeout_ns,
            CLIENT_LIVENESS_TIMEOUT_DEFAULT_NS
        );
        assert!(!config.spies_simulate_connection);
        assert_eq!(
            config.untethered_window_limit_timeout_ns,
            UNTETHERED_WINDOW_LIMIT_TIMEOUT_DEFAULT_NS
        );
        assert_eq!(
            config.untethered_resting_timeout_ns,
            UNTETHERED_RESTING_TIMEOUT_DEFAULT_NS
        );
        assert_eq!(
            config.untethered_linger_timeout_ns,
            UNTETHERED_LINGER_TIMEOUT_DEFAULT_NS
        );
        assert_eq!(config.max_resend, MAX_RESEND_DEFAULT);
        assert!(!config.allow_termination);
        assert!(config.perform_storage_checks);
        assert_eq!(
            config.low_file_store_warning_threshold,
            LOW_FILE_STORE_WARNING_THRESHOLD_DEFAULT
        );
        assert_eq!(
            config.nak_multicast_max_backoff_ns,
            NAK_MULTICAST_MAX_BACKOFF_DEFAULT_NS
        );
        assert_eq!(config.nak_unicast_delay_ns, NAK_UNICAST_DELAY_DEFAULT_NS);
        assert_eq!(
            config.nak_unicast_retry_delay_ratio,
            NAK_UNICAST_RETRY_DELAY_RATIO_DEFAULT
        );
    }

    #[test]
    fn should_load_termination_permission_from_property() {
        let config = Configuration::load_from_getter(|property| {
            (property == TERMINATION_ALLOW_PROP_NAME).then(|| "true".to_string())
        });

        assert!(config.allow_termination);
    }

    #[test]
    fn should_load_storage_policy_from_properties() {
        let config = Configuration::load_from_getter(|property| match property {
            PERFORM_STORAGE_CHECKS_PROP_NAME => Some("false".to_string()),
            LOW_FILE_STORE_WARNING_THRESHOLD_PROP_NAME => Some("123".to_string()),
            _ => None,
        });

        assert!(!config.perform_storage_checks);
        assert_eq!(config.low_file_store_warning_threshold, 123);
    }

    #[test]
    fn should_load_cnc_geometry_from_properties() {
        let config = Configuration::load_from_getter(|property| match property {
            CONDUCTOR_BUFFER_LENGTH_PROP_NAME => {
                Some((DEFAULT_TO_DRIVER_LENGTH + (1 << 20)).to_string())
            }
            TO_CLIENTS_BUFFER_LENGTH_PROP_NAME => {
                Some((DEFAULT_TO_CLIENTS_LENGTH + (1 << 20)).to_string())
            }
            COUNTERS_VALUES_BUFFER_LENGTH_PROP_NAME => Some("2097152".to_string()),
            ERROR_BUFFER_LENGTH_PROP_NAME => Some("2097152".to_string()),
            FILE_PAGE_SIZE_PROP_NAME => Some("8192".to_string()),
            _ => None,
        });

        assert_eq!(
            config.conductor_buffer_length,
            DEFAULT_TO_DRIVER_LENGTH + (1 << 20)
        );
        assert_eq!(
            config.to_clients_buffer_length,
            DEFAULT_TO_CLIENTS_LENGTH + (1 << 20)
        );
        assert_eq!(config.counters_values_buffer_length, 2 * 1024 * 1024);
        assert_eq!(config.error_buffer_length, 2 * 1024 * 1024);
        assert_eq!(config.file_page_size, 8192);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn should_load_client_liveness_timeout_from_property() {
        let config = Configuration::load_from_getter(|property| {
            (property == CLIENT_LIVENESS_TIMEOUT_PROP_NAME).then(|| "123456789".to_string())
        });

        assert_eq!(config.client_liveness_timeout_ns, 123_456_789);
    }

    #[test]
    fn should_load_nak_timing_from_properties() {
        let config = Configuration::load_from_getter(|property| match property {
            NAK_MULTICAST_MAX_BACKOFF_PROP_NAME => Some("12ms".to_string()),
            NAK_UNICAST_DELAY_PROP_NAME => Some("2us".to_string()),
            NAK_UNICAST_RETRY_DELAY_RATIO_PROP_NAME => Some("250".to_string()),
            _ => None,
        });

        assert_eq!(config.nak_multicast_max_backoff_ns, 12_000_000);
        assert_eq!(config.nak_unicast_delay_ns, 2_000);
        assert_eq!(config.nak_unicast_retry_delay_ratio, 250);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn should_reject_invalid_nak_timing() {
        let delay = Configuration {
            nak_unicast_delay_ns: NAK_UNICAST_DELAY_MIN_NS - 1,
            ..Configuration::default()
        };
        assert!(
            delay
                .validate()
                .unwrap_err()
                .to_string()
                .contains("nak-unicast-delay")
        );

        let ratio = Configuration {
            nak_unicast_retry_delay_ratio: 0,
            ..Configuration::default()
        };
        assert!(
            ratio
                .validate()
                .unwrap_err()
                .to_string()
                .contains("nak-unicast-retry-delay-ratio")
        );
    }

    #[test]
    fn should_validate_default_geometry() {
        assert!(Configuration::default().validate().is_ok());
    }

    #[test]
    fn should_reject_invalid_term_geometry() {
        let config = Configuration {
            ipc_term_buffer_length: 65_535,
            ..Configuration::default()
        };
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("ipc-term-buffer-length"));
    }

    #[test]
    fn should_reject_invalid_mtu_and_window_geometry() {
        let mtu_config = Configuration {
            mtu_length: 1_500,
            ..Configuration::default()
        };
        assert!(
            mtu_config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("mtu-length")
        );

        let window_config = Configuration {
            publication_term_window_length: 1_024,
            ..Configuration::default()
        };
        assert!(
            window_config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("publication-term-window-length")
        );
    }

    #[test]
    fn should_reject_non_positive_liveness_timeout() {
        let config = Configuration {
            client_liveness_timeout_ns: 0,
            ..Configuration::default()
        };
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("client-liveness-timeout")
        );
    }
}
