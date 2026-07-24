//! Media driver configuration defaults and property names.
//!
//! Mirrors `io.aeron.driver.Configuration.java`.

use std::env;

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

/// Driver configuration settings, populated from environment variables or defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Configuration {
    pub term_buffer_sparse_file: bool,
    pub publication_term_buffer_length: usize,
    pub ipc_term_buffer_length: usize,
    pub mtu_length: usize,
    pub ipc_mtu_length: usize,
    pub publication_term_window_length: usize,
    pub ipc_publication_term_window_length: usize,
    pub publication_linger_timeout_ns: u64,
    pub spies_simulate_connection: bool,
    pub untethered_window_limit_timeout_ns: u64,
    pub untethered_resting_timeout_ns: u64,
    pub untethered_linger_timeout_ns: i64,
    pub max_resend: i32,
    /// Maximum number of active receive sessions allowed for one stream.
    /// Mirrors `Configuration.streamSessionLimit()` in vendored Aeron.
    pub stream_session_limit: usize,
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            term_buffer_sparse_file: true,
            publication_term_buffer_length: TERM_BUFFER_LENGTH_DEFAULT,
            ipc_term_buffer_length: TERM_BUFFER_IPC_LENGTH_DEFAULT,
            mtu_length: MTU_LENGTH_DEFAULT,
            ipc_mtu_length: IPC_MTU_LENGTH_DEFAULT,
            publication_term_window_length: 0,
            ipc_publication_term_window_length: 0,
            publication_linger_timeout_ns: PUBLICATION_LINGER_DEFAULT_NS,
            spies_simulate_connection: false,
            untethered_window_limit_timeout_ns: UNTETHERED_WINDOW_LIMIT_TIMEOUT_DEFAULT_NS,
            untethered_resting_timeout_ns: UNTETHERED_RESTING_TIMEOUT_DEFAULT_NS,
            untethered_linger_timeout_ns: UNTETHERED_LINGER_TIMEOUT_DEFAULT_NS,
            max_resend: MAX_RESEND_DEFAULT,
            stream_session_limit: STREAM_SESSION_LIMIT_DEFAULT,
        }
    }
}

impl Configuration {
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
    }
}
