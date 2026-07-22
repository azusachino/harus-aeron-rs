//! Client and shared Aeron context configuration.

use std::env;
use std::path::{Component, Path, PathBuf};

use crate::error::{AeronError, Result};

pub const AERON_DIR_PROP_NAME: &str = "AERON_DIR";
pub const DEFAULT_DRIVER_TIMEOUT_MS: u64 = 10_000;
pub const MAX_CLIENT_NAME_LENGTH: usize = 100;
pub const IPC_MEDIA: &str = "ipc";
pub const UDP_MEDIA: &str = "udp";
pub const IPC_CHANNEL: &str = "aeron:ipc";
pub const UDP_CHANNEL: &str = "aeron:udp";
pub const SPY_PREFIX: &str = "aeron-spy:";
pub const ENDPOINT_PARAM_NAME: &str = "endpoint";
pub const INTERFACE_PARAM_NAME: &str = "interface";

/// Shared directory and timeout configuration used by the client and driver.
#[derive(Clone, Debug)]
pub struct CommonContext {
    is_concluded: bool,
    driver_timeout_ms: u64,
    aeron_directory_name: PathBuf,
    aeron_directory: Option<PathBuf>,
    enable_experimental_features: bool,
}

impl Default for CommonContext {
    fn default() -> Self {
        Self::new()
    }
}

impl CommonContext {
    pub fn new() -> Self {
        Self {
            is_concluded: false,
            driver_timeout_ms: DEFAULT_DRIVER_TIMEOUT_MS,
            aeron_directory_name: default_directory_name(),
            aeron_directory: None,
            enable_experimental_features: false,
        }
    }

    pub fn conclude(&mut self) -> Result<&mut Self> {
        if self.is_concluded {
            return Err(AeronError::ConcurrentConclude);
        }

        self.conclude_aeron_directory()?;
        self.is_concluded = true;
        Ok(self)
    }

    pub fn is_concluded(&self) -> bool {
        self.is_concluded
    }

    pub fn conclude_aeron_directory(&mut self) -> Result<&mut Self> {
        if self.aeron_directory.is_none() {
            self.aeron_directory = Some(canonicalize_path(&self.aeron_directory_name)?);
        }
        Ok(self)
    }

    pub fn aeron_directory_name(&self) -> &Path {
        &self.aeron_directory_name
    }

    pub fn set_aeron_directory_name<P: Into<PathBuf>>(&mut self, name: P) -> &mut Self {
        self.aeron_directory_name = name.into();
        self.aeron_directory = None;
        self
    }

    pub fn aeron_directory(&self) -> Option<&Path> {
        self.aeron_directory.as_deref()
    }

    pub fn driver_timeout_ms(&self) -> u64 {
        self.driver_timeout_ms
    }

    pub fn set_driver_timeout_ms(&mut self, timeout_ms: u64) -> &mut Self {
        self.driver_timeout_ms = timeout_ms;
        self
    }

    pub fn enable_experimental_features(&self) -> bool {
        self.enable_experimental_features
    }

    pub fn set_enable_experimental_features(&mut self, enabled: bool) -> &mut Self {
        self.enable_experimental_features = enabled;
        self
    }
}

/// Configuration for an Aeron client instance.
#[derive(Clone, Debug, Default)]
pub struct AeronContext {
    common: CommonContext,
    client_name: String,
    use_conductor_agent_invoker: bool,
    driver_agent_invoker_configured: bool,
}

impl AeronContext {
    pub fn new() -> Self {
        Self {
            common: CommonContext::new(),
            client_name: String::new(),
            use_conductor_agent_invoker: false,
            driver_agent_invoker_configured: false,
        }
    }

    pub fn common_context(&self) -> &CommonContext {
        &self.common
    }

    pub fn common_context_mut(&mut self) -> &mut CommonContext {
        &mut self.common
    }

    pub fn client_name(&self) -> &str {
        &self.client_name
    }

    pub fn set_client_name<S: Into<String>>(&mut self, name: S) -> &mut Self {
        let name = name.into();
        self.client_name = if name.is_empty() { String::new() } else { name };
        self
    }

    pub fn use_conductor_agent_invoker(&self) -> bool {
        self.use_conductor_agent_invoker
    }

    pub fn set_use_conductor_agent_invoker(&mut self, enabled: bool) -> &mut Self {
        self.use_conductor_agent_invoker = enabled;
        self
    }

    pub fn set_driver_agent_invoker_configured(&mut self, configured: bool) -> &mut Self {
        self.driver_agent_invoker_configured = configured;
        self
    }

    pub fn conclude(&mut self) -> Result<&mut Self> {
        if self.client_name.len() > MAX_CLIENT_NAME_LENGTH {
            return Err(AeronError::Configuration(format!(
                "clientName length must <= {MAX_CLIENT_NAME_LENGTH}"
            )));
        }

        if self.driver_agent_invoker_configured && !self.use_conductor_agent_invoker {
            return Err(AeronError::Configuration(
                "Must use AeronContext.useConductorAgentInvoker(true) when AeronContext.driverAgentInvoker() is set"
                    .to_string(),
            ));
        }

        self.common.conclude()?;
        Ok(self)
    }
}

fn default_directory_name() -> PathBuf {
    env::var_os(AERON_DIR_PROP_NAME)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let user = env::var("USER").unwrap_or_else(|_| "default".to_string());
            env::temp_dir().join(format!("aeron-{user}"))
        })
}

fn canonicalize_path(path: &Path) -> Result<PathBuf> {
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return Ok(canonical);
    }

    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn common_context_concludes_only_once() {
        let mut context = CommonContext::new();
        assert!(context.conclude().is_ok());
        assert!(matches!(
            context.conclude(),
            Err(AeronError::ConcurrentConclude)
        ));
    }

    #[test]
    fn common_context_canonicalizes_directory_without_creating_it() {
        let mut context = CommonContext::new();
        context.set_aeron_directory_name("one/two/../three/./four");
        context.conclude_aeron_directory().unwrap();

        assert!(
            context
                .aeron_directory()
                .unwrap()
                .ends_with("one/three/four")
        );
        assert!(!context.aeron_directory().unwrap().exists());
    }

    #[test]
    fn aeron_context_uses_empty_name_for_empty_input() {
        let mut context = AeronContext::new();
        context.set_client_name("");
        assert_eq!(context.client_name(), "");
    }

    #[test]
    fn aeron_context_rejects_long_names() {
        let mut context = AeronContext::new();
        context.set_client_name("x".repeat(MAX_CLIENT_NAME_LENGTH + 1));
        let error = context.conclude().unwrap_err();
        assert_eq!(error.to_string(), "clientName length must <= 100");
    }

    #[test]
    fn aeron_context_requires_conductor_invoker_for_driver_invoker() {
        let mut context = AeronContext::new();
        context.set_driver_agent_invoker_configured(true);
        let error = context.conclude().unwrap_err();
        assert!(error.to_string().contains("useConductorAgentInvoker(true)"));
    }
}
