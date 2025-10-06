//! Error types for OTP patterns
//!
//! This module defines the error types used throughout the OTP patterns
//! for better error handling and debugging.

use std::fmt;

/// Errors that can occur in OTP pattern operations
#[derive(Debug, Clone, PartialEq)]
pub enum OtpError {
    /// Process communication failed
    CommunicationError(String),
    /// Process terminated unexpectedly
    ProcessTerminated(String),
    /// Timeout occurred
    Timeout(String),
    /// Invalid message format
    InvalidMessage(String),
    /// Supervisor restart intensity exceeded
    RestartIntensityExceeded {
        restarts: usize,
        max_restarts: usize,
        window_seconds: u32,
    },
    /// Child process failed to start
    ChildStartFailure(String),
    /// Invalid configuration
    InvalidConfig(String),
    /// Operation not implemented
    NotImplemented(String),
}

impl fmt::Display for OtpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OtpError::CommunicationError(msg) => write!(f, "Communication error: {}", msg),
            OtpError::ProcessTerminated(msg) => write!(f, "Process terminated: {}", msg),
            OtpError::Timeout(msg) => write!(f, "Timeout: {}", msg),
            OtpError::InvalidMessage(msg) => write!(f, "Invalid message: {}", msg),
            OtpError::RestartIntensityExceeded { restarts, max_restarts, window_seconds } => {
                write!(f, "Restart intensity exceeded: {} restarts in {} seconds (max: {})",
                    restarts, window_seconds, max_restarts)
            }
            OtpError::ChildStartFailure(msg) => write!(f, "Child start failure: {}", msg),
            OtpError::InvalidConfig(msg) => write!(f, "Invalid config: {}", msg),
            OtpError::NotImplemented(msg) => write!(f, "Not implemented: {}", msg),
        }
    }
}

impl std::error::Error for OtpError {}

/// Result type alias for OTP operations
pub type OtpResult<T> = Result<T, OtpError>;

/// Convert from anyhow::Error to OtpError
impl From<anyhow::Error> for OtpError {
    fn from(err: anyhow::Error) -> Self {
        OtpError::CommunicationError(err.to_string())
    }
}

/// Convert from String to OtpError
impl From<String> for OtpError {
    fn from(msg: String) -> Self {
        OtpError::CommunicationError(msg)
    }
}

/// Convert from &str to OtpError
impl From<&str> for OtpError {
    fn from(msg: &str) -> Self {
        OtpError::CommunicationError(msg.to_string())
    }
}