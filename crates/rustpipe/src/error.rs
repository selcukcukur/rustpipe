use thiserror::Error;

/// The main categorized error type for pipeline execution.
///
/// **Errors**
/// - `StepFailure` - Raised when a pipeline step fails with a specific message.
/// - `InputMissing` - Raised when the pipeline is executed without any input provided.
/// - `DispatchError` - Raised when an invalid or unsupported method dispatch occurs.
/// - `RescueFailure` - Raised when a rescue fallback closure itself fails.
#[derive(Debug, Error)]
pub enum PipelineError {
    /// Raised when a pipeline step fails with a specific message.
    #[error(transparent)]
    StepFailure(#[from] StepFailure),

    /// Raised when the pipeline is executed without any input provided.
    #[error("Pipeline input not set")]
    InputMissing,

    /// Raised when an invalid or unsupported method dispatch occurs.
    #[error(transparent)]
    DispatchError(#[from] DispatchError),

    /// Raised when a rescue fallback closure itself fails.
    #[error(transparent)]
    RescueFailure(#[from] RescueFailure),
}

/// Represents a failure that occurred in a specific pipeline step.
///
/// **Parameters**
/// - `step` - The name of the step where the failure happened.
/// - `message` - A human-readable description of the failure.
#[derive(Debug, Error)]
#[error("Step `{step}` failed: {message}")]
pub struct StepFailure {
    /// The name of the step where the failure happened.
    pub step: &'static str,

    /// A human-readable description of the failure.
    pub message: String,
}

/// Represents a failure that occurred during a dispatch operation.
///
/// **Parameters**
/// - `method` - The name of the dispatch method that failed.
/// - `message` - A human-readable description of the failure.
#[derive(Debug, Error)]
#[error("Dispatch `{method}` error: {message}")]
pub struct DispatchError {
    /// The name of the dispatch method that failed.
    pub method: &'static str,

    /// A human-readable description of the failure.
    pub message: String,
}

/// Represents a failure that occurred during a rescue operation.
///
/// **Parameters**
/// - `rescue` - The name of the rescue handler that failed.
/// - `message` - A human-readable description of the failure.
#[derive(Debug, Error)]
#[error("Rescue `{rescue}` failed: {message}")]
pub struct RescueFailure {
    /// The name of the rescue handler that failed.
    pub rescue: &'static str,

    /// A human-readable description of the failure.
    pub message: String,
}
