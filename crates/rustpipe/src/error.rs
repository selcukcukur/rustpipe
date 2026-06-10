use thiserror::Error;

/// Ana hata tipi
#[derive(Debug, Error)]
pub enum PipelineError {
    #[error(transparent)]
    StepFailure(#[from] StepFailure),

    #[error("Pipeline input not set")]
    InputMissing,

    #[error(transparent)]
    DispatchError(#[from] DispatchError),

    #[error(transparent)]
    RescueFailure(#[from] RescueFailure),
}

/// Alt hata tipleri
#[derive(Debug, Error)]
#[error("Step `{step}` failed: {message}")]
pub struct StepFailure {
    pub step: &'static str,
    pub message: String,
}

#[derive(Debug, Error)]
#[error("Dispatch `{method}` error: {message}")]
pub struct DispatchError {
    pub method: &'static str,
    pub message: String,
}

#[derive(Debug, Error)]
#[error("Rescue `{rescue}` failed: {message}")]
pub struct RescueFailure {
    pub rescue: &'static str,
    pub message: String,
}

/// Convenient alias
pub type PipelineResult<T> = Result<T, PipelineError>;
