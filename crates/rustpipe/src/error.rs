#[derive(Debug)]
pub enum PipelineError {
    StepFailure(String),
    InputMissing,
    DispatchError(String),
    RescueFailure(String),
}

pub type PipelineResult<T> = Result<T, PipelineError>;

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineError::StepFailure(msg) => write!(f, "Step failed: {}", msg),
            PipelineError::InputMissing => write!(f, "Pipeline input not set"),
            PipelineError::DispatchError(msg) => write!(f, "Dispatch error: {}", msg),
            PipelineError::RescueFailure(msg) => write!(f, "Rescue failed: {}", msg),
        }
    }
}

impl From<String> for PipelineError {
    fn from(err: String) -> Self {
        PipelineError::StepFailure(err)
    }
}

impl From<&'static str> for PipelineError {
    fn from(err: &'static str) -> Self {
        PipelineError::StepFailure(err.to_string())
    }
}
