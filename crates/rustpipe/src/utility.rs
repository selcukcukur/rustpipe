use crate::errors::PipelineError;
use crate::types::Tap;

/// Requires a pipeline input value before execution starts.
///
/// **Parameters**
/// - `passable` - The optional passable value stored by the pipeline.
///
/// **Returns**
/// - `Ok(T)` - The passable value exists.
/// - `Err(PipelineError::InputMissing)` - The pipeline was executed without `send`.
pub fn require_passable<T>(passable: Option<T>) -> Result<T, PipelineError> {
    passable.ok_or(PipelineError::InputMissing)
}

/// Runs observer callbacks after successful pipeline stages.
///
/// **Parameters**
/// - `taps` - Registered observer callbacks.
/// - `passable` - The current passable value observed by each callback.
pub fn run_taps<T>(taps: &[Tap<T>], passable: &T) {
    for tap in taps {
        tap(passable);
    }
}
