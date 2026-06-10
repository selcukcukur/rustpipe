use crate::error::PipelineError;

/// Convenient alias for results returned by pipeline operations.
///
/// **Generics**
/// - `TPassable` - The successful output type produced by the pipeline.
///
/// **Returns**
/// - `Ok(TPassable)`: The pipeline completed successfully and produced a value.
/// - `Err(PipelineError)`: The pipeline failed at some stage and returned a categorized error.
pub type PipelineResult<TPassable> = Result<TPassable, PipelineError>;

/// Alias for results returned by individual pipes.
///
/// **Generics**
/// - `TPassable`: The successful output type produced by a single pipe.
///
/// **Returns**
/// - `Ok(TPassable)` - The pipe successfully transformed or validated the input.
/// - `Err(PipelineError)` - The pipe failed and signaled an error to the pipeline.
pub type PipeResult<TPassable> = Result<TPassable, PipelineError>;
