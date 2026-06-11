use crate::errors::PipelineError;
use crate::types::{Finalizer, PipelineResult, TransformPipeType};
use crate::utility;

#[cfg(feature = "async")]
use crate::types::AsyncTransformPipeType;

/// A configurable pipeline for simple sequential transformations.
///
/// `TransformPipeline` is the direct, carry-free pipeline variant. Each
/// [`TransformPipe`](crate::TransformPipe) receives the current value and returns
/// the next value. Pipes cannot see or control the remaining stack.
///
/// **Generics**
/// - `TPassable` - The type of the value that flows through the pipeline.
/// - `TError` - The error type returned by transform pipes.
pub struct TransformPipeline<TPassable, TError = PipelineError> {
    passable: Option<TPassable>,
    pipes: Vec<TransformPipeType<TPassable, TError>>,
    finally: Option<Finalizer<TPassable>>,

    #[cfg(feature = "taps")]
    taps: Vec<crate::types::Tap<TPassable>>,
}

impl<TPassable, TError> Default for TransformPipeline<TPassable, TError> {
    fn default() -> Self {
        Self::new()
    }
}

impl<TPassable, TError> TransformPipeline<TPassable, TError> {
    /// Creates a new, empty transform pipeline instance.
    ///
    /// **Returns**
    /// - A fresh transform pipeline with no passable value and no pipes.
    pub fn new() -> Self {
        Self {
            passable: None,
            pipes: Vec::new(),
            finally: None,
            #[cfg(feature = "taps")]
            taps: Vec::new(),
        }
    }

    /// Provides the initial passable value to the transform pipeline.
    ///
    /// **Parameters**
    /// - `passable` - The initial value that will flow through all transforms.
    ///
    /// **Returns**
    /// - The pipeline instance with the initial passable value set.
    pub fn send(mut self, passable: TPassable) -> Self {
        self.passable = Some(passable);
        self
    }

    /// Adds a sequence of transform pipes to the pipeline.
    ///
    /// **Parameters**
    /// - `pipes` - Transform pipes executed in the order they are provided.
    ///
    /// **Returns**
    /// - The pipeline instance with the provided pipes appended.
    pub fn through(mut self, pipes: Vec<TransformPipeType<TPassable, TError>>) -> Self {
        self.pipes.extend(pipes);
        self
    }

    /// Adds a transform pipe when the condition is `true`.
    ///
    /// **Parameters**
    /// - `condition` - Controls whether the pipe is appended.
    /// - `pipe` - The transform pipe to append when the condition matches.
    pub fn when(mut self, condition: bool, pipe: TransformPipeType<TPassable, TError>) -> Self {
        if condition {
            self.pipes.push(pipe);
        }
        self
    }

    /// Adds a transform pipe when the condition is `false`.
    ///
    /// **Parameters**
    /// - `condition` - Controls whether the pipe is skipped.
    /// - `pipe` - The transform pipe to append when the condition is false.
    pub fn unless(mut self, condition: bool, pipe: TransformPipeType<TPassable, TError>) -> Self {
        if !condition {
            self.pipes.push(pipe);
        }
        self
    }

    /// Registers a finalizer that runs after successful or failed execution.
    ///
    /// **Parameters**
    /// - `callback` - A closure that receives the final pipeline result.
    pub fn finally<F>(mut self, callback: F) -> Self
    where
        F: Fn(&PipelineResult<TPassable>) + Send + Sync + 'static,
    {
        self.finally = Some(Box::new(callback));
        self
    }

    /// Registers an observer callback for successful transform stages.
    ///
    /// When the `taps` feature is disabled the callback observes only the
    /// current value at registration time, matching a zero-storage core build.
    #[cfg(feature = "taps")]
    pub fn tap<F>(mut self, callback: F) -> Self
    where
        F: Fn(&TPassable) + Send + Sync + 'static,
    {
        self.taps.push(Box::new(callback));
        self
    }

    /// Registers an observer callback without storing it when `taps` is disabled.
    #[cfg(not(feature = "taps"))]
    pub fn tap<F>(self, callback: F) -> Self
    where
        F: Fn(&TPassable) + Send + Sync + 'static,
    {
        if let Some(passable) = &self.passable {
            callback(passable);
        }
        self
    }
}

impl<TPassable, TError> TransformPipeline<TPassable, TError>
where
    TError: Into<PipelineError>,
{
    /// Executes all transforms and applies a final destination closure.
    ///
    /// **Parameters**
    /// - `destination` - A closure that maps the final passable value into `R`.
    ///
    /// **Returns**
    /// - `Ok(R)` - The transform pipeline completed and the destination ran.
    /// - `Err(PipelineError)` - Input was missing or a transform failed.
    pub fn then<F, R>(self, destination: F) -> PipelineResult<R>
    where
        F: FnOnce(TPassable) -> R,
    {
        self.then_return().map(destination)
    }

    /// Finalizes the transform pipeline and returns the processed output.
    ///
    /// **Returns**
    /// - `Ok(TPassable)` - All transforms completed successfully.
    /// - `Err(PipelineError)` - Input was missing or a transform failed.
    pub fn then_return(self) -> PipelineResult<TPassable> {
        let mut passable = utility::require_passable(self.passable)?;

        for pipe in &self.pipes {
            match pipe.handle(passable) {
                Ok(next) => {
                    passable = next;

                    #[cfg(feature = "taps")]
                    utility::run_taps(&self.taps, &passable);
                }
                Err(err) => {
                    let result = Err(err.into());
                    if let Some(finally) = &self.finally {
                        finally(&result);
                    }
                    return result;
                }
            }
        }

        let result = Ok(passable);
        if let Some(finally) = &self.finally {
            finally(&result);
        }
        result
    }

    /// Intercepts transform errors and allows recovery with a fallback value.
    ///
    /// **Parameters**
    /// - `recovery` - A closure that maps [`PipelineError`] into `TPassable`.
    ///
    /// **Returns**
    /// - `Ok(TPassable)` - The successful or recovered pipeline value.
    /// - `Err(PipelineError)` - Input was missing before any recovery was possible.
    pub fn rescue<F>(self, recovery: F) -> PipelineResult<TPassable>
    where
        F: FnOnce(PipelineError) -> TPassable,
    {
        match self.then_return() {
            Ok(passable) => Ok(passable),
            Err(PipelineError::InputMissing) => Err(PipelineError::InputMissing),
            Err(err) => Ok(recovery(err)),
        }
    }
}

/// A configurable asynchronous transform pipeline.
#[cfg(feature = "async")]
pub struct AsyncTransformPipeline<TPassable, TError = PipelineError> {
    passable: Option<TPassable>,
    pipes: Vec<AsyncTransformPipeType<TPassable, TError>>,
}

#[cfg(feature = "async")]
impl<TPassable, TError> Default for AsyncTransformPipeline<TPassable, TError> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "async")]
impl<TPassable, TError> AsyncTransformPipeline<TPassable, TError> {
    /// Creates a new asynchronous transform pipeline.
    pub fn new() -> Self {
        Self {
            passable: None,
            pipes: Vec::new(),
        }
    }

    /// Provides the initial passable value to the async transform pipeline.
    pub fn send(mut self, passable: TPassable) -> Self {
        self.passable = Some(passable);
        self
    }

    /// Adds asynchronous transform pipes to the pipeline.
    pub fn through(mut self, pipes: Vec<AsyncTransformPipeType<TPassable, TError>>) -> Self {
        self.pipes.extend(pipes);
        self
    }
}

#[cfg(feature = "async")]
impl<TPassable, TError> AsyncTransformPipeline<TPassable, TError>
where
    TError: Into<PipelineError>,
{
    /// Finalizes the asynchronous transform pipeline and returns the processed output.
    pub async fn then_return(self) -> PipelineResult<TPassable> {
        let mut passable = utility::require_passable(self.passable)?;

        for pipe in &self.pipes {
            passable = pipe.handle(passable).await.map_err(Into::into)?;
        }

        Ok(passable)
    }

    /// Executes all asynchronous transforms and applies a destination closure.
    pub async fn then<F, R>(self, destination: F) -> PipelineResult<R>
    where
        F: FnOnce(TPassable) -> R,
    {
        self.then_return().await.map(destination)
    }
}
