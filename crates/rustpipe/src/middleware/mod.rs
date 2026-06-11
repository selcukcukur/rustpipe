use crate::errors::PipelineError;
use crate::types::{Finalizer, Next, PipeResult, PipeType, PipelineResult};
use crate::utility;

#[cfg(feature = "async")]
use crate::types::{AsyncNext, AsyncPipeType};

/// A Laravel-inspired middleware pipeline.
///
/// Each [`Pipe`](crate::Pipe) receives the current value and a [`Next`]
/// continuation. Middleware can call `next.handle(passable)` to continue,
/// return early to stop the chain, or wrap the downstream result.
///
/// **Generics**
/// - `TPassable` - The type of the value that flows through the pipeline.
/// - `TError` - The error type returned by middleware pipes.
pub struct Pipeline<TPassable, TError = PipelineError> {
    passable: Option<TPassable>,
    pipes: Vec<PipeType<TPassable, TError>>,
    finally: Option<Finalizer<TPassable>>,

    #[cfg(feature = "taps")]
    taps: Vec<crate::types::Tap<TPassable>>,
}

impl<TPassable, TError> Default for Pipeline<TPassable, TError> {
    fn default() -> Self {
        Self::new()
    }
}

impl<TPassable, TError> Pipeline<TPassable, TError> {
    /// Creates a new, empty middleware pipeline instance.
    ///
    /// **Returns**
    /// - A fresh middleware pipeline with no passable value and no pipes.
    pub fn new() -> Self {
        Self {
            passable: None,
            pipes: Vec::new(),
            finally: None,
            #[cfg(feature = "taps")]
            taps: Vec::new(),
        }
    }

    /// Provides the initial passable value to the middleware pipeline.
    ///
    /// **Parameters**
    /// - `passable` - The initial value that will flow through the middleware chain.
    ///
    /// **Returns**
    /// - The pipeline instance with the initial passable value set.
    pub fn send(mut self, passable: TPassable) -> Self {
        self.passable = Some(passable);
        self
    }

    /// Adds a sequence of middleware pipes to the pipeline.
    ///
    /// **Parameters**
    /// - `pipes` - Middleware pipes executed in the order they are provided.
    ///
    /// **Returns**
    /// - The pipeline instance with the provided middleware appended.
    pub fn through(mut self, pipes: Vec<PipeType<TPassable, TError>>) -> Self {
        self.pipes.extend(pipes);
        self
    }

    /// Adds a middleware pipe when the condition is `true`.
    ///
    /// **Parameters**
    /// - `condition` - Controls whether the pipe is appended.
    /// - `pipe` - The middleware pipe to append when the condition matches.
    pub fn when(mut self, condition: bool, pipe: PipeType<TPassable, TError>) -> Self {
        if condition {
            self.pipes.push(pipe);
        }
        self
    }

    /// Adds a middleware pipe when the condition is `false`.
    ///
    /// **Parameters**
    /// - `condition` - Controls whether the pipe is skipped.
    /// - `pipe` - The middleware pipe to append when the condition is false.
    pub fn unless(mut self, condition: bool, pipe: PipeType<TPassable, TError>) -> Self {
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

    /// Registers an observer callback for the current value.
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

impl<TPassable, TError> Pipeline<TPassable, TError>
where
    TError: Into<PipelineError>,
{
    /// Executes the middleware chain and applies a final destination closure.
    ///
    /// **Parameters**
    /// - `destination` - A closure that maps the post-middleware value into `R`.
    ///
    /// **Returns**
    /// - `Ok(R)` - The middleware chain completed and the destination ran.
    /// - `Err(PipelineError)` - Input was missing or middleware failed.
    pub fn then<F, R>(self, destination: F) -> PipelineResult<R>
    where
        F: FnOnce(TPassable) -> R,
    {
        self.run(|passable| Ok(passable)).map(destination)
    }

    /// Finalizes the middleware pipeline and returns the processed output.
    ///
    /// **Returns**
    /// - `Ok(TPassable)` - The middleware chain completed successfully.
    /// - `Err(PipelineError)` - Input was missing or middleware failed.
    pub fn then_return(self) -> PipelineResult<TPassable> {
        self.run(|passable| Ok(passable))
    }

    /// Intercepts middleware errors and allows recovery with a fallback value.
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

    fn run<F>(self, destination: F) -> PipelineResult<TPassable>
    where
        F: Fn(TPassable) -> PipeResult<TPassable, TError>,
    {
        let passable = utility::require_passable(self.passable)?;

        #[cfg(feature = "taps")]
        let destination = |passable: TPassable| {
            utility::run_taps(&self.taps, &passable);
            destination(passable)
        };

        // Build the first continuation from the whole middleware slice. Every
        // middleware receives a shorter slice through `Next`, so execution stays
        // stack-safe for typical pipeline sizes without heap-building closures.
        let next = Next::new(&self.pipes, &destination);
        let result = next.handle(passable).map_err(Into::into);

        if let Some(finally) = &self.finally {
            finally(&result);
        }

        result
    }
}

/// A Laravel-inspired asynchronous middleware pipeline.
#[cfg(feature = "async")]
pub struct AsyncPipeline<TPassable, TError = PipelineError> {
    passable: Option<TPassable>,
    pipes: Vec<AsyncPipeType<TPassable, TError>>,
}

#[cfg(feature = "async")]
impl<TPassable, TError> Default for AsyncPipeline<TPassable, TError> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "async")]
impl<TPassable, TError> AsyncPipeline<TPassable, TError> {
    /// Creates a new asynchronous middleware pipeline.
    pub fn new() -> Self {
        Self {
            passable: None,
            pipes: Vec::new(),
        }
    }

    /// Provides the initial passable value to the async middleware pipeline.
    pub fn send(mut self, passable: TPassable) -> Self {
        self.passable = Some(passable);
        self
    }

    /// Adds asynchronous middleware pipes to the pipeline.
    pub fn through(mut self, pipes: Vec<AsyncPipeType<TPassable, TError>>) -> Self {
        self.pipes.extend(pipes);
        self
    }
}

#[cfg(feature = "async")]
impl<TPassable, TError> AsyncPipeline<TPassable, TError>
where
    TPassable: Send,
    TError: Into<PipelineError> + Send,
{
    /// Finalizes the asynchronous middleware pipeline and returns the processed output.
    pub async fn then_return(self) -> PipelineResult<TPassable> {
        let passable = utility::require_passable(self.passable)?;
        let destination = |passable| {
            Box::pin(async move { Ok(passable) })
                as crate::types::AsyncPipeFuture<'_, TPassable, TError>
        };
        let next = AsyncNext::new(&self.pipes, &destination);
        next.handle(passable).await.map_err(Into::into)
    }

    /// Executes async middleware and applies a final destination closure.
    pub async fn then<F, R>(self, destination: F) -> PipelineResult<R>
    where
        F: FnOnce(TPassable) -> R,
    {
        self.then_return().await.map(destination)
    }
}
