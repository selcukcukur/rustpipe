use std::fmt;
use std::sync::{Arc, Mutex};

use rustpipe::{
    Next, Pipe, PipeResult, Pipeline, PipelineError, StepFailure, TransformPipe,
    TransformPipeResult, TransformPipeline,
};

struct Prefix(&'static str);

impl Pipe<String> for Prefix {
    fn handle(&self, passable: String, next: Next<'_, String>) -> PipeResult<String> {
        next.handle(format!("{}{}", self.0, passable))
    }
}

struct Wrap(&'static str, &'static str);

impl Pipe<String> for Wrap {
    fn handle(&self, passable: String, next: Next<'_, String>) -> PipeResult<String> {
        let passable = next.handle(passable)?;
        Ok(format!("{}{}{}", self.0, passable, self.1))
    }
}

struct Stop;

impl Pipe<String> for Stop {
    fn handle(&self, passable: String, _next: Next<'_, String>) -> PipeResult<String> {
        Ok(format!("{passable}:stopped"))
    }
}

#[test]
fn middleware_can_wrap_the_downstream_chain() {
    let result = Pipeline::new()
        .send("core".to_string())
        .through(vec![Arc::new(Wrap("[", "]")), Arc::new(Prefix("app:"))])
        .then_return()
        .unwrap();

    assert_eq!(result, "[app:core]");
}

#[test]
fn middleware_can_short_circuit_the_chain() {
    let result = Pipeline::new()
        .send("core".to_string())
        .through(vec![Arc::new(Stop), Arc::new(Prefix("never:"))])
        .then_return()
        .unwrap();

    assert_eq!(result, "core:stopped");
}

#[test]
fn middleware_when_and_unless_append_conditionally() {
    let result = Pipeline::new()
        .send("core".to_string())
        .when(true, Arc::new(Prefix("a:")))
        .when(false, Arc::new(Prefix("b:")))
        .unless(false, Arc::new(Prefix("c:")))
        .unless(true, Arc::new(Prefix("d:")))
        .then_return()
        .unwrap();

    assert_eq!(result, "c:a:core");
}

#[test]
fn middleware_runs_finally_on_success() {
    let seen = Arc::new(Mutex::new(None));
    let seen_for_callback = Arc::clone(&seen);

    let result = Pipeline::new()
        .send("core".to_string())
        .through(vec![Arc::new(Prefix("app:"))])
        .finally(move |result| {
            *seen_for_callback.lock().unwrap() = Some(result.is_ok());
        })
        .then_return()
        .unwrap();

    assert_eq!(result, "app:core");
    assert_eq!(*seen.lock().unwrap(), Some(true));
}

struct Upper;

impl TransformPipe<String> for Upper {
    fn handle(&self, passable: String) -> TransformPipeResult<String> {
        Ok(passable.to_uppercase())
    }
}

struct Suffix(&'static str);

impl TransformPipe<String> for Suffix {
    fn handle(&self, passable: String) -> TransformPipeResult<String> {
        Ok(format!("{}{}", passable, self.0))
    }
}

#[test]
fn transform_pipeline_runs_without_middleware_next() {
    let result = TransformPipeline::new()
        .send("hello".to_string())
        .through(vec![Arc::new(Upper), Arc::new(Suffix("!"))])
        .then_return()
        .unwrap();

    assert_eq!(result, "HELLO!");
}

#[derive(Debug)]
struct DomainError;

impl fmt::Display for DomainError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("domain error")
    }
}

impl std::error::Error for DomainError {}

impl From<DomainError> for PipelineError {
    fn from(value: DomainError) -> Self {
        PipelineError::Custom(Box::new(value))
    }
}

struct Fails;

impl TransformPipe<String, DomainError> for Fails {
    fn handle(&self, _passable: String) -> TransformPipeResult<String, DomainError> {
        Err(DomainError)
    }
}

#[test]
fn custom_errors_can_escape_as_pipeline_errors() {
    let result = TransformPipeline::new()
        .send("hello".to_string())
        .through(vec![Arc::new(Fails)])
        .then_return();

    assert!(matches!(result, Err(PipelineError::Custom(_))));
}

struct BuiltInFailure;

impl TransformPipe<String> for BuiltInFailure {
    fn handle(&self, _passable: String) -> TransformPipeResult<String> {
        Err(StepFailure {
            step: "BuiltInFailure",
            message: "failed".to_string(),
        }
        .into())
    }
}

#[test]
fn rescue_recovers_from_pipeline_errors() {
    let result = TransformPipeline::new()
        .send("hello".to_string())
        .through(vec![Arc::new(BuiltInFailure)])
        .rescue(|_| "fallback".to_string())
        .unwrap();

    assert_eq!(result, "fallback");
}

#[test]
fn executing_without_send_returns_input_missing() {
    let result: Result<String, PipelineError> = Pipeline::<String>::new().then_return();

    assert!(matches!(result, Err(PipelineError::InputMissing)));
}
