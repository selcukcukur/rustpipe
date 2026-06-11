#![cfg(feature = "async")]

use std::pin::Pin;
use std::sync::Arc;

use rustpipe::{
    AsyncNext, AsyncPipe, AsyncPipeline, AsyncTransformPipe, AsyncTransformPipeline, PipeResult,
    TransformPipeResult,
};

struct AsyncPrefix(&'static str);

impl AsyncPipe<String> for AsyncPrefix {
    fn handle<'a>(
        &'a self,
        passable: String,
        next: AsyncNext<'a, String>,
    ) -> Pin<Box<dyn std::future::Future<Output = PipeResult<String>> + Send + 'a>> {
        Box::pin(async move { next.handle(format!("{}{}", self.0, passable)).await })
    }
}

struct AsyncStop;

impl AsyncPipe<String> for AsyncStop {
    fn handle<'a>(
        &'a self,
        passable: String,
        _next: AsyncNext<'a, String>,
    ) -> Pin<Box<dyn std::future::Future<Output = PipeResult<String>> + Send + 'a>> {
        Box::pin(async move { Ok(format!("{passable}:stopped")) })
    }
}

struct AsyncUpper;

impl AsyncTransformPipe<String> for AsyncUpper {
    fn handle<'a>(
        &'a self,
        passable: String,
    ) -> Pin<Box<dyn std::future::Future<Output = TransformPipeResult<String>> + Send + 'a>> {
        Box::pin(async move { Ok(passable.to_uppercase()) })
    }
}

struct AsyncBatchAdd(u64);

impl AsyncTransformPipe<Vec<u64>> for AsyncBatchAdd {
    fn handle<'a>(
        &'a self,
        mut passable: Vec<u64>,
    ) -> Pin<Box<dyn std::future::Future<Output = TransformPipeResult<Vec<u64>>> + Send + 'a>> {
        Box::pin(async move {
            for value in &mut passable {
                *value += self.0;
            }

            Ok(passable)
        })
    }
}

#[tokio::test]
async fn async_middleware_pipeline_runs_next_chain() {
    let result = AsyncPipeline::new()
        .send("core".to_string())
        .through(vec![Arc::new(AsyncPrefix("app:"))])
        .then_return()
        .await
        .unwrap();

    assert_eq!(result, "app:core");
}

#[tokio::test]
async fn async_middleware_can_short_circuit() {
    let result = AsyncPipeline::new()
        .send("core".to_string())
        .through(vec![Arc::new(AsyncStop), Arc::new(AsyncPrefix("never:"))])
        .then_return()
        .await
        .unwrap();

    assert_eq!(result, "core:stopped");
}

#[tokio::test]
async fn async_transform_pipeline_runs_transforms() {
    let result = AsyncTransformPipeline::new()
        .send("hello".to_string())
        .through(vec![Arc::new(AsyncUpper)])
        .then_return()
        .await
        .unwrap();

    assert_eq!(result, "HELLO");
}

#[tokio::test]
async fn async_transform_pipeline_processes_batches() {
    let result = AsyncTransformPipeline::new()
        .send((0_u64..1_000).collect::<Vec<_>>())
        .through(vec![
            Arc::new(AsyncBatchAdd(10)),
            Arc::new(AsyncBatchAdd(5)),
        ])
        .then(|values| values.into_iter().sum::<u64>())
        .await
        .unwrap();

    assert_eq!(result, 514_500);
}
