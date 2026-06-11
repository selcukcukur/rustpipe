#![cfg(feature = "async")]

use std::pin::Pin;
use std::sync::Arc;

use rustpipe::{AsyncTransformPipe, AsyncTransformPipeline, TransformPipeResult};

#[derive(Debug)]
struct Job {
    id: u64,
    attempts: u8,
    events: Vec<String>,
}

struct LoadFromQueue;

impl AsyncTransformPipe<Job> for LoadFromQueue {
    fn handle<'a>(
        &'a self,
        mut passable: Job,
    ) -> Pin<Box<dyn std::future::Future<Output = TransformPipeResult<Job>> + Send + 'a>> {
        Box::pin(async move {
            passable.events.push("queue:loaded".to_string());
            Ok(passable)
        })
    }
}

struct ExecuteJob;

impl AsyncTransformPipe<Job> for ExecuteJob {
    fn handle<'a>(
        &'a self,
        mut passable: Job,
    ) -> Pin<Box<dyn std::future::Future<Output = TransformPipeResult<Job>> + Send + 'a>> {
        Box::pin(async move {
            passable.attempts += 1;
            passable.events.push("job:executed".to_string());
            Ok(passable)
        })
    }
}

#[tokio::main]
async fn main() -> rustpipe::PipelineResult<()> {
    let job = Job {
        id: 100,
        attempts: 0,
        events: Vec::new(),
    };

    let job = AsyncTransformPipeline::new()
        .send(job)
        .through(vec![Arc::new(LoadFromQueue), Arc::new(ExecuteJob)])
        .then_return()
        .await?;

    println!("job={} attempts={} {:?}", job.id, job.attempts, job.events);
    Ok(())
}
