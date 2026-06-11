#![cfg(feature = "async")]

use std::hint::black_box;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rustpipe::{AsyncNext, AsyncPipe, AsyncPipeType, AsyncPipeline, PipeResult};

struct AsyncAdd(u64);

impl AsyncPipe<u64> for AsyncAdd {
    fn handle<'a>(
        &'a self,
        passable: u64,
        next: AsyncNext<'a, u64>,
    ) -> Pin<Box<dyn std::future::Future<Output = PipeResult<u64>> + Send + 'a>> {
        Box::pin(async move { next.handle(passable.wrapping_add(self.0)).await })
    }
}

struct AsyncStop(u64);

impl AsyncPipe<u64> for AsyncStop {
    fn handle<'a>(
        &'a self,
        passable: u64,
        _next: AsyncNext<'a, u64>,
    ) -> Pin<Box<dyn std::future::Future<Output = PipeResult<u64>> + Send + 'a>> {
        Box::pin(async move { Ok(passable + self.0) })
    }
}

fn pipes(count: usize) -> Vec<AsyncPipeType<u64>> {
    (0..count)
        .map(|index| Arc::new(AsyncAdd(index as u64 + 1)) as AsyncPipeType<u64>)
        .collect()
}

fn bench_async_middleware(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("async_middleware/pipe_count");

    for pipe_count in [1usize, 10, 100] {
        let stack = pipes(pipe_count);
        group.throughput(Throughput::Elements(pipe_count as u64));

        group.bench_with_input(
            BenchmarkId::new("u64_value", pipe_count),
            &stack,
            |b, stack| {
                b.iter(|| {
                    runtime.block_on(async {
                        let output = AsyncPipeline::new()
                            .send(black_box(0_u64))
                            .through(stack.clone())
                            .then_return()
                            .await
                            .unwrap();

                        black_box(output);
                    });
                });
            },
        );
    }

    group.finish();
}

fn bench_async_middleware_short_circuit(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("async_middleware/short_circuit");
    let tail = pipes(100);

    group.bench_function("stop_before_100_pipe_tail", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let mut stack: Vec<AsyncPipeType<u64>> = vec![Arc::new(AsyncStop(10))];
                stack.extend(tail.clone());

                let output = AsyncPipeline::new()
                    .send(black_box(1_u64))
                    .through(stack)
                    .then_return()
                    .await
                    .unwrap();

                black_box(output);
            });
        });
    });

    group.finish();
}

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(8))
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets =
        bench_async_middleware,
        bench_async_middleware_short_circuit
}

criterion_main!(benches);
