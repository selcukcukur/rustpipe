#![cfg(feature = "async")]

use std::hint::black_box;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rustpipe::{
    AsyncTransformPipe, AsyncTransformPipeType, AsyncTransformPipeline, TransformPipeResult,
};

struct AsyncAdd(u64);

impl AsyncTransformPipe<Vec<u64>> for AsyncAdd {
    fn handle<'a>(
        &'a self,
        mut passable: Vec<u64>,
    ) -> Pin<Box<dyn std::future::Future<Output = TransformPipeResult<Vec<u64>>> + Send + 'a>> {
        Box::pin(async move {
            for value in &mut passable {
                *value = value.wrapping_add(self.0);
            }

            Ok(passable)
        })
    }
}

fn values(count: usize) -> Vec<u64> {
    (0..count as u64).collect()
}

fn pipes(count: usize) -> Vec<AsyncTransformPipeType<Vec<u64>>> {
    (0..count)
        .map(|index| Arc::new(AsyncAdd(index as u64 + 1)) as AsyncTransformPipeType<Vec<u64>>)
        .collect()
}

fn bench_async_transform(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("async_transform/pipe_count");

    for pipe_count in [1usize, 10, 100] {
        let stack = pipes(pipe_count);
        group.throughput(Throughput::Elements(pipe_count as u64));

        group.bench_with_input(
            BenchmarkId::new("1000_values", pipe_count),
            &stack,
            |b, stack| {
                b.iter(|| {
                    runtime.block_on(async {
                        let output = AsyncTransformPipeline::new()
                            .send(black_box(values(1_000)))
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

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(8))
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets = bench_async_transform
}

criterion_main!(benches);
