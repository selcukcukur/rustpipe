use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rustpipe::{
    Next, Pipe, PipeResult, Pipeline, TransformPipe, TransformPipeResult, TransformPipeline,
};

struct AddOne;

impl TransformPipe<u64> for AddOne {
    fn handle(&self, input: u64) -> TransformPipeResult<u64> {
        Ok(input + 1)
    }
}

struct MiddlewareAddOne;

impl Pipe<u64> for MiddlewareAddOne {
    fn handle(&self, input: u64, next: Next<'_, u64>) -> PipeResult<u64> {
        next.handle(input + 1)
    }
}

struct ShortCircuitAt(u64);

impl Pipe<u64> for ShortCircuitAt {
    fn handle(&self, input: u64, _next: Next<'_, u64>) -> PipeResult<u64> {
        Ok(input + self.0)
    }
}

struct Upper;

impl TransformPipe<String> for Upper {
    fn handle(&self, input: String) -> TransformPipeResult<String> {
        Ok(input.to_uppercase())
    }
}

fn transform_pipes(count: usize) -> Vec<Arc<dyn TransformPipe<u64> + Send + Sync>> {
    (0..count)
        .map(|_| Arc::new(AddOne) as Arc<dyn TransformPipe<u64> + Send + Sync>)
        .collect()
}

fn middleware_pipes(count: usize) -> Vec<Arc<dyn Pipe<u64> + Send + Sync>> {
    (0..count)
        .map(|_| Arc::new(MiddlewareAddOne) as Arc<dyn Pipe<u64> + Send + Sync>)
        .collect()
}

fn string_pipes(count: usize) -> Vec<Arc<dyn TransformPipe<String> + Send + Sync>> {
    (0..count)
        .map(|_| Arc::new(Upper) as Arc<dyn TransformPipe<String> + Send + Sync>)
        .collect()
}

fn bench_large_transform_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_transform_pipeline");
    let pipe_counts = [10usize, 100, 1_000];

    for count in pipe_counts {
        let pipes = transform_pipes(count);
        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(
            BenchmarkId::new("u64_transforms", count),
            &pipes,
            |b, pipes| {
                b.iter(|| {
                    let result = TransformPipeline::new()
                        .send(black_box(0_u64))
                        .through(pipes.clone())
                        .then_return()
                        .unwrap();

                    black_box(result);
                });
            },
        );
    }

    group.finish();
}

fn bench_large_middleware_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_middleware_pipeline");
    let pipe_counts = [10usize, 100, 1_000];

    for count in pipe_counts {
        let pipes = middleware_pipes(count);
        group.throughput(Throughput::Elements(count as u64));

        group.bench_with_input(
            BenchmarkId::new("u64_middleware", count),
            &pipes,
            |b, pipes| {
                b.iter(|| {
                    let result = Pipeline::new()
                        .send(black_box(0_u64))
                        .through(pipes.clone())
                        .then_return()
                        .unwrap();

                    black_box(result);
                });
            },
        );
    }

    group.finish();
}

fn bench_short_circuit_middleware(c: &mut Criterion) {
    let mut group = c.benchmark_group("short_circuit_middleware");
    let tail = middleware_pipes(1_000);

    group.bench_function("stop_before_large_tail", |b| {
        b.iter(|| {
            let mut pipes: Vec<Arc<dyn Pipe<u64> + Send + Sync>> =
                vec![Arc::new(ShortCircuitAt(10))];
            pipes.extend(tail.clone());

            let result = Pipeline::new()
                .send(black_box(1_u64))
                .through(pipes)
                .then_return()
                .unwrap();

            black_box(result);
        });
    });

    group.finish();
}

fn bench_string_transform_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_transform_workload");
    let sizes = [1_000usize, 10_000, 100_000];
    let pipes = string_pipes(5);

    for size in sizes {
        let inputs: Vec<String> = (0..size).map(|i| format!("data{i}")).collect();
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(
            BenchmarkId::new("uppercase_batch", size),
            &inputs,
            |b, inputs| {
                b.iter(|| {
                    for input in inputs.iter().cloned() {
                        let result = TransformPipeline::new()
                            .send(black_box(input))
                            .through(pipes.clone())
                            .then_return()
                            .unwrap();

                        black_box(result);
                    }
                });
            },
        );
    }

    group.finish();
}

fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(10))
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets =
        bench_large_transform_pipeline,
        bench_large_middleware_pipeline,
        bench_short_circuit_middleware,
        bench_string_transform_workload
}

criterion_main!(benches);
