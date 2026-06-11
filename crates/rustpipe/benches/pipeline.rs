use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rustpipe::{
    Next, Pipe, PipeResult, PipeType, Pipeline, TransformPipe, TransformPipeResult,
    TransformPipeType, TransformPipeline,
};

#[derive(Clone)]
struct WorkItem {
    id: usize,
    tenant: String,
    payload: String,
    checksum: u64,
    accepted: bool,
    audit_count: u8,
}

struct AttachTenant;

impl Pipe<Vec<WorkItem>> for AttachTenant {
    fn handle(
        &self,
        mut passable: Vec<WorkItem>,
        next: Next<'_, Vec<WorkItem>>,
    ) -> PipeResult<Vec<WorkItem>> {
        for item in &mut passable {
            item.tenant = "tenant-a".to_string();
            item.audit_count += 1;
        }

        next.handle(passable)
    }
}

struct AcceptBatch;

impl Pipe<Vec<WorkItem>> for AcceptBatch {
    fn handle(
        &self,
        mut passable: Vec<WorkItem>,
        next: Next<'_, Vec<WorkItem>>,
    ) -> PipeResult<Vec<WorkItem>> {
        for item in &mut passable {
            item.accepted = true;
            item.audit_count += 1;
        }

        next.handle(passable)
    }
}

struct NormalizePayload;

impl TransformPipe<Vec<WorkItem>> for NormalizePayload {
    fn handle(&self, mut passable: Vec<WorkItem>) -> TransformPipeResult<Vec<WorkItem>> {
        for item in &mut passable {
            item.payload = item.payload.trim().to_ascii_uppercase();
            item.audit_count += 1;
        }

        Ok(passable)
    }
}

struct CalculateChecksum;

impl TransformPipe<Vec<WorkItem>> for CalculateChecksum {
    fn handle(&self, mut passable: Vec<WorkItem>) -> TransformPipeResult<Vec<WorkItem>> {
        for item in &mut passable {
            item.checksum = item.payload.bytes().fold(item.id as u64, |checksum, byte| {
                checksum.wrapping_mul(31) + byte as u64
            });
            item.audit_count += 1;
        }

        Ok(passable)
    }
}

fn work_items(count: usize) -> Vec<WorkItem> {
    (0..count)
        .map(|id| WorkItem {
            id,
            tenant: String::new(),
            payload: format!(" event-payload-{id} "),
            checksum: 0,
            accepted: false,
            audit_count: 0,
        })
        .collect()
}

fn bench_full_pipeline_stress(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline/full_stress");
    let size = 1_000usize;
    group.throughput(Throughput::Elements(size as u64));

    let middleware: Vec<PipeType<Vec<WorkItem>>> =
        vec![Arc::new(AttachTenant), Arc::new(AcceptBatch)];
    let transforms: Vec<TransformPipeType<Vec<WorkItem>>> =
        vec![Arc::new(NormalizePayload), Arc::new(CalculateChecksum)];

    group.bench_function("1000_items_middleware_then_transform", |b| {
        b.iter(|| {
            let middleware_output = Pipeline::new()
                .send(black_box(work_items(size)))
                .through(middleware.clone())
                .then_return()
                .unwrap();

            let output = TransformPipeline::new()
                .send(middleware_output)
                .through(transforms.clone())
                .then_return()
                .unwrap();

            black_box(output);
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
    targets = bench_full_pipeline_stress
}

criterion_main!(benches);
