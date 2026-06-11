use std::sync::Arc;

use rustpipe::{
    PipelineError, TransformPipe, TransformPipeResult, TransformPipeType, TransformPipeline,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Record {
    id: usize,
    name: String,
    tags: Vec<String>,
    score: i64,
    active: bool,
}

struct NormalizeName;

impl TransformPipe<Vec<Record>> for NormalizeName {
    fn handle(&self, mut records: Vec<Record>) -> TransformPipeResult<Vec<Record>> {
        for record in &mut records {
            record.name = record.name.trim().to_lowercase();
        }

        Ok(records)
    }
}

struct AddTag(&'static str);

impl TransformPipe<Vec<Record>> for AddTag {
    fn handle(&self, mut records: Vec<Record>) -> TransformPipeResult<Vec<Record>> {
        for record in &mut records {
            record.tags.push(self.0.to_string());
        }

        Ok(records)
    }
}

struct ScoreActiveUsers(i64);

impl TransformPipe<Vec<Record>> for ScoreActiveUsers {
    fn handle(&self, mut records: Vec<Record>) -> TransformPipeResult<Vec<Record>> {
        for record in &mut records {
            if record.active {
                record.score += self.0;
            }
        }

        Ok(records)
    }
}

struct RejectInactive;

impl TransformPipe<Vec<Record>> for RejectInactive {
    fn handle(&self, records: Vec<Record>) -> TransformPipeResult<Vec<Record>> {
        if records.iter().any(|record| !record.active) {
            Err(PipelineError::StepFailure(rustpipe::StepFailure {
                step: "RejectInactive",
                message: "inactive record found".to_string(),
            }))
        } else {
            Ok(records)
        }
    }
}

fn records(count: usize) -> Vec<Record> {
    (0..count)
        .map(|id| Record {
            id,
            name: format!(" User {id} "),
            tags: vec!["raw".to_string()],
            score: id as i64,
            active: id % 2 == 0,
        })
        .collect()
}

fn transform_stack() -> Vec<TransformPipeType<Vec<Record>>> {
    vec![
        Arc::new(NormalizeName),
        Arc::new(AddTag("normalized")),
        Arc::new(ScoreActiveUsers(10)),
    ]
}

#[test]
fn transform_pipeline_processes_large_record_batches_in_order() {
    let result = TransformPipeline::new()
        .send(records(1_000))
        .through(transform_stack())
        .then_return()
        .unwrap();

    assert_eq!(result.len(), 1_000);
    assert_eq!(result[0].name, "user 0");
    assert_eq!(result[0].tags, vec!["raw", "normalized"]);
    assert_eq!(result[0].score, 10);
    assert_eq!(result[1].name, "user 1");
    assert_eq!(result[1].score, 1);
    assert_eq!(result[999].name, "user 999");
}

#[test]
fn transform_then_maps_final_output() {
    let total_score = TransformPipeline::new()
        .send(records(100))
        .through(transform_stack())
        .then(|records| records.into_iter().map(|record| record.score).sum::<i64>())
        .unwrap();

    assert_eq!(total_score, 5_450);
}

#[test]
fn transform_conditionals_keep_pipeline_order_predictable() {
    let result = TransformPipeline::new()
        .send(records(3))
        .when(true, Arc::new(AddTag("when")))
        .when(false, Arc::new(AddTag("skipped-when")))
        .unless(false, Arc::new(AddTag("unless")))
        .unless(true, Arc::new(AddTag("skipped-unless")))
        .then_return()
        .unwrap();

    assert_eq!(result[0].tags, vec!["raw", "when", "unless"]);
}

#[test]
fn transform_finally_runs_on_error() {
    let called = Arc::new(std::sync::Mutex::new(false));
    let called_in_finally = Arc::clone(&called);

    let result = TransformPipeline::new()
        .send(records(4))
        .through(vec![Arc::new(RejectInactive)])
        .finally(move |result| {
            *called_in_finally.lock().unwrap() = result.is_err();
        })
        .then_return();

    assert!(matches!(result, Err(PipelineError::StepFailure(_))));
    assert!(*called.lock().unwrap());
}

#[test]
fn transform_rescue_does_not_hide_missing_input() {
    let result = TransformPipeline::<Vec<Record>>::new().rescue(|_| Vec::new());

    assert!(matches!(result, Err(PipelineError::InputMissing)));
}
