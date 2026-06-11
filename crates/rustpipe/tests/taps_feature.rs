#![cfg(feature = "taps")]

use std::sync::{Arc, Mutex};

use rustpipe::{TransformPipe, TransformPipeResult, TransformPipeline};

struct AddOne;

impl TransformPipe<u64> for AddOne {
    fn handle(&self, passable: u64) -> TransformPipeResult<u64> {
        Ok(passable + 1)
    }
}

#[test]
fn transform_taps_observe_each_successful_stage() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_in_tap = Arc::clone(&seen);

    let result = TransformPipeline::new()
        .send(0_u64)
        .tap(move |value| {
            seen_in_tap.lock().unwrap().push(*value);
        })
        .through(vec![Arc::new(AddOne), Arc::new(AddOne), Arc::new(AddOne)])
        .then_return()
        .unwrap();

    assert_eq!(result, 3);
    assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3]);
}
