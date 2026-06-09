use rustpipe::{Pipeline, Pipe};

struct TrimStep;
impl Pipe<String, String> for TrimStep {
    fn handle(&self, input: String) -> Result<String, String> {
        Ok(input.trim().to_string())
    }
}

struct UpperStep;
impl Pipe<String, String> for UpperStep {
    fn handle(&self, input: String) -> Result<String, String> {
        Ok(input.to_uppercase())
    }
}

fn main() {
    let result = Pipeline::new()
        .send("   hello rustpipe   ".to_string())
        .through(vec![TrimStep, UpperStep])
        .then_return();

    match result {
        Ok(out) => println!("{}", out), // "HELLO RUSTPIPE"
        Err(e) => eprintln!("Pipeline error: {}", e),
    }
}
