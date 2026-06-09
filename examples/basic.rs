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
        .through(vec![
            Box::new(TrimStep),
            Box::new(UpperStep),
        ])
        .tap(|val| println!("Pipeline state: {}", val))
        .then(|val| format!("Final result: {}", val));

    match result {
        Ok(out) => println!("{}", out),
        Err(e) => eprintln!("Pipeline error: {}", e),
    }
}