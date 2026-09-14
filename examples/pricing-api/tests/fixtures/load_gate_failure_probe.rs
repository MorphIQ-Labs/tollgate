//! Production-profile assurance fixture; never launches the pricing workload.

#[allow(dead_code)] // Successful measurements are intentionally unused here.
#[path = "../../src/bin/load_gate/client.rs"]
mod client;
mod load_failures;
#[path = "../../src/bin/load_gate/report.rs"]
mod report;

fn main() -> std::process::ExitCode {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    for arg in args.iter().take_while(|arg| *arg != "--") {
        match arg.to_str() {
            Some("--help" | "-h") => {
                println!(
                    "usage: load_gate_failure_probe (fixed protocol fixtures; requires --profile production)"
                );
                return std::process::ExitCode::SUCCESS;
            }
            Some("--version" | "-V") => {
                println!("load_gate_failure_probe {}", env!("CARGO_PKG_VERSION"));
                return std::process::ExitCode::SUCCESS;
            }
            _ => {}
        }
    }
    if !(args.is_empty() || args.len() == 1 && args[0] == "--") {
        eprintln!("load_gate_failure_probe: unexpected argument; use --help");
        return std::process::ExitCode::from(2);
    }
    if !cfg!(panic = "abort") {
        eprintln!("run this fixture with --profile production");
        return std::process::ExitCode::FAILURE;
    }
    run();
    std::process::ExitCode::SUCCESS
}

#[tokio::main]
async fn run() {
    let directory = std::path::Path::new("reports/load-failure-fixtures");
    load_failures::exercise(directory).await;
    println!("load failure reporting: PASS (panic=abort; five fixed failure cases)");
}
