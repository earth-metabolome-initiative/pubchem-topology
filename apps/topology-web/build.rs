//! Build-time metadata for the topology web app.

use std::process::Command;

/// Captures the current git commit and exposes it to the web app.
fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/refs");

    let commit = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .map(|stdout| stdout.trim().to_owned())
        .filter(|stdout| !stdout.is_empty())
        .unwrap_or_else(|| "unknown".to_owned());

    println!("cargo:rustc-env=PUBCHEM_TOPOLOGY_GIT_COMMIT={commit}");
}
