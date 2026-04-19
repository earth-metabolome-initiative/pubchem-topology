//! Build-time metadata and generated worker assets for the topology web app.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

const WORKER_PACKAGE: &str = "topology-web-worker";
const WORKER_STEM: &str = "topology_web_worker";

/// Captures the current git commit and refreshes the dedicated wasm worker.
fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/refs");
    println!("cargo:rerun-if-changed=../../Cargo.lock");
    println!("cargo:rerun-if-changed=../../crates/topology-classifier/Cargo.toml");
    println!("cargo:rerun-if-changed=../../crates/topology-classifier/src");
    println!("cargo:rerun-if-changed=../topology-worker/Cargo.toml");
    println!("cargo:rerun-if-changed=../topology-worker/src");

    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let manifest_dir = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR")
            .ok_or_else(|| "cargo did not provide CARGO_MANIFEST_DIR".to_owned())?,
    );
    let workspace_root = manifest_dir
        .join("../..")
        .canonicalize()
        .map_err(|error| format!("failed to resolve workspace root: {error}"))?;
    let generated_dir = manifest_dir.join("public/generated");

    build_worker_assets(&workspace_root, &generated_dir)?;
    emit_git_commit();
    Ok(())
}

fn build_worker_assets(workspace_root: &Path, generated_dir: &Path) -> Result<(), String> {
    fs::create_dir_all(generated_dir)
        .map_err(|error| format!("failed to create generated worker directory: {error}"))?;

    let out_dir = PathBuf::from(
        env::var_os("OUT_DIR").ok_or_else(|| "cargo did not provide OUT_DIR".to_owned())?,
    );
    let bindgen_dir = out_dir.join("topology-worker-bindgen");
    let target_dir = out_dir.join("topology-worker-target");
    let _ = fs::remove_dir_all(&bindgen_dir);
    fs::create_dir_all(&bindgen_dir)
        .map_err(|error| format!("failed to create worker bindgen directory: {error}"))?;

    let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    let profile = env::var("PROFILE").unwrap_or_else(|_| "debug".to_owned());

    let mut build = Command::new(cargo);
    build
        .current_dir(workspace_root)
        .args([
            "build",
            "--package",
            WORKER_PACKAGE,
            "--lib",
            "--target",
            "wasm32-unknown-unknown",
            "--target-dir",
        ])
        .arg(&target_dir);

    match profile.as_str() {
        "debug" => {}
        "release" => {
            build.arg("--release");
        }
        other => {
            build.args(["--profile", other]);
        }
    }

    let status = build
        .status()
        .map_err(|error| format!("failed to launch worker cargo build: {error}"))?;
    if !status.success() {
        return Err(format!("worker cargo build failed with status {status}"));
    }

    let worker_wasm = target_dir
        .join("wasm32-unknown-unknown")
        .join(&profile)
        .join(format!("{WORKER_STEM}.wasm"));
    if !worker_wasm.exists() {
        return Err(format!("expected worker wasm at {}", worker_wasm.display()));
    }

    let mut bindgen = wasm_bindgen_cli_support::Bindgen::new();
    bindgen
        .input_path(&worker_wasm)
        .out_name(WORKER_STEM)
        .typescript(false)
        .web(true)
        .map_err(|error| format!("failed to configure worker bindgen for web output: {error}"))?
        .generate(&bindgen_dir)
        .map_err(|error| format!("worker bindgen generation failed: {error}"))?;

    copy_file(
        &bindgen_dir.join(format!("{WORKER_STEM}.js")),
        &generated_dir.join(format!("{WORKER_STEM}.js")),
    )?;
    copy_file(
        &bindgen_dir.join(format!("{WORKER_STEM}_bg.wasm")),
        &generated_dir.join(format!("{WORKER_STEM}_bg.wasm")),
    )?;
    fs::write(
        generated_dir.join("classifier-worker.js"),
        worker_loader_script(),
    )
    .map_err(|error| format!("failed to write worker bootstrap script: {error}"))?;
    Ok(())
}

fn copy_file(source: &Path, destination: &Path) -> Result<(), String> {
    fs::copy(source, destination).map_err(|error| {
        format!(
            "failed to copy generated worker asset from {} to {}: {error}",
            source.display(),
            destination.display()
        )
    })?;
    Ok(())
}

fn worker_loader_script() -> &'static str {
    r#"import init from "./topology_web_worker.js";

await init(new URL("./topology_web_worker_bg.wasm", import.meta.url));
"#
}

fn emit_git_commit() {
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
