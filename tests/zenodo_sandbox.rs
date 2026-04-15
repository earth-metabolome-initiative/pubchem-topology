//! Live Zenodo sandbox integration test.
//!
//! The test is skipped when `ZENODO_SANDBOX_TOKEN` is not present.
//!
#![allow(
    clippy::expect_used,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::panic,
    clippy::unwrap_used
)]

use std::{env, fs, path::Path};

use anyhow::Result;
use pubchem_topology::{InputMode, PipelineConfig, PublishMode, run_with_config};
use tempfile::tempdir;

fn copy_fixture(destination: &Path) -> Result<()> {
    fs::copy("tests/fixtures/sample_pubchem.tsv", destination)?;
    Ok(())
}

#[test]
fn publishes_fixture_run_to_zenodo_sandbox_when_token_is_present() -> Result<()> {
    if env::var("ZENODO_SANDBOX_TOKEN").is_err() {
        eprintln!("skipping sandbox publish test because ZENODO_SANDBOX_TOKEN is not set");
        return Ok(());
    }

    let tempdir = tempdir()?;
    let compressed_path = tempdir.path().join("CID-SMILES.gz");
    let decompressed_path = tempdir.path().join("CID-SMILES.tsv");
    let parquet_path = tempdir.path().join("pubchem-topology.parquet");
    let summary_path = tempdir.path().join("pubchem-topology-summary.json");
    let infographic_path = tempdir.path().join("pubchem-topology-infographic.svg");
    let zenodo_state_path = tempdir.path().join("sandbox-state.toml");

    copy_fixture(&decompressed_path)?;

    let config = PipelineConfig {
        pubchem_url: "fixture://sample_pubchem.tsv".to_owned(),
        compressed_path,
        decompressed_path,
        parquet_path,
        summary_path,
        infographic_path: infographic_path.clone(),
        zenodo_state_path: zenodo_state_path.clone(),
        batch_size: 2,
        parquet_batch_rows: 2,
        input_mode: InputMode::UseExistingDecompressed,
        publish_mode: PublishMode::Sandbox,
    };

    let outcome = run_with_config(&config)?;
    let publication = outcome.publication.expect("sandbox run should publish");

    assert_eq!(publication.endpoint, "sandbox");
    assert!(publication.root_deposition_id > 0);
    assert!(publication.latest_deposition_id > 0);
    assert!(zenodo_state_path.exists());
    assert!(infographic_path.exists());

    Ok(())
}
