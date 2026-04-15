//! Stable fixture-based SVG preview generator.
//!
//! Run this ignored test when you want a persistent infographic artifact under
//! `results/` without downloading the full PubChem snapshot.
//!
#![allow(
    clippy::expect_used,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::panic,
    clippy::unwrap_used
)]

use anyhow::Result;
use pubchem_topology::{InputMode, PipelineConfig, PublishMode, run_with_config};

#[test]
#[ignore = "writes a persistent fixture preview into results/ for manual review"]
fn render_fixture_svg_preview() -> Result<()> {
    let config = PipelineConfig {
        pubchem_url: "fixture://sample_pubchem.tsv".to_owned(),
        compressed_path: "results/review/pubchem-topology-preview.gz".into(),
        decompressed_path: "tests/fixtures/sample_pubchem.tsv".into(),
        parquet_path: "results/review/pubchem-topology-preview.parquet".into(),
        summary_path: "results/review/pubchem-topology-preview-summary.json".into(),
        infographic_path: "results/review/pubchem-topology-preview.svg".into(),
        zenodo_state_path: ".zenodo/review-preview-state.toml".into(),
        batch_size: 2,
        parquet_batch_rows: 2,
        input_mode: InputMode::UseExistingDecompressed,
        publish_mode: PublishMode::Skip,
    };

    let outcome = run_with_config(&config)?;
    assert!(outcome.report.infographic_path.exists());
    Ok(())
}
