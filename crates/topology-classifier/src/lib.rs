//! Shared graph-topology classification for SMILES strings.
//!
//! This crate exposes a browser-safe classification API used both by the
//! batch PubChem pipeline and by the Dioxus web app.
#![cfg_attr(
    test,
    allow(
        clippy::expect_used,
        clippy::missing_errors_doc,
        clippy::missing_panics_doc,
        clippy::panic,
        clippy::unwrap_used
    )
)]

use core::str::FromStr;

mod graphlets;

pub use graphlets::graphlet_svg;

use geometric_traits::traits::{
    BipartiteDetection, CactusDetection, ChordalDetection, Diameter, K4HomeomorphDetection,
    K23HomeomorphDetection, K33HomeomorphDetection, OuterplanarityDetection, PlanarityDetection,
    TreeDetection,
};
pub use smiles_parser::smiles::Smiles;
use thiserror::Error;

/// Total number of emitted topology checks.
pub const CHECK_COUNT: usize = 10;

/// Boolean topology checks emitted per molecule.
#[repr(usize)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Check {
    /// Whether the topology is a tree.
    Tree = 0,
    /// Whether the topology is a forest.
    Forest = 1,
    /// Whether the topology is a cactus graph.
    Cactus = 2,
    /// Whether the topology is chordal.
    Chordal = 3,
    /// Whether the topology is planar.
    Planar = 4,
    /// Whether the topology is outerplanar.
    Outerplanar = 5,
    /// Whether the topology contains a `K2,3` homeomorph.
    K23Homeomorph = 6,
    /// Whether the topology contains a `K3,3` homeomorph.
    K33Homeomorph = 7,
    /// Whether the topology contains a `K4` homeomorph.
    K4Homeomorph = 8,
    /// Whether the topology is bipartite.
    Bipartite = 9,
}

impl Check {
    /// Checks in the same order used by the count array and Parquet columns.
    pub const ALL: [Self; CHECK_COUNT] = [
        Self::Tree,
        Self::Forest,
        Self::Cactus,
        Self::Chordal,
        Self::Planar,
        Self::Outerplanar,
        Self::K23Homeomorph,
        Self::K33Homeomorph,
        Self::K4Homeomorph,
        Self::Bipartite,
    ];

    /// Stable column and count label for the check.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Tree => "tree",
            Self::Forest => "forest",
            Self::Cactus => "cactus",
            Self::Chordal => "chordal",
            Self::Planar => "planar",
            Self::Outerplanar => "outerplanar",
            Self::K23Homeomorph => "k23_homeomorph",
            Self::K33Homeomorph => "k33_homeomorph",
            Self::K4Homeomorph => "k4_homeomorph",
            Self::Bipartite => "bipartite",
        }
    }

    /// Human-friendly label for UI rendering.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Tree => "Tree",
            Self::Forest => "Forest",
            Self::Cactus => "Cactus",
            Self::Chordal => "Chordal",
            Self::Planar => "Planar",
            Self::Outerplanar => "Outerplanar",
            Self::K23Homeomorph => "K2,3 homeomorph",
            Self::K33Homeomorph => "K3,3 homeomorph",
            Self::K4Homeomorph => "K4 homeomorph",
            Self::Bipartite => "Bipartite",
        }
    }

    /// Short explanatory text for the check.
    pub const fn description(self) -> &'static str {
        match self {
            Self::Tree => "Connected and acyclic.",
            Self::Forest => "Acyclic, but not necessarily connected.",
            Self::Cactus => "Each edge lies on at most one simple cycle.",
            Self::Chordal => "Every cycle of length at least four has a chord.",
            Self::Planar => "Embeddable in the plane without crossings.",
            Self::Outerplanar => "Planar with all vertices on the outer face.",
            Self::K23Homeomorph => "Contains a subdivision of K2,3.",
            Self::K33Homeomorph => "Contains a subdivision of K3,3.",
            Self::K4Homeomorph => "Contains a subdivision of K4.",
            Self::Bipartite => "Contains no odd cycle.",
        }
    }

    /// Whether the check is an obstruction-detection predicate.
    pub const fn is_obstruction(self) -> bool {
        matches!(
            self,
            Self::K23Homeomorph | Self::K33Homeomorph | Self::K4Homeomorph
        )
    }
}

/// Topology data derived from a SMILES graph.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TopologyClassification {
    /// Number of connected components in the molecular graph.
    pub connected_components: u16,
    /// Graph diameter for connected molecules. `None` for disconnected graphs.
    pub diameter: Option<u16>,
    /// Topology predicates in `Check::ALL` order.
    pub checks: [bool; CHECK_COUNT],
}

impl TopologyClassification {
    /// Returns the boolean value of a specific topology predicate.
    pub const fn check(&self, check: Check) -> bool {
        self.checks[check as usize]
    }
}

/// Errors that can happen while parsing or classifying a molecular graph.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ClassificationError {
    /// The input string was not valid SMILES.
    #[error("failed to parse SMILES: {0}")]
    Parse(String),
    /// The number of connected components did not fit into `u16`.
    #[error("too many connected components to fit in u16")]
    ConnectedComponentsOverflow,
    /// The graph diameter did not fit into `u16`.
    #[error("diameter does not fit in u16")]
    DiameterOverflow,
    /// Diameter computation failed inside the graph library.
    #[error("diameter failed: {0}")]
    Diameter(String),
    /// Planarity detection failed inside the graph library.
    #[error("planarity detection failed: {0}")]
    Planarity(String),
    /// Outerplanarity detection failed inside the graph library.
    #[error("outerplanarity detection failed: {0}")]
    Outerplanarity(String),
    /// K2,3 detection failed inside the graph library.
    #[error("K2,3 detection failed: {0}")]
    K23(String),
    /// K3,3 detection failed inside the graph library.
    #[error("K3,3 detection failed: {0}")]
    K33(String),
    /// K4 detection failed inside the graph library.
    #[error("K4 detection failed: {0}")]
    K4(String),
}

/// Parses and classifies one SMILES string.
///
/// # Errors
///
/// Returns an error if the SMILES string is invalid or if one of the topology
/// algorithms fails.
pub fn classify_smiles_text(
    smiles_text: &str,
) -> Result<TopologyClassification, ClassificationError> {
    let smiles = Smiles::from_str(smiles_text)
        .map_err(|error| ClassificationError::Parse(error.to_string()))?;
    classify_smiles(&smiles)
}

/// Classifies an already parsed molecular graph.
///
/// # Errors
///
/// Returns an error if one of the topology algorithms fails or if a metric does
/// not fit in the chosen serialized representation.
pub fn classify_smiles(smiles: &Smiles) -> Result<TopologyClassification, ClassificationError> {
    let connected_components = u16::try_from(smiles.connected_components().number_of_components())
        .map_err(|_| ClassificationError::ConnectedComponentsOverflow)?;
    let diameter = if connected_components == 1 {
        Some(
            u16::try_from(
                smiles
                    .diameter()
                    .map_err(|error| ClassificationError::Diameter(error.to_string()))?,
            )
            .map_err(|_| ClassificationError::DiameterOverflow)?,
        )
    } else {
        None
    };

    let is_tree = smiles.is_tree();
    let is_forest = if is_tree { true } else { smiles.is_forest() };
    let is_cactus = if is_forest { true } else { smiles.is_cactus() };
    let is_chordal = smiles.is_chordal();
    let is_planar = smiles
        .is_planar()
        .map_err(|error| ClassificationError::Planarity(error.to_string()))?;
    let is_outerplanar = if is_planar {
        smiles
            .is_outerplanar()
            .map_err(|error| ClassificationError::Outerplanarity(error.to_string()))?
    } else {
        false
    };
    let has_k23_homeomorph = if is_outerplanar {
        false
    } else {
        smiles
            .has_k23_homeomorph()
            .map_err(|error| ClassificationError::K23(error.to_string()))?
    };
    let has_k33_homeomorph = if is_planar {
        false
    } else {
        smiles
            .has_k33_homeomorph()
            .map_err(|error| ClassificationError::K33(error.to_string()))?
    };
    let has_k4_homeomorph = if is_outerplanar {
        false
    } else {
        smiles
            .has_k4_homeomorph()
            .map_err(|error| ClassificationError::K4(error.to_string()))?
    };
    let is_bipartite = smiles.is_bipartite();

    Ok(TopologyClassification {
        connected_components,
        diameter,
        checks: [
            is_tree,
            is_forest,
            is_cactus,
            is_chordal,
            is_planar,
            is_outerplanar,
            has_k23_homeomorph,
            has_k33_homeomorph,
            has_k4_homeomorph,
            is_bipartite,
        ],
    })
}

#[cfg(test)]
mod tests {
    use super::{Check, classify_smiles_text};

    #[test]
    fn classifies_ethanol() {
        let classification = classify_smiles_text("CCO").expect("CCO should classify");

        assert_eq!(classification.connected_components, 1);
        assert_eq!(classification.diameter, Some(2));
        assert!(classification.check(Check::Tree));
        assert!(classification.check(Check::Forest));
        assert!(classification.check(Check::Cactus));
        assert!(classification.check(Check::Chordal));
        assert!(classification.check(Check::Planar));
        assert!(classification.check(Check::Outerplanar));
        assert!(classification.check(Check::Bipartite));
        assert!(!classification.check(Check::K23Homeomorph));
        assert!(!classification.check(Check::K33Homeomorph));
        assert!(!classification.check(Check::K4Homeomorph));
    }

    #[test]
    fn classifies_tetrahedrane_topology() {
        let classification =
            classify_smiles_text("*12*3*1*23").expect("tetrahedrane topology should classify");

        assert!(classification.check(Check::Planar));
        assert!(!classification.check(Check::Outerplanar));
        assert!(classification.check(Check::K4Homeomorph));
    }
}
