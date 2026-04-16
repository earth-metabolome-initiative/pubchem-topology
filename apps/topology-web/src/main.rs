//! Browser-side SMILES topology explorer built with Dioxus.

use dioxus::prelude::*;
use topology_classifier::{Check, TopologyClassification, classify_smiles_text, graphlet_svg};

const MAIN_CSS: Asset = asset!("/assets/main.css");
const DEFAULT_SMILES: &str = "CCO";
const REPOSITORY_URL: &str = "https://github.com/earth-metabolome-initiative/pubchem-topology";
const ZENODO_URL: &str = "https://doi.org/10.5281/zenodo.19599330";
const EXAMPLES: [Example; 6] = [
    Example {
        label: "Ethanol",
        smiles: "CCO",
        detail: "Tree scaffold",
        highlight: Check::Tree,
    },
    Example {
        label: "Cyclopropane",
        smiles: "C1CC1",
        detail: "Odd cycle, chordal",
        highlight: Check::Chordal,
    },
    Example {
        label: "Benzene",
        smiles: "C1=CC=CC=C1",
        detail: "Even cycle, bipartite",
        highlight: Check::Bipartite,
    },
    Example {
        label: "Naphthalene",
        smiles: "c1cccc2c1cccc2",
        detail: "Fused rings, not cactus",
        highlight: Check::Planar,
    },
    Example {
        label: "Tetrahedrane",
        smiles: "C12C3C1C23",
        detail: "Compact K4 homeomorph",
        highlight: Check::K4Homeomorph,
    },
    Example {
        label: "Cubane",
        smiles: "C12C3C4C1C5C2C3C45",
        detail: "K2,3 and K4 obstructions",
        highlight: Check::K23Homeomorph,
    },
];

#[derive(Clone, Copy, PartialEq, Eq)]
struct Example {
    label: &'static str,
    smiles: &'static str,
    detail: &'static str,
    highlight: Check,
}

fn main() {
    dioxus::launch(App);
}

#[component]
fn App() -> Element {
    let mut smiles_text = use_signal(|| DEFAULT_SMILES.to_owned());
    let mut outcome = use_signal(|| classify_input(DEFAULT_SMILES));
    let current_smiles = smiles_text();
    let current_outcome = outcome();

    rsx! {
        document::Stylesheet { href: MAIN_CSS }

        main { class: "page-shell",
            section { class: "hero",
                p { class: "eyebrow", "Earth Metabolome Initiative" }
                h1 { "Molecular topology" }
                p { class: "lede",
                    "Classify SMILES graph topological properties."
                }
                div { class: "hero-links",
                    a {
                        class: "action-link tone-planar",
                        href: REPOSITORY_URL,
                        target: "_blank",
                        rel: "noopener noreferrer",
                        i { class: "fa-brands fa-github" }
                        span { "Repository" }
                    }
                    a {
                        class: "action-link tone-k23",
                        href: ZENODO_URL,
                        target: "_blank",
                        rel: "noopener noreferrer",
                        i { class: "fa-solid fa-database" }
                        span { "Zenodo snapshot" }
                    }
                }
            }

            section { class: "workspace",
                article { class: "input-panel",
                    div { class: "panel-head",
                        div {
                            p { class: "kicker", "SMILES input" }
                            h2 { "Classify a molecular graph" }
                        }
                        button {
                            class: "primary-button",
                            r#type: "button",
                            onclick: move |_| {
                                let input = smiles_text.read().trim().to_owned();
                                outcome.set(classify_input(&input));
                            },
                            i { class: "fa-solid fa-bolt" }
                            span { "Classify" }
                        }
                    }
                    textarea {
                        class: "smiles-box",
                        rows: "4",
                        spellcheck: "false",
                        autocomplete: "off",
                        value: current_smiles,
                        oninput: move |event| smiles_text.set(event.value()),
                    }
                    p { class: "hint",
                        i { class: "fa-solid fa-circle-info" }
                        span { "Large PubChem-scale work still belongs in the batch pipeline. This utility is for quick local inspection and explanation." }
                    }
                    div { class: "example-head",
                        p { class: "kicker", "Examples" }
                        h3 { "Diverse molecules to probe" }
                    }
                    div { class: "example-grid",
                        for example in EXAMPLES {
                            button {
                                class: "example-card {tone_class(example.highlight)}",
                                r#type: "button",
                                onclick: move |_| {
                                    smiles_text.set(example.smiles.to_owned());
                                    outcome.set(classify_input(example.smiles));
                                },
                                div { class: "example-topline",
                                    GraphletFrame { check: example.highlight }
                                    div { class: "example-copy",
                                        p { class: "example-detail",
                                            i { class: "{icon_for_check(example.highlight)}" }
                                            span { "{example.detail}" }
                                        }
                                        p { class: "example-title", "{example.label}" }
                                        code { class: "example-smiles", "{example.smiles}" }
                                    }
                                }
                            }
                        }
                    }
                }

                article { class: "result-panel",
                    match current_outcome {
                        Ok(classification) => rsx! { ResultPanel { classification } },
                        Err(message) => rsx! { ErrorPanel { message } },
                    }
                }
            }
        }
    }
}

fn classify_input(smiles_text: &str) -> Result<TopologyClassification, String> {
    let trimmed = smiles_text.trim();
    if trimmed.is_empty() {
        return Err("Enter a SMILES string to classify.".to_owned());
    }

    classify_smiles_text(trimmed).map_err(|error| error.to_string())
}

fn tone_class(check: Check) -> &'static str {
    match check {
        Check::Tree => "tone-tree",
        Check::Forest => "tone-forest",
        Check::Cactus => "tone-cactus",
        Check::Chordal => "tone-chordal",
        Check::Planar => "tone-planar",
        Check::Outerplanar => "tone-outerplanar",
        Check::K23Homeomorph => "tone-k23",
        Check::K33Homeomorph => "tone-k33",
        Check::K4Homeomorph => "tone-k4",
        Check::Bipartite => "tone-bipartite",
    }
}

fn icon_for_check(check: Check) -> &'static str {
    match check {
        Check::Tree => "fa-solid fa-tree",
        Check::Forest => "fa-solid fa-seedling",
        Check::Cactus => "fa-solid fa-circle-nodes",
        Check::Chordal => "fa-solid fa-draw-polygon",
        Check::Planar => "fa-solid fa-map",
        Check::Outerplanar => "fa-solid fa-vector-square",
        Check::K23Homeomorph => "fa-solid fa-diagram-project",
        Check::K33Homeomorph => "fa-solid fa-table-cells-large",
        Check::K4Homeomorph => "fa-solid fa-cube",
        Check::Bipartite => "fa-solid fa-code-branch",
    }
}

fn graphlet_markup(check: Check) -> String {
    graphlet_svg(check)
        .replace(" aria-labelledby=\"title desc\"", "")
        .replace(" id=\"title\"", "")
        .replace(" id=\"desc\"", "")
}

#[component]
fn GraphletFrame(check: Check) -> Element {
    let markup = graphlet_markup(check);

    rsx! {
        div {
            class: "graphlet-frame",
            dangerous_inner_html: markup,
        }
    }
}

#[component]
fn ResultPanel(classification: TopologyClassification) -> Element {
    let diameter = classification
        .diameter
        .map(|value| value.to_string())
        .unwrap_or_else(|| "disconnected".to_owned());

    rsx! {
        div { class: "result-stack",
            div { class: "metric-grid",
                MetricCard {
                    label: "Connected components",
                    value: classification.connected_components.to_string(),
                    detail: "Graph components in the parsed molecule.",
                    icon: "fa-solid fa-diagram-project",
                    tone: "tone-planar",
                }
                MetricCard {
                    label: "Graph diameter",
                    value: diameter,
                    detail: "Only defined for connected graphs in this utility.",
                    icon: "fa-solid fa-ruler-horizontal",
                    tone: "tone-bipartite",
                }
            }

            section { class: "check-section",
                div { class: "section-head families-head tone-tree",
                    div { class: "section-headline",
                        i { class: "fa-solid fa-shapes" }
                        div {
                            p { class: "kicker", "Families" }
                            h2 { "Positive graph classes" }
                        }
                    }
                }
                div { class: "check-grid",
                    for check in Check::ALL.into_iter().filter(|check| !check.is_obstruction()) {
                        CheckCard {
                            check,
                            active: classification.check(check),
                        }
                    }
                }
            }

            section { class: "check-section obstruction-section",
                div { class: "section-head obstruction-head tone-k23",
                    div { class: "section-headline",
                        i { class: "fa-solid fa-road-barrier" }
                        div {
                            p { class: "kicker", "Obstructions" }
                            h2 { "Detected subdivisions" }
                        }
                    }
                }
                div { class: "check-grid",
                    for check in Check::ALL.into_iter().filter(|check| check.is_obstruction()) {
                        CheckCard {
                            check,
                            active: classification.check(check),
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn ErrorPanel(message: String) -> Element {
    rsx! {
        div { class: "error-card tone-k33",
            div { class: "error-topline",
                i { class: "fa-solid fa-triangle-exclamation" }
                p { class: "kicker", "Classification error" }
            }
            h2 { "This SMILES did not classify" }
            p { class: "error-message", "{message}" }
            p { class: "hint",
                i { class: "fa-solid fa-circle-info" }
                span { "The browser utility uses the same Rust parser and topology checks as the batch pipeline, so parser failures here should match release runs." }
            }
        }
    }
}

#[component]
fn MetricCard(
    label: &'static str,
    value: String,
    detail: &'static str,
    icon: &'static str,
    tone: &'static str,
) -> Element {
    rsx! {
        article { class: "metric-card {tone}",
            div { class: "metric-topline",
                i { class: "metric-icon {icon}" }
                p { class: "metric-label", "{label}" }
            }
            p { class: "metric-value", "{value}" }
            p { class: "metric-detail", "{detail}" }
        }
    }
}

#[component]
fn CheckCard(check: Check, active: bool) -> Element {
    let card_class = format!(
        "check-card {}{}",
        tone_class(check),
        if active { " is-active" } else { "" }
    );
    let status_class = if active {
        "check-status is-active"
    } else {
        "check-status"
    };
    let status = if active { "yes" } else { "no" };

    rsx! {
        article { class: card_class,
            div { class: "check-hero",
                GraphletFrame { check }
                div { class: "check-copy",
                    div { class: "check-topline",
                        div { class: "check-name",
                            i { class: "check-icon {icon_for_check(check)}" }
                            p { class: "check-title", "{check.label()}" }
                        }
                        span { class: status_class, "{status}" }
                    }
                    p { class: "check-detail", "{check.description()}" }
                }
            }
        }
    }
}
