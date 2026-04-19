//! Browser-side SMILES topology explorer built with Dioxus.

use dioxus::prelude::*;
use dioxus_core::Task;
use serde_json::{Map, Value, json};
use std::{cell::Cell, rc::Rc};
use topology_classifier::{
    BatchEntry, BatchInputLine, Check, TopologyClassification, WorkerBatchRequest,
    WorkerBatchResponse, classify_batch_line, graphlet_svg,
};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
#[cfg(target_arch = "wasm32")]
use web_sys::{ErrorEvent, MessageEvent, Worker, WorkerOptions, WorkerType};

const MAIN_CSS: Asset = asset!("/assets/main.css");
const FAVICON_SVG: Asset = asset!("/assets/favicon.svg");
const DEFAULT_SMILES: &str = "CCO";
const CLASSIFIER_WORKER_SCRIPT: &str = "/generated/classifier-worker.js";
const REPOSITORY_URL: &str = "https://github.com/earth-metabolome-initiative/pubchem-topology";
const COMMIT_URL_PREFIX: &str =
    "https://github.com/earth-metabolome-initiative/pubchem-topology/commit/";
const ZENODO_URL: &str = "https://doi.org/10.5281/zenodo.19599330";
const LOADING_DELAY_MS: u64 = 200;
const BUILD_COMMIT: &str = match option_env!("PUBCHEM_TOPOLOGY_GIT_COMMIT") {
    Some(commit) => commit,
    None => "unknown",
};
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

#[derive(Clone, PartialEq)]
struct BatchClassification {
    entries: Vec<BatchEntry>,
}

impl BatchClassification {
    fn len(&self) -> usize {
        self.entries.len()
    }

    fn success_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.result.is_ok())
            .count()
    }

    fn error_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.result.is_err())
            .count()
    }

    fn selected(&self, index: usize) -> Option<&BatchEntry> {
        self.entries.get(index)
    }
}

#[derive(Clone, PartialEq)]
struct LoadingState {
    label: String,
    completed: usize,
    total: usize,
}

#[derive(Clone, PartialEq)]
enum BatchState {
    Empty,
    Loading(LoadingState),
    Ready(Rc<BatchClassification>),
    Fatal(String),
}

#[cfg(target_arch = "wasm32")]
struct ClassifierWorker {
    worker: Worker,
    onmessage: Closure<dyn FnMut(MessageEvent)>,
    onerror: Closure<dyn FnMut(ErrorEvent)>,
}

#[cfg(target_arch = "wasm32")]
impl ClassifierWorker {
    fn new(
        mut batch_state: Signal<BatchState>,
        request_token: Rc<Cell<u64>>,
        request_inflight: Rc<Cell<Option<u64>>>,
        loading_visible: Rc<Cell<bool>>,
        loading_timeout_id: Rc<Cell<Option<i32>>>,
    ) -> Result<Self, String> {
        let options = WorkerOptions::new();
        options.set_type(WorkerType::Module);
        let worker = Worker::new_with_options(CLASSIFIER_WORKER_SCRIPT, &options)
            .map_err(|error| format!("failed to start worker: {}", js_error_text(error)))?;

        let onmessage_request_inflight = request_inflight.clone();
        let onmessage_loading_visible = loading_visible.clone();
        let onmessage_loading_timeout_id = loading_timeout_id.clone();
        let onmessage = {
            Closure::wrap(Box::new(move |event: MessageEvent| {
                let response = serde_wasm_bindgen::from_value::<WorkerBatchResponse>(event.data());
                let response = match response {
                    Ok(response) => response,
                    Err(error) => {
                        onmessage_request_inflight.set(None);
                        onmessage_loading_visible.set(false);
                        clear_loading_timeout(&onmessage_loading_timeout_id);
                        batch_state.set(BatchState::Fatal(format!(
                            "failed to decode worker response: {error}"
                        )));
                        return;
                    }
                };
                if response.token() != request_token.get() {
                    return;
                }
                match response {
                    WorkerBatchResponse::Progress {
                        completed, total, ..
                    } => {
                        if onmessage_loading_visible.get() {
                            batch_state.set(BatchState::Loading(LoadingState {
                                label: format!("Classifying {total} SMILES"),
                                completed,
                                total,
                            }));
                        }
                    }
                    WorkerBatchResponse::Complete { entries, .. } => {
                        onmessage_request_inflight.set(None);
                        onmessage_loading_visible.set(false);
                        clear_loading_timeout(&onmessage_loading_timeout_id);
                        batch_state.set(BatchState::Ready(Rc::new(BatchClassification { entries })))
                    }
                    WorkerBatchResponse::Fatal { message, .. } => {
                        onmessage_request_inflight.set(None);
                        onmessage_loading_visible.set(false);
                        clear_loading_timeout(&onmessage_loading_timeout_id);
                        batch_state.set(BatchState::Fatal(message))
                    }
                }
            }) as Box<dyn FnMut(MessageEvent)>)
        };
        worker.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));

        let onerror_request_inflight = request_inflight.clone();
        let onerror_loading_visible = loading_visible.clone();
        let onerror_loading_timeout_id = loading_timeout_id.clone();
        let onerror = Closure::wrap(Box::new(move |event: ErrorEvent| {
            onerror_request_inflight.set(None);
            onerror_loading_visible.set(false);
            clear_loading_timeout(&onerror_loading_timeout_id);
            batch_state.set(BatchState::Fatal(format!(
                "classifier worker crashed: {}",
                event.message()
            )));
        }) as Box<dyn FnMut(ErrorEvent)>);
        worker.set_onerror(Some(onerror.as_ref().unchecked_ref()));

        Ok(Self {
            worker,
            onmessage,
            onerror,
        })
    }

    fn post(&self, message: &WorkerBatchRequest) -> Result<(), String> {
        let payload = serde_wasm_bindgen::to_value(message)
            .map_err(|error| format!("failed to encode worker request: {error}"))?;
        self.worker
            .post_message(&payload)
            .map_err(|error| format!("failed to post worker request: {}", js_error_text(error)))
    }
}

#[cfg(target_arch = "wasm32")]
impl Drop for ClassifierWorker {
    fn drop(&mut self) {
        self.worker.set_onmessage(None);
        self.worker.set_onerror(None);
        self.worker.terminate();
        let _ = &self.onmessage;
        let _ = &self.onerror;
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct ClassifierWorker;

#[cfg(not(target_arch = "wasm32"))]
impl ClassifierWorker {
    fn new(
        _batch_state: Signal<BatchState>,
        _request_token: Rc<Cell<u64>>,
        _request_inflight: Rc<Cell<Option<u64>>>,
        _loading_visible: Rc<Cell<bool>>,
        _loading_timeout_id: Rc<Cell<Option<i32>>>,
    ) -> Result<Self, String> {
        Err("worker classification is only available in the browser build".to_owned())
    }

    fn post(&self, _message: &WorkerBatchRequest) -> Result<(), String> {
        Err("worker classification is only available in the browser build".to_owned())
    }
}

fn create_worker_client(
    batch_state: Signal<BatchState>,
    request_token: Rc<Cell<u64>>,
    request_inflight: Rc<Cell<Option<u64>>>,
    loading_visible: Rc<Cell<bool>>,
    loading_timeout_id: Rc<Cell<Option<i32>>>,
) -> Result<Rc<ClassifierWorker>, String> {
    ClassifierWorker::new(
        batch_state,
        request_token,
        request_inflight,
        loading_visible,
        loading_timeout_id,
    )
    .map(Rc::new)
}

fn main() {
    dioxus::launch(App);
}

#[component]
fn App() -> Element {
    let mut smiles_text = use_signal(|| DEFAULT_SMILES.to_owned());
    let batch_state = use_signal(|| BatchState::Ready(default_batch()));
    let mut selected_index = use_signal(|| 0_usize);
    let request_token = use_hook(|| Rc::new(Cell::new(0_u64)));
    let request_inflight = use_hook(|| Rc::new(Cell::new(None::<u64>)));
    let loading_visible = use_hook(|| Rc::new(Cell::new(false)));
    let loading_timeout_id = use_hook(|| Rc::new(Cell::new(None::<i32>)));
    let worker_request_token = request_token.clone();
    let worker_request_inflight = request_inflight.clone();
    let worker_loading_visible = loading_visible.clone();
    let worker_loading_timeout_id = loading_timeout_id.clone();
    let worker_client = use_signal(move || {
        create_worker_client(
            batch_state,
            worker_request_token,
            worker_request_inflight,
            worker_loading_visible,
            worker_loading_timeout_id,
        )
    });

    let current_smiles = smiles_text();
    let build_commit_url = format!("{COMMIT_URL_PREFIX}{BUILD_COMMIT}");

    rsx! {
        document::Stylesheet { href: MAIN_CSS }
        document::Link {
            rel: "icon",
            href: FAVICON_SVG,
            r#type: "image/svg+xml",
        }

        main { class: "page-shell",
            section { class: "hero",
                p { class: "eyebrow", "Earth Metabolome Initiative" }
                h1 { "Molecular topology" }
            }

            section { class: "workspace",
                article { class: "input-panel",
                    div { class: "panel-head",
                        h2 { "Paste SMILES, one per line" }
                    }
                    div { class: "dropzone",
                        textarea {
                            id: "smiles-input",
                            class: "smiles-box",
                            rows: "10",
                            spellcheck: "false",
                            autocomplete: "off",
                            aria_describedby: "smiles-input-help",
                            value: current_smiles,
                            oninput: move |event| {
                                let value = event.value();
                                smiles_text.set(value.clone());
                                schedule_classification(
                                    value,
                                    batch_state,
                                    selected_index,
                                    request_token.clone(),
                                    request_inflight.clone(),
                                    loading_visible.clone(),
                                    loading_timeout_id.clone(),
                                    worker_client(),
                                );
                            },
                        }
                        p { id: "smiles-input-help", class: "sr-only",
                            "Paste one SMILES per line. Non-empty lines are classified as a batch, and the result panel will show one selected entry at a time."
                        }
                    }
                    div { class: "example-head",
                        h3 { "Try an example" }
                    }
                    div { class: "example-grid",
                        for example in EXAMPLES {
                            button {
                                class: "example-card {tone_class(example.highlight)}",
                                r#type: "button",
                                onclick: {
                                    let example_request_token = request_token.clone();
                                    let example_request_inflight = request_inflight.clone();
                                    let example_loading_visible = loading_visible.clone();
                                    let example_loading_timeout_id = loading_timeout_id.clone();
                                    move |_| {
                                        let next_input = example.smiles.to_owned();
                                        smiles_text.set(next_input.clone());
                                        schedule_classification(
                                            next_input,
                                            batch_state,
                                            selected_index,
                                            example_request_token.clone(),
                                            example_request_inflight.clone(),
                                            example_loading_visible.clone(),
                                            example_loading_timeout_id.clone(),
                                            worker_client(),
                                        );
                                    }
                                },
                                div { class: "example-topline",
                                    GraphletFrame { check: example.highlight }
                                    div { class: "example-copy",
                                        p { class: "example-detail",
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
                    ResultPane {
                        batch_state,
                        selected_index,
                        on_select: move |index| selected_index.set(index),
                    }
                }
            }

            footer { class: "app-footer",
                div { class: "footer-copy",
                    p { class: "footer-text",
                        "Browser UI for the PubChem topology workflow."
                    }
                }
                div { class: "footer-links",
                    a {
                        class: "footer-link tone-planar",
                        href: REPOSITORY_URL,
                        target: "_blank",
                        rel: "noopener noreferrer",
                        i { class: "fa-brands fa-github" }
                        span { "Repository" }
                    }
                    a {
                        class: "footer-link tone-k23",
                        href: ZENODO_URL,
                        target: "_blank",
                        rel: "noopener noreferrer",
                        i { class: "fa-solid fa-database" }
                        span { "Zenodo" }
                    }
                    if BUILD_COMMIT == "unknown" {
                        div { class: "footer-commit tone-bipartite",
                            i { class: "fa-solid fa-code-commit" }
                            code { "{BUILD_COMMIT}" }
                        }
                    } else {
                        a {
                            class: "footer-link footer-commit tone-bipartite",
                            href: build_commit_url,
                            target: "_blank",
                            rel: "noopener noreferrer",
                            i { class: "fa-solid fa-code-commit" }
                            code { "{BUILD_COMMIT}" }
                        }
                    }
                }
            }
        }
    }
}

fn default_batch() -> Rc<BatchClassification> {
    Rc::new(BatchClassification {
        entries: vec![classify_batch_line(BatchInputLine {
            line_number: 1,
            smiles: DEFAULT_SMILES.to_owned(),
        })],
    })
}

fn parse_smiles_lines(smiles_text: &str) -> Vec<BatchInputLine> {
    smiles_text
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let trimmed = line.trim();
            (!trimmed.is_empty()).then(|| BatchInputLine {
                line_number: index + 1,
                smiles: trimmed.to_owned(),
            })
        })
        .collect()
}

fn cancel_task(mut task_signal: Signal<Option<Task>>) {
    if let Some(task) = task_signal.take() {
        task.cancel();
    }
}

fn next_request_token(request_token: &Cell<u64>) -> u64 {
    let next = request_token.get().wrapping_add(1).max(1);
    request_token.set(next);
    next
}

fn send_worker_request(
    worker_client: Result<Rc<ClassifierWorker>, String>,
    request: &WorkerBatchRequest,
) -> Result<(), String> {
    match worker_client {
        Ok(worker_client) => worker_client.post(request),
        Err(message) => Err(message.clone()),
    }
}

fn schedule_classification(
    smiles_text: String,
    mut batch_state: Signal<BatchState>,
    mut selected_index: Signal<usize>,
    request_token: Rc<Cell<u64>>,
    request_inflight: Rc<Cell<Option<u64>>>,
    loading_visible: Rc<Cell<bool>>,
    loading_timeout_id: Rc<Cell<Option<i32>>>,
    worker_client: Result<Rc<ClassifierWorker>, String>,
) {
    let token = next_request_token(&request_token);
    request_inflight.set(Some(token));
    loading_visible.set(false);
    clear_loading_timeout(&loading_timeout_id);
    selected_index.set(0);

    let lines = parse_smiles_lines(&smiles_text);
    if lines.is_empty() {
        request_inflight.set(None);
        batch_state.set(BatchState::Empty);
        let _ = send_worker_request(worker_client, &WorkerBatchRequest::Cancel { token });
        return;
    }

    schedule_loading_timeout(
        batch_state,
        request_inflight.clone(),
        loading_visible,
        loading_timeout_id,
        token,
        lines.len(),
    );
    let request = WorkerBatchRequest::Classify { token, lines };
    if let Err(message) = send_worker_request(worker_client, &request) {
        request_inflight.set(None);
        batch_state.set(BatchState::Fatal(message));
    }
}

#[cfg(target_arch = "wasm32")]
fn clear_loading_timeout(loading_timeout_id: &Cell<Option<i32>>) {
    if let Some(timeout_id) = loading_timeout_id.take() {
        if let Some(window) = web_sys::window() {
            window.clear_timeout_with_handle(timeout_id);
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn clear_loading_timeout(_loading_timeout_id: &Cell<Option<i32>>) {}

#[cfg(target_arch = "wasm32")]
fn schedule_loading_timeout(
    mut batch_state: Signal<BatchState>,
    request_inflight: Rc<Cell<Option<u64>>>,
    loading_visible: Rc<Cell<bool>>,
    loading_timeout_id: Rc<Cell<Option<i32>>>,
    token: u64,
    total: usize,
) {
    let callback_loading_timeout_id = loading_timeout_id.clone();
    let callback = Closure::once_into_js(move || {
        callback_loading_timeout_id.set(None);
        if request_inflight.get() == Some(token) {
            loading_visible.set(true);
            batch_state.set(BatchState::Loading(LoadingState {
                label: format!("Classifying {total} SMILES"),
                completed: 0,
                total,
            }));
        }
    });
    if let Some(window) = web_sys::window() {
        if let Ok(timeout_id) = window.set_timeout_with_callback_and_timeout_and_arguments_0(
            callback.unchecked_ref(),
            LOADING_DELAY_MS as i32,
        ) {
            loading_timeout_id.set(Some(timeout_id));
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn schedule_loading_timeout(
    _batch_state: Signal<BatchState>,
    _request_inflight: Rc<Cell<Option<u64>>>,
    _loading_visible: Rc<Cell<bool>>,
    _loading_timeout_id: Rc<Cell<Option<i32>>>,
    _token: u64,
    _total: usize,
) {
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

fn classification_value(smiles_text: &str, classification: &TopologyClassification) -> Value {
    let mut checks = Map::new();
    for check in Check::ALL {
        checks.insert(
            check.name().to_owned(),
            Value::Bool(classification.check(check)),
        );
    }

    json!({
        "smiles": smiles_text.trim(),
        "connected_components": classification.connected_components,
        "diameter": classification.diameter,
        "triangle_count": classification.triangle_count,
        "square_count": classification.square_count,
        "clustering_coefficient": classification.clustering_coefficient,
        "square_clustering_coefficient": classification.square_clustering_coefficient,
        "checks": checks,
    })
}

fn batch_json(batch: &BatchClassification) -> Result<String, serde_json::Error> {
    let entries = batch
        .entries
        .iter()
        .enumerate()
        .map(|(index, entry)| match &entry.result {
            Ok(classification) => json!({
                "index": index + 1,
                "line_number": entry.line_number,
                "classification": classification_value(&entry.smiles, classification),
            }),
            Err(error) => json!({
                "index": index + 1,
                "line_number": entry.line_number,
                "smiles": entry.smiles,
                "error": error,
            }),
        })
        .collect::<Vec<_>>();

    serde_json::to_string_pretty(&json!({
        "smiles_count": batch.len(),
        "successful": batch.success_count(),
        "failed": batch.error_count(),
        "entries": entries,
    }))
}

fn format_coefficient(value: f64) -> String {
    format!("{value:.3}")
}

#[cfg(target_arch = "wasm32")]
fn js_error_text(error: JsValue) -> String {
    error
        .as_string()
        .filter(|message| !message.is_empty())
        .unwrap_or_else(|| format!("{error:?}"))
}

#[cfg(target_arch = "wasm32")]
async fn copy_text_to_clipboard(text: String) -> Result<(), String> {
    use wasm_bindgen_futures::JsFuture;

    let window = web_sys::window().ok_or_else(|| "window is unavailable".to_owned())?;
    let clipboard = window.navigator().clipboard();
    let promise = clipboard.write_text(&text);
    JsFuture::from(promise)
        .await
        .map_err(|_| "clipboard write failed".to_owned())?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn copy_text_to_clipboard(_text: String) -> Result<(), String> {
    Err("clipboard is only available in the browser build".to_owned())
}

#[component]
fn ResultPane(
    batch_state: ReadSignal<BatchState>,
    selected_index: ReadSignal<usize>,
    on_select: EventHandler<usize>,
) -> Element {
    match batch_state() {
        BatchState::Ready(batch) => rsx! {
            ResultPanel {
                batch,
                selected_index: selected_index(),
                on_select,
            }
        },
        BatchState::Loading(progress) => rsx! {
            LoadingPanel { progress }
        },
        BatchState::Fatal(message) => rsx! {
            FatalPanel {
                title: "Could not load input".to_owned(),
                message,
            }
        },
        BatchState::Empty => rsx! {
            EmptyPanel {}
        },
    }
}

#[component]
fn ResultPanel(
    batch: Rc<BatchClassification>,
    selected_index: usize,
    on_select: EventHandler<usize>,
) -> Element {
    let mut copy_feedback = use_signal(String::new);
    let mut copy_task = use_signal(|| None::<Task>);
    let total = batch.len();
    let success_count = batch.success_count();
    let error_count = batch.error_count();
    let clamped_index = selected_index.min(total.saturating_sub(1));
    let entry = batch.selected(clamped_index).cloned();
    let copy_feedback_text = copy_feedback();

    match entry {
        Some(entry) => {
            rsx! {
                div { class: "result-stack",
                    div { class: "result-toolbar",
                        div { class: "result-toolbar-main",
                            code { class: "selection-smiles", "{entry.smiles}" }
                            if error_count > 0 {
                                p { class: "result-toolbar-meta", "{success_count} ok, {error_count} errors" }
                            }
                        }
                        div { class: "result-toolbar-actions",
                            if total > 1 {
                                div { class: "batch-nav",
                                    button {
                                        class: "nav-button tone-tree",
                                        r#type: "button",
                                        disabled: clamped_index == 0,
                                        onclick: move |_| {
                                            if clamped_index > 0 {
                                                on_select.call(clamped_index - 1);
                                            }
                                        },
                                        i { class: "fa-solid fa-arrow-left" }
                                        span { "Prev" }
                                    }
                                    p { class: "nav-status", "{clamped_index + 1} of {total}" }
                                    button {
                                        class: "nav-button tone-tree",
                                        r#type: "button",
                                        disabled: clamped_index + 1 >= total,
                                        onclick: move |_| {
                                            if clamped_index + 1 < total {
                                                on_select.call(clamped_index + 1);
                                            }
                                        },
                                        span { "Next" }
                                        i { class: "fa-solid fa-arrow-right" }
                                    }
                                }
                            }
                            button {
                                class: "copy-button tone-planar",
                                r#type: "button",
                                onclick: {
                                    let batch = batch.clone();
                                    move |_| {
                                        cancel_task(copy_task);
                                        let payload = match batch_json(&batch) {
                                            Ok(payload) => payload,
                                            Err(_) => {
                                                copy_feedback.set("JSON serialization failed".to_owned());
                                                return;
                                            }
                                        };
                                        copy_feedback.set("Copying…".to_owned());
                                        let task = spawn(async move {
                                            let message = match copy_text_to_clipboard(payload).await {
                                                Ok(()) => "Copied batch JSON to clipboard".to_owned(),
                                                Err(error) => format!("Copy failed: {error}"),
                                            };
                                            copy_task.set(None);
                                            copy_feedback.set(message);
                                        });
                                        copy_task.set(Some(task));
                                    }
                                },
                                i { class: "fa-solid fa-copy" }
                                span { "Copy batch JSON" }
                            }
                            if !copy_feedback_text.is_empty() {
                                p {
                                    class: "copy-feedback",
                                    role: "status",
                                    aria_live: "polite",
                                    "{copy_feedback_text}"
                                }
                            }
                        }
                    }

                    match entry.result {
                        Ok(classification) => rsx! {
                            ClassificationPanel { classification }
                        },
                        Err(message) => rsx! {
                            FatalPanel {
                                title: "Could not classify this line".to_owned(),
                                message,
                            }
                        },
                    }
                }
            }
        }
        None => rsx! { EmptyPanel {} },
    }
}

#[component]
fn ClassificationPanel(classification: TopologyClassification) -> Element {
    let diameter = classification
        .diameter
        .map(|value| value.to_string())
        .unwrap_or_else(|| "disconnected".to_owned());
    let clustering = format_coefficient(classification.clustering_coefficient);
    let square_clustering = format_coefficient(classification.square_clustering_coefficient);

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
                    detail: "Longest shortest-path distance in the graph.",
                    icon: "fa-solid fa-ruler-horizontal",
                    tone: "tone-bipartite",
                }
                MetricCard {
                    label: "Triangle count",
                    value: classification.triangle_count.to_string(),
                    detail: "Distinct 3-cycles in the molecular graph.",
                    icon: "fa-solid fa-caret-up",
                    tone: "tone-cactus",
                }
                MetricCard {
                    label: "Square count",
                    value: classification.square_count.to_string(),
                    detail: "Distinct 4-cycles in the molecular graph.",
                    icon: "fa-solid fa-square",
                    tone: "tone-k4",
                }
                MetricCard {
                    label: "Clustering coefficient",
                    value: clustering,
                    detail: "Mean local clustering across all graph nodes.",
                    icon: "fa-solid fa-share-nodes",
                    tone: "tone-tree",
                }
                MetricCard {
                    label: "Square clustering",
                    value: square_clustering,
                    detail: "Mean square clustering across all graph nodes.",
                    icon: "fa-solid fa-border-all",
                    tone: "tone-bipartite",
                }
            }

            section { class: "check-section",
                div { class: "section-head families-head tone-tree",
                    div { class: "section-headline",
                        i { class: "fa-solid fa-shapes" }
                        h2 { "Graph classes" }
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
                        h2 { "Subdivisions" }
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
fn LoadingPanel(progress: LoadingState) -> Element {
    let percent = if progress.total == 0 {
        0.0
    } else {
        (progress.completed as f64 / progress.total as f64) * 100.0
    };

    rsx! {
        div {
            class: "loading-card tone-planar",
            aria_busy: "true",
            h2 { "{progress.label}" }
            progress {
                class: "loading-progress",
                aria_label: "Batch processing progress",
                aria_valuetext: "{progress.completed} of {progress.total} complete",
                max: "{progress.total.max(1)}",
                value: "{progress.completed}",
            }
            p { class: "loading-meta",
                "{progress.completed} / {progress.total} complete ({percent:.0}%)"
            }
        }
    }
}

#[component]
fn EmptyPanel() -> Element {
    rsx! {
        div { class: "error-card tone-planar",
            h2 { "Paste SMILES to begin" }
            p { class: "error-message",
                "The first result will appear here. Use the arrows to move through the batch."
            }
            p { class: "hint",
                i { class: "fa-solid fa-align-left" }
                span {
                    "Use one line per SMILES."
                }
            }
        }
    }
}

#[component]
fn FatalPanel(title: String, message: String) -> Element {
    rsx! {
        div {
            class: "error-card tone-k33",
            role: "alert",
            aria_live: "assertive",
            h2 { "{title}" }
            p { class: "error-message", "{message}" }
            p { class: "hint",
                i { class: "fa-solid fa-circle-info" }
                span { "Uses the same Rust parser and checks as the batch pipeline." }
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
