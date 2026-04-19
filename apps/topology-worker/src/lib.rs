//! Dedicated wasm worker entrypoint for browser batch classification.

use std::cell::Cell;

use gloo_timers::future::TimeoutFuture;
use js_sys::global;
use topology_classifier::{WorkerBatchRequest, WorkerBatchResponse, classify_batch_line};
use wasm_bindgen::{JsCast, JsValue, closure::Closure, prelude::wasm_bindgen};
use wasm_bindgen_futures::spawn_local;
use web_sys::{DedicatedWorkerGlobalScope, MessageEvent};

thread_local! {
    static ACTIVE_TOKEN: Cell<u64> = const { Cell::new(0) };
}

/// Starts the dedicated topology worker message loop.
///
/// # Errors
///
/// Returns a JavaScript exception if the worker cannot register its message
/// handler.
#[wasm_bindgen(start)]
pub fn start() -> Result<(), JsValue> {
    let scope = worker_scope();
    let onmessage = Closure::wrap(Box::new(move |event: MessageEvent| {
        let request = match serde_wasm_bindgen::from_value::<WorkerBatchRequest>(event.data()) {
            Ok(request) => request,
            Err(error) => {
                let _ = post_response(&WorkerBatchResponse::Fatal {
                    token: ACTIVE_TOKEN.with(Cell::get),
                    message: format!("invalid worker request: {error}"),
                });
                return;
            }
        };

        match request {
            WorkerBatchRequest::Cancel { token } => ACTIVE_TOKEN.with(|active| active.set(token)),
            WorkerBatchRequest::Classify { token, lines } => {
                ACTIVE_TOKEN.with(|active| active.set(token));
                spawn_local(async move {
                    let total = lines.len();
                    let mut entries = Vec::with_capacity(total);

                    for line in lines {
                        if is_stale(token) {
                            return;
                        }

                        entries.push(classify_batch_line(line));
                        let completed = entries.len();
                        if post_response(&WorkerBatchResponse::Progress {
                            token,
                            completed,
                            total,
                        })
                        .is_err()
                        {
                            return;
                        }
                        TimeoutFuture::new(0).await;
                    }

                    if is_stale(token) {
                        return;
                    }

                    let _ = post_response(&WorkerBatchResponse::Complete { token, entries });
                });
            }
        }
    }) as Box<dyn FnMut(MessageEvent)>);

    scope.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
    onmessage.forget();
    Ok(())
}

fn is_stale(token: u64) -> bool {
    ACTIVE_TOKEN.with(|active| active.get() != token)
}

fn post_response(response: &WorkerBatchResponse) -> Result<(), JsValue> {
    let payload = serde_wasm_bindgen::to_value(response)
        .map_err(|error| JsValue::from_str(&format!("invalid worker response: {error}")))?;
    worker_scope().post_message(&payload)
}

fn worker_scope() -> DedicatedWorkerGlobalScope {
    global().unchecked_into::<DedicatedWorkerGlobalScope>()
}
