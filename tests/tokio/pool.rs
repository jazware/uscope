//! Connections spawned as hyper-util's pool spawns them: through the
//! futures crate's `map_err` and `map`, boxed as a trait object. The walk
//! goes through each to the connection within: a future written by hand,
//! which is the leaf each of two tasks waits on, or an async function,
//! which the third began in and waits in.

use uscope::{ExecutionContext, FrameKind, StackFrameId, StopContext, StopReason, TaskState};

use crate::invariants::checked;
use crate::stops::line;
use crate::workers::tasks;

const BUILDS: [&str; 4] = [
    "tokio-pool-o0",
    "tokio-pool-o3",
    "tokio-pool-1.52-o0",
    "tokio-pool-1.52-o3",
];
const SOURCE: &str = "pool/src/main.rs";

#[tokio::test]
async fn pooled_connections_are_walked_to_the_connection_within() {
    for fixture in BUILDS {
        let mut scenario = checked(fixture);
        scenario.add_breakpoint("truth_reached").await;
        assert!(matches!(
            scenario.run_to_stop().await,
            StopReason::Breakpoint { .. }
        ));
        let (listed, gaps) = tasks(&scenario, 64).await;
        assert!(gaps.is_empty(), "{fixture}: {gaps:?}");
        assert_eq!(listed.len(), 3, "{fixture}: {listed:#?}");
        let image = scenario.handle().module_image().clone();
        let stop = scenario.snapshot().await.stop_id.expect("stopped");
        let (mut written, mut functions) = (0, 0);
        for task in &listed {
            assert_eq!(task.state, TaskState::Blocked, "{fixture}: {task:#?}");
            let trace = scenario
                .operation(
                    "backtrace",
                    scenario
                        .handle()
                        .at(StopContext {
                            stop,
                            execution: ExecutionContext::Task(task.id),
                            frame: StackFrameId::INNERMOST,
                        })
                        .backtrace(),
                )
                .await;
            let Some(FrameKind::Awaited { ty, .. }) = trace.frames.first().map(|frame| &frame.kind)
            else {
                panic!("{fixture}: no future awaited: {trace:#?}");
            };
            let awaited = image
                .type_info(*ty)
                .unwrap_or_else(|| panic!("{fixture}: an undescribed future: {trace:#?}"));
            let identity = awaited.identity.as_ref().expect("a Rust type's identity");
            let entry = task
                .entry
                .as_ref()
                .and_then(|entry| entry.function.as_deref());
            let async_frame = trace
                .frames
                .iter()
                .find(|frame| matches!(frame.kind, FrameKind::Async { .. }));
            if let Some(frame) = async_frame {
                // The async function, at its await of the oneshot.
                functions += 1;
                assert_eq!(&*identity.base, "Receiver", "{fixture}: {trace:#?}");
                assert!(
                    frame
                        .function
                        .as_ref()
                        .is_some_and(|function| function.name.starts_with("connection")),
                    "{fixture}: {trace:#?}"
                );
                assert_eq!(
                    frame.source.as_ref().map(|source| source.line.get()),
                    Some(line(SOURCE, "// AWAIT: request")),
                    "{fixture}: {trace:#?}"
                );
            } else {
                // The connection written by hand, past the trait object, the
                // `Map`, the `MapErr` and its `IntoFuture`, and nothing else.
                written += 1;
                assert_eq!(entry, None, "{fixture}: {task:#?}");
                assert_eq!(
                    (
                        &*identity.base,
                        identity
                            .path
                            .iter()
                            .map(AsRef::as_ref)
                            .collect::<Vec<&str>>()
                    ),
                    ("Connection", vec!["pool"]),
                    "{fixture}: {trace:#?}"
                );
            }
        }
        assert_eq!((written, functions), (2, 1), "{fixture}: {listed:#?}");
        scenario.shutdown().await;
    }
}
