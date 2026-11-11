# Optimistic Start (SDK: Speculative Blocks)

## Overview

Local activities exist to cut latency: they skip the server round trip for scheduling, dispatch and
completion. That latency win comes with a separate programming model:

* History records markers instead of activity events, so the same logic looks different depending on
  which activity flavor was used.
* No visibility while running (not in pending activities, not in describe).
* No server-side timeouts, heartbeats, cancellation or routing to other task queues.
* Long-running local activities keep the workflow task alive through heartbeats indefinitely.
* Timers in local-activity-heavy code still force a server round trip each.

This proposal lets **normal activities and timers start on the worker immediately**, before the
server knows about them, inside a scope the workflow author opts into. The server learns about them
at the next checkpoint, and history ends up containing ordinary activity and timer events. Local
activities can't be used with speculation, and are expected to be deprecated once this feature
matures.

```python
async with workflow.speculative():
    await workflow.execute_activity(activity1, ...)
    await workflow.sleep(3)
    await workflow.execute_activity(activity2, ...)
# The window ends here unless other speculative blocks keep it going.
```

The server/API concept is called **optimistic start**. SDKs expose it as **speculative blocks**. The
name "speculative" is avoided on the server side because it already refers to speculative workflow
tasks (used for updates) and the `discard_speculative_workflow_task_with_events` capability.

**Goals**

* Latency close to local activities for chains of short activities and timers.
* History indistinguishable from normal activities and timers, apart from informational flags.
* Long-running work moves to the server automatically, getting server-side timeouts, heartbeats,
  cancellation and visibility.
* Small server footprint for the MVP, reusing the eager activity code paths.

**Non-Goals (MVP)**

* Worker ownership of activities or timers across workflow task heartbeats (leases).
* Local retries of optimistic activities.
* Respecting task queue rate limits, fairness or priority for optimistic activities.
* Child workflows, Nexus operations or any command the worker can't execute itself.

## Prototype

> ⚠️ **Vibe coded.** The prototype was written almost entirely by AI coding agents to validate this
> design end to end. It hasn't been reviewed and isn't meant to be merged as is. Expect rough edges.

Each link shows the prototype's commits against the upstream commit it's based on.

| Component | Branch |
|---|---|
| API | [`bergundy/temporal-api:optimistic-start`](https://github.com/temporalio/api/compare/3c0912a20f53833700b6e1461b8cdf1f015bf43f...bergundy:temporal-api:optimistic-start) |
| api-go | [`bergundy/temporal-api-go:optimistic-start`](https://github.com/temporalio/api-go/compare/fa71e29e58da601008e728e87b4c78122bb5c79d...bergundy:temporal-api-go:optimistic-start) |
| Server | [`bergundy/temporal:optimistic-start`](https://github.com/temporalio/temporal/compare/a4fbe3f81ad282b1940c17d0e88465e2cb87c1c8...bergundy:temporal:optimistic-start) |
| Core | [`bergundy/sdk-core:optimistic-start`](https://github.com/temporalio/sdk-rust/compare/2c1ee2debb851a0183d46764ce1eee73a96b35b3...bergundy:sdk-core:optimistic-start) |
| Python | [`bergundy/temporal-sdk-python:optimistic-start`](https://github.com/temporalio/sdk-python/compare/3fd033971d884cfccc753635dc424a0f0e2a93df...bergundy:temporal-sdk-python:optimistic-start) |
| TypeScript | [`bergundy/sdk-typescript:optimistic-start`](https://github.com/temporalio/sdk-typescript/compare/eed56ef29bd355f84c72d1c5706f8459ae382846...bergundy:sdk-typescript:optimistic-start) |
| .NET | [`bergundy/temporal-sdk-dotnet:optimistic-start`](https://github.com/temporalio/sdk-dotnet/compare/98276d0c9fa15c74fc48d4b396c1c8a82a425b7f...bergundy:temporal-sdk-dotnet:optimistic-start) |
| Ruby | [`bergundy/temporal-sdk-ruby:optimistic-start`](https://github.com/temporalio/sdk-ruby/compare/c2fd140f15e2c4241611a592346b7ff6ea7377db...bergundy:temporal-sdk-ruby:optimistic-start) |

Building it:

* The server's `go.mod` replaces `go.temporal.io/api` with `../api-go`, so check out the api-go branch
  next to the server.
* Each SDK pins its own Core version, so the same Core commits are cherry-picked onto four
  `bergundy/sdk-core` branches: `optimistic-start` (Python), `optimistic-start-typescript`,
  `optimistic-start-dotnet` and `optimistic-start-ruby`. The submodule URLs still point at
  `temporalio/sdk-rust`, so fetch the pinned commit from the fork.
* The server gates the feature with the namespace-scoped dynamic config `system.enableOptimisticStart`.
  `config/development-sqlite-optimistic-start.yaml` runs a dev server with it on (frontend
  `localhost:17233`).

Pre-existing Core bugs found along the way are fixed separately in temporalio/sdk-rust#1789 (Nexus
operation cancellation) and temporalio/sdk-rust#1790 (late outcomes of abandoned activities).

## Terminology

* **Speculation window**: the time within a single workflow task (WFT) during which the worker
  runs optimistic commands without telling the server.
* **Optimistic log**: the ordered list of commands the worker sends when the window ends.
* **Checkpoint**: the WFT completion that carries the optimistic log and ends the window.
* **Handoff**: at a checkpoint, anything still in flight becomes server-owned (activities become
  started activities, timers become server timers).
* **Observed resolution**: an optimistic activity result or timer firing that was delivered to
  workflow code in an activation that completed before the checkpoint.

## Design Principles

1. **Speculation is invisible to replay.** The same code must replay against both history shapes:
   speculated, or done with normal round trips. As a result, the SDK may end a window at any moment
   for any reason, and most hard problems reduce to "checkpoint earlier".
2. **The server owns events.** The worker sends commands. The server validates them, creates events
   and assigns event IDs.
3. **Hand everything off at each checkpoint** (MVP). No server-side state is "owned by a worker
   across WFTs" beyond what eager activities already do today.

## User-Facing Semantics

* Activities on the workflow's own task queue and timers started inside a speculative block may
  start optimistically. In Python the scope follows contextvars, so tasks spawned inside the block
  are covered too.
* Whether a command actually runs optimistically is up to the SDK: an activity slot may not be
  free, the server may not support it, the window may be about to close. Workflow code behaves the
  same either way.
* **Durability**: progress inside a window is not durable until the checkpoint. If the worker
  crashes, the next worker replays from the last checkpoint and runs the activities again, without
  increasing `attempt`. This matches local activities today.
* **Block exit** ends the window unless another speculative block keeps it going (see
  [Block exit](#block-exit)). Code after the block doesn't wait for a round trip. Its commands land
  in the same batch after the block's entries, and the server applies the batch atomically and in
  order, so nothing it does can get ahead of the block's results.
* **Local activities** can't be scheduled inside a speculative block. That's a workflow task
  failure, like other API misuse. Outside a block, local activities and a speculation window never
  share a WFT (see [Local activities](#local-activities)).
* **Async activity completion** isn't supported for optimistically started activities (see
  [Edge cases](#edge-cases)).
* Signals, updates, workflow cancellation and results of non-optimistic work that arrive during a
  window are delivered at the next checkpoint (at most ~80% of the WFT timeout later).
* Queries are answered from in-memory state, including progress that hasn't been checkpointed yet
  (same as local activities).

## Lifecycle

### Speculation window

A window opens when workflow code inside a speculative block issues an eligible command and the SDK
decides to run it optimistically. While it's open, the SDK:

* Dispatches optimistic activities to the local activity worker, reserving a slot the same way
  eager activities do.
* Runs optimistic timers locally.
* Delivers resolutions back to workflow code, **one resolution per activation** (see
  [Determinism](#determinism)).
* Appends every command and every observed resolution to the optimistic log, in order.

### Checkpoint triggers

The SDK checkpoints when any of these happens:

1. A speculative block exits and the window can't continue (see [Block exit](#block-exit)).
2. Workflow code anywhere, inside or outside a speculative block, issues a command that can't be
   deferred (see below). This includes scheduling a local activity outside a block.
3. ~80% of the WFT timeout has elapsed (the same threshold used for local activity heartbeats).
4. An optimistic activity fails or times out locally.
5. A speculative command can't run optimistically (different task queue, no free slot). It becomes
   a normal command, which can't be deferred.
6. The workflow completes, fails or continues as new.

There's no payload budget. Large batches rely on paginated WFT completions, which Core already uses
when the namespace has the `workflow_task_completion_pagination` capability.

If workflow code is blocked only on optimistic work, the WFT stays open. If it's blocked only on
server-side work, the WFT completes as usual.

### Block exit

At the end of an activation in which a speculative block exited, the window continues only if:

* another speculative block is active at that point, and
* no non-speculative work is pending: no non-optimistic activities, timers, child workflows or
  Nexus operations awaiting resolution.

Otherwise the SDK checkpoints, handing off any optimistic work still in flight.

The rule is evaluated at the end of the activation, not at the moment of exit, so a loop of
sequential blocks (exit, then enter the next block in the same activation) keeps a single window
going.

Coroutines waiting on a condition (for example a signal) don't count as pending work, because the
SDK has no record of them. Events for them wait for the next checkpoint, as with any other window.

This decision doesn't need to be deterministic. It only decides when the window ends, and replay
follows what history recorded, not the decision itself.

### Local activities

Local activities can't be used with speculation:

* Scheduling a local activity inside a speculative block is an error. Scope state is workflow
  state, so this is deterministic.
* Scheduling a local activity outside a block while a window is open triggers a checkpoint first.
* While local activities are in flight, speculative commands are sent as normal commands.

### Deferrable commands

These commands join the log without triggering a checkpoint:

* `ScheduleActivityTask` and `StartTimer` with `optimistic_start`.
* `CancelTimer` and `RequestCancelActivityTask` targeting optimistic commands from the current
  window.
* `ResolveOptimisticActivity` and `FireOptimisticTimer` (new, see below).
* `RecordMarker` (side effects, patches, versions). Local activity markers can't occur in a window.
* `UpsertWorkflowSearchAttributes` and `ModifyWorkflowProperties`.

Everything else triggers a checkpoint: normal activities and timers, child workflows, Nexus
operations, external signals and cancellations, update protocol messages, and workflow completion.

### Handoff

At a checkpoint, for every optimistic command without an observed resolution in the log:

* **Activities** become started activities owned by this worker, exactly like eager activities.
  The server backdates `ScheduledTime`, `FirstScheduledTime` and `StartedTime` from the reported
  offsets, so the existing timeout timer logic arms start-to-close and schedule-to-close with the
  elapsed time already counted. The heartbeat timeout counts from the checkpoint. The response
  returns a task token for each one. The worker keeps running them and reports heartbeats and
  outcomes through the normal `RecordActivityTaskHeartbeat` and `RespondActivityTask*` RPCs.
* **Timers** become server timers with `ExpiryTime` = backdated start + duration. A timer whose
  expiry has already passed fires right away.

Outcomes the workflow did **not** observe are never sent in the log. They go through the existing
activity RPCs after handoff:

* **Failures**: the server applies retry policy exactly as for `RespondActivityTaskFailed`. The SDK
  can't reliably make that call itself because the server fills in the default retry policy from
  dynamic config.
* **Timeouts**: the SDK stops the local execution and hands the activity off. The deadline is
  backdated, so the server's normal timeout fires (immediately if it has already passed). Timeouts
  stay server-generated.
* **Results that arrived after the last activation**: the SDK completes them through
  `RespondActivityTaskCompleted`.

The SDK may request a new WFT with the checkpoint (`force_create_new_workflow_task` +
`return_new_workflow_task`) when workflow code has more runnable work.

### Cancellation

To workflow code, cancelling optimistic work inside a window works the same as cancelling normal
work. Nothing reaches the server until the checkpoint.

| What the workflow cancels | What happens |
|---|---|
| An activity or timer started and cancelled in the same activation | Dropped, as outside speculative blocks: it never runs, isn't in the batch and gets no predicted ID. The workflow sees the cancellation right away. Replay drops it the same way. |
| Optimistic timer | `CancelTimer` joins the batch (1 event: `TimerCanceled`) and the SDK stops the local timer. A firing that happened locally but wasn't delivered yet is discarded: no `FireOptimisticTimer`. |
| Optimistic activity, `TRY_CANCEL` | `RequestCancelActivityTask` joins the batch (1 event: `ActivityTaskCancelRequested`; the activity counts as started). The workflow sees the cancellation right away and the SDK cancels the local execution. The workflow never observes what the activity reports afterwards, including an outcome that arrived but wasn't delivered yet. That outcome is handed off and arrives later as a normal event. |
| Optimistic activity, `WAIT_CANCELLATION_COMPLETED` | The same cancel request. The activity's outcome joins the batch as a resolution: canceled (`ActivityTaskStarted` + `ActivityTaskCanceled`), or completed if it finished anyway. |
| Optimistic activity, `ABANDON` | No command. The activity keeps running and its outcome is handed off. |
| Work handed off at an earlier checkpoint | Normal cancellation. The cancel can't be deferred, so it ends any open window. The server delivers the cancel with the task token returned at handoff, and the SDK maps it to the local execution. |

**Event IDs.** SDKs normally drop a command that's cancelled before it reaches the server, which
is harmless because a dropped command produces no events. Optimistic commands don't reach the
server until the checkpoint, and later commands' predicted IDs already count them. So a cancelled
optimistic command stays in the batch, followed by its cancel command, and every later prediction
stays valid. A command started and cancelled in the same activation is dropped before the activation's
commands get positions, so it never had a predicted ID. Normal commands cancelled before they're sent
can still be dropped, but only after the batch's first non-deferrable command, when no further
optimistic starts or resolutions can follow. The server writes the cancellation of a normal activity
that hasn't started after the batch, so it doesn't move any IDs either.

**Invalid orderings** fail the WFT: a cancel request after the activity's resolution or a duplicate
cancel request (`BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES`), `CancelTimer` after
`FireOptimisticTimer` (`BAD_CANCEL_TIMER_ATTRIBUTES`), `FireOptimisticTimer` after `CancelTimer`
(`BAD_FIRE_OPTIMISTIC_TIMER_ATTRIBUTES`), and a resolution after a canceled resolution
(`BAD_RESOLVE_OPTIMISTIC_ACTIVITY_ATTRIBUTES`).

**Workflow closes in the same batch.** The server still returns task tokens for still-running
optimistic activities, and the SDK cancels their local executions. Cancel commands the server
sends on close carry the same tokens.

## Determinism

### The optimistic log is ordered

Putting outcomes inline on the commands loses ordering. With `gather(a1, a2)` where `a2` finishes
first, writing `A1Scheduled, A1Completed, A2Scheduled, A2Completed` would make replay resolve `a1`
first, and any code that depends on completion order would hit a nondeterminism error. The same
applies to commands generated between resolutions.

So resolutions are their own entries in the command list, placed where the workflow observed them.
The server already processes commands in sequence, so events come out in observed order.

### Event IDs are predictable

The server processes commands in order, `WorkflowTaskCompleted` is always `WorkflowTaskStarted + 1`,
each command produces a fixed number of events, and buffered events are written after the command
events. So the SDK knows every event ID during the window. The Go SDK already relies on this:
`setCurrentWorkflowTaskStartedEventID` predicts command event IDs as `WorkflowTaskStarted + 2 + n`.

For optimistic commands, the protocol fixes the event counts:

| Command | Events |
|---|---|
| `ScheduleActivityTask` (optimistic) | 1: `ActivityTaskScheduled` |
| `StartTimer` (optimistic) | 1: `TimerStarted` |
| `RequestCancelActivityTask` (optimistic activity) | 1: `ActivityTaskCancelRequested`. The activity counts as started, so no immediate `ActivityTaskCanceled`. |
| `ResolveOptimisticActivity` | 2: `ActivityTaskStarted` + `ActivityTaskCompleted`/`ActivityTaskCanceled` |
| `FireOptimisticTimer` | 1: `TimerFired` |
| `CancelTimer`, `RecordMarker`, `UpsertWorkflowSearchAttributes`, `ModifyWorkflowProperties` | 1 |

`ActivityTaskStarted` for an optimistic activity is always written together with its outcome, both
for outcomes in the batch and for outcomes reported after handoff. That's already today's behavior,
because the server always sets a retry policy on scheduled activities (`command_attr_validator.go`).
The protocol makes it explicit.

Consequences:

* Commands reference optimistic activities by `scheduled_event_id`, like `RequestCancelActivityTask`
  does today, so cancellation and the SDK state machines work the same as for normal activities.
* The server validates that each reference points to an optimistic command from the same batch. A
  wrong prediction fails the WFT loudly instead of resolving the wrong activity.
* History length could also advance during the window. The MVP keeps it frozen at the WFT's value,
  because history size and continue-as-new-suggested can't be predicted, and mixing fresh and stale
  values would be confusing.

### One resolution per activation

If two optimistic resolutions were delivered in the same activation during the original run, replay
couldn't tell that apart from two activations where the first produced no commands. Activation
grouping affects which coroutines see what, so during a window the SDK delivers **at most one
optimistic resolution per activation**. Activations are in-process and cheap.

### Workflow time

After a local `sleep(3)`, `workflow.now()` has to move forward, or loops like
`while workflow.now() < deadline` spin until the next checkpoint. This follows Core's existing
behavior for local activities, where the marker records `complete_time = wf_time_when_started +
runtime` and Core advances workflow time from it.

* The SDK measures offsets from the moment it received the WFT, on its monotonic clock.
* The server computes `workflow_time = WorkflowTaskStarted.event_time + offset` and stores it
  verbatim in a dedicated attribute on the resolution event. `event_time` stays the server's clock.
* During the original run the SDK uses the same formula, so replay reproduces it exactly.
* For deadline computation (handoff), the server clamps backdated times to `now`.

History length and continue-as-new-suggested stay those of the current WFT throughout a window, both
during the original run and on replay.

### SDK-produced non-command events

Today SDKs split history into two classes:

* **Command events** are produced by the SDK and matched against the commands its activations
  generated.
* **All other events** are produced by the server and become jobs in the activation that follows
  the next WFT boundary.

This design adds a third class: **non-command events produced by the SDK**. These are the events
written from `ResolveOptimisticActivity` and `FireOptimisticTimer`:

* `ActivityTaskStarted` + `ActivityTaskCompleted`/`ActivityTaskCanceled`
* `TimerFired`

The workflow already observed them during the WFT that produced the batch, so they must **not**
become jobs after the next boundary. They're identified by an explicit flag, not by position:

* `optimistic_resolution` on the outcome events, which also carries `workflow_time`.
* `optimistic_resolution` on the `ActivityTaskStarted` written with them.

Position alone is ambiguous. A resolution at the end of the batch with no commands after it could
have been observed in this WFT, or could be a normal event for the next WFT. Mixing those up changes
whether it's grouped with the next WFT's signals in one activation.

### Replay algorithm

This follows the mechanism Core already uses for local activities, not a pause in the middle of
applying history.

How Core works today:

* `find_end_index_of_next_wft_seq` (`history_update.rs`) ends a sequence at a `WorkflowTaskStarted`
  followed by `WorkflowTaskCompleted`. So a sequence is: `WorkflowTaskCompleted`, the command events
  for the previous WFT's activations, server events, `WorkflowTaskScheduled`, `WorkflowTaskStarted`.
* `apply_next_wft_from_history` (`workflow_machines.rs`) applies a whole sequence at a time. Command
  events are matched in order against the queue of commands from **all** activations of the
  previous WFT. Other events become jobs for the next activation.
* Local activities already run several activations within one WFT. After applying a sequence, Core
  peeks into the **next** sequence for local activity markers and stores them as pre-resolutions.
  When the workflow schedules the local activity, Core resolves it right away, which causes another
  activation in the same WFT (`apply_local_activity_peeked_resolutions`; its doc comment calls out
  preserving activation grouping). The marker command events are matched against the accumulated
  queue later, when that next sequence is applied.

Optimistic resolutions plug into the same lookahead:

1. **Peek**: while peeking the next sequence, collect events flagged with `optimistic_resolution`
   as pre-resolutions. Record each one's key and how many command events come before it in the
   sequence. The key is the scheduled event ID for activities and the timer ID for timers.
2. **Key on predicted IDs**: when the workflow schedules a speculative activity during replay, Core
   assigns the same predicted event ID it assigned in the original run (see
   [Event IDs are predictable](#event-ids-are-predictable)). That's what lets the pre-resolution
   find its machine.
3. **Deliver in order, one per activation**: after each activation completes, the next
   pre-resolution R is delivered in a new activation, with workflow time advanced to R's
   `workflow_time`. The workflow must by then have produced exactly as many commands in this WFT
   as there are command events before R. Any other count is a nondeterminism error. Nothing else
   can drive an activation in the middle of a WFT during replay (local activities can't coexist
   with a window), so "fewer" can't be fixed by waiting.

   This check is what guarantees each command is produced in the same activation, relative to each
   resolution, as in the original run.
4. **Apply without new jobs**: when the next sequence is applied, command events are matched
   against the queue as today. Flagged events are fed to machines that have already resolved, so
   they produce no jobs. Local activity markers work the same way via `will_accept_resolve_marker`.
5. Unflagged events (handed-off outcomes, signals, and so on) are untouched and become jobs after
   the boundary as usual.

**Original execution uses the same path.** After a checkpoint, the next WFT's history contains the
batch Core itself produced. Core matches the command events and applies the flagged events to
machines that already resolved locally.

**Sequence boundaries don't change.** Batches still sit between a `WorkflowTaskCompleted` and the
next `WorkflowTaskScheduled`. A batch with resolutions always contains the commands they refer to,
so WFT-heartbeat detection (`saw_command`) isn't affected either.

**Other places in Core that assume "non-command means server-produced"** have to treat flagged events
as SDK-produced. Notably, the `replaying` flag flips off at the first non-command event of the last
WFT. Without the change, replay would be marked over in the middle of our own batch.

**Older SDKs fail loudly.** They treat flagged events as ordinary jobs for the next activation.
They then reach a command event (e.g. `TimerStarted`) whose command doesn't exist yet and fail with
"No command scheduled for event", rather than replaying wrongly. Workflow code that uses speculative
blocks needs a new SDK anyway.

**Go and Java** are out of scope for now.

### Rejected alternative: synthetic WFT boundaries

The server could insert `WorkflowTaskScheduled/Started/Completed` events between activations so
that old replayers work unchanged. Rejected because it changes event IDs, history length and
continue-as-new-suggested compared with what the code saw during the window, which breaks the common
"continue as new after N events" pattern. It also adds 3 events per step.

## Example

```python
async with workflow.speculative():
    a1 = workflow.start_activity(act1, ...)
    a2 = workflow.start_activity(act2, ...)
    await asyncio.wait([a1, a2])          # a2 finishes first, then a1
    await workflow.sleep(3)
    await workflow.execute_activity(act3, ...)  # still running at the 80% deadline
```

Optimistic log sent at the checkpoint (offsets relative to receiving WFT 3). The SDK predicts the
event IDs in brackets:

```
[5]     ScheduleActivityTask      act1  optimistic_start{start_offset=10ms}
[6]     ScheduleActivityTask      act2  optimistic_start{start_offset=10ms}
[7,8]   ResolveOptimisticActivity scheduled_event_id=6 completed  resolve_offset=200ms
[9,10]  ResolveOptimisticActivity scheduled_event_id=5 completed  resolve_offset=350ms
[11]    StartTimer                timer_id=3 3s  optimistic_start{start_offset=351ms}
[12]    FireOptimisticTimer       timer_id=3     fire_offset=3351ms
[13]    ScheduleActivityTask      act3  optimistic_start{start_offset=3352ms}
```

Resulting history (`ActivityTaskStarted` is written with the outcome):

```
 1 WorkflowExecutionStarted
 2 WorkflowTaskScheduled
 3 WorkflowTaskStarted                 (time T)
 4 WorkflowTaskCompleted
 5 ActivityTaskScheduled  act1         optimistic_start{start_time=T+10ms}
 6 ActivityTaskScheduled  act2         optimistic_start{start_time=T+10ms}
 7 ActivityTaskStarted    act2
 8 ActivityTaskCompleted  act2         optimistic_resolution{workflow_time=T+200ms}
 9 ActivityTaskStarted    act1
10 ActivityTaskCompleted  act1         optimistic_resolution{workflow_time=T+350ms}
11 TimerStarted           3            optimistic_start{start_time=T+351ms}
12 TimerFired             3            optimistic_resolution{workflow_time=T+3351ms}
13 ActivityTaskScheduled  act3         optimistic_start{start_time=T+3352ms}  <- handed off, task token returned
   ... later, via RespondActivityTaskCompleted:
14 ActivityTaskStarted    act3
15 ActivityTaskCompleted  act3
16 WorkflowTaskScheduled
```

Replay of WFT 3. Core peeks the next sequence (events 4–13, up to the next `WorkflowTaskStarted`)
and finds three pre-resolutions:

| Event | Resolves | Command events before it |
|---|---|---|
| 8 | activity scheduled at 6 | 2 |
| 10 | activity scheduled at 5 | 2 |
| 12 | timer 3 | 3 |

| Activation | Delivered | Commands produced | Total commands |
|---|---|---|---|
| 1 | — | `Schedule act1` (predicted 5), `Schedule act2` (predicted 6) | 2 |
| 2 | event 8 | none | 2 |
| 3 | event 10 | `StartTimer` (predicted 11) | 3 |
| 4 | event 12 | `Schedule act3` (predicted 13) | 4 |

When the next sequence is applied, command events 5, 6, 11 and 13 match the queued commands. Events
7–10 and 12 are applied to already-resolved machines and produce no jobs. Events 14–15 arrive later
unflagged, and become jobs in the next WFT's activation.

## API Changes (sketch)

```protobuf
// temporal/api/command/v1/message.proto

// Set on a command the worker already started executing inside a speculation window.
message OptimisticStart {
    // Time between the worker receiving the current workflow task and starting this command,
    // measured on the worker's monotonic clock.
    google.protobuf.Duration start_offset = 1;
    // ScheduleActivityTask only: details from the latest local heartbeat, if any.
    temporal.api.common.v1.Payloads heartbeat_details = 2;
}

message ScheduleActivityTaskCommandAttributes {
    // ...
    OptimisticStart optimistic_start = 15;
}

message StartTimerCommandAttributes {
    // ...
    OptimisticStart optimistic_start = 3;
}

// Records an outcome the workflow observed for an activity started optimistically earlier in
// the same command list. Produces ActivityTaskStarted followed by the outcome event.
message ResolveOptimisticActivityCommandAttributes {
    message Completed { temporal.api.common.v1.Payloads result = 1; }
    // Only valid after a RequestCancelActivityTask for the same activity.
    message Canceled { temporal.api.common.v1.Payloads details = 1; }

    // The ACTIVITY_TASK_SCHEDULED event ID, predicted by the worker.
    int64 scheduled_event_id = 1;
    oneof outcome {
        Completed completed = 2;
        Canceled canceled = 3;
    }
    google.protobuf.Duration resolve_offset = 4;
}

// Records that a timer started optimistically earlier in the same command list fired and the
// workflow observed it.
message FireOptimisticTimerCommandAttributes {
    string timer_id = 1;
    google.protobuf.Duration fire_offset = 2;
}

// Command.attributes oneof
ResolveOptimisticActivityCommandAttributes resolve_optimistic_activity_command_attributes = 20;
FireOptimisticTimerCommandAttributes fire_optimistic_timer_command_attributes = 21;

// temporal/api/enums/v1/command_type.proto
COMMAND_TYPE_RESOLVE_OPTIMISTIC_ACTIVITY = 19;
COMMAND_TYPE_FIRE_OPTIMISTIC_TIMER = 20;
```

```protobuf
// temporal/api/history/v1/message.proto

// Set on resolution events written from ResolveOptimisticActivity / FireOptimisticTimer.
// SDKs apply these within the current workflow task during replay.
message OptimisticResolution {
    // The workflow task's started time plus the offset reported by the worker. SDKs advance
    // workflow time to this value when applying the event.
    google.protobuf.Timestamp workflow_time = 1;
}

// Set on scheduled/started events of commands the worker started optimistically.
message OptimisticStartInfo {
    // The workflow task's started time plus the reported start offset, capped at the time the
    // server processed the command. Activity scheduled/started times, timeouts and timer expiry
    // count from this time, so state rebuilt from events matches the active cluster.
    google.protobuf.Timestamp start_time = 1;
}

message ActivityTaskScheduledEventAttributes { /* ... */ OptimisticStartInfo optimistic_start = 15; }
message TimerStartedEventAttributes          { /* ... */ OptimisticStartInfo optimistic_start = 4; }
message ActivityTaskCompletedEventAttributes { /* ... */ OptimisticResolution optimistic_resolution = 6; }
message ActivityTaskCanceledEventAttributes  { /* ... */ OptimisticResolution optimistic_resolution = 7; }
message TimerFiredEventAttributes            { /* ... */ OptimisticResolution optimistic_resolution = 3; }
// Set on the started event written by ResolveOptimisticActivity, so SDKs can classify it as
// SDK-produced without looking at the following event.
message ActivityTaskStartedEventAttributes   { /* ... */ bool optimistic_resolution = 8; }
```

```protobuf
// temporal/api/workflowservice/v1/request_response.proto
message RespondWorkflowTaskCompletedResponse {
    // ...
    // Activities started optimistically in this task that are still running and are now owned
    // by the server. The worker keeps executing them and uses these task tokens for heartbeats
    // and completion.
    repeated PollActivityTaskQueueResponse optimistic_activity_tasks = 4;
}

// temporal/api/namespace/v1/message.proto (NamespaceInfo.Capabilities)
bool optimistic_start = 18;
```

A separate response field is used instead of the existing `activity_tasks`, so handed-off
activities can't be mistaken for new eager tasks to start.

## Server Changes

All gated by a per-namespace dynamic config flag, which also drives the namespace capability.

**History builder: buffering is decided by event type.** The server makes the same assumption as
the SDKs, and it lives in `EventStore.bufferEvent` (`historybuilder/event_store.go`):

* Event types produced from commands are appended immediately and get their IDs right away.
* Every other type, including `ActivityTaskStarted`, `ActivityTaskCompleted`,
  `ActivityTaskCanceled` and `TimerFired`, **always** goes to the buffer, whether or not a WFT is
  in flight.

`FlushBufferToCurrentBatch` runs at the end of the completion transaction, once no WFT is started.
It then:

1. Moves all activity, child workflow and Nexus completions to the end of the buffered events
   (`reorderBuffer`).
2. Allocates event IDs.
3. Fills `started_event_id` on completion events (`wireEventIDs`).
4. Flushes signals that arrived during the window (already in the DB buffer) into the same batch,
   ahead of the reordered completions.

If the new command handlers called the existing `Add*` methods unchanged, the example would come
out as `5 Sched act1, 6 Sched act2, 7 TimerStarted, 8 Sched act3`, followed by any signals, the
started events, `TimerFired`, and finally both completions. All interleaving would be lost, every
predicted ID would be wrong, and a signal the workflow hadn't seen would land before resolutions it
had already seen.

The required changes:

* **Decide buffering per event, not per type.** Events written from `ResolveOptimisticActivity`
  and `FireOptimisticTimer` skip the buffer: their IDs are allocated immediately and they're
  appended to the current batch in order. The natural hook is for `bufferEvent` to check the
  `optimistic_resolution` flag that the event factory sets, which is the same flag SDKs use.
* **Wire `started_event_id` directly.** `wireEventIDs` won't run for these events. Today
  `AddActivityTaskCompletedEvent` passes the transient placeholder as `started_event_id` and relies
  on the flush to fix it (`mutable_state_impl.go`, `addStartedEventForTransientActivity`). The
  optimistic path must pass the ID of the started event it just wrote.

**What already works.**

* **New WFT decision and `UNHANDLED_COMMAND`**: both use `hasBufferedEventsOrMessages`, which is
  computed before commands are processed (`respondworkflowtaskcompleted/api.go`). Events that skip
  the buffer never affect either. `closeTransactionHandleWorkflowTaskScheduling` only looks at
  state machine events.
* **Mutable state rebuilder** (`mutable_state_rebuilder.go`): applies events one at a time by
  type, with no assumption about what precedes command events. Batches already mix
  `WorkflowTaskCompleted`, command events and flushed non-command events today.
* **Activities handed off at a checkpoint**: started but with no started event in history yet, the
  same state as eager activities or any activity with a retry policy. Standby clusters learn it
  through activity sync replication.
* **Backdated start times**: `addStartedEventForTransientActivity` already sets the started event's
  time from `ActivityInfo.StartedTime`, so they show up in history with no extra work.
* **Rebuilt and standby state**: the backdated start time is recorded on the scheduled/started
  event (`OptimisticStartInfo.start_time`), and event-driven paths use it instead of the event time.
  Without it, state rebuilt from events (admin rebuild, standby clusters) would fire handed-off
  timers late by up to the window length, since timers have no sync replication.
* **Paused workflows**: the server already rejects `force_create_new_workflow_task` with
  `FailedPrecondition` before touching mutable state (`api.go`), and never schedules a follow-up
  WFT while paused. The decided rule is existing behavior, so no new failure cause is needed.

**Command handling** (`workflow_task_completed_handler.go`):

* `ScheduleActivityTask` with `optimistic_start`:
  * Validate: capability enabled, task queue is the workflow's normal task queue.
  * Add the scheduled event with task generation bypassed (as eager does) and backdate the
    scheduled times.
  * Record the start like `handlePostCommandEagerExecuteActivity`: workflow build ID, worker
    control task queue, backdated `StartedTime`. The `ActivityTaskStarted` event is always
    deferred until the outcome (see [Event IDs are predictable](#event-ids-are-predictable)).
* `StartTimer` with `optimistic_start`: add the started event. Defer timer task generation to the
  end of the batch.
* `RequestCancelActivityTask` targeting an optimistic activity: record the cancel request without
  dispatching a worker command, since the worker already knows.
* `ResolveOptimisticActivity`:
  * `scheduled_event_id` must point to an activity started optimistically in this batch and not
    yet resolved.
  * `completed` → started event + completed event with `optimistic_resolution`.
  * `canceled` → requires an earlier cancel request. Started event + canceled event.
* `FireOptimisticTimer`: the timer must have been started optimistically in this batch. Write the
  fired event with `optimistic_resolution`.
* Validate that offsets are non-negative and non-decreasing across the log.
* Events written from these commands must not schedule a new WFT, because the worker has already
  seen them.
* **Paused workflows**: nothing new on the server (see above). When the completion is rejected,
  the worker drops the log and cancels its local executions, and after unpause the work runs again
  from the last checkpoint. Activities handed off from an accepted batch keep running, like any
  activity that was already running when the workflow was paused.

**Post-command handoff**:

* Optimistic activities that are still pending: build task tokens (reusing the eager token code),
  return them in `optimistic_activity_tasks`, and set the last heartbeat to now. The existing timer
  sequence arms timeouts from the backdated times.
* Optimistic timers that haven't fired: generate the user timer task with the backdated expiry.

**Observability**: metrics for optimistic activities and timers that are started, resolved and
handed off. The `optimistic_start` event fields allow a UI badge.

## SDK Changes

### Core

* **Lang → Core**: a `speculative` flag on `ScheduleActivity` and `StartTimer`. On activation
  completion, whether a speculative block exited during the activation and whether any block is
  still active. Core applies the [Block exit](#block-exit) rule using its own record of pending
  non-optimistic commands.
* **Eligibility**: namespace capability, task queue match, eager slot reservation, no local
  activities in flight and remaining window time. If a command isn't eligible,
  it's sent as a normal command, which triggers a checkpoint.
* **Event ID prediction**: assign predicted IDs to optimistic commands as they enter the log, so
  `RequestCancelActivityTask` and `ResolveOptimisticActivity` can reference them.
* **Local dispatch**: push optimistic activities into the activity task stream with a synthetic
  task token, so lang activity workers don't change. Route heartbeats and completions inside Core
  until handoff. After handoff, map the synthetic token to the real one.
* **Window runtime**: local timers, one resolution per activation, `UpdateWFTime` on each
  resolution, building the log, checkpoint triggers, and the 80% deadline (reusing the
  local activity heartbeat timer).
* **Handoff**: attach real tokens from `optimistic_activity_tasks`. Send outcomes that weren't
  observed through the normal RPCs.
* **WFT rejected or workflow closed**: cancel locally running optimistic activities and drop the
  log. The WFT retry replays from the last checkpoint.
* **Replay**: the algorithm above.

### Python

* `workflow.speculative()` is an async context manager.
* The contextvar points to a scope object with an `active` flag, not a bool. Exiting the block
  deactivates the scope for tasks spawned inside it that are still running.
* Sets `speculative` on activity and timer commands issued while the scope is active.
* `__aexit__` deactivates the scope and records the exit for the activation completion. It doesn't
  await anything.
* Scheduling a local activity while a scope is active fails the workflow task.

### Other SDKs

TypeScript, .NET and Ruby get most of this through Core and add the scope API (a
`CancellationScope`-like construct in TypeScript). Go and Java would need their own runtime and
replay work, and are out of scope for now.

### Scope API per SDK

| SDK | API |
|---|---|
| Python | `async with workflow.speculative(): ...` |
| TypeScript | `await speculative(async () => { ... })`, shorthand for `new SpeculativeScope().run(fn)` (`@experimental`) |
| .NET | `await Workflow.SpeculativeAsync(async () => { ... })`, plus a `Func<Task<T>>` overload |
| Ruby | `Temporalio::Workflow.speculative { ... }`, returning the block's value |

Rules shared by all SDKs:

* A scope is active until it exits, and after that for as long as its enclosing scope is active.
  Work started in an inner block that outlives it stays speculative while an outer block is still
  active. Only work that outlives every enclosing block is sent normally.
* Activities (not local activities or child workflows) and timers (sleeps, timer-backed timeouts
  and condition timeouts) are flagged when they're created inside an active scope.
* Each activation completion reports whether a block exited during it and whether any block is
  still active.
* A local activity scheduled inside an active scope fails the workflow task, even if workflow code
  catches the error.
* Exiting a block never waits for anything.

## Edge Cases

* **Async activity completion**: not supported for the MVP. An optimistic activity only has a
  synthetic token, which an external completer can't use. If an optimistic activity asks to
  complete asynchronously, the SDK fails the attempt with a descriptive error. That goes through
  the normal failure handoff: retry policy applies, and retries are dispatched normally, so they
  can complete asynchronously.
* **Heartbeats**: local during the window. The latest details travel with the handoff.
* **Payload limits**: large batches rely on paginated WFT completions
  (`workflow_task_completion_pagination`), which Core already uses when the namespace supports
  them. Without pagination, a batch over the gRPC limit fails the WFT. Per-payload
  blob limits still apply. A result over the blob limit is handed off and rejected through the
  normal completion RPC, as it would be for any activity.
* **Event count**: the same as normal activities. There are fewer WFT events than with round
  trips, but more events than local activity markers.
* **Matching bypass**: optimistic activities skip task queue rate limits, fairness and priority.
  Accepted for the MVP; local activities skip these too.
* **Versioning**: optimistic activities implicitly use the workflow's build ID, since they run on
  the same worker.
* **`UNHANDLED_COMMAND`**: if a signal arrives during a window and the checkpoint also completes the
  workflow, the server rejects the whole completion as it does today. The worker drops the log and
  cancels its local executions. Everything since the last checkpoint runs again on replay,
  activities included, without increasing `attempt`. That's the same exposure as local activities
  today. The SDK must not resend the old batch in the next WFT, because the workflow saw the old
  WFT's frozen history length during the window.

## Decisions (2026-11-05)

* Hand everything off at each checkpoint. No worker ownership across WFT heartbeats in the MVP.
* A command that can't be deferred, from anywhere in the workflow, forces a checkpoint. Concurrent
  code doesn't disable speculation.
* Skipping task queue rate limits, fairness and priority is acceptable for the MVP.
* Naming: "optimistic start" in the API and server, `workflow.speculative()` in SDKs.

## Decisions (2026-11-09)

* Event IDs are predictable. Optimistic commands are referenced by `scheduled_event_id`, with no
  `activity_id` alternative.
* Block exit ends the window unless another speculative block is active and no non-speculative work
  is pending. There's no awaited barrier.
* Paused workflows: a batch is accepted only if the completion doesn't request a new WFT. The
  server already behaves this way.
* Local activities can't be used with speculation, and are expected to be deprecated once this
  matures.
* Async activity completion is unsupported to start.
* `UNHANDLED_COMMAND` keeps today's behavior: the server rejects the entire WFT completion,
  optimistic batch included.

## Decisions (2026-11-10)

* The backdated start time of optimistic activities and timers is recorded in history
  (`OptimisticStartInfo.start_time` on `ActivityTaskScheduled` and `TimerStarted`), so state rebuilt
  from events matches the active cluster.
* No payload budget. Large batches rely on paginated WFT completions
  (`workflow_task_completion_pagination`).
* No free slot: the command falls back to a normal command. There's no waiting for a slot.
* Invalid optimistic logs fail the WFT with new per-command causes,
  `BAD_RESOLVE_OPTIMISTIC_ACTIVITY_ATTRIBUTES` (41) and `BAD_FIRE_OPTIMISTIC_TIMER_ATTRIBUTES` (42).
  Bad `optimistic_start` fields use `BAD_SCHEDULE_ACTIVITY_ATTRIBUTES` /
  `BAD_START_TIMER_ATTRIBUTES`, and using optimistic start without the namespace capability fails
  with `FEATURE_DISABLED`.
* Eligibility is decided per activation, all or nothing.
* Activity pollers hold slot permits, as with eager activities, so a worker with very few activity
  slots may never start anything optimistically.
* If the workflow closes in the same batch, the server still returns task tokens for optimistic
  activities that are still running, and the SDK maps them to its local executions.

* Starting and cancelling an activity or timer in the same activation drops the command, as
  outside speculative blocks.
* When an activation's speculative commands fall back to normal commands, they're all counted under
  the one fallback reason that blocked them.
* A duplicate cancel request fails the WFT in Core, as it does today for normal activities.
* Core holds lang's activity completion for as long as it's needed, and no longer.

## Related Bugs

* **Late outcome of an abandoned activity** (pre-existing in Core, affects normal activities):
  after a cancel with `ABANDON`, Core's activity state machine is in `Canceled`, which only accepts
  `ActivityTaskStarted` and `ActivityTaskCompleted`. A later `ActivityTaskFailed` or
  `ActivityTaskTimedOut` (non-retryable failure or retries exhausted) has no transition. To be fixed
  in a separate PR.
  Reproduced for `ActivityTaskFailed` and `ActivityTaskTimedOut` after an abandon from
  `ScheduledEventRecorded` or `Started`, live and on replay. The WFT fails with a fatal "invalid
  transition" error (not nondeterminism), so the workflow is stuck. `ActivityTaskCanceled` can't
  occur (the server rejects `RespondActivityTaskCanceled` without a cancel request). A workflow
  reset (`failInflightActivity`) can also write the late `ActivityTaskFailed`. Fixed locally in
  commit `b0674319` on `fix-abandoned-activity-late-outcome` (on sdk-rust `main` 4e751813), with unit,
  replay and integration tests. Draft PR: temporalio/sdk-rust#1790.
* **Nexus operation cancellation** (pre-existing in Core, confirmed): three bugs, fixed locally in
  commit `916867a3` on `fix-nexus-operation-abandon` (on sdk-rust `main` 4e751813), separate from the
  activity fix. Draft PR: temporalio/sdk-rust#1789.
  * The `ABANDON` check in `Cancelled` was inverted (`!` dropped when copied from the child workflow
    machine in #927), so a late completed/failed/timed-out event after abandoning a started operation
    failed every WFT with a nondeterminism error.
  * Abandon or TryCancel before the operation started resolved lang but stayed in
    `ScheduledEventRecorded`, so the later start or outcome resolved lang a second time (TS throws,
    Python raises, Rust panics, .NET throws). Now it resolves with a failed
    `ResolveNexusOperationStart` and moves to `Cancelled`.
  * WaitCancellationRequested with a failed cancel request resolved lang but stayed in `Started`, so
    the eventual outcome resolved lang a second time.
* **Java test server (time-skipping v1) rejects cancelling a timer that fired during the WFT**
  (pre-existing, reported by the Python work and reproduced there without speculative blocks): a
  workflow task that cancels a timer which fired while the task was running fails. Not
  investigated further.

## Open Questions

None at the moment.

## Implementation Notes (prototype, 2026-11-10)

Choices the prototype made where this doc was silent, and known gaps. See [Prototype](#prototype)
for the branches.

**Core**

* A window opens, and stays open, only while every command in the batch is deferrable. Eligibility
  is decided per activation, all or nothing: if one speculative command can't run optimistically
  (another task queue, not enough free slots), every command from that activation goes out normally.
  This keeps event ID prediction simple.
* New optimistic starts stop when less than 10% of the WFT timeout remains before the 80% deadline.
* The window also checkpoints by the earliest start-to-close or schedule-to-close deadline of a
  running optimistic activity. After handoff, Core arms the same local timeouts it runs for polled
  activities (start-to-close and schedule-to-close from the backdated start, heartbeat from the
  handoff), so a handed-off activity is cancelled locally when the server would time it out.
  Server-driven cancellation (heartbeat responses, activity not found, worker-control cancels)
  reaches the local execution through the task token mapping.
* Core holds lang's `complete_activity_task` call for an observable result until its fate is known,
  and no longer: until the workflow observes it (it travels in the batch, no RPC), it's handed off
  (Core then sends the RPC with the real task token, which only exists after the checkpoint
  response), or it's dropped. Until then Core can't tell whether an RPC is needed, and it doesn't
  have the token to send one. Decided on 2026-11-10 to keep this.
* Optimistic activity tasks get the same timeout defaults the server applies (schedule-to-close,
  heartbeat, the cap at the workflow run timeout), so lang sees the same activity info either way.
* Slots work like eager activities: pollers hold slot permits, so with very few activity slots
  nothing starts optimistically. Workers with a task queue activity rate limit never start
  optimistically.
* A local activity scheduled while a window is open checkpoints with
  `force_create_new_workflow_task`, like a local activity heartbeat, and the local activity starts
  right away.
* Query responses sent with the WFT checkpoint the window. Eviction while a window is open reports
  the log as a checkpoint. A rejected or failed WFT drops the window and its local executions. A
  checkpoint that closes the workflow hands its activities off (the server returns their tokens)
  and cancels their local executions; reporting their outcome afterwards gets NotFound, which Core
  logs at debug level.
* Replay gating leaves patch markers, and the `TemporalChangeVersion` upserts that go with them,
  out of the command count on both sides, and lines commands up with history by order. Otherwise
  `deprecate_patch` would trip the count check.
* Async completion fails the attempt with error type `OptimisticAsyncCompletionUnsupported`.
* Not done: exposing speculative blocks in the Rust SDK.

**Metrics (Core)**

All carry the worker's usual `namespace`, `task_queue` and `workflow_type` attributes.

| Name | Type | Extra attributes | Meaning |
|---|---|---|---|
| `optimistic_activity_started` | counter | `activity_type` | Activities started optimistically |
| `optimistic_activity_observed` | counter | `activity_type`, `outcome` (`completed`/`canceled`) | Outcomes the workflow observed inside the window |
| `optimistic_activity_handed_off` | counter | `activity_type` | Still-running activities the server took over at a checkpoint |
| `optimistic_activity_dropped` | counter | `activity_type` | Discarded with their window (rejected or failed WFT, eviction); local execution cancelled |
| `optimistic_timer_started` | counter | | Timers started optimistically |
| `optimistic_timer_fired` | counter | | Firings observed inside the window |
| `optimistic_timer_cancelled` | counter | | Timers the workflow cancelled inside the window |
| `optimistic_timer_handed_off` | counter | | Timers that became server timers at a checkpoint |
| `optimistic_start_fallback` | counter | `fallback_reason` | Speculative commands sent as normal commands |
| `speculation_window_checkpoint` | counter | `checkpoint_reason` | Windows checkpointed |
| `speculation_window_duration` | histogram | | Time from opening a window to sending its checkpoint or discarding it |

`fallback_reason`: `capability`, `task_queue`, `no_slot`, `rate_limited`, `no_activity_worker`,
`window_time`, `local_activities`, `non_deferrable_batch`, `window_closing`. All speculative
commands from one activation fall back together and are counted with the reason that blocked them.

`checkpoint_reason`: `block_exit`, `non_deferrable_command`, `ineligible_command`,
`workflow_completion`, `deadline` (80% of the WFT timeout), `activity_timeout`, `activity_failed`
(also async completion and cancellations the workflow didn't request), `local_activity`, `query`,
`eviction`, `idle` (nothing the workflow waits on is still in flight).

**Server**

* The activity's task queue must be the workflow's normal task queue. A sticky queue is rejected.
* A missing offset counts as 0. `heartbeat_details` on `StartTimer` is rejected.
  `request_eager_execution` is ignored when `optimistic_start` is set.
* The `ActivityTaskStarted` written with an outcome takes its `event_time` from the backdated
  start, as transient activities already do, so event times aren't monotonic within a batch.
* If the workflow closes in the same batch (complete, fail, cancel or continue-as-new), task tokens
  for still-running optimistic activities are returned anyway. The worker cancel commands sent on
  close carry the same tokens, so the SDK's token mapping routes them to the local executions.
* `RequestCancelActivityTask` after the activity was resolved earlier in the batch, or a duplicate
  cancel request, fails the WFT with `BAD_REQUEST_CANCEL_ACTIVITY_ATTRIBUTES`. `CancelTimer` after
  `FireOptimisticTimer` fails with `BAD_CANCEL_TIMER_ATTRIBUTES`, and a resolution after a
  `canceled` resolution with `BAD_RESOLVE_OPTIMISTIC_ACTIVITY_ATTRIBUTES`.
* Cancellation doesn't change event ID sequencing on the server. A cancel request for an optimistic
  activity writes exactly one event. A cancel request for a normal activity that hasn't started
  writes its `ActivityTaskCanceled` as a buffered event after the whole batch.
* Paginated completions: the server merges all pages before handling any command, so history and
  event IDs are identical to an unpaginated batch. Handoff tokens come back on the final page only,
  and request-level fields (`worker_control_task_queue`, `identity`) are read from the final page.
* Handed-off activities rebuilt from history alone come back as scheduled but not started, like
  eager activities. Standby clusters get the started state through activity sync replication.
* Paused workflows still accept and hand off optimistic starts, as long as the completion doesn't
  request a new WFT.

## Plan

All four steps are done in the [prototype](#prototype), on personal forks. No upstream PRs yet.

1. API changes in an `api` branch, consumed by the server and Core builds.
2. Server behind dynamic config. Functional tests drive raw WFT completions, so no SDK is needed.
3. Core: replay first, then the window runtime and handoff. Python `workflow.speculative()`.
4. TypeScript, .NET and Ruby.

Go and Java are out of scope for now.

## Future Work

* Worker ownership across WFT heartbeats: the server keeps timers paused while the worker holds the
  WFT, so long activities avoid the handoff round trip.
* Local retries of optimistic activities.
* The server telling a worker to checkpoint (through `worker_control_task_queue`) when signals,
  updates or cancellations arrive during a window.
* Optimistic start across workers in the same process (for example several task queues in one
  Spring Boot app), so activities on another task queue can still start locally.
* Speculative child workflows: a child workflow whose code contains speculative blocks could start
  running on the same worker before the server knows about it.
* A server API to consume task queue rate limit quota before starting an activity locally. It adds
  latency, but at a lower action cost and without the durability of a normal schedule.
* Turning optimistic start off per task queue, for example when rate limits are configured.
