---
name: pi-link-coordination
description: Guidance for coordinating work across Pi terminals using pi-link. Use when delegating tasks, choosing between link_prompt and link_send, planning async vs sync work, batching parallel jobs, managing a remote terminal's context with link_compact, or avoiding busy/conflict patterns.
---

# Pi-Link Coordination

How to coordinate work across Pi terminals via pi-link.

Each terminal is an independent agent: they share no memory or conversation history — link only passes messages and prompts between them. Anything a remote terminal needs — file paths, task state, expected output, where to send its callback — must be in the message itself.

---

## Tool Selection Rule

- Need the answer back now? → `link_prompt`
- Need autonomous work done? → `link_send(triggerTurn: true)`
- Need to notify only? → `link_send(triggerTurn: false)`
- Need to free a terminal's context? → `link_compact`

---

## The Golden Rule

> After `link_send(triggerTurn: true)` to terminal X, do not `link_prompt` X until X sends a completion callback.

The `DONE` / `BLOCKED` callback arrives as a normal later user message; treat that callback as the signal that it is safe to send a follow-up `link_prompt`.

Pick one mode per terminal per task. Mixing sync and async on the same terminal is a common coordination failure.

---

## The Tools

### `link_list`

Returns connected terminals with names, live status (`idle`, `thinking`, `tool:<name>`), and working directory (cwd). Some terminals also report context usage as `45K/272K (17%)` — how full that window is: high usage means less room remaining, though a loaded terminal may also be the one already holding the relevant task state. Your own entry is marked `(you)` — use this to discover your link name when replying to broadcast tasks.

Only currently connected terminals are visible. If a target is missing, it is offline; messages to offline terminals are not queued.

### `link_prompt`

Synchronous RPC. Send a prompt, wait for the response.

- Fails fast if the target is yourself, not connected, or busy (local work, another remote prompt, or compaction); a mid-wait disconnect resolves as an error
- 90s inactivity timeout, 30min hard ceiling
- Remote agent doesn't share your context — include enough detail to complete the task

### `link_send`

Fire-and-forget. Send to one terminal or `to: "*"` to broadcast to every other connected terminal; there is no exclusion filter. Sending to yourself is rejected. A send to a name that isn't connected fails with an error listing who _is_ connected — delivery failure is visible to the sender, not silent.

Set `triggerTurn: true` to queue async work on the receiver. The sender does **not** get an automatic response back.

Delivery shape depends on the message's `triggerTurn`:

- **`triggerTurn: true`** messages go through the receiver's idle-gated inbox and surface at a turn boundary wrapped as `[Link: N message(s) received]` followed by one `From "name":` block per message. Multiple pending messages are batched into one turn.
- **`triggerTurn: false`** messages bypass the inbox and surface directly as raw content — no wrapper, no `From` block. The receiver sees only the message text, so the sender must include their own identity, task tag, or artifact paths in the body.

**Callback convention for `triggerTurn: true`:** the sender gets no automatic response, so ask the receiver to report back via `link_send(..., triggerTurn: true)` (which lands at a proper turn boundary with the wrapper intact). A useful convention:

- `DONE` / `BLOCKED`
- Output paths / artifacts created
- Result summary or next question

Use `triggerTurn: false` for fire-and-forget status notifications only — when you don't need to act on the reply.

**Receiver rule:** If your turn opens with `[Link: N message(s) received]`, treat each `From "name":` block as assigned work. When done, send the sender a `DONE` / `BLOCKED` callback via `link_send(..., triggerTurn: true)` — unless the task says otherwise.

### `link_compact`

Blocks until the target finishes compacting, then returns (3 min ceiling — the call then errors for you, though the target may still finish compacting on its own; check it before assuming failure). Ask another terminal to compact its context window into a summary, freeing space. `link_list` exposes context usage; `link_compact` is the lever when you decide a terminal should free context — and because the call only returns once compaction is done, you can immediately hand it more work with `link_send`/`link_prompt` (no sleep, no busy-bounce). Busy targets (mid-turn or already compacting) decline; retry when `link_list` shows them idle. You decide the threshold; pi-link just runs the compaction you ask for. Its optional `instructions` add an extra focus to that summary.

---

## Operating Constraints

- **One remote prompt at a time per target.** Concurrent requests rejected as busy.
- **Messages are ephemeral.** Offline terminals lose messages.
- **Localhost only.** Same machine.
- **Cwd is a hint, not proof.** Same cwd ≠ same workspace/branch/access. Use explicit paths; absolute when cwds differ or shared-root assumptions are unclear.
- **Naming:** Link names are user-defined identities (often `role@domain`, e.g. `builder@pi-link`); the hub keeps them unique, suffixing collisions (`builder@pi-link-2`). Use `link_list` to confirm a target's exact name, and its cwd to tell similar-named terminals apart.

---

## Parallel batch

`link_send(triggerTurn: true)` can dispatch independent tasks to several terminals at once. Their callbacks may arrive separately or batched into one turn when you next become idle — track which workers are still outstanding. Each task must be self-contained (explicit paths, absolute if cwds differ). Don't `link_prompt` a dispatched terminal until its callback arrives (Golden Rule).

---

## Anti-Patterns

**❌ Mixing async and sync on the same terminal**
Dispatched with `link_send(triggerTurn: true)` then sent a `link_prompt` → rejected as busy. See Golden Rule.

**❌ No completion callback on async work**
`triggerTurn: true` returns no result to the sender — without an agreed callback you never learn the outcome.

**❌ Circular delegation**
A → B → C → A won't complete — the cycle-closing `link_prompt` hits a busy target (rejected), and an async `link_send` cycle just loops. Keep delegation acyclic.

---

## Quick Reference

| I need to...                     | Tool                            | Mode             |
| -------------------------------- | ------------------------------- | ---------------- |
| See who's available              | `link_list`                     | —                |
| Get an answer from another agent | `link_prompt`                   | Synchronous      |
| Delegate autonomous work         | `link_send(triggerTurn: true)`  | Asynchronous     |
| Notify without activating        | `link_send(triggerTurn: false)` | Fire-and-forget  |
| Broadcast to all                 | `link_send(to: "*")`            | Broadcast        |
| Free a loaded worker's context   | `link_compact`                  | Await-completion |
