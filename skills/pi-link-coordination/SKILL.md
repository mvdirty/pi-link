---
name: pi-link-coordination
description: Mechanics of coordinating work across Pi terminals with link_send, link_list, and link_compact — how delivery, batching, callbacks, and remote compaction actually behave.
---

# Pi-Link Coordination

How the pi-link transport behaves between Pi terminals.

**Terminals share no conversation.** Each is an independent agent with its own
context. Nothing you hold — task state, file paths, an approval you were given,
what you decided a moment ago — is visible to a terminal you message. The message
is the entire shared state.

---

## Tools

### `link_list`

Returns connected terminals with names, status (`idle`, `thinking`,
`compacting`, `tool:<name>`), cwd, and (when available) context usage such as
`45K/272K (17%)`. Your own entry is marked `(you)`; its status and context are
computed when listed, while peer values are their latest published snapshots.

Pi runs tools in parallel by default, and `tool:<name>` then names the first
still-active call it reported rather than all of them. It advances only when that
call ends, and becomes `thinking` only after the last call ends.

`thinking` covers every kind of unsettled work, not just an LLM call: an automatic
retry, an automatic compaction and a queued continuation all run after the visible
turn ends, and the terminal reads `thinking` until Pi reports the run settled.

`compacting` means a manual compaction has raised that terminal's delivery gate;
messages sent to it wait until the gate clears. An automatic (threshold or
overflow) compaction never shows it — it is not gated, and reads `thinking` like
the rest of the run it belongs to.

Only connected terminals are visible; nothing is stored for disconnected
terminals, so a reconnecting terminal receives no backlog.

A terminal's queued messages are invisible to you, and silence is
indistinguishable from work in progress.

### `link_send`

The message is delivered to the receiver's model. The first message to arrive opens
a batching window of about 200ms; later arrivals do not move its deadline, so a
steady stream is delivered window by window instead of waiting for a pause. A batch
arrives as one `[Link: N message(s) received]` block, in arrival order, containing
one `From "name":` block per message.

The receiver's state is read when that batch is delivered, not when you send and
not when you last ran `link_list`. If the receiver is still running then, the batch
is steered into that run at Pi's next safe boundary — current tool calls finish
first, before the next LLM call. Otherwise it starts a turn. A receiver can settle
within the delay, so a message sent to a busy terminal may still arrive as a new
turn. There is no way to send without entering the receiver's reasoning.

Each send has exactly one recipient. There is no fan-out.

The call returns send status, not the receiver's eventual work result. A definitely
absent target fails against the local terminal list; beyond that, for a client a
successful send means the message was written to its hub connection, not that it
arrived. If the target has vanished, the routing failure is shown to the human as a
notification and never reaches the sending model.

A terminal reported as `compacting` receives nothing until its gate clears. The
messages wait and are delivered afterwards. A cancelled compaction has no ending
pi-link can see, so they wait for the terminal's next agent run, a later
successful compaction, or a three-minute deadline — whichever comes first. The
sender is told nothing meanwhile.

### `link_compact`

Asks another terminal to compact its context and waits for a result, with a
three-minute ceiling. A target accepts only when Pi reports its session idle and no
manual compaction holds its gate; anything else declines rather than being
interrupted, so a target reading `thinking` for a retry or an automatic compaction
declines exactly as one mid-turn does. Optional `instructions` focus the summary.

The timeout bounds your wait only. Nothing aborts the target, so a timed-out call
may mean the compaction is still running.

Compaction discards detail. What survives is whatever the summary keeps, so
anything the target learned but has not written down or reported can be lost.

---

## Callbacks

A callback is an ordinary `link_send` from the worker back to you. There is no
request ID, no automatic response, no delivery receipt, and no protocol timeout —
nothing correlates a callback with the dispatch that asked for it except the text
of both, and nothing produces one except the receiver choosing to send it.

Waiting for one requires no live run: if the terminal is idle when the batch is
delivered, the message starts a turn by itself. Keeping a run alive only to wait —
by sleeping or polling `link_list` — is unnecessary and can postpone delivery to
the model until active tool calls end.

A callback can be sent before its sender's run settles; receiving it does not
prove the sender is idle, so a `link_compact` aimed at it can still decline as
busy.

An accepted send does not wait for a reply, so several tasks can be dispatched
before any callback arrives, and callbacks may arrive separately or batched into
one of your turns. For the same reason the protocol supplies no exit condition for
an A → B → C → A delegation chain.

---

## Constraints

- **Localhost only.** All terminals run on the same machine.
- **Cwd is a hint, not proof.** Same cwd does not prove the same workspace, branch,
  or access. Paths named in a message are only text: they do not change the
  receiver's cwd, and relative commands resolve from the receiver's own cwd.
- **Names are identities.** The hub suffixes collisions, so the name you remember
  may not be the name that is connected; `link_list` shows the current one.
- **Mixed-version meshes are unsupported.** Across the current protocol break, a
  message from a new sender can reach a 0.2.0 receiver as bare text — without the
  `[Link: N message(s) received]` header or the `From "name":` line — and nothing
  reports a fault.
