#!/usr/bin/env bash
# Run all acceptance tests in sequence, displaying acceptance criteria
# before each test and echoing every command via set -x.
#
# Usage:
#   ./acceptance/run.sh                  # run all tests
#   ACCEPTANCE_SOAK_MINUTES=0.5 ./acceptance/run.sh  # shorter soak test

set -euo pipefail

cd "$(dirname "$0")/.."

PASS=0
FAIL=0
FAILED_TESTS=()

run_test() {
    local module="$1"
    local title="$2"
    shift 2
    local criteria=("$@")

    echo ""
    echo "================================================================"
    echo "$title"
    echo "================================================================"
    echo ""
    echo "Acceptance Criteria:"
    for c in "${criteria[@]}"; do
        echo "  - $c"
    done
    echo ""
    echo "----------------------------------------------------------------"

    set -x
    if python -m unittest "$module" -v; then
        set +x
        PASS=$((PASS + 1))
        echo ">>> PASSED: $title"
    else
        set +x
        FAIL=$((FAIL + 1))
        FAILED_TESTS+=("$title")
        echo ">>> FAILED: $title"
    fi

    echo "----------------------------------------------------------------"
    echo ""
}

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║          JOBSERVER ACCEPTANCE TEST SUITE                       ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""
echo "Started at: $(date)"
echo ""

# ── Section 1: Baseline Functional Correctness ──────────────────────

run_test "acceptance.test_01_baseline_jobserver" \
    "Acceptance 1.1: Baseline Functional Correctness -- Jobserver Round-Trip" \
    "All 7 scenarios pass on every supported start method." \
    "No exceptions other than those explicitly expected." \
    "result() returns the exact expected value and type."

run_test "acceptance.test_02_baseline_executor" \
    "Acceptance 1.2: Baseline Functional Correctness -- JobserverExecutor" \
    "executor.submit() returns concurrent.futures.Future resolving correctly." \
    "executor.map() yields results in submission order." \
    "Context manager calls shutdown on exit." \
    "as_completed() / wait() from concurrent.futures work without modification."

# ── Section 2: Concurrency and Load Testing ─────────────────────────

run_test "acceptance.test_03_concurrency_slots" \
    "Acceptance 2.1: Slot Saturation" \
    "At most N functions execute simultaneously when slots=N." \
    "All submitted work completes without deadlock." \
    "Serialized execution (slots=1) handles 100 items."

run_test "acceptance.test_04_high_throughput" \
    "Acceptance 2.2: High-Throughput Stress" \
    "1,000 trivial jobs complete via Jobserver within reasonable wall time." \
    "10,000 trivial jobs complete via JobserverExecutor." \
    "60-second sustained loop shows no monotonic resource growth."

run_test "acceptance.test_05_large_payloads" \
    "Acceptance 2.3: Large Payloads" \
    "1 MB and 50 MB results transfer correctly." \
    "128 MB+ result raises an exception (not a hang)." \
    "10 MB argument transfers correctly into the child."

# ── Section 3: Error Handling and Exception Propagation ─────────────

run_test "acceptance.test_06_work_exceptions" \
    "Acceptance 3.1: Work Exceptions" \
    "result() re-raises the exact exception type from the child." \
    "BaseExceptions (SystemExit, KeyboardInterrupt) produce SubmissionDied." \
    "preexec_fn exceptions propagate correctly."

run_test "acceptance.test_07_process_death" \
    "Acceptance 3.2: Process Death" \
    "All forms of abrupt process termination produce SubmissionDied." \
    "done(timeout=T) returns False for an incomplete future (not killed)."

run_test "acceptance.test_08_blocked_timeouts" \
    "Acceptance 3.3: Blocked / Timeout Behavior" \
    "submit(timeout=0) raises Blocked when slots are exhausted." \
    "submit(timeout=T) raises Blocked after approximately T seconds." \
    "done(timeout=0) returns False (never raises Blocked)." \
    "result(timeout=0) raises Blocked on incomplete future." \
    "Blocking calls with timeout=None return when work eventually completes."

run_test "acceptance.test_09_executor_exceptions" \
    "Acceptance 3.4: JobserverExecutor Exception Propagation" \
    "Worker exceptions propagate through concurrent.futures.Future." \
    "Submit after shutdown raises RuntimeError." \
    "Unpicklable fn produces an exception (not a hang)."

# ── Section 4: Callback Chaos ───────────────────────────────────────

run_test "acceptance.test_10_callbacks_normal" \
    "Acceptance 4.1: Normal Callback Operation" \
    "Single callback fires exactly once after done()." \
    "Callback on already-complete future fires immediately." \
    "100 callbacks all fire in registration order." \
    "Callback receives forwarded args and kwargs."

run_test "acceptance.test_11_callbacks_errors" \
    "Acceptance 4.2: Callback Error Draining" \
    "A raising callback produces CallbackRaised with __cause__ set." \
    "Mixed OK/RAISE callbacks require multiple done() calls to drain." \
    "All 5 raising callbacks require 5 done() calls." \
    "result() may raise CallbackRaised; once drained, returns result."

run_test "acceptance.test_12_callbacks_reentrant" \
    "Acceptance 4.3: Re-Entrant Callbacks" \
    "Callback A registering callback B from within when_done: both fire." \
    "Three-level re-entrant chain: A -> B -> C all fire." \
    "Re-entrant callback that raises produces CallbackRaised."

run_test "acceptance.test_13_callbacks_threaded" \
    "Acceptance 4.4: Callback + Thread Safety" \
    "No deadlock when done() and when_done() race from different threads." \
    "10 threads calling done() on same future: all see True, no crash." \
    "10 threads registering callbacks while future completes: all fire once."

# ── Section 5: Pathological and Adversarial Scenarios ───────────────

run_test "acceptance.test_14_resource_exhaustion" \
    "Acceptance 5.1: Resource Exhaustion Attacks" \
    "With callbacks=True, eager reclamation prevents deadlock on submit()." \
    "With callbacks=False, manual reclaim_resources() frees slots." \
    "Slot is not corrupted after Blocked on a saturated jobserver."

run_test "acceptance.test_15_timing_attacks" \
    "Acceptance 5.2: Timing Attacks" \
    "Tight-loop done(timeout=0.001) eventually returns True." \
    "Near-zero timeout on submit either succeeds or raises Blocked cleanly." \
    "Near-zero timeout on result either returns or raises Blocked cleanly." \
    "Rapid submit/done alternation maintains consistent state."

run_test "acceptance.test_16_nested_submission" \
    "Acceptance 5.3: Nested Submission (Recursive Work)" \
    "Child can submit grandchild work when slots >= 2." \
    "Three-level nesting works when slots >= 3." \
    "slots=1 with nested submit(timeout=0) raises Blocked (no deadlock)." \
    "slots=1 with nested submit(timeout=2) raises Blocked after timeout."

run_test "acceptance.test_17_sleep_fn" \
    "Acceptance 5.4: sleep_fn Adversarial Behavior" \
    "Always-vetoing sleep_fn with timeout produces Blocked." \
    "Countdown sleep_fn eventually allows submission." \
    "Huge sleep value clamped by timeout (doesn't sleep 1000s)." \
    "sleep_fn that raises propagates the exception."

run_test "acceptance.test_18_pickling" \
    "Acceptance 5.5: Pickling Edge Cases" \
    "copy/deepcopy of Jobserver returns the same instance." \
    "pickle round-trip of Jobserver works (excluding in-flight futures)." \
    "copy/pickle of Future raises NotImplementedError."

run_test "acceptance.test_19_env_preexec" \
    "Acceptance 5.6: Environment and preexec_fn Edge Cases" \
    "env sets variables visible in child." \
    "env with None values unsets variables in child." \
    "Set-then-unset (last write wins) works correctly." \
    "preexec_fn modifies child state without affecting parent." \
    "preexec_fn that raises propagates to result()."

# ── Section 6: JobserverExecutor-Specific Stress Tests ──────────────

run_test "acceptance.test_20_executor_cancellation" \
    "Acceptance 6.1: Cancellation Matrix" \
    "cancel() before dispatch returns True; future.cancelled() is True." \
    "cancel() after dispatch returns False; result eventually available." \
    "shutdown(cancel_futures=True) cancels pending, completes running." \
    "Cancelled future raises CancelledError on result()."

run_test "acceptance.test_21_executor_shutdown" \
    "Acceptance 6.2: Shutdown Robustness" \
    "shutdown(wait=True) blocks until all work is done." \
    "shutdown(wait=False) returns promptly." \
    "Double/triple shutdown is idempotent; no errors." \
    "Concurrent shutdowns from multiple threads: no crash, no deadlock." \
    "shutdown() with no submitted work is clean."

run_test "acceptance.test_22_executor_throughput" \
    "Acceptance 6.3: Executor Throughput" \
    "5,000 trivial jobs all resolve via as_completed." \
    "4 threads each submitting 250 jobs concurrently: all 1,000 resolve." \
    "Mixed fast/slow workload: all complete; no starvation."

run_test "acceptance.test_23_dispatcher_crash" \
    "Acceptance 6.4: Dispatcher Crash Recovery" \
    "Killing the dispatcher makes outstanding futures fail with RuntimeError." \
    "Executor rejects new submissions after dispatcher dies."

# ── Section 7: Multiprocessing Context Compatibility ────────────────

run_test "acceptance.test_24_context_compat" \
    "Acceptance 7: Multiprocessing Context Compatibility" \
    "Core submit/result round-trip works under fork, forkserver, and spawn." \
    "Module-level functions work under spawn." \
    "Closures fail under spawn with a pickling error (expected)." \
    "Nested jobserver shared via pickle works under all contexts." \
    "Explicit context construction works correctly."

# ── Section 8: Chaos Monkey ─────────────────────────────────────────

run_test "acceptance.test_25_chaos_jobserver" \
    "Acceptance 8.1: Combined Chaos Monkey -- Jobserver" \
    "200 jobs (mixed normal/exception/death/large/callback) complete within 120s." \
    "No leaked child processes (all futures reach terminal state)." \
    "Every future has a deterministic outcome matching its category." \
    "Concurrent submit, collect, and reclaim threads do not deadlock."

run_test "acceptance.test_26_chaos_executor" \
    "Acceptance 8.2: Combined Chaos Monkey -- JobserverExecutor" \
    "4 threads each submit 100 jobs (mixed normal/exception/death)." \
    "A 5th thread randomly cancels pending futures." \
    "After shutdown, all futures are in a terminal state." \
    "No zombie processes or hanging threads." \
    "Executor rejects new submissions after shutdown."

run_test "acceptance.test_27_soak" \
    "Acceptance 8.3: Long-Running Soak Test" \
    "3-minute sustained submit/collect with bounded resource usage." \
    "FD count does not grow monotonically." \
    "Slot queue returns to full capacity after draining all work." \
    "No OOM or FD exhaustion."

# ── Summary ─────────────────────────────────────────────────────────

echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                        SUMMARY                                 ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""
echo "Finished at: $(date)"
echo "Passed: $PASS"
echo "Failed: $FAIL"
echo "Total:  $((PASS + FAIL))"
echo ""

if [ ${#FAILED_TESTS[@]} -gt 0 ]; then
    echo "Failed tests:"
    for t in "${FAILED_TESTS[@]}"; do
        echo "  - $t"
    done
    echo ""
    exit 1
else
    echo "All acceptance tests passed."
    echo ""
    exit 0
fi
