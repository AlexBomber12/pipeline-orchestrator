from __future__ import annotations

import asyncio
import ctypes
import errno
import math
import os
import signal
import sys
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
import src.process_supervisor as process_supervisor
from src.process_supervisor import (
    CleanupStatus,
    ProcessIdentity,
    SupervisedProcess,
    _GroupMember,
    _GroupObservation,
    _GroupState,
    _parse_proc_stat,
    launch_process,
)

SLEEPING_PROCESS = """
import os
import signal

print(os.getpid(), flush=True)
while True:
    signal.pause()
"""

TERM_IGNORING_PROCESS = """
import os
import signal

signal.signal(signal.SIGTERM, signal.SIG_IGN)
print(os.getpid(), flush=True)
while True:
    signal.pause()
"""

GRANDCHILD_PROCESS = """
import signal

while True:
    signal.pause()
"""

CHILD_WITH_GRANDCHILD = f"""
import os
import signal
import subprocess
import sys

grandchild = subprocess.Popen([sys.executable, "-c", {GRANDCHILD_PROCESS!r}])
print(os.getpid(), grandchild.pid, flush=True)
while True:
    signal.pause()
"""

EARLY_EXIT_LEADER = f"""
import subprocess
import sys

subprocess.Popen([sys.executable, "-c", {CHILD_WITH_GRANDCHILD!r}])
"""

EARLY_EXIT_WITH_TERM_IGNORING_CHILD = f"""
import subprocess
import sys

subprocess.Popen([sys.executable, "-c", {TERM_IGNORING_PROCESS!r}])
"""


class ProcessPool:
    def __init__(self) -> None:
        self.supervised: list[tuple[SupervisedProcess, int]] = []
        self.raw: list[asyncio.subprocess.Process] = []

    async def launch(self, code: str) -> SupervisedProcess:
        managed = await launch_process(
            sys.executable,
            "-c",
            code,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self.supervised.append((managed, managed.identity.process_group_id))
        return managed

    async def launch_raw(self, code: str) -> asyncio.subprocess.Process:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            code,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        self.raw.append(process)
        return process

    async def close(self) -> None:
        for managed, pgid in self.supervised:
            if managed._cleanup_task is None:
                await managed.cleanup(term_grace=0.05, kill_grace=0.5)
            await _force_stop(managed.process, pgid)
        for process in self.raw:
            await _force_stop(process, process.pid)


@pytest.fixture
async def process_pool() -> Any:
    pool = ProcessPool()
    try:
        yield pool
    finally:
        await pool.close()


async def _force_stop(process: asyncio.subprocess.Process, pgid: int) -> None:
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        await asyncio.wait_for(process.wait(), timeout=1)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
    await asyncio.wait_for(process.communicate(), timeout=1)


async def _read_pids(process: asyncio.subprocess.Process) -> list[int]:
    assert process.stdout is not None
    line = await asyncio.wait_for(process.stdout.readline(), timeout=2)
    assert line
    return [int(value) for value in line.split()]


def _pid_is_live(pid: int) -> bool:
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as stat_file:
            stat = stat_file.read()
    except FileNotFoundError:
        return False
    state, _, _, _ = _parse_proc_stat(stat)
    return state != "Z"


async def _wait_not_live(*pids: int) -> None:
    async with asyncio.timeout(2):
        while any(_pid_is_live(pid) for pid in pids):
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_normal_completion_is_reaped_without_signals(
    process_pool: ProcessPool,
) -> None:
    managed = await process_pool.launch("print('complete')")

    stdout, stderr = await managed.process.communicate()
    result = await managed.cleanup(term_grace=0.05, kill_grace=0.05)

    assert stdout == b"complete\n"
    assert stderr == b""
    assert result.status is CleanupStatus.QUIESCENT
    assert result.quiescent
    assert result.leader_returncode == 0
    assert not result.term_sent
    assert not result.kill_sent
    assert managed.identity.leader_pid == managed.process.pid
    assert managed.identity.process_group_id == managed.process.pid
    assert managed.identity.session_id == managed.process.pid
    assert managed.identity.leader_start_time is not None
    assert managed.identity.process_group_id != os.getpgrp()


@pytest.mark.asyncio
async def test_repeated_fast_completion_reconciles_disappeared_members(
    process_pool: ProcessPool,
) -> None:
    for _ in range(10):
        managed = await launch_process(
            "/bin/true",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        process_pool.supervised.append(
            (managed, managed.identity.process_group_id)
        )
        assert await managed.process.wait() == 0
        result = await managed.cleanup(term_grace=0.1, kill_grace=0.1)
        assert result.quiescent
        assert not result.term_sent
        assert not result.kill_sent


@pytest.mark.asyncio
async def test_launch_restores_default_sigpipe_disposition(
    process_pool: ProcessPool,
) -> None:
    managed = await launch_process(
        "/bin/sleep",
        "30",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    process_pool.supervised.append(
        (managed, managed.identity.process_group_id)
    )

    managed.process.send_signal(signal.SIGPIPE)

    assert await asyncio.wait_for(managed.process.wait(), timeout=1) == -signal.SIGPIPE
    result = await managed.cleanup(term_grace=0.5, kill_grace=0.5)
    assert result.quiescent
    assert not result.term_sent
    assert not result.kill_sent


@pytest.mark.asyncio
async def test_launch_preserves_default_sigpipe_when_restore_is_disabled(
    process_pool: ProcessPool,
) -> None:
    previous_sigpipe = signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    try:
        managed = await launch_process(
            "/bin/sleep",
            "30",
            restore_signals=False,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
    finally:
        signal.signal(signal.SIGPIPE, previous_sigpipe)
    process_pool.supervised.append(
        (managed, managed.identity.process_group_id)
    )

    managed.process.send_signal(signal.SIGPIPE)

    assert await asyncio.wait_for(managed.process.wait(), timeout=1) == -signal.SIGPIPE
    result = await managed.cleanup(term_grace=0.5, kill_grace=0.5)
    assert result.quiescent


@pytest.mark.asyncio
async def test_target_environment_is_isolated_from_launcher(
    process_pool: ProcessPool,
) -> None:
    managed = await launch_process(
        "/bin/sh",
        "-c",
        'test "$TARGET_ONLY" = expected',
        env={
            "PYTHONHOME": "/definitely/invalid",
            "PYTHONPATH": "/also/invalid",
            "TARGET_ONLY": "expected",
        },
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    process_pool.supervised.append(
        (managed, managed.identity.process_group_id)
    )

    assert await managed.process.wait() == 0
    result = await managed.cleanup(term_grace=0.5, kill_grace=0.5)
    assert result.quiescent


@pytest.mark.asyncio
async def test_early_leader_exit_cleans_child_grandchild_and_retained_pipes(
    process_pool: ProcessPool,
) -> None:
    managed = await process_pool.launch(EARLY_EXIT_LEADER)
    child_pid, grandchild_pid = await _read_pids(managed.process)
    assert await managed.process.wait() == 0
    assert _pid_is_live(child_pid)
    assert _pid_is_live(grandchild_pid)

    result = await managed.cleanup(term_grace=0.5, kill_grace=0.5)
    await _wait_not_live(child_pid, grandchild_pid)
    stdout, stderr = await managed.process.communicate()

    assert result.quiescent
    assert result.term_sent
    assert not result.kill_sent
    assert stdout == b""
    assert stderr == b""


@pytest.mark.asyncio
async def test_early_leader_exit_cleanup_is_independent_of_output_pipes(
    process_pool: ProcessPool,
) -> None:
    report_read, report_write = os.pipe()
    code = f"""
import os
import signal
import subprocess
import sys

child = subprocess.Popen([
    sys.executable,
    "-c",
    "import signal; signal.pause()",
])
os.write({report_write}, str(child.pid).encode("ascii"))
"""
    managed = await launch_process(
        sys.executable,
        "-c",
        code,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
        pass_fds=(report_write,),
    )
    process_pool.supervised.append(
        (managed, managed.identity.process_group_id)
    )
    os.close(report_write)
    child_pid = int(await asyncio.to_thread(os.read, report_read, 64))
    os.close(report_read)
    assert await managed.process.wait() == 0
    assert _pid_is_live(child_pid)

    result = await managed.cleanup(term_grace=0.03, kill_grace=0.5)
    await _wait_not_live(child_pid)

    assert result.quiescent
    assert result.term_sent
    assert not result.kill_sent


@pytest.mark.asyncio
async def test_cleanup_reaps_adopted_workload_descendant(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    previous = ctypes.c_int()
    assert libc.prctl(37, ctypes.byref(previous), 0, 0, 0) == 0
    report_read, report_write = os.pipe()
    child_pid: int | None = None
    try:
        assert libc.prctl(36, 1, 0, 0, 0) == 0
        code = f"""
import os
import subprocess
import sys

child = subprocess.Popen([
    sys.executable,
    "-c",
    "import signal; signal.pause()",
])
os.write({report_write}, str(child.pid).encode("ascii"))
"""
        managed = await launch_process(
            sys.executable,
            "-c",
            code,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            pass_fds=(report_write,),
        )
        process_pool.supervised.append(
            (managed, managed.identity.process_group_id)
        )
        os.close(report_write)
        report_write = -1
        child_pid = int(await asyncio.to_thread(os.read, report_read, 64))
        assert await managed.process.wait() == 0

        result = await managed.cleanup(term_grace=0.5, kill_grace=0.5)

        assert result.quiescent
        assert result.term_sent
        assert not os.path.exists(f"/proc/{child_pid}")

        failure_managed = await process_pool.launch(
            """
import subprocess
import sys

subprocess.Popen([
    sys.executable,
    "-c",
    "import os, signal; print(os.getpid(), flush=True); signal.pause()",
])
"""
        )
        (child_pid,) = await _read_pids(failure_managed.process)
        assert await failure_managed.process.wait() == 0
        observation_failure = _GroupObservation(
            _GroupState.UNKNOWN, "transient inspection failure"
        )

        async def fail_after_child_becomes_zombie(
            _timeout: float, *, repeat_signal: signal.Signals | None = None
        ) -> tuple[_GroupObservation, bool]:
            del repeat_signal
            async with asyncio.timeout(1):
                while _pid_is_live(child_pid):
                    await asyncio.sleep(0)
            assert os.path.exists(f"/proc/{child_pid}")
            return observation_failure, False

        monkeypatch.setattr(
            failure_managed,
            "_wait_for_quiescence",
            fail_after_child_becomes_zombie,
        )

        failed = await failure_managed.cleanup(term_grace=0, kill_grace=0)

        assert failed.status is CleanupStatus.FAILED
        assert failed.detail == "transient inspection failure"
        assert not os.path.exists(f"/proc/{child_pid}")
    finally:
        os.close(report_read)
        if report_write >= 0:
            os.close(report_write)
        if child_pid is not None:
            try:
                os.kill(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                os.waitpid(child_pid, 0)
            except ChildProcessError:
                pass
        assert libc.prctl(36, previous.value, 0, 0, 0) == 0


@pytest.mark.asyncio
async def test_term_ignoring_descendant_requires_kill(
    process_pool: ProcessPool,
) -> None:
    managed = await process_pool.launch(EARLY_EXIT_WITH_TERM_IGNORING_CHILD)
    (child_pid,) = await _read_pids(managed.process)
    assert await managed.process.wait() == 0

    result = await managed.cleanup(term_grace=0.03, kill_grace=0.5)
    await _wait_not_live(child_pid)

    assert result.quiescent
    assert result.term_sent
    assert result.kill_sent
    assert result.leader_returncode == 0


@pytest.mark.asyncio
async def test_repeated_cleanup_returns_cached_result(
    process_pool: ProcessPool,
) -> None:
    managed = await process_pool.launch(SLEEPING_PROCESS)
    await _read_pids(managed.process)

    first, concurrent = await asyncio.gather(
        managed.cleanup(term_grace=0.5, kill_grace=0.5),
        managed.cleanup(term_grace=0, kill_grace=0),
    )
    repeated = await managed.cleanup(term_grace=0.5, kill_grace=0.5)

    assert first is concurrent is repeated
    assert first.quiescent
    assert first.term_sent


@pytest.mark.asyncio
async def test_cancellation_waits_for_shielded_cleanup(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    managed = await process_pool.launch(TERM_IGNORING_PROCESS)
    await _read_pids(managed.process)
    term_sent = asyncio.Event()
    real_pidfd_send_signal = signal.pidfd_send_signal

    def tracked_pidfd_signal(pidfd: int, sig: int) -> None:
        real_pidfd_send_signal(pidfd, sig)
        if sig == signal.SIGTERM:
            term_sent.set()

    monkeypatch.setattr(signal, "pidfd_send_signal", tracked_pidfd_signal)
    cleanup = asyncio.create_task(
        managed.cleanup(term_grace=0.05, kill_grace=0.5)
    )
    await asyncio.wait_for(term_sent.wait(), timeout=1)
    cleanup.cancel()
    await asyncio.sleep(0)
    cleanup.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(cleanup, timeout=1)

    result = await managed.cleanup(term_grace=0, kill_grace=0)
    assert result.quiescent
    assert result.kill_sent
    assert managed.process.returncode == -signal.SIGKILL


@pytest.mark.asyncio
async def test_unrelated_process_group_is_untouched(
    process_pool: ProcessPool,
) -> None:
    unrelated = await process_pool.launch_raw(SLEEPING_PROCESS)
    unrelated_pid = (await _read_pids(unrelated))[0]
    managed = await process_pool.launch(SLEEPING_PROCESS)
    await _read_pids(managed.process)

    result = await managed.cleanup(term_grace=0.5, kill_grace=0.5)

    assert result.quiescent
    assert unrelated.returncode is None
    assert _pid_is_live(unrelated_pid)


@pytest.mark.asyncio
async def test_reused_group_without_lifecycle_proof_is_not_signalled(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    replacement = await process_pool.launch_raw(
        EARLY_EXIT_WITH_TERM_IGNORING_CHILD
    )
    (replacement_child_pid,) = await _read_pids(replacement)
    assert await replacement.wait() == 0
    assert not os.path.exists(f"/proc/{replacement.pid}")

    managed = await process_pool.launch(SLEEPING_PROCESS)
    await _read_pids(managed.process)
    managed._identity = ProcessIdentity(
        replacement.pid,
        replacement.pid,
        replacement.pid,
        1,
    )
    managed._known_members = {(replacement.pid, 1)}
    delivered: list[int] = []

    def record_signal(_pidfd: int, sig: int) -> None:
        delivered.append(sig)

    monkeypatch.setattr(signal, "pidfd_send_signal", record_signal)
    result = await managed.cleanup(term_grace=0, kill_grace=0)

    assert result.status is CleanupStatus.FAILED
    assert result.detail == "lifecycle ownership witness is no longer live"
    assert delivered == []
    assert _pid_is_live(replacement_child_pid)


@pytest.mark.asyncio
async def test_unproven_or_daemon_group_is_never_signalled(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    managed = await process_pool.launch(SLEEPING_PROCESS)
    await _read_pids(managed.process)
    signalled = False

    def forbidden_killpg(_pgid: int, _sig: int) -> None:
        nonlocal signalled
        signalled = True

    managed._identity = replace(
        managed.identity, process_group_id=os.getpgrp(), session_id=os.getsid(0)
    )
    monkeypatch.setattr(os, "killpg", forbidden_killpg)

    result = await managed.cleanup(term_grace=0, kill_grace=0)

    assert result.status is CleanupStatus.FAILED
    assert not result.quiescent
    assert "daemon process group" in (result.detail or "")
    assert not signalled
    assert managed.process.returncode is None


@pytest.mark.asyncio
async def test_wrong_session_identity_is_not_signalled(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    managed = await process_pool.launch(SLEEPING_PROCESS)
    await _read_pids(managed.process)
    managed._identity = replace(
        managed.identity, session_id=managed.identity.session_id + 1
    )
    real_killpg = os.killpg
    signals: list[int] = []

    def record_killpg(pgid: int, sig: int) -> None:
        if sig:
            signals.append(sig)
        real_killpg(pgid, sig)

    monkeypatch.setattr(os, "killpg", record_killpg)
    result = await managed.cleanup(term_grace=0, kill_grace=0)

    assert result.status is CleanupStatus.FAILED
    assert "owned session" in (result.detail or "")
    assert signals == []
    assert managed.process.returncode is None

    reused = await process_pool.launch(SLEEPING_PROCESS)
    await _read_pids(reused.process)
    assert reused.identity.leader_start_time is not None
    reused._identity = replace(
        reused.identity, leader_start_time=reused.identity.leader_start_time + 1
    )
    result = await reused.cleanup(term_grace=0, kill_grace=0)
    assert result.status is CleanupStatus.FAILED
    assert result.detail == "process-group leader identity is unproven"


@pytest.mark.asyncio
async def test_cleanup_failure_is_bounded(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    managed = await process_pool.launch(SLEEPING_PROCESS)
    await _read_pids(managed.process)
    live = _GroupObservation(_GroupState.LIVE, "still live")
    monkeypatch.setattr(managed, "_observe_group", lambda: live)
    monkeypatch.setattr(managed, "_signal_group", lambda _sig: (True, None))

    started = time.monotonic()
    result = await managed.cleanup(term_grace=0.02, kill_grace=0.02)
    elapsed = time.monotonic() - started

    assert result.status is CleanupStatus.FAILED
    assert result.detail == "process group remained live after the grace period"
    assert result.term_sent
    assert result.kill_sent
    assert elapsed < 0.5


@pytest.mark.asyncio
async def test_cleanup_reports_control_and_observation_failures(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    live = _GroupObservation(_GroupState.LIVE, "live")
    unknown = _GroupObservation(_GroupState.UNKNOWN, "unknown")

    term_error = await process_pool.launch(SLEEPING_PROCESS)
    await _read_pids(term_error.process)
    monkeypatch.setattr(term_error, "_observe_group", lambda: live)
    monkeypatch.setattr(
        term_error, "_signal_group", lambda _sig: (False, "term failed")
    )
    result = await term_error.cleanup(term_grace=0, kill_grace=0)
    assert result.detail == "term failed"

    observation_error = await process_pool.launch(SLEEPING_PROCESS)
    await _read_pids(observation_error.process)
    monkeypatch.setattr(observation_error, "_observe_group", lambda: live)
    monkeypatch.setattr(
        observation_error, "_signal_group", lambda _sig: (True, None)
    )

    async def unknown_after_term(
        _timeout: float, *, repeat_signal: signal.Signals | None = None
    ) -> tuple[_GroupObservation, bool]:
        return unknown, False

    monkeypatch.setattr(
        observation_error, "_wait_for_quiescence", unknown_after_term
    )
    result = await observation_error.cleanup(term_grace=0, kill_grace=0)
    assert result.detail == "unknown"

    kill_error = await process_pool.launch(SLEEPING_PROCESS)
    await _read_pids(kill_error.process)
    monkeypatch.setattr(kill_error, "_observe_group", lambda: live)
    signal_results = iter(((True, None), (False, "kill failed")))
    monkeypatch.setattr(kill_error, "_signal_group", lambda _sig: next(signal_results))

    async def still_live(
        _timeout: float, *, repeat_signal: signal.Signals | None = None
    ) -> tuple[_GroupObservation, bool]:
        return live, False

    monkeypatch.setattr(kill_error, "_wait_for_quiescence", still_live)
    result = await kill_error.cleanup(term_grace=0, kill_grace=0)
    assert result.detail == "kill failed"


@pytest.mark.asyncio
async def test_wait_and_signal_refuse_unconfirmed_states(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    managed = await process_pool.launch(SLEEPING_PROCESS)
    await _read_pids(managed.process)
    unknown = _GroupObservation(_GroupState.UNKNOWN, "unknown")
    quiet = _GroupObservation(_GroupState.QUIESCENT, "quiet")

    monkeypatch.setattr(
        managed, "_snapshot_group", lambda **_kwargs: (unknown, [])
    )
    assert await managed._wait_for_quiescence(0) == (unknown, False)
    assert managed._signal_group(signal.SIGTERM) == (False, "unknown")

    monkeypatch.setattr(
        managed, "_snapshot_group", lambda **_kwargs: (quiet, [])
    )
    timed_out, signal_sent = await managed._wait_for_quiescence(0)
    assert timed_out.state is _GroupState.LIVE
    assert timed_out.detail == "process group is quiet but leader was not reaped"
    assert not signal_sent
    assert managed._signal_group(signal.SIGTERM) == (False, None)

    changed = _GroupObservation(_GroupState.CHANGED, "member disappeared")
    snapshots = iter(((changed, []), (quiet, [])))
    monkeypatch.setattr(
        managed, "_snapshot_group", lambda **_kwargs: next(snapshots)
    )
    assert managed._signal_group(signal.SIGTERM) == (False, None)

    monkeypatch.setattr(
        managed, "_snapshot_group", lambda **_kwargs: (changed, [])
    )
    assert managed._signal_group(signal.SIGTERM) == (
        False,
        "process-group membership kept changing during signaling",
    )


@pytest.mark.asyncio
async def test_quiet_snapshot_is_reconciled_before_success(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    managed = await process_pool.launch("pass")
    assert await managed.process.wait() == 0
    parent_only = _GroupObservation(
        _GroupState.QUIESCENT,
        "parent became a zombie during enumeration",
        ((111, 10, "Z"),),
    )
    omitted_descendant = _GroupObservation(
        _GroupState.LIVE,
        "a descendant omitted from the prior scan is live",
        ((112, 11, "S"),),
    )
    observations = iter((parent_only, omitted_descendant))
    real_observe_group = managed._observe_group
    real_reap_adopted_zombies = managed._reap_adopted_zombies
    monkeypatch.setattr(
        managed, "_reap_adopted_zombies", lambda _members: (False, False, None)
    )
    monkeypatch.setattr(managed, "_observe_group", lambda: next(observations))

    result, signal_sent = await managed._wait_for_quiescence(0)
    monkeypatch.setattr(managed, "_observe_group", real_observe_group)

    assert result.state is _GroupState.LIVE
    assert result.detail == "process group remained live after the grace period"
    assert not signal_sent

    changed_parent = replace(parent_only, members=((113, 12, "Z"),))
    observations = iter((parent_only, changed_parent))
    monkeypatch.setattr(managed, "_observe_group", lambda: next(observations))

    result, signal_sent = await managed._wait_for_quiescence(0)
    monkeypatch.setattr(managed, "_observe_group", real_observe_group)

    assert result.state is _GroupState.LIVE
    assert result.detail == (
        "process-group quiescence could not be confirmed before the grace period"
    )
    assert not signal_sent

    observations = iter((parent_only, parent_only))
    monkeypatch.setattr(managed, "_observe_group", lambda: next(observations))
    assert await managed._wait_for_quiescence(0) == (parent_only, False)

    definitive = _GroupObservation(
        _GroupState.QUIESCENT,
        "group absent",
        definitive=True,
    )
    observations = iter((omitted_descendant, definitive))
    repeated_signals: list[signal.Signals] = []
    real_signal_group = managed._signal_group

    def record_repeated_signal(sig: signal.Signals) -> tuple[bool, None]:
        repeated_signals.append(sig)
        return True, None

    monkeypatch.setattr(managed, "_observe_group", lambda: next(observations))
    monkeypatch.setattr(managed, "_signal_group", record_repeated_signal)
    assert await managed._wait_for_quiescence(
        0.05, repeat_signal=signal.SIGKILL
    ) == (definitive, True)
    assert repeated_signals == [signal.SIGKILL]

    monkeypatch.setattr(managed, "_observe_group", lambda: omitted_descendant)
    monkeypatch.setattr(
        managed,
        "_signal_group",
        lambda _sig: (False, "delivery failed"),
    )
    failed, signal_sent = await managed._wait_for_quiescence(
        0.05, repeat_signal=signal.SIGKILL
    )
    assert failed.state is _GroupState.UNKNOWN
    assert failed.detail == "delivery failed"
    assert not signal_sent

    observations = iter((parent_only, definitive))
    reap_results = iter(((True, False, None), (False, False, None)))
    monkeypatch.setattr(managed, "_observe_group", lambda: next(observations))
    monkeypatch.setattr(
        managed, "_reap_adopted_zombies", lambda _members: next(reap_results)
    )
    assert await managed._wait_for_quiescence(0.05) == (definitive, False)

    monkeypatch.setattr(managed, "_observe_group", lambda: parent_only)
    monkeypatch.setattr(
        managed,
        "_reap_adopted_zombies",
        lambda _members: (False, False, "reaping failed"),
    )
    failed, signal_sent = await managed._wait_for_quiescence(0.05)
    assert failed.state is _GroupState.UNKNOWN
    assert failed.detail == "reaping failed"
    assert not signal_sent

    with monkeypatch.context() as patch:
        changing_reaps = 0

        def continually_reaped(
            _members: tuple[tuple[int, int, str], ...],
        ) -> tuple[bool, bool, None]:
            nonlocal changing_reaps
            changing_reaps += 1
            return True, False, None

        patch.setattr(
            process_supervisor,
            "_RECONCILIATION_RETRIES_AFTER_DEADLINE",
            2,
        )
        patch.setattr(managed, "_observe_group", lambda: parent_only)
        patch.setattr(managed, "_reap_adopted_zombies", continually_reaped)
        bounded, signal_sent = await managed._wait_for_quiescence(0)
    assert bounded.state is _GroupState.LIVE
    assert bounded.detail == (
        "process-group membership kept changing after the grace period"
    )
    assert not signal_sent
    assert changing_reaps == 3

    witness = managed._lifecycle_witness
    assert witness is not None
    changing_witness_scans = 0

    def continually_changing_witness() -> _GroupObservation:
        nonlocal changing_witness_scans
        changing_witness_scans += 1
        return _GroupObservation(
            _GroupState.QUIESCENT,
            "changing witness membership",
            (
                (witness.pid, witness.start_time, "S"),
                (500 + changing_witness_scans, changing_witness_scans, "Z"),
            ),
            witness_live=True,
        )

    with monkeypatch.context() as patch:
        patch.setattr(
            process_supervisor,
            "_RECONCILIATION_RETRIES_AFTER_DEADLINE",
            2,
        )
        patch.setattr(managed, "_observe_group", continually_changing_witness)
        patch.setattr(
            managed,
            "_reap_adopted_zombies",
            lambda _members: (False, False, None),
        )
        bounded, signal_sent = await managed._wait_for_quiescence(0)
    assert bounded.state is _GroupState.LIVE
    assert bounded.detail == (
        "lifecycle witness membership kept changing after the grace period"
    )
    assert not signal_sent
    assert changing_witness_scans == 3

    monkeypatch.setattr(managed, "_observe_group", real_observe_group)
    monkeypatch.setattr(managed, "_signal_group", real_signal_group)
    monkeypatch.setattr(
        managed, "_reap_adopted_zombies", real_reap_adopted_zombies
    )


@pytest.mark.asyncio
async def test_adopted_zombie_reaping_handles_races_and_errors(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    managed = await process_pool.launch("pass")
    assert await managed.process.wait() == 0
    witness = managed._lifecycle_witness
    assert witness is not None
    skipped_members = (
        (managed.identity.leader_pid, 1, "Z"),
        (witness.pid, witness.start_time, "Z"),
        (333, 3, "S"),
    )

    with monkeypatch.context() as patch:
        patch.setattr(
            managed,
            "_open_owned_pidfd",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError()),
        )
        assert managed._reap_adopted_zombies(skipped_members) == (
            False,
            False,
            None,
        )

    unknown = _GroupObservation(_GroupState.UNKNOWN, "handle unavailable")
    with monkeypatch.context() as patch:
        patch.setattr(
            managed,
            "_open_owned_pidfd",
            lambda *_args, **_kwargs: (None, unknown),
        )
        assert managed._reap_adopted_zombies(((444, 4, "Z"),)) == (
            False,
            False,
            "handle unavailable",
        )

    with monkeypatch.context() as patch:
        patch.setattr(
            managed,
            "_open_owned_pidfd",
            lambda *_args, **_kwargs: (None, None),
        )
        assert managed._reap_adopted_zombies(((444, 4, "Z"),)) == (
            True,
            False,
            None,
        )

    cases = (
        (ChildProcessError(), (False, False, None)),
        (ProcessLookupError(errno.ESRCH, "gone"), (True, False, None)),
        (
            PermissionError(errno.EPERM, "denied"),
            (
                False,
                False,
                "could not reap owned process-group member: [Errno 1] denied",
            ),
        ),
    )
    for wait_error, expected in cases:
        closed: list[int] = []
        with monkeypatch.context() as patch:
            patch.setattr(
                managed,
                "_open_owned_pidfd",
                lambda *_args, **_kwargs: (99, None),
            )
            patch.setattr(
                os,
                "waitid",
                lambda *_args, wait_error=wait_error: (
                    _ for _ in ()
                ).throw(wait_error),
            )
            patch.setattr(os, "close", closed.append)
            assert managed._reap_adopted_zombies(((444, 4, "Z"),)) == expected
        assert closed == [99]

    with monkeypatch.context() as patch:
        patch.setattr(
            managed,
            "_open_owned_pidfd",
            lambda *_args, **_kwargs: (99, None),
        )
        patch.setattr(os, "waitid", lambda *_args: None)
        patch.setattr(os, "close", lambda _fd: None)
        assert managed._reap_adopted_zombies(((444, 4, "Z"),)) == (
            False,
            True,
            None,
        )

    drain_results = iter(((False, True, None), (False, False, None)))
    with monkeypatch.context() as patch:
        patch.setattr(
            managed,
            "_reap_adopted_zombies",
            lambda _members: next(drain_results),
        )
        await managed._drain_known_adopted_zombies()

    with monkeypatch.context() as patch:
        patch.setattr(process_supervisor, "_WITNESS_EXIT_GRACE_SECONDS", 0)
        patch.setattr(
            managed,
            "_reap_adopted_zombies",
            lambda _members: (False, True, None),
        )
        await managed._drain_known_adopted_zombies()


@pytest.mark.asyncio
async def test_unknown_group_state_and_signal_errors_return_failure(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    managed = await process_pool.launch(SLEEPING_PROCESS)
    await _read_pids(managed.process)

    def broken_scandir(_path: str) -> Any:
        raise OSError("proc unavailable")

    monkeypatch.setattr(os, "scandir", broken_scandir)
    result = await managed.cleanup(term_grace=0, kill_grace=0)
    assert result.status is CleanupStatus.FAILED
    assert result.detail == "could not inspect /proc: proc unavailable"


@pytest.mark.parametrize("disappearance", (FileNotFoundError, ProcessLookupError))
@pytest.mark.asyncio
async def test_proc_scan_ignores_disappeared_unrelated_entry(
    process_pool: ProcessPool,
    monkeypatch: pytest.MonkeyPatch,
    disappearance: type[OSError],
) -> None:
    managed = await process_pool.launch(SLEEPING_PROCESS)
    await _read_pids(managed.process)
    real_open = open
    skipped = False

    def disappearing_entry(path: str, *args: Any, **kwargs: Any) -> Any:
        nonlocal skipped
        if path == "/proc/1/stat":
            skipped = True
            raise disappearance(errno.ESRCH, "disappeared")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(
        process_supervisor, "open", disappearing_entry, raising=False
    )
    observation = managed._observe_group()

    assert skipped
    assert observation.state is _GroupState.LIVE


@pytest.mark.asyncio
async def test_proc_scan_fails_closed_for_unreadable_owned_member(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    managed = await process_pool.launch(SLEEPING_PROCESS)
    await _read_pids(managed.process)
    real_open = open
    member_path = f"/proc/{managed.process.pid}/stat"
    monkeypatch.setattr(
        managed, "_check_leader_identity", lambda: (True, None)
    )

    def unreadable_member(path: str, *args: Any, **kwargs: Any) -> Any:
        if path == member_path:
            raise PermissionError("member denied")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(
        process_supervisor, "open", unreadable_member, raising=False
    )
    observation = managed._observe_group()

    assert observation.state is _GroupState.UNKNOWN
    assert observation.detail == "could not inspect process-group member: member denied"


@pytest.mark.asyncio
async def test_signal_races_do_not_turn_disappearance_into_failure(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    managed = await process_pool.launch(SLEEPING_PROCESS)
    await _read_pids(managed.process)
    real_pidfd_send_signal = signal.pidfd_send_signal

    def disappeared(_pidfd: int, _sig: int) -> None:
        raise ProcessLookupError

    monkeypatch.setattr(signal, "pidfd_send_signal", disappeared)
    assert managed._signal_group(signal.SIGTERM) == (False, None)

    def denied(_pidfd: int, _sig: int) -> None:
        raise PermissionError("denied")

    monkeypatch.setattr(signal, "pidfd_send_signal", denied)
    sent, detail = managed._signal_group(signal.SIGTERM)
    assert not sent
    assert detail == "could not signal owned process-group member: denied"
    monkeypatch.setattr(signal, "pidfd_send_signal", real_pidfd_send_signal)


@pytest.mark.asyncio
async def test_term_is_delivered_once_per_owned_identity(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    managed = await process_pool.launch(TERM_IGNORING_PROCESS)
    await _read_pids(managed.process)
    real_pidfd_send_signal = signal.pidfd_send_signal
    delivered: list[int] = []

    def record_delivery(pidfd: int, sig: int) -> None:
        delivered.append(sig)
        real_pidfd_send_signal(pidfd, sig)

    monkeypatch.setattr(signal, "pidfd_send_signal", record_delivery)

    assert managed._signal_group(signal.SIGTERM) == (True, None)
    assert managed._signal_group(signal.SIGTERM) == (False, None)
    assert delivered == [signal.SIGTERM]
    monkeypatch.setattr(signal, "pidfd_send_signal", real_pidfd_send_signal)


@pytest.mark.asyncio
async def test_pidfd_delivery_fails_closed_when_host_support_is_missing(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    managed = await process_pool.launch(SLEEPING_PROCESS)
    await _read_pids(managed.process)
    real_sender = signal.pidfd_send_signal
    real_opener = os.pidfd_open

    monkeypatch.delattr(signal, "pidfd_send_signal")
    sent, detail = managed._signal_group(signal.SIGTERM)
    assert not sent
    assert detail == "pidfd signaling is unavailable on this Linux host"
    monkeypatch.setattr(signal, "pidfd_send_signal", real_sender, raising=False)

    monkeypatch.delattr(os, "pidfd_open")
    pidfd, error = managed._open_owned_pidfd(
        managed.process.pid, managed.identity.leader_start_time or 0
    )
    assert pidfd is None
    assert error is not None
    assert error.detail == "pidfd_open is unavailable on this Linux host"
    monkeypatch.setattr(os, "pidfd_open", real_opener, raising=False)


@pytest.mark.asyncio
async def test_snapshot_refuses_identity_changes_and_handle_races(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    managed = await process_pool.launch(SLEEPING_PROCESS)
    await _read_pids(managed.process)
    assert managed.identity.leader_start_time is not None
    monkeypatch.setattr(
        managed, "_check_leader_identity", lambda: (True, None)
    )
    managed._identity = replace(
        managed.identity, leader_start_time=managed.identity.leader_start_time + 1
    )

    observation, pidfds = managed._snapshot_group(acquire_pidfds=True)
    assert observation.state is _GroupState.UNPROVEN
    assert pidfds == []

    managed._identity = replace(
        managed.identity,
        leader_start_time=managed.identity.leader_start_time - 1,
        session_id=managed.identity.session_id + 1,
    )
    observation, pidfds = managed._snapshot_group(acquire_pidfds=True)
    assert observation.state is _GroupState.UNPROVEN
    assert "owned session" in observation.detail
    assert pidfds == []

    managed._identity = replace(
        managed.identity,
        session_id=managed.identity.session_id - 1,
    )
    unknown = _GroupObservation(_GroupState.UNKNOWN, "handle error")
    monkeypatch.setattr(
        managed, "_open_owned_pidfd", lambda _pid, _start: (None, unknown)
    )
    observation, pidfds = managed._snapshot_group(acquire_pidfds=True)
    assert observation is unknown
    assert pidfds == []

    monkeypatch.setattr(
        managed, "_open_owned_pidfd", lambda _pid, _start: (None, None)
    )
    observation, pidfds = managed._snapshot_group(acquire_pidfds=True)
    assert observation.state is _GroupState.CHANGED
    assert "members changed" in observation.detail
    assert pidfds == []


@pytest.mark.asyncio
async def test_pidfd_acquisition_revalidates_process_identity(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    managed = await process_pool.launch(SLEEPING_PROCESS)
    await _read_pids(managed.process)
    pid = managed.process.pid
    assert managed.identity.leader_start_time is not None
    start_time = managed.identity.leader_start_time
    real_opener = os.pidfd_open

    monkeypatch.setattr(
        os, "pidfd_open", lambda _pid: (_ for _ in ()).throw(ProcessLookupError())
    )
    assert managed._open_owned_pidfd(pid, start_time) == (None, None)

    monkeypatch.setattr(
        os,
        "pidfd_open",
        lambda _pid: (_ for _ in ()).throw(PermissionError("denied")),
    )
    pidfd, error = managed._open_owned_pidfd(pid, start_time)
    assert pidfd is None
    assert error is not None
    assert error.detail == "could not open stable process handle: denied"
    monkeypatch.setattr(os, "pidfd_open", real_opener)

    def unreadable(*_args: Any, **_kwargs: Any) -> Any:
        raise PermissionError("unreadable")

    monkeypatch.setattr(process_supervisor, "open", unreadable, raising=False)
    _, leader_error = managed._check_leader_identity()
    assert leader_error is not None
    assert leader_error.detail == "could not confirm leader identity: unreadable"
    pidfd, error = managed._open_owned_pidfd(pid, start_time)
    assert pidfd is None
    assert error is not None
    assert error.detail == "could not revalidate stable process handle: unreadable"
    monkeypatch.delattr(process_supervisor, "open")

    def disappeared(*_args: Any, **_kwargs: Any) -> Any:
        raise FileNotFoundError

    monkeypatch.setattr(process_supervisor, "open", disappeared, raising=False)
    assert managed._open_owned_pidfd(pid, start_time) == (None, None)
    assert managed._check_leader_identity() == (False, None)
    monkeypatch.delattr(process_supervisor, "open")

    def disappeared_with_esrch(*_args: Any, **_kwargs: Any) -> Any:
        raise ProcessLookupError(errno.ESRCH, "disappeared")

    monkeypatch.setattr(
        process_supervisor,
        "open",
        disappeared_with_esrch,
        raising=False,
    )
    assert managed._check_leader_identity() == (False, None)
    assert managed._revalidate_member(
        _GroupMember(pid, "S", start_time)
    ) == (False, None)
    assert managed._open_owned_pidfd(pid, start_time) == (None, None)
    monkeypatch.delattr(process_supervisor, "open")

    pidfd, error = managed._open_owned_pidfd(pid, start_time + 1)
    assert pidfd is None
    assert error is not None
    assert error.detail == "process identity changed while acquiring stable handle"

    original_identity = managed.identity
    managed._identity = replace(
        original_identity, process_group_id=original_identity.process_group_id + 1
    )
    assert managed._open_owned_pidfd(pid, start_time) == (None, None)
    managed._identity = original_identity


@pytest.mark.asyncio
async def test_lifecycle_witness_failures_are_bounded_and_fail_closed(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    managed = await process_pool.launch(SLEEPING_PROCESS)
    await _read_pids(managed.process)
    witness = managed._lifecycle_witness
    assert witness is not None
    witness_member = _GroupMember(witness.pid, "S", witness.start_time)
    unknown = _GroupObservation(_GroupState.UNKNOWN, "proof unavailable")

    with monkeypatch.context() as patch:
        patch.setattr(managed, "_revalidate_member", lambda _member: (False, unknown))
        assert managed._prove_group_ownership([witness_member]) is unknown

    with monkeypatch.context() as patch:
        patch.setattr(managed, "_revalidate_member", lambda _member: (False, None))
        observation = managed._prove_group_ownership([witness_member])
        assert observation is not None
        assert observation.detail == "lifecycle ownership witness is no longer live"

    leader_member = _GroupMember(
        managed.process.pid,
        "S",
        managed.identity.leader_start_time or 0,
    )
    witness.released = True
    assert managed._prove_group_ownership([leader_member]) is None
    unowned = replace(leader_member, start_time=leader_member.start_time + 1)
    observation = managed._prove_group_ownership([unowned])
    assert observation is not None
    assert observation.detail == (
        "process-group continuity is unproven; refusing to signal"
    )
    witness.released = False

    real_witness = managed._lifecycle_witness
    managed._lifecycle_witness = None
    assert not managed._witness_is_live()
    managed._reap_lifecycle_witness()
    await managed._close_lifecycle_witness()
    managed._lifecycle_witness = real_witness

    reaped: list[tuple[int, int]] = []
    with monkeypatch.context() as patch:
        patch.setattr(
            os,
            "waitpid",
            lambda pid, options: reaped.append((pid, options)) or (pid, 0),
        )
        managed._reap_lifecycle_witness()
    assert reaped == [(witness.pid, os.WNOHANG)]

    for error, expected in (
        (FileNotFoundError(), None),
        (PermissionError("denied"), "could not revalidate lifecycle ownership: denied"),
    ):
        with monkeypatch.context() as patch:
            patch.setattr(
                process_supervisor,
                "open",
                lambda *_args, error=error, **_kwargs: (_ for _ in ()).throw(error),
                raising=False,
            )
            stable, observation = managed._revalidate_member(leader_member)
        assert not stable
        if expected is None:
            assert observation is None
        else:
            assert observation is not None
            assert observation.detail == expected

    managed.process.terminate()
    assert await managed.process.wait() == -signal.SIGTERM
    witness_quiet = _GroupObservation(
        _GroupState.QUIESCENT,
        "witness still live",
        ((witness.pid, witness.start_time, "S"),),
        witness_live=True,
    )
    definitive = _GroupObservation(
        _GroupState.QUIESCENT,
        "group absent",
        definitive=True,
    )
    real_observe_group = managed._observe_group
    witness.released = True
    observations = iter((witness_quiet, definitive))
    monkeypatch.setattr(managed, "_observe_group", lambda: next(observations))
    assert await managed._wait_for_quiescence(0) == (definitive, False)

    with monkeypatch.context() as patch:
        patch.setattr(process_supervisor, "_WITNESS_EXIT_GRACE_SECONDS", 0)
        patch.setattr(managed, "_observe_group", lambda: witness_quiet)
        timed_out, signal_sent = await managed._wait_for_quiescence(0)
    assert timed_out.detail == "lifecycle witness remained live after release"
    assert not signal_sent

    monkeypatch.setattr(managed, "_observe_group", real_observe_group)
    witness.released = False

    liveness = iter((True, True, True, True, False))
    with monkeypatch.context() as patch:
        patch.setattr(process_supervisor, "_WITNESS_EXIT_GRACE_SECONDS", 0.01)
        patch.setattr(managed, "_witness_is_live", lambda: next(liveness))
        patch.setattr(
            signal,
            "pidfd_send_signal",
            lambda _pidfd, _sig: (_ for _ in ()).throw(ProcessLookupError()),
        )
        await managed._close_lifecycle_witness()
    assert managed._lifecycle_witness is None


@pytest.mark.asyncio
async def test_launch_deadline_helpers_bound_stubborn_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def stubborn(started: asyncio.Event, release: asyncio.Event) -> None:
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            await release.wait()

    started = asyncio.Event()
    release = asyncio.Event()
    task = asyncio.create_task(stubborn(started, release))
    await started.wait()
    await process_supervisor._settle_cancelled_task(task, 0)
    assert not task.done()
    release.set()
    await task

    started = asyncio.Event()
    release = asyncio.Event()
    task = asyncio.create_task(stubborn(started, release))
    await started.wait()
    with monkeypatch.context() as patch:
        patch.setattr(
            process_supervisor, "_LAUNCH_CLEANUP_TIMEOUT_SECONDS", 0.01
        )
        with pytest.raises(RuntimeError, match="cleanup exceeded"):
            await process_supervisor._wait_without_cancelling(task)
    release.set()
    await task

    class StubbornProcess:
        pid = 987654

        def __init__(self) -> None:
            self.kill_calls = 0

        def kill(self) -> None:
            self.kill_calls += 1
            if self.kill_calls == 2:
                raise ProcessLookupError

        async def wait(self) -> int:
            await asyncio.Event().wait()
            return 0

    fake_process = StubbornProcess()
    control_read, control_write = os.pipe()
    with monkeypatch.context() as patch:
        patch.setattr(
            process_supervisor, "_LAUNCH_CLEANUP_TIMEOUT_SECONDS", 0
        )
        await process_supervisor._finish_failed_launch(
            fake_process,  # type: ignore[arg-type]
            group_contained=False,
            control_fd=control_read,
            witness_pid=None,
            witness_pidfd=None,
        )
    os.close(control_write)
    assert fake_process.kill_calls == 2

    control_read, control_write = os.pipe()
    with monkeypatch.context() as patch:
        patch.setattr(os, "waitpid", lambda pid, _options: (pid, 0))
        await process_supervisor._finish_failed_launch(
            None,
            group_contained=False,
            control_fd=control_read,
            witness_pid=4321,
            witness_pidfd=None,
        )
    os.close(control_write)

    wait_results = iter(((0, 0), (4321, 0)))
    control_read, control_write = os.pipe()
    with monkeypatch.context() as patch:
        patch.setattr(
            os, "waitpid", lambda _pid, _options: next(wait_results)
        )
        patch.setattr(
            process_supervisor, "_LAUNCH_CLEANUP_TIMEOUT_SECONDS", 0.01
        )
        await process_supervisor._finish_failed_launch(
            None,
            group_contained=False,
            control_fd=control_read,
            witness_pid=4321,
            witness_pidfd=None,
        )
    os.close(control_write)

    control_read, control_write = os.pipe()
    stable_pidfd = os.pidfd_open(os.getpid())
    with monkeypatch.context() as patch:
        patch.setattr(os, "waitpid", lambda _pid, _options: (0, 0))
        patch.setattr(
            process_supervisor, "_LAUNCH_CLEANUP_TIMEOUT_SECONDS", 0
        )
        patch.setattr(
            signal,
            "pidfd_send_signal",
            lambda _pidfd, _sig: (_ for _ in ()).throw(ProcessLookupError()),
        )
        await process_supervisor._finish_failed_launch(
            None,
            group_contained=False,
            control_fd=control_read,
            witness_pid=4321,
            witness_pidfd=stable_pidfd,
        )
    os.close(control_write)


@pytest.mark.asyncio
async def test_launch_rejects_conflicts_and_unverified_session(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with pytest.raises(TypeError, match="preexec_fn, start_new_session"):
        await launch_process(
            sys.executable,
            "-c",
            "pass",
            preexec_fn=lambda: None,
            start_new_session=False,
        )
    with pytest.raises(TypeError, match=r"use launch_process\(\)"):
        SupervisedProcess(  # type: ignore[arg-type]
            object(), ProcessIdentity(2, 2, 2, 1), _proof=object()
        )
    with pytest.raises(TypeError, match="requires close_fds=True"):
        await launch_process("/bin/true", close_fds=False)
    with pytest.raises(TypeError):
        await launch_process("/bin/true", env={"INVALID_VALUE": 1})
    with pytest.raises(ValueError, match="illegal environment variable name"):
        await launch_process("/bin/true", env={"INVALID=NAME": "value"})
    with pytest.raises(ValueError, match="embedded null byte"):
        await launch_process("/bin/true", env={"VALID_NAME": "bad\0value"})
    with pytest.raises(ValueError, match="names collide after encoding"):
        await launch_process(
            "true",
            env={"PATH": os.defpath, b"PATH": os.fsencode(os.defpath)},
        )
    with pytest.raises(FileNotFoundError):
        await launch_process("/definitely/missing/process-supervisor-command")

    with monkeypatch.context() as patch:
        patch.setattr(process_supervisor, "_WITNESS_LAUNCHER", "pass")
        patch.setattr(
            asyncio.subprocess.Process,
            "kill",
            lambda _process: (_ for _ in ()).throw(ProcessLookupError()),
        )
        with pytest.raises(RuntimeError, match="witness did not start"):
            await launch_process(sys.executable, "-c", "pass")

    with monkeypatch.context() as patch:
        patch.setattr(os, "getpgid", lambda _pid: -1)
        with pytest.raises(RuntimeError, match="dedicated session"):
            await launch_process(
                sys.executable,
                "-c",
                "import time; time.sleep(10)",
                stdout=asyncio.subprocess.PIPE,
            )

    with monkeypatch.context() as patch:
        patch.setattr(
            process_supervisor,
            "_parse_proc_stat",
            lambda _stat: ("S", -1, -1, 1),
        )
        patch.setattr(
            asyncio.subprocess.Process,
            "kill",
            lambda _process: (_ for _ in ()).throw(ProcessLookupError()),
        )
        with pytest.raises(RuntimeError, match="ownership witness"):
            await launch_process(sys.executable, "-c", "pass")

    real_parse_proc_stat = process_supervisor._parse_proc_stat
    parse_calls = 0

    def changed_witness(stat: str) -> tuple[str, int, int, int]:
        nonlocal parse_calls
        parse_calls += 1
        parsed = real_parse_proc_stat(stat)
        if parse_calls == 2:
            return parsed[0], parsed[1], parsed[2], parsed[3] + 1
        return parsed

    target_marker = tmp_path / "target-ran"
    with monkeypatch.context() as patch:
        patch.setattr(process_supervisor, "_parse_proc_stat", changed_witness)
        with pytest.raises(RuntimeError, match="witness changed"):
            await launch_process(
                sys.executable,
                "-c",
                f"from pathlib import Path; Path({str(target_marker)!r}).touch()",
            )
    assert not target_marker.exists()

    spawn_cancellation_marker = tmp_path / "spawn-cancelled-target-ran"
    spawn_started = asyncio.Event()

    async def stalled_spawn(*_args: Any, **_kwargs: Any) -> Any:
        spawn_started.set()
        await asyncio.Event().wait()

    with monkeypatch.context() as patch:
        patch.setattr(asyncio, "create_subprocess_exec", stalled_spawn)
        patch.setattr(
            process_supervisor,
            "_LAUNCH_CANCELLATION_GRACE_SECONDS",
            0.01,
        )
        patch.setattr(
            process_supervisor, "_LAUNCH_CLEANUP_TIMEOUT_SECONDS", 0.01
        )
        launch_task = asyncio.create_task(
            launch_process(
                sys.executable,
                "-c",
                "from pathlib import Path; "
                f"Path({str(spawn_cancellation_marker)!r}).touch()",
            )
        )
        await asyncio.wait_for(spawn_started.wait(), timeout=1)
        launch_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(launch_task, timeout=1)
    assert not spawn_cancellation_marker.exists()

    cancellation_marker = tmp_path / "cancelled-target-ran"
    stalled_ready_started = asyncio.Event()
    real_read_witness_ready = process_supervisor._read_witness_ready

    async def observe_stalled_ready(ready_socket: Any) -> bytes:
        stalled_ready_started.set()
        return await real_read_witness_ready(ready_socket)

    with monkeypatch.context() as patch:
        patch.setattr(
            process_supervisor,
            "_WITNESS_LAUNCHER",
            "import signal; signal.pause()",
        )
        patch.setattr(
            process_supervisor, "_read_witness_ready", observe_stalled_ready
        )
        patch.setattr(
            process_supervisor,
            "_LAUNCH_CANCELLATION_GRACE_SECONDS",
            0.01,
        )
        patch.setattr(
            process_supervisor, "_LAUNCH_CLEANUP_TIMEOUT_SECONDS", 0.05
        )
        launch_task = asyncio.create_task(
            launch_process(
                sys.executable,
                "-c",
                f"from pathlib import Path; Path({str(cancellation_marker)!r}).touch()",
            )
        )
        await asyncio.wait_for(stalled_ready_started.wait(), timeout=1)
        launch_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(launch_task, timeout=1)

    with monkeypatch.context() as patch:
        patch.setattr(
            process_supervisor,
            "_WITNESS_LAUNCHER",
            "import signal; signal.pause()",
        )
        patch.setattr(process_supervisor, "_LAUNCH_PHASE_TIMEOUT_SECONDS", 0.01)
        patch.setattr(
            process_supervisor, "_LAUNCH_CLEANUP_TIMEOUT_SECONDS", 0.05
        )
        with pytest.raises(RuntimeError, match="readiness exceeded"):
            await asyncio.wait_for(
                launch_process(sys.executable, "-c", "pass"), timeout=1
            )

    exec_wait_started = asyncio.Event()

    async def briefly_delayed_exec_result(_ready_socket: Any) -> bytes:
        exec_wait_started.set()
        await asyncio.sleep(0.02)
        return b""

    with monkeypatch.context() as patch:
        patch.setattr(
            process_supervisor,
            "_read_until_eof",
            briefly_delayed_exec_result,
        )
        patch.setattr(
            process_supervisor,
            "_LAUNCH_CANCELLATION_GRACE_SECONDS",
            0.1,
        )
        patch.setattr(
            process_supervisor, "_LAUNCH_CLEANUP_TIMEOUT_SECONDS", 0.05
        )
        launch_task = asyncio.create_task(
            launch_process(
                sys.executable,
                "-c",
                "import signal; signal.pause()",
            )
        )
        await asyncio.wait_for(exec_wait_started.wait(), timeout=1)
        launch_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(launch_task, timeout=1)

    ready_received = asyncio.Event()
    release_ready = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    real_finish_failed_launch = process_supervisor._finish_failed_launch

    async def delayed_ready(ready_socket: Any) -> bytes:
        payload = await real_read_witness_ready(ready_socket)
        ready_received.set()
        await release_ready.wait()
        return payload

    async def delayed_cleanup(*args: Any, **kwargs: Any) -> None:
        cleanup_started.set()
        await release_cleanup.wait()
        await real_finish_failed_launch(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(process_supervisor, "_read_witness_ready", delayed_ready)
        patch.setattr(process_supervisor, "_finish_failed_launch", delayed_cleanup)
        launch_task = asyncio.create_task(
            launch_process(
                sys.executable,
                "-c",
                f"from pathlib import Path; Path({str(cancellation_marker)!r}).touch()",
            )
        )
        await asyncio.wait_for(ready_received.wait(), timeout=1)
        launch_task.cancel()
        release_ready.set()
        await asyncio.wait_for(cleanup_started.wait(), timeout=1)
        launch_task.cancel()
        release_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(launch_task, timeout=1)
    assert not cancellation_marker.exists()

    with monkeypatch.context() as patch:
        patch.setattr(process_supervisor, "_get_pidfd_opener", lambda: None)
        with pytest.raises(RuntimeError, match="pidfd_open is unavailable"):
            await launch_process(sys.executable, "-c", "pass")

    async def spawn_failed(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("spawn failed")

    with monkeypatch.context() as patch:
        patch.setattr(asyncio, "create_subprocess_exec", spawn_failed)
        with pytest.raises(RuntimeError, match="spawn failed"):
            await launch_process(sys.executable, "-c", "pass")


@pytest.mark.asyncio
async def test_early_launch_observation_and_unreadable_group_fallbacks(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_getpgid = os.getpgid
    monkeypatch.setattr(
        os, "getpgid", lambda _pid: (_ for _ in ()).throw(ProcessLookupError())
    )
    managed = await process_pool.launch("pass")
    assert await managed.process.wait() == 0
    monkeypatch.setattr(os, "getpgid", real_getpgid)
    witness = managed._lifecycle_witness
    assert witness is not None
    managed._release_lifecycle_witness()
    async with asyncio.timeout(1):
        while os.path.exists(f"/proc/{witness.pid}"):
            await asyncio.sleep(0.01)

    real_killpg = os.killpg
    monkeypatch.setattr(
        os, "killpg", lambda _pgid, _sig: (_ for _ in ()).throw(PermissionError("denied"))
    )
    observation = managed._observe_group()
    assert observation.state is _GroupState.UNKNOWN
    assert observation.detail == "could not confirm process-group state: denied"

    monkeypatch.setattr(os, "killpg", lambda _pgid, _sig: None)
    observation = managed._observe_group()
    assert observation.state is _GroupState.UNKNOWN
    assert observation.detail == "process group exists but no member could be proven owned"
    monkeypatch.setattr(os, "killpg", real_killpg)


@pytest.mark.asyncio
async def test_missing_leader_birth_token_can_confirm_quiescence(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_open = open
    stat_reads = 0

    def leader_disappeared(path: str, *args: Any, **kwargs: Any) -> Any:
        nonlocal stat_reads
        if path.startswith("/proc/") and path.endswith("/stat"):
            stat_reads += 1
            if stat_reads == 3:
                raise FileNotFoundError
        return real_open(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(
            process_supervisor,
            "open",
            leader_disappeared,
            raising=False,
        )
        managed = await process_pool.launch("pass")
    assert managed.identity.leader_start_time is None
    assert await managed.process.wait() == 0

    result = await managed.cleanup(term_grace=0.5, kill_grace=0.5)

    assert result.quiescent
    assert not result.term_sent
    assert not result.kill_sent


def test_parse_proc_stat_and_invalid_grace_validation() -> None:
    fields = " ".join(str(value) for value in range(1, 21))
    assert _parse_proc_stat(f"12 (name with ) paren) S {fields}") == (
        "S",
        2,
        3,
        19,
    )
    with pytest.raises(ValueError):
        _parse_proc_stat("invalid")


@pytest.mark.parametrize(
    ("term_grace", "kill_grace"),
    ((-1, 0), (math.inf, 0), (0, math.nan)),
)
@pytest.mark.asyncio
async def test_invalid_grace_is_rejected(
    process_pool: ProcessPool, term_grace: float, kill_grace: float
) -> None:
    managed = await process_pool.launch("pass")
    with pytest.raises(ValueError, match="finite and non-negative"):
        await managed.cleanup(term_grace=term_grace, kill_grace=kill_grace)
