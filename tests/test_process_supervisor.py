from __future__ import annotations

import asyncio
import math
import os
import signal
import sys
import time
from dataclasses import replace
from typing import Any

import pytest
import src.process_supervisor as process_supervisor
from src.process_supervisor import (
    CleanupStatus,
    ProcessIdentity,
    SupervisedProcess,
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

    async def unknown_after_term(_timeout: float) -> _GroupObservation:
        return unknown

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

    async def still_live(_timeout: float) -> _GroupObservation:
        return live

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
    assert await managed._wait_for_quiescence(0) is unknown
    assert managed._signal_group(signal.SIGTERM) == (False, "unknown")

    monkeypatch.setattr(
        managed, "_snapshot_group", lambda **_kwargs: (quiet, [])
    )
    timed_out = await managed._wait_for_quiescence(0)
    assert timed_out.state is _GroupState.LIVE
    assert timed_out.detail == "process group is quiet but leader was not reaped"
    assert managed._signal_group(signal.SIGTERM) == (False, None)


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
    monkeypatch.setattr(managed, "_check_leader_identity", lambda: None)
    managed._identity = replace(
        managed.identity, leader_start_time=managed.identity.leader_start_time + 1
    )

    observation, pidfds = managed._snapshot_group(acquire_pidfds=True)
    assert observation.state is _GroupState.UNPROVEN
    assert pidfds == []

    managed._identity = replace(
        managed.identity, leader_start_time=managed.identity.leader_start_time - 1
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
    assert observation.state is _GroupState.UNKNOWN
    assert "stable handles" in observation.detail
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
    leader_error = managed._check_leader_identity()
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
async def test_launch_rejects_conflicts_and_unverified_session(
    monkeypatch: pytest.MonkeyPatch,
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

    monkeypatch.setattr(os, "getpgid", lambda _pid: -1)
    with pytest.raises(RuntimeError, match="dedicated session"):
        await launch_process(sys.executable, "-c", "import time; time.sleep(10)")


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
async def test_launch_without_birth_token_refuses_live_group(
    process_pool: ProcessPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    def disappeared(*_args: Any, **_kwargs: Any) -> Any:
        raise FileNotFoundError

    monkeypatch.setattr(process_supervisor, "open", disappeared, raising=False)
    managed = await process_pool.launch(SLEEPING_PROCESS)
    assert managed.identity.leader_start_time is None
    monkeypatch.delattr(process_supervisor, "open")
    await _read_pids(managed.process)
    result = await managed.cleanup(term_grace=0, kill_grace=0)
    assert result.status is CleanupStatus.FAILED
    assert result.detail == "process-group leader identity is unproven"


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
