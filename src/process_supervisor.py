"""Bounded lifecycle management for one owned Linux process group.

The supervisor contains ordinary descendants that remain in the dedicated
session/process group created at launch.  A descendant that creates another
session, moves to another process group, or crosses a container boundary needs
separate containment (for example, a cgroup or container supervisor).
TERM and KILL are delivered only through revalidated per-member Linux pidfds;
the recyclable numeric process-group ID is never a signal-delivery target.
An internal same-session witness keeps ownership continuity independent of
the caller's stdio choices.  If that proof is lost, cleanup fails without
signaling a numerically matching group.
"""

from __future__ import annotations

import asyncio
import errno
import logging
import math
import os
import pickle
import select
import signal
import socket
import sys
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

_POLL_INTERVAL_SECONDS = 0.01
_SNAPSHOT_RETRIES = 3
# Zero-grace cleanup still gets a few immediate reconciliation scans, but
# membership churn cannot extend a caller's TERM/KILL grace indefinitely.
_RECONCILIATION_RETRIES_AFTER_DEADLINE = 3
_LEADER_EXIT_GRACE_SECONDS = 1.0
_WITNESS_EXIT_GRACE_SECONDS = 1.0
_LAUNCH_PHASE_TIMEOUT_SECONDS = 10.0
_LAUNCH_CANCELLATION_GRACE_SECONDS = 0.25
_LAUNCH_CLEANUP_TIMEOUT_SECONDS = 1.0
# Adapter execution waits for pipe EOF only after owned-process cleanup.  These
# bounds keep a broken or escaped pipe holder from leaving output-reader tasks
# pending indefinitely on failure paths.
_OUTPUT_DRAIN_TIMEOUT_SECONDS = 1.0
_OUTPUT_CANCEL_TIMEOUT_SECONDS = 1.0
_ADAPTER_TERM_GRACE_SECONDS = 1.0
_ADAPTER_KILL_GRACE_SECONDS = 1.0
_DISAPPEARED_ERRNOS = {errno.ENOENT, errno.ESRCH}
_RESTORED_SIGNAL_NAMES = ("SIGPIPE", "SIGXFZ", "SIGXFSZ")
_CANCELLED_RESULT_ATTR = "_pipeline_process_run_result"
_LAUNCH_PROOF = object()
_WITNESS_LAUNCHER = r"""
import os
import pickle
import signal
import sys


def read_exact(fd, size):
    data = bytearray()
    while len(data) < size:
        chunk = os.read(fd, size - len(data))
        if not chunk:
            break
        data.extend(chunk)
    return bytes(data)

control_fd = int(sys.argv[1])
ready_fd = int(sys.argv[2])
signal_modes = sys.argv[3]
target_executable = sys.argv[4]
target_argv = sys.argv[5:]
max_fd = os.sysconf("SC_OPEN_MAX")
pid_read, pid_write = os.pipe()

broker_pid = os.fork()
if broker_pid == 0:
    os.close(pid_read)
    witness_pid = os.fork()
    if witness_pid == 0:
        os.close(ready_fd)
        os.closerange(0, control_fd)
        os.closerange(control_fd + 1, max_fd)
        while os.read(control_fd, 1):
            pass
        os._exit(0)
    os.write(pid_write, str(witness_pid).encode("ascii"))
    os._exit(0)

os.close(pid_write)
witness_pid = os.read(pid_read, 64)
os.close(pid_read)
os.waitpid(broker_pid, 0)
os.write(ready_fd, b"W" + witness_pid + b"\n")
header = read_exact(ready_fd, 9)
if len(header) != 9 or header[:1] != b"A":
    os._exit(126)
target_env = pickle.loads(read_exact(ready_fd, int.from_bytes(header[1:], "big")))
os.close(control_fd)
os.set_inheritable(ready_fd, False)
for signal_name, mode in zip(("SIGPIPE", "SIGXFZ", "SIGXFSZ"), signal_modes):
    target_signal = getattr(signal, signal_name, None)
    if target_signal is not None:
        disposition = signal.SIG_IGN if mode == "I" else signal.SIG_DFL
        signal.signal(target_signal, disposition)
try:
    os.execvpe(target_executable, target_argv, target_env)
except OSError as exc:
    os.write(ready_fd, b"E" + str(exc.errno).encode("ascii") + b"\n")
    os._exit(127)
"""


class CleanupStatus(str, Enum):
    """Final state of a supervised process-group cleanup."""

    QUIESCENT = "quiescent"
    FAILED = "cleanup_failed"


@dataclass(frozen=True)
class ProcessIdentity:
    """Linux identifiers proven by ``start_new_session`` at launch."""

    leader_pid: int
    process_group_id: int
    session_id: int
    leader_start_time: int | None


@dataclass(frozen=True)
class CleanupResult:
    """Outcome cached by :meth:`SupervisedProcess.cleanup`."""

    status: CleanupStatus
    leader_returncode: int | None
    term_sent: bool
    kill_sent: bool
    detail: str | None = None

    @property
    def quiescent(self) -> bool:
        return self.status is CleanupStatus.QUIESCENT


@dataclass(frozen=True)
class ProcessRunResult:
    """Completed leader result plus output captured through group cleanup."""

    returncode: int
    stdout: bytes
    stderr: bytes
    timed_out: bool = False
    failure_detail: str | None = None


def cancelled_process_result(
    cancellation: asyncio.CancelledError,
) -> ProcessRunResult | None:
    """Return output captured before a supervised wait was cancelled.

    Cancellation still propagates to every existing caller. The attached
    result is an opt-in handoff for callers that intentionally cancelled an
    already-cleaned process and need its real exit status and diagnostics.
    """
    result = getattr(cancellation, _CANCELLED_RESULT_ATTR, None)
    return result if isinstance(result, ProcessRunResult) else None


class ProcessSupervisionError(RuntimeError):
    """An explicit launch-independent lifecycle or output-drain failure."""

    def __init__(self, detail: str, *, stdout: bytes, stderr: bytes) -> None:
        super().__init__(detail)
        self.stdout = stdout
        self.stderr = stderr


class _GroupState(Enum):
    LIVE = "live"
    QUIESCENT = "quiescent"
    CHANGED = "changed"
    UNPROVEN = "unproven"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class _GroupObservation:
    state: _GroupState
    detail: str
    members: tuple[tuple[int, int, str], ...] = ()
    definitive: bool = False
    witness_live: bool = False


@dataclass(frozen=True)
class _GroupMember:
    pid: int
    state: str
    start_time: int

    @property
    def identity(self) -> tuple[int, int]:
        return self.pid, self.start_time


@dataclass
class _LifecycleWitness:
    pid: int
    start_time: int
    pidfd: int
    control_fd: int
    released: bool = False


class SupervisedProcess:
    """An asyncio subprocess and the dedicated Linux group it owns.

    Construct instances with :func:`launch_process`.  ``process`` remains the
    original ``asyncio.subprocess.Process`` so later CLI-adapter integration can
    preserve existing communicate and callback contracts.
    """

    def __init__(
        self,
        process: asyncio.subprocess.Process,
        identity: ProcessIdentity,
        *,
        _proof: object,
        _lifecycle_witness: _LifecycleWitness | None = None,
    ) -> None:
        if _proof is not _LAUNCH_PROOF:
            raise TypeError("use launch_process() to create a supervised process")
        self._process = process
        self._identity = identity
        self._cleanup_task: asyncio.Task[CleanupResult] | None = None
        self._lifecycle_witness = _lifecycle_witness
        self._signaled_members: dict[
            signal.Signals, set[tuple[int, int]]
        ] = {
            signal.SIGTERM: set(),
            signal.SIGKILL: set(),
        }
        self._known_members: set[tuple[int, int]] = set()
        if identity.leader_start_time is not None:
            self._known_members.add(
                (identity.leader_pid, identity.leader_start_time)
            )
        if _lifecycle_witness is not None:
            self._known_members.add(
                (_lifecycle_witness.pid, _lifecycle_witness.start_time)
            )

    @property
    def process(self) -> asyncio.subprocess.Process:
        return self._process

    @property
    def identity(self) -> ProcessIdentity:
        return self._identity

    async def cleanup(
        self,
        *,
        term_grace: float = 5.0,
        kill_grace: float = 5.0,
    ) -> CleanupResult:
        """Stop the owned group once and return its confirmed final state.

        Concurrent and repeated calls share one result; the first call's grace
        values win.  Caller cancellation is remembered and re-raised only after
        the shielded, bounded cleanup task finishes.  Each workload grace may
        be followed by at most ``_RECONCILIATION_RETRIES_AFTER_DEADLINE`` fresh
        snapshots; leader reaping and lifecycle-witness exit have separate
        bounded confirmation graces.
        """
        if self._cleanup_task is None:
            if (
                not math.isfinite(term_grace)
                or not math.isfinite(kill_grace)
                or term_grace < 0
                or kill_grace < 0
            ):
                raise ValueError("cleanup grace periods must be finite and non-negative")
            self._cleanup_task = asyncio.create_task(
                self._cleanup_impl(term_grace, kill_grace)
            )

        cancellation: asyncio.CancelledError | None = None
        while not self._cleanup_task.done():
            try:
                await asyncio.shield(self._cleanup_task)
            except asyncio.CancelledError as exc:
                cancellation = exc
        result = self._cleanup_task.result()
        if cancellation is not None:
            raise cancellation
        return result

    async def _cleanup_impl(
        self, term_grace: float, kill_grace: float
    ) -> CleanupResult:
        try:
            term_sent = False
            kill_sent = False

            observation = self._observe_group()
            if observation.state in {_GroupState.UNPROVEN, _GroupState.UNKNOWN}:
                return self._failure(term_sent, kill_sent, observation.detail)
            if observation.state is _GroupState.LIVE:
                term_sent, error = self._signal_group(signal.SIGTERM)
                if error is not None:
                    return self._failure(term_sent, kill_sent, error)

            observation, repeated_term = await self._wait_for_quiescence(
                term_grace, repeat_signal=signal.SIGTERM
            )
            term_sent = term_sent or repeated_term
            if observation.state is _GroupState.QUIESCENT:
                return await self._success(term_sent, kill_sent)
            if observation.state in {_GroupState.UNPROVEN, _GroupState.UNKNOWN}:
                return self._failure(term_sent, kill_sent, observation.detail)

            kill_sent, error = self._signal_group(signal.SIGKILL)
            if error is not None:
                return self._failure(term_sent, kill_sent, error)
            observation, repeated_kill = await self._wait_for_quiescence(
                kill_grace, repeat_signal=signal.SIGKILL
            )
            kill_sent = kill_sent or repeated_kill
            if observation.state is _GroupState.QUIESCENT:
                return await self._success(term_sent, kill_sent)
            return self._failure(term_sent, kill_sent, observation.detail)
        finally:
            await self._drain_known_adopted_zombies()
            await self._close_lifecycle_witness()

    async def _success(self, term_sent: bool, kill_sent: bool) -> CleanupResult:
        # A non-None returncode means asyncio's child watcher reaped the leader;
        # Process.wait() may still depend on inherited stdout/stderr reaching EOF.
        return CleanupResult(
            status=CleanupStatus.QUIESCENT,
            leader_returncode=self.process.returncode,
            term_sent=term_sent,
            kill_sent=kill_sent,
        )

    def _failure(
        self, term_sent: bool, kill_sent: bool, detail: str
    ) -> CleanupResult:
        return CleanupResult(
            status=CleanupStatus.FAILED,
            leader_returncode=self.process.returncode,
            term_sent=term_sent,
            kill_sent=kill_sent,
            detail=detail,
        )

    async def _wait_for_quiescence(
        self,
        timeout: float,
        *,
        repeat_signal: signal.Signals | None = None,
    ) -> tuple[_GroupObservation, bool]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        witness_exit_deadline: float | None = None
        retries_after_deadline = _RECONCILIATION_RETRIES_AFTER_DEADLINE
        quiet_members: tuple[tuple[int, int, str], ...] | None = None
        signal_sent = False

        def retry_is_bounded() -> bool:
            nonlocal retries_after_deadline
            if loop.time() < deadline:
                return True
            if retries_after_deadline <= 0:
                return False
            retries_after_deadline -= 1
            return True

        while True:
            observation = self._observe_group()
            if (
                observation.state is _GroupState.QUIESCENT
                and self.process.returncode is None
            ):
                try:
                    await asyncio.wait_for(
                        self.process.wait(), _LEADER_EXIT_GRACE_SECONDS
                    )
                except TimeoutError:
                    return _GroupObservation(
                        _GroupState.UNKNOWN,
                        "process group is quiet but leader was not reaped",
                    ), signal_sent
                # Re-enumerate after the child watcher publishes the return
                # code; the earlier snapshot may have overlapped the exit.
                quiet_members = None
                continue
            if (
                observation.state is _GroupState.QUIESCENT
                and self.process.returncode is not None
            ):
                membership_changed, _, reap_error = (
                    self._reap_adopted_zombies(observation.members)
                )
                if reap_error is not None:
                    return _GroupObservation(
                        _GroupState.UNKNOWN, reap_error
                    ), signal_sent
                if membership_changed:
                    quiet_members = None
                    if not retry_is_bounded():
                        return _GroupObservation(
                            _GroupState.LIVE,
                            "process-group membership kept changing after "
                            "the grace period",
                        ), signal_sent
                    # Re-enumerate after every reap or disappearance.  The
                    # resulting snapshot, not the stale zombie-only one, is
                    # the evidence used to establish quiescence.
                    await asyncio.sleep(0)
                    continue
                if observation.definitive:
                    return observation, signal_sent
                if observation.witness_live:
                    witness = self._lifecycle_witness
                    if (
                        witness is not None
                        and not witness.released
                        and quiet_members == observation.members
                    ):
                        self._release_lifecycle_witness()
                        witness_exit_deadline = (
                            loop.time() + _WITNESS_EXIT_GRACE_SECONDS
                        )
                        quiet_members = None
                    elif witness is not None and witness.released:
                        if witness_exit_deadline is None:
                            witness_exit_deadline = (
                                loop.time() + _WITNESS_EXIT_GRACE_SECONDS
                            )
                        remaining = witness_exit_deadline - loop.time()
                        if remaining <= 0:
                            return _GroupObservation(
                                _GroupState.LIVE,
                                "lifecycle witness remained live after release",
                            ), signal_sent
                        await asyncio.sleep(
                            min(_POLL_INTERVAL_SECONDS, remaining)
                        )
                        continue
                    else:
                        quiet_members = observation.members
                        if not retry_is_bounded():
                            return _GroupObservation(
                                _GroupState.LIVE,
                                "lifecycle witness membership kept changing "
                                "after the grace period",
                            ), signal_sent
                    await asyncio.sleep(0)
                    continue
                if quiet_members == observation.members:
                    return observation, signal_sent
                if quiet_members is not None and loop.time() >= deadline:
                    return _GroupObservation(
                        _GroupState.LIVE,
                        "process-group quiescence could not be confirmed "
                        "before the grace period",
                    ), signal_sent
                quiet_members = observation.members
                # A second, fresh /proc enumeration is required because a
                # parent can fork and exit while the first scan is in flight.
                await asyncio.sleep(0)
                continue
            quiet_members = None
            if observation.state in {_GroupState.UNPROVEN, _GroupState.UNKNOWN}:
                return observation, signal_sent
            if observation.state is _GroupState.LIVE and repeat_signal is not None:
                sent, error = self._signal_group(repeat_signal)
                signal_sent = signal_sent or sent
                if error is not None:
                    return (
                        _GroupObservation(_GroupState.UNKNOWN, error),
                        signal_sent,
                    )
            remaining = deadline - loop.time()
            if remaining <= 0:
                detail = "process group remained live after the grace period"
                return _GroupObservation(_GroupState.LIVE, detail), signal_sent
            await asyncio.sleep(min(_POLL_INTERVAL_SECONDS, remaining))

    def _signal_group(self, sig: signal.Signals) -> tuple[bool, str | None]:
        already_signaled = self._signaled_members.setdefault(sig, set())
        for _ in range(_SNAPSHOT_RETRIES):
            observation, pidfds = self._snapshot_group(
                acquire_pidfds=True,
                exclude_identities=already_signaled,
            )
            if observation.state is not _GroupState.CHANGED:
                break
        else:
            return False, "process-group membership kept changing during signaling"
        if observation.state is _GroupState.QUIESCENT:
            return False, None
        if observation.state is not _GroupState.LIVE:
            return False, observation.detail
        sender = getattr(signal, "pidfd_send_signal", None)
        if sender is None:
            self._close_pidfds(pidfds)
            return False, "pidfd signaling is unavailable on this Linux host"
        sent = False
        try:
            for identity, pidfd in pidfds:
                try:
                    sender(pidfd, sig)
                except ProcessLookupError:
                    continue
                except OSError as exc:
                    return sent, f"could not signal owned process-group member: {exc}"
                already_signaled.add(identity)
                sent = True
        finally:
            self._close_pidfds(pidfds)
        return sent, None

    def _observe_group(self) -> _GroupObservation:
        observation, pidfds = self._snapshot_group(acquire_pidfds=False)
        self._close_pidfds(pidfds)
        return observation

    def _snapshot_group(
        self,
        *,
        acquire_pidfds: bool,
        exclude_identities: set[tuple[int, int]] | None = None,
    ) -> tuple[_GroupObservation, list[tuple[tuple[int, int], int]]]:
        pgid = self.identity.process_group_id
        if pgid <= 1 or pgid == os.getpgrp():
            return (
                _GroupObservation(
                    _GroupState.UNPROVEN,
                    "refusing to inspect or signal the daemon process group",
                ),
                [],
            )

        _, leader_error = self._check_leader_identity()
        if leader_error is not None:
            return leader_error, []
        members: list[_GroupMember] = []
        pidfds: list[tuple[tuple[int, int], int]] = []
        try:
            with os.scandir("/proc") as entries:
                for entry in entries:
                    if not entry.name.isdigit():
                        continue
                    try:
                        with open(
                            f"/proc/{entry.name}/stat", encoding="utf-8"
                        ) as stat_file:
                            stat = stat_file.read()
                        state, member_pgid, member_sid, start_time = _parse_proc_stat(
                            stat
                        )
                    except FileNotFoundError:
                        continue
                    except (OSError, IndexError, ValueError) as exc:
                        if (
                            isinstance(exc, OSError)
                            and exc.errno in _DISAPPEARED_ERRNOS
                        ):
                            continue
                        self._close_pidfds(pidfds)
                        return (
                            _GroupObservation(
                                _GroupState.UNKNOWN,
                                f"could not inspect process-group member: {exc}",
                            ),
                            [],
                        )
                    if member_pgid != pgid:
                        continue
                    if member_sid != self.identity.session_id:
                        self._close_pidfds(pidfds)
                        return (
                            _GroupObservation(
                                _GroupState.UNPROVEN,
                                "process-group identity no longer belongs to the owned session",
                            ),
                            [],
                        )
                    if (
                        int(entry.name) == self.identity.leader_pid
                        and self.identity.leader_start_time is not None
                        and start_time != self.identity.leader_start_time
                    ):
                        self._close_pidfds(pidfds)
                        return (
                            _GroupObservation(
                                _GroupState.UNPROVEN,
                                "process-group leader identity is unproven",
                            ),
                            [],
                        )
                    members.append(
                        _GroupMember(int(entry.name), state, start_time)
                    )
        except OSError as exc:
            self._close_pidfds(pidfds)
            return (
                _GroupObservation(
                    _GroupState.UNKNOWN, f"could not inspect /proc: {exc}"
                ),
                [],
            )

        witness = self._lifecycle_witness
        witness_identity = (
            (witness.pid, witness.start_time) if witness is not None else None
        )
        witness_live = any(
            member.identity == witness_identity and member.state != "Z"
            for member in members
        )
        live_members = [
            member
            for member in members
            if member.state != "Z" and member.identity != witness_identity
        ]
        signal_candidates = [
            member
            for member in live_members
            if exclude_identities is None
            or member.identity not in exclude_identities
        ]
        if acquire_pidfds:
            for member in signal_candidates:
                pidfd, error = self._open_owned_pidfd(
                    member.pid, member.start_time
                )
                if error is not None:
                    self._close_pidfds(pidfds)
                    return error, []
                if pidfd is not None:
                    pidfds.append((member.identity, pidfd))

        if members:
            ownership_error = self._prove_group_ownership(members)
            if ownership_error is not None:
                self._close_pidfds(pidfds)
                return ownership_error, []
            self._known_members.update(member.identity for member in members)

        fingerprint = tuple(
            sorted(
                (member.pid, member.start_time, member.state)
                for member in members
            )
        )
        if live_members:
            if acquire_pidfds and signal_candidates and not pidfds:
                return (
                    _GroupObservation(
                        _GroupState.CHANGED,
                        "owned members changed before stable handles were acquired",
                    ),
                    [],
                )
            return (
                _GroupObservation(
                    _GroupState.LIVE,
                    "owned process group is live",
                    fingerprint,
                    witness_live=witness_live,
                ),
                pidfds,
            )
        if members:
            return (
                _GroupObservation(
                    _GroupState.QUIESCENT,
                    "owned process group has no live workload members",
                    fingerprint,
                    witness_live=witness_live,
                ),
                [],
            )
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return (
                _GroupObservation(
                    _GroupState.QUIESCENT,
                    "process group disappeared",
                    definitive=True,
                ),
                [],
            )
        except OSError as exc:
            return (
                _GroupObservation(
                    _GroupState.UNKNOWN,
                    f"could not confirm process-group state: {exc}",
                ),
                [],
            )
        return (
            _GroupObservation(
                _GroupState.UNKNOWN,
                "process group exists but no member could be proven owned",
            ),
            [],
        )

    def _check_leader_identity(
        self,
    ) -> tuple[bool, _GroupObservation | None]:
        try:
            with open(
                f"/proc/{self.identity.leader_pid}/stat", encoding="utf-8"
            ) as stat_file:
                _, pgid, sid, start_time = _parse_proc_stat(stat_file.read())
        except FileNotFoundError:
            return False, None
        except (OSError, IndexError, ValueError) as exc:
            if (
                isinstance(exc, OSError)
                and exc.errno in _DISAPPEARED_ERRNOS
            ):
                return False, None
            return False, _GroupObservation(
                _GroupState.UNKNOWN,
                f"could not confirm leader identity: {exc}",
            )
        if (
            self.identity.leader_start_time is not None
            and start_time != self.identity.leader_start_time
        ):
            # This PID is no longer the unreaped direct child.  Treat it like
            # a missing leader; the later full scan must still prove the
            # original group through the stable witness or known identities.
            return False, None
        if (
            pgid != self.identity.process_group_id
            or sid != self.identity.session_id
        ):
            return False, _GroupObservation(
                _GroupState.UNPROVEN,
                "process-group identity no longer belongs to the owned session",
            )
        return True, None

    def _prove_group_ownership(
        self, members: list[_GroupMember]
    ) -> _GroupObservation | None:
        witness = self._lifecycle_witness
        if witness is not None:
            for member in members:
                if member.identity != (witness.pid, witness.start_time):
                    continue
                stable, error = self._revalidate_member(member)
                if error is not None:
                    return error
                if stable and member.state != "Z" and self._witness_is_live():
                    return None
                break
            if not witness.released:
                return _GroupObservation(
                    _GroupState.UNPROVEN,
                    "lifecycle ownership witness is no longer live",
                )

        if all(member.identity in self._known_members for member in members):
            return None
        return _GroupObservation(
            _GroupState.UNPROVEN,
            "process-group continuity is unproven; refusing to signal",
        )

    def _witness_is_live(self) -> bool:
        witness = self._lifecycle_witness
        if witness is None:
            return False
        poller = select.poll()
        poller.register(witness.pidfd, select.POLLIN)
        if not poller.poll(0):
            return True
        self._reap_lifecycle_witness()
        return False

    def _reap_lifecycle_witness(self) -> None:
        witness = self._lifecycle_witness
        if witness is None:
            return
        try:
            os.waitpid(witness.pid, os.WNOHANG)
        except ChildProcessError:
            # Outside a PID-1 daemon, the host's init process owns reaping.
            pass

    def _reap_adopted_zombies(
        self, members: tuple[tuple[int, int, str], ...]
    ) -> tuple[bool, bool, str | None]:
        """Reap owned descendants adopted by a PID-1/subreaper caller."""
        witness = self._lifecycle_witness
        witness_identity = (
            (witness.pid, witness.start_time) if witness is not None else None
        )
        membership_changed = False
        adopted_child_pending = False
        for pid, start_time, state in members:
            identity = (pid, start_time)
            if (
                state != "Z"
                or identity == witness_identity
                or pid == self.identity.leader_pid
            ):
                continue
            pidfd, error = self._open_owned_pidfd(
                pid, start_time, allow_zombie=True
            )
            if error is not None:
                return membership_changed, adopted_child_pending, error.detail
            if pidfd is None:
                membership_changed = True
                continue
            try:
                try:
                    waited = os.waitid(
                        os.P_PIDFD, pidfd, os.WEXITED | os.WNOHANG
                    )
                except ChildProcessError:
                    # A different parent (normally host init) owns reaping.
                    continue
                except OSError as exc:
                    if exc.errno in _DISAPPEARED_ERRNOS:
                        membership_changed = True
                        continue
                    return (
                        membership_changed,
                        adopted_child_pending,
                        f"could not reap owned process-group member: {exc}",
                    )
                if waited is None:
                    adopted_child_pending = True
                else:
                    membership_changed = True
            finally:
                os.close(pidfd)
        return membership_changed, adopted_child_pending, None

    async def _drain_known_adopted_zombies(self) -> None:
        """Boundedly reap known descendants before caching a cleanup result."""
        known_members = tuple(
            (pid, start_time, "Z")
            for pid, start_time in self._known_members
        )
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _WITNESS_EXIT_GRACE_SECONDS
        while known_members:
            _, pending, error = self._reap_adopted_zombies(known_members)
            if error is None and not pending:
                return
            remaining = deadline - loop.time()
            if remaining <= 0:
                return
            await asyncio.sleep(min(_POLL_INTERVAL_SECONDS, remaining))

    def _revalidate_member(
        self, member: _GroupMember
    ) -> tuple[bool, _GroupObservation | None]:
        try:
            with open(f"/proc/{member.pid}/stat", encoding="utf-8") as stat_file:
                _, pgid, sid, start_time = _parse_proc_stat(stat_file.read())
        except FileNotFoundError:
            return False, None
        except (OSError, IndexError, ValueError) as exc:
            if (
                isinstance(exc, OSError)
                and exc.errno in _DISAPPEARED_ERRNOS
            ):
                return False, None
            return False, _GroupObservation(
                _GroupState.UNKNOWN,
                f"could not revalidate lifecycle ownership: {exc}",
            )
        return (
            start_time == member.start_time
            and pgid == self.identity.process_group_id
            and sid == self.identity.session_id,
            None,
        )

    def _open_owned_pidfd(
        self,
        pid: int,
        expected_start_time: int,
        *,
        allow_zombie: bool = False,
    ) -> tuple[int | None, _GroupObservation | None]:
        opener = getattr(os, "pidfd_open", None)
        if opener is None:
            return None, _GroupObservation(
                _GroupState.UNKNOWN, "pidfd_open is unavailable on this Linux host"
            )
        try:
            pidfd = opener(pid)
        except ProcessLookupError:
            return None, None
        except OSError as exc:
            return None, _GroupObservation(
                _GroupState.UNKNOWN, f"could not open stable process handle: {exc}"
            )
        keep_open = False
        try:
            try:
                with open(f"/proc/{pid}/stat", encoding="utf-8") as stat_file:
                    state, pgid, sid, start_time = _parse_proc_stat(stat_file.read())
            except FileNotFoundError:
                return None, None
            except (OSError, IndexError, ValueError) as exc:
                if (
                    isinstance(exc, OSError)
                    and exc.errno in _DISAPPEARED_ERRNOS
                ):
                    return None, None
                return None, _GroupObservation(
                    _GroupState.UNKNOWN,
                    f"could not revalidate stable process handle: {exc}",
                )
            if start_time != expected_start_time:
                return None, _GroupObservation(
                    _GroupState.UNPROVEN,
                    "process identity changed while acquiring stable handle",
                )
            if (
                (state == "Z" and not allow_zombie)
                or pgid != self.identity.process_group_id
                or sid != self.identity.session_id
            ):
                return None, None
            keep_open = True
            return pidfd, None
        finally:
            if not keep_open:
                os.close(pidfd)

    @staticmethod
    def _close_pidfds(
        pidfds: list[tuple[tuple[int, int], int]]
    ) -> None:
        for _, pidfd in pidfds:
            os.close(pidfd)

    def _release_lifecycle_witness(self) -> None:
        witness = self._lifecycle_witness
        if witness is None or witness.released:
            return
        os.close(witness.control_fd)
        witness.released = True

    async def _close_lifecycle_witness(self) -> None:
        witness = self._lifecycle_witness
        if witness is None:
            return
        self._release_lifecycle_witness()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _WITNESS_EXIT_GRACE_SECONDS
        while self._witness_is_live() and loop.time() < deadline:
            await asyncio.sleep(_POLL_INTERVAL_SECONDS)
        if self._witness_is_live():
            sender = getattr(signal, "pidfd_send_signal", None)
            if sender is not None:
                try:
                    sender(witness.pidfd, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            force_deadline = loop.time() + _WITNESS_EXIT_GRACE_SECONDS
            while self._witness_is_live() and loop.time() < force_deadline:
                await asyncio.sleep(0)
        self._reap_lifecycle_witness()
        os.close(witness.pidfd)
        self._lifecycle_witness = None


def _parse_proc_stat(stat: str) -> tuple[str, int, int, int]:
    """Return state, group, session, and start time from a Linux proc stat."""
    fields = stat[stat.rindex(")") + 2 :].split()
    return fields[0], int(fields[2]), int(fields[3]), int(fields[19])


def _get_pidfd_opener() -> Any:
    return getattr(os, "pidfd_open", None)


def _validated_environment(environment: Any) -> dict[Any, Any]:
    copied = dict(os.environ if environment is None else environment)
    encoded_names: set[bytes] = set()
    for key, value in copied.items():
        encoded_key = os.fsencode(key)
        encoded_value = os.fsencode(value)
        if b"=" in encoded_key:
            raise ValueError("illegal environment variable name")
        if b"\0" in encoded_key or b"\0" in encoded_value:
            raise ValueError("embedded null byte")
        if encoded_key in encoded_names:
            raise ValueError("environment variable names collide after encoding")
        encoded_names.add(encoded_key)
    return copied


def _target_signal_modes(restore_signals: bool) -> str:
    modes = []
    for signal_name in _RESTORED_SIGNAL_NAMES:
        target_signal = getattr(signal, signal_name, None)
        ignored = (
            not restore_signals
            and target_signal is not None
            and signal.getsignal(target_signal) == signal.SIG_IGN
        )
        modes.append("I" if ignored else "D")
    return "".join(modes)


async def _read_witness_ready(ready_socket: socket.socket) -> bytes:
    ready_payload = bytearray()
    loop = asyncio.get_running_loop()
    while b"\n" not in ready_payload:
        chunk = await loop.sock_recv(ready_socket, 64)
        if not chunk:
            break
        ready_payload.extend(chunk)
    return bytes(ready_payload)


async def _read_until_eof(ready_socket: socket.socket) -> bytes:
    payload = bytearray()
    loop = asyncio.get_running_loop()
    while chunk := await loop.sock_recv(ready_socket, 64):
        payload.extend(chunk)
    return bytes(payload)


def _consume_task_result(task: asyncio.Task[Any]) -> None:
    try:
        task.result()
    except BaseException:
        pass


async def _settle_cancelled_task(task: asyncio.Task[Any], timeout: float) -> None:
    task.cancel()
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not task.done():
        remaining = deadline - loop.time()
        if remaining <= 0:
            task.add_done_callback(_consume_task_result)
            return
        try:
            await asyncio.wait_for(
                asyncio.shield(task),
                min(_POLL_INTERVAL_SECONDS, remaining),
            )
        except (asyncio.CancelledError, TimeoutError):
            continue
    _consume_task_result(task)


async def _await_launch_phase(
    task: asyncio.Task[Any],
    *,
    timeout_detail: str,
    cancellation: asyncio.CancelledError | None,
    cancellation_deadline: float | None,
) -> tuple[Any, asyncio.CancelledError | None, float | None]:
    """Await one launch phase with startup and post-cancellation bounds."""
    loop = asyncio.get_running_loop()
    phase_deadline = loop.time() + _LAUNCH_PHASE_TIMEOUT_SECONDS
    while not task.done():
        active_deadline = phase_deadline
        if cancellation_deadline is not None:
            active_deadline = min(active_deadline, cancellation_deadline)
        remaining = active_deadline - loop.time()
        if remaining <= 0:
            await _settle_cancelled_task(
                task, _LAUNCH_CLEANUP_TIMEOUT_SECONDS
            )
            if cancellation is not None:
                raise cancellation
            raise RuntimeError(timeout_detail)
        try:
            await asyncio.wait_for(asyncio.shield(task), remaining)
        except asyncio.CancelledError as exc:
            if cancellation is None:
                cancellation = exc
                cancellation_deadline = (
                    loop.time() + _LAUNCH_CANCELLATION_GRACE_SECONDS
                )
        except TimeoutError:
            continue
    return task.result(), cancellation, cancellation_deadline


async def _finish_failed_launch(
    process: asyncio.subprocess.Process | None,
    *,
    owned_process: SupervisedProcess | None,
    control_fd: int,
    witness_pid: int | None,
    witness_pidfd: int | None,
) -> None:
    if owned_process is not None:
        result = await owned_process.cleanup(
            term_grace=0,
            kill_grace=_LAUNCH_CLEANUP_TIMEOUT_SECONDS,
        )
        if result.quiescent:
            return
        raise RuntimeError(
            "failed launch cleanup could not confirm quiescence: "
            f"{result.detail or 'unknown cleanup failure'}"
        )

    try:
        if process is not None:
            try:
                process.kill()
            except OSError:
                pass
        os.close(control_fd)
        if process is not None:
            try:
                await asyncio.wait_for(
                    process.wait(), _LAUNCH_CLEANUP_TIMEOUT_SECONDS
                )
            except TimeoutError:
                try:
                    process.kill()
                except OSError:
                    pass
                try:
                    await asyncio.wait_for(
                        process.wait(), _LAUNCH_CLEANUP_TIMEOUT_SECONDS
                    )
                except TimeoutError:
                    pass
        if witness_pid is not None:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + _LAUNCH_CLEANUP_TIMEOUT_SECONDS
            force_sent = False
            while True:
                try:
                    reaped_pid, _ = os.waitpid(witness_pid, os.WNOHANG)
                except ChildProcessError:
                    break
                if reaped_pid == witness_pid:
                    break
                if loop.time() >= deadline:
                    if force_sent:
                        break
                    sender = getattr(signal, "pidfd_send_signal", None)
                    if sender is not None and witness_pidfd is not None:
                        try:
                            sender(witness_pidfd, signal.SIGKILL)
                        except OSError:
                            pass
                    force_sent = True
                    deadline = loop.time() + _LAUNCH_CLEANUP_TIMEOUT_SECONDS
                    continue
                await asyncio.sleep(_POLL_INTERVAL_SECONDS)
    finally:
        if witness_pidfd is not None:
            os.close(witness_pidfd)


async def _wait_without_cancelling(task: asyncio.Task[None]) -> None:
    loop = asyncio.get_running_loop()
    # Owned cleanup may spend one bound each on KILL, leader publication,
    # adopted-child reaping, witness exit, and forced witness exit.
    deadline = loop.time() + (6 * _LAUNCH_CLEANUP_TIMEOUT_SECONDS)
    while not task.done():
        remaining = deadline - loop.time()
        if remaining <= 0:
            await _settle_cancelled_task(
                task, _LAUNCH_CLEANUP_TIMEOUT_SECONDS
            )
            raise RuntimeError("failed launch cleanup exceeded its deadline")
        try:
            await asyncio.wait_for(asyncio.shield(task), remaining)
        except asyncio.CancelledError:
            continue
        except TimeoutError:
            continue
    task.result()


async def _capture_output(
    stream: asyncio.StreamReader, buffer: bytearray
) -> None:
    """Read one subprocess pipe incrementally so cancellation keeps diagnostics."""
    while chunk := await stream.read(64 * 1024):
        buffer.extend(chunk)


async def _wait_for_leader_exit(process: asyncio.subprocess.Process) -> int:
    """Observe child-watcher returncode publication without waiting for pipe EOF."""
    while process.returncode is None:
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)
    return process.returncode


async def _await_task_preserving_cancellation(
    task: asyncio.Task[Any],
) -> tuple[Any, asyncio.CancelledError | None]:
    """Let a bounded lifecycle task finish before propagating cancellation."""
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            cancellation = exc
    return task.result(), cancellation


async def _settle_execution_tasks(
    tasks: dict[str, asyncio.Task[Any]],
) -> tuple[str | None, asyncio.CancelledError | None]:
    """Settle output/wait tasks within the documented drain and cancel bounds."""
    async def settle() -> str | None:
        pending = {task for task in tasks.values() if not task.done()}
        if pending:
            _, pending = await asyncio.wait(
                pending, timeout=_OUTPUT_DRAIN_TIMEOUT_SECONDS
            )
        timed_out_names = sorted(
            name for name, task in tasks.items() if task in pending
        )
        for task in pending:
            task.cancel()
        if pending:
            _, pending = await asyncio.wait(
                pending, timeout=_OUTPUT_CANCEL_TIMEOUT_SECONDS
            )
            for task in pending:
                task.add_done_callback(_consume_task_result)

        failures = (
            [
                "execution task(s) did not finish within the output drain "
                "bound: " + ", ".join(timed_out_names)
            ]
            if timed_out_names
            else []
        )
        for name, task in tasks.items():
            if not task.done() or task.cancelled():
                continue
            try:
                task.result()
            except BaseException as exc:
                failures.append(f"{name} failed: {type(exc).__name__}: {exc}")
        return "; ".join(failures) or None

    settle_task = asyncio.create_task(settle())
    return await _await_task_preserving_cancellation(settle_task)


def _record_cancellation_failure(
    cancellation: asyncio.CancelledError, detail: str
) -> None:
    logger = logging.getLogger(__name__)
    logger.error("process cleanup during cancellation failed: %s", detail)
    cancellation.add_note(f"process cleanup failed: {detail}")


async def run_supervised_process(
    managed: SupervisedProcess,
    *,
    timeout: float | None,
    on_process_start: Callable[[asyncio.subprocess.Process], None] | None = None,
    on_supervised_process_start: Callable[[SupervisedProcess], None] | None = None,
) -> ProcessRunResult:
    """Run callbacks, wait for the leader, clean its group, and drain output.

    Leader completion is observed through child-watcher returncode publication,
    independently of pipe EOF.  Cleanup is allowed to finish despite caller
    cancellation.  After cleanup, stdout/stderr readers receive one second to
    observe EOF and one second to settle cancellation; any failure is explicit
    and carries bytes captured so far.  The cleanup implementation itself is
    bounded by its TERM/KILL, leader-reap, adopted-child, and lifecycle-witness
    grace periods.
    """
    process = managed.process
    stdout_buffer = bytearray()
    stderr_buffer = bytearray()
    execution_tasks: dict[str, asyncio.Task[Any]] = {
        "leader exit": asyncio.create_task(_wait_for_leader_exit(process))
    }
    if process.stdout is not None:
        execution_tasks["stdout reader"] = asyncio.create_task(
            _capture_output(process.stdout, stdout_buffer)
        )
    if process.stderr is not None:
        execution_tasks["stderr reader"] = asyncio.create_task(
            _capture_output(process.stderr, stderr_buffer)
        )

    primary_error: BaseException | None = None
    cancellation: asyncio.CancelledError | None = None
    timed_out = False
    leader_task = execution_tasks["leader exit"]
    try:
        if on_process_start is not None:
            on_process_start(process)
        if on_supervised_process_start is not None:
            on_supervised_process_start(managed)
        if timeout is None:
            await asyncio.shield(leader_task)
        else:
            await asyncio.wait_for(asyncio.shield(leader_task), timeout=timeout)
    except asyncio.TimeoutError:
        timed_out = True
    except asyncio.CancelledError as exc:
        cancellation = exc
    except BaseException as exc:
        primary_error = exc

    cleanup_result: CleanupResult | None = None
    cleanup_error: BaseException | None = None
    cleanup_task = asyncio.create_task(
        managed.cleanup(
            term_grace=_ADAPTER_TERM_GRACE_SECONDS,
            kill_grace=_ADAPTER_KILL_GRACE_SECONDS,
        )
    )
    try:
        cleanup_result, cleanup_cancellation = (
            await _await_task_preserving_cancellation(cleanup_task)
        )
        if cleanup_cancellation is not None:
            cancellation = cleanup_cancellation
    except BaseException as exc:
        cleanup_error = exc

    task_error, settle_cancellation = await _settle_execution_tasks(
        execution_tasks
    )
    if settle_cancellation is not None:
        cancellation = settle_cancellation

    stdout = bytes(stdout_buffer)
    stderr = bytes(stderr_buffer)
    failure_detail: str | None = None
    if cleanup_error is not None:
        failure_detail = (
            "cleanup raised "
            f"{type(cleanup_error).__name__}: {cleanup_error}"
        )
    elif cleanup_result is None:
        failure_detail = "cleanup produced no result"
    elif not cleanup_result.quiescent:
        failure_detail = cleanup_result.detail or "cleanup could not confirm quiescence"
    if task_error is not None:
        failure_detail = (
            f"{failure_detail}; {task_error}" if failure_detail else task_error
        )

    returncode = process.returncode
    if returncode is None and cleanup_result is not None:
        returncode = cleanup_result.leader_returncode
    run_result = (
        ProcessRunResult(
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
            timed_out=timed_out,
            failure_detail=failure_detail,
        )
        if returncode is not None
        else None
    )

    if cancellation is not None:
        if run_result is not None:
            setattr(cancellation, _CANCELLED_RESULT_ATTR, run_result)
        if failure_detail is not None:
            _record_cancellation_failure(cancellation, failure_detail)
        raise cancellation
    if failure_detail is not None:
        error = ProcessSupervisionError(
            failure_detail,
            stdout=stdout,
            stderr=stderr,
        )
        if primary_error is not None:
            raise error from primary_error
        raise error
    if primary_error is not None:
        raise primary_error

    if run_result is None:
        raise ProcessSupervisionError(
            "cleanup confirmed quiescence without a leader return code",
            stdout=stdout,
            stderr=stderr,
        )
    return run_result


async def launch_process(*program: str, **kwargs: Any) -> SupervisedProcess:
    """Launch ``program`` in a new Linux session and retain its group identity."""
    conflicts = {"start_new_session", "process_group", "preexec_fn"} & kwargs.keys()
    if conflicts:
        names = ", ".join(sorted(conflicts))
        raise TypeError(f"launch_process owns subprocess option(s): {names}")
    if "close_fds" in kwargs and not kwargs["close_fds"]:
        raise TypeError("launch_process requires close_fds=True")
    target_executable = os.fspath(kwargs.pop("executable", program[0]))
    if not target_executable:
        raise ValueError("executable must not be empty")
    caller_pass_fds = tuple(kwargs.pop("pass_fds", ()))
    restore_signals = kwargs.get("restore_signals", True)
    signal_modes = _target_signal_modes(restore_signals)
    requested_env = kwargs.pop("env", None)
    target_env = _validated_environment(requested_env)
    target_env_payload = pickle.dumps(
        target_env, protocol=pickle.HIGHEST_PROTOCOL
    )
    control_read, control_write = os.pipe()
    try:
        ready_parent, ready_child = socket.socketpair()
    except BaseException:
        os.close(control_read)
        os.close(control_write)
        raise
    process: asyncio.subprocess.Process | None = None
    witness_pid: int | None = None
    witness_start_time: int | None = None
    witness_pidfd: int | None = None
    leader_start_time: int | None = None
    witness_proven = False
    launch_cancellation: asyncio.CancelledError | None = None
    cancellation_deadline: float | None = None
    try:
        ready_parent.setblocking(False)
        spawn_task = asyncio.create_task(
            asyncio.create_subprocess_exec(
                sys.executable,
                "-I",
                "-S",
                "-c",
                _WITNESS_LAUNCHER,
                str(control_read),
                str(ready_child.fileno()),
                signal_modes,
                target_executable,
                *program,
                start_new_session=True,
                pass_fds=tuple(
                    sorted(
                        {
                            *caller_pass_fds,
                            control_read,
                            ready_child.fileno(),
                        }
                    )
                ),
                **kwargs,
            )
        )
        (
            process,
            launch_cancellation,
            cancellation_deadline,
        ) = await _await_launch_phase(
            spawn_task,
            timeout_detail="subprocess creation exceeded its startup deadline",
            cancellation=launch_cancellation,
            cancellation_deadline=cancellation_deadline,
        )
        os.close(control_read)
        control_read = -1
        ready_child.close()
        loop = asyncio.get_running_loop()
        ready_task = asyncio.create_task(_read_witness_ready(ready_parent))
        (
            ready_payload,
            launch_cancellation,
            cancellation_deadline,
        ) = await _await_launch_phase(
            ready_task,
            timeout_detail="lifecycle witness readiness exceeded its startup deadline",
            cancellation=launch_cancellation,
            cancellation_deadline=cancellation_deadline,
        )
        ready_line, separator, exec_payload = ready_payload.partition(b"\n")
        if not separator or not ready_line.startswith(b"W"):
            raise RuntimeError("lifecycle ownership witness did not start")
        witness_pid = int(ready_line[1:])
        if launch_cancellation is not None:
            raise launch_cancellation
        with open(f"/proc/{witness_pid}/stat", encoding="utf-8") as stat_file:
            state, witness_pgid, witness_sid, witness_start_time = _parse_proc_stat(
                stat_file.read()
            )
        if (
            state == "Z"
            or witness_pgid != process.pid
            or witness_sid != process.pid
        ):
            raise RuntimeError("could not establish lifecycle ownership witness")
        witness_proven = True
        opener = _get_pidfd_opener()
        if opener is None:
            raise RuntimeError("pidfd_open is unavailable on this Linux host")
        witness_pidfd = opener(witness_pid)
        with open(f"/proc/{witness_pid}/stat", encoding="utf-8") as stat_file:
            current_state, current_pgid, current_sid, current_start_time = (
                _parse_proc_stat(stat_file.read())
            )
        if (
            current_state == "Z"
            or current_pgid != witness_pgid
            or current_sid != witness_sid
            or current_start_time != witness_start_time
        ):
            raise RuntimeError("lifecycle ownership witness changed during launch")
        await loop.sock_sendall(
            ready_parent,
            b"A"
            + len(target_env_payload).to_bytes(8, "big")
            + target_env_payload,
        )
        exec_task = asyncio.create_task(_read_until_eof(ready_parent))
        trailing_payload, launch_cancellation, cancellation_deadline = (
            await _await_launch_phase(
                exec_task,
                timeout_detail="target exec exceeded its startup deadline",
                cancellation=launch_cancellation,
                cancellation_deadline=cancellation_deadline,
            )
        )
        exec_payload += trailing_payload
        if launch_cancellation is not None:
            raise launch_cancellation
        exec_lines = exec_payload.splitlines()
        exec_errno = (
            int(exec_lines[0][1:])
            if exec_lines and exec_lines[0].startswith(b"E")
            else None
        )
        if exec_errno is not None:
            raise OSError(
                exec_errno,
                os.strerror(exec_errno),
                target_executable,
            )

        try:
            with open(f"/proc/{process.pid}/stat", encoding="utf-8") as stat_file:
                leader_start_time = _parse_proc_stat(stat_file.read())[3]
        except (OSError, IndexError, ValueError) as exc:
            disappeared = isinstance(exc, FileNotFoundError) or (
                isinstance(exc, OSError)
                and exc.errno in _DISAPPEARED_ERRNOS
            )
            if not disappeared:
                raise RuntimeError(
                    f"could not establish process-group leader identity: {exc}"
                ) from exc
            leader_start_time = None
        identity = ProcessIdentity(
            process.pid, process.pid, process.pid, leader_start_time
        )
        try:
            pgid = os.getpgid(process.pid)
            sid = os.getsid(process.pid)
        except ProcessLookupError:
            # The proven witness retains the dedicated session if the target
            # exits before this observational check.
            pass
        else:
            if pgid != process.pid or sid != process.pid:
                raise RuntimeError("subprocess did not enter its dedicated session")
        return SupervisedProcess(
            process,
            identity,
            _proof=_LAUNCH_PROOF,
            _lifecycle_witness=_LifecycleWitness(
                witness_pid,
                witness_start_time,
                witness_pidfd,
                control_write,
            ),
        )
    except BaseException:
        owned_process: SupervisedProcess | None = None
        if (
            process is not None
            and witness_proven
            and witness_pid is not None
            and witness_start_time is not None
            and witness_pidfd is not None
        ):
            owned_process = SupervisedProcess(
                process,
                ProcessIdentity(
                    process.pid,
                    process.pid,
                    process.pid,
                    leader_start_time,
                ),
                _proof=_LAUNCH_PROOF,
                _lifecycle_witness=_LifecycleWitness(
                    witness_pid,
                    witness_start_time,
                    witness_pidfd,
                    control_write,
                ),
            )
        cleanup_task = asyncio.create_task(
            _finish_failed_launch(
                process,
                owned_process=owned_process,
                control_fd=control_write,
                witness_pid=witness_pid,
                witness_pidfd=witness_pidfd,
            )
        )
        control_write = -1
        witness_pidfd = None
        await _wait_without_cancelling(cleanup_task)
        raise
    finally:
        if control_read >= 0:
            os.close(control_read)
        ready_parent.close()
        ready_child.close()
