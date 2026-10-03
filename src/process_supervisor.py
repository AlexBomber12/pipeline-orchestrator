"""Bounded lifecycle management for one owned Linux process group.

The supervisor contains ordinary descendants that remain in the dedicated
session/process group created at launch.  A descendant that creates another
session, moves to another process group, or crosses a container boundary needs
separate containment (for example, a cgroup or container supervisor).
TERM and KILL are delivered only through revalidated per-member Linux pidfds;
the recyclable numeric process-group ID is never a signal-delivery target.
After the leader exits, ownership continuity is proven by a member birth
identity observed earlier or by an inherited, pinned stdout/stderr pipe from
the launch.  If neither proof remains available, cleanup fails without
signaling the numerically matching group.
"""

from __future__ import annotations

import asyncio
import math
import os
import signal
from dataclasses import dataclass
from enum import Enum
from typing import Any

_POLL_INTERVAL_SECONDS = 0.01
_LAUNCH_PROOF = object()


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


class _GroupState(Enum):
    LIVE = "live"
    QUIESCENT = "quiescent"
    UNPROVEN = "unproven"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class _GroupObservation:
    state: _GroupState
    detail: str
    members: tuple[tuple[int, int, str], ...] = ()
    definitive: bool = False


@dataclass(frozen=True)
class _GroupMember:
    pid: int
    state: str
    start_time: int

    @property
    def identity(self) -> tuple[int, int]:
        return self.pid, self.start_time


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
        _lifecycle_proof_fds: tuple[int, ...] = (),
    ) -> None:
        if _proof is not _LAUNCH_PROOF:
            raise TypeError("use launch_process() to create a supervised process")
        self._process = process
        self._identity = identity
        self._cleanup_task: asyncio.Task[CleanupResult] | None = None
        self._lifecycle_proof_fds = list(_lifecycle_proof_fds)
        self._lifecycle_proof_files = frozenset(
            (file_stat.st_dev, file_stat.st_ino)
            for file_stat in map(os.fstat, _lifecycle_proof_fds)
        )
        self._known_members: set[tuple[int, int]] = set()
        if identity.leader_start_time is not None:
            self._known_members.add(
                (identity.leader_pid, identity.leader_start_time)
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
        the shielded, bounded cleanup task finishes.
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

            observation = await self._wait_for_quiescence(term_grace)
            if observation.state is _GroupState.QUIESCENT:
                return await self._success(term_sent, kill_sent)
            if observation.state in {_GroupState.UNPROVEN, _GroupState.UNKNOWN}:
                return self._failure(term_sent, kill_sent, observation.detail)

            kill_sent, error = self._signal_group(signal.SIGKILL)
            if error is not None:
                return self._failure(term_sent, kill_sent, error)
            observation = await self._wait_for_quiescence(kill_grace)
            if observation.state is _GroupState.QUIESCENT:
                return await self._success(term_sent, kill_sent)
            return self._failure(term_sent, kill_sent, observation.detail)
        finally:
            self._close_lifecycle_proofs()

    async def _success(self, term_sent: bool, kill_sent: bool) -> CleanupResult:
        # A non-None returncode means asyncio's child watcher reaped the leader.
        await self.process.wait()
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

    async def _wait_for_quiescence(self, timeout: float) -> _GroupObservation:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        quiet_members: tuple[tuple[int, int, str], ...] | None = None
        while True:
            observation = self._observe_group()
            if (
                observation.state is _GroupState.QUIESCENT
                and self.process.returncode is not None
            ):
                if observation.definitive or quiet_members == observation.members:
                    return observation
                if quiet_members is not None and loop.time() >= deadline:
                    return _GroupObservation(
                        _GroupState.LIVE,
                        "process-group quiescence could not be confirmed "
                        "before the grace period",
                    )
                quiet_members = observation.members
                # A second, fresh /proc enumeration is required because a
                # parent can fork and exit while the first scan is in flight.
                await asyncio.sleep(0)
                continue
            quiet_members = None
            if observation.state in {_GroupState.UNPROVEN, _GroupState.UNKNOWN}:
                return observation
            remaining = deadline - loop.time()
            if remaining <= 0:
                if observation.state is _GroupState.QUIESCENT:
                    detail = "process group is quiet but leader was not reaped"
                else:
                    detail = "process group remained live after the grace period"
                return _GroupObservation(_GroupState.LIVE, detail)
            await asyncio.sleep(min(_POLL_INTERVAL_SECONDS, remaining))

    def _signal_group(self, sig: signal.Signals) -> tuple[bool, str | None]:
        observation, pidfds = self._snapshot_group(acquire_pidfds=True)
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
            for pidfd in pidfds:
                try:
                    sender(pidfd, sig)
                except ProcessLookupError:
                    continue
                except OSError as exc:
                    return sent, f"could not signal owned process-group member: {exc}"
                sent = True
        finally:
            self._close_pidfds(pidfds)
        return sent, None

    def _observe_group(self) -> _GroupObservation:
        observation, pidfds = self._snapshot_group(acquire_pidfds=False)
        self._close_pidfds(pidfds)
        return observation

    def _snapshot_group(
        self, *, acquire_pidfds: bool
    ) -> tuple[_GroupObservation, list[int]]:
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
        pidfds: list[int] = []
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

        live_members = [member for member in members if member.state != "Z"]
        if acquire_pidfds:
            for member in live_members:
                pidfd, error = self._open_owned_pidfd(
                    member.pid, member.start_time
                )
                if error is not None:
                    self._close_pidfds(pidfds)
                    return error, []
                if pidfd is not None:
                    pidfds.append(pidfd)

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
            if acquire_pidfds and not pidfds:
                return (
                    _GroupObservation(
                        _GroupState.UNKNOWN,
                        "owned members disappeared before stable handles were acquired",
                    ),
                    [],
                )
            return (
                _GroupObservation(
                    _GroupState.LIVE,
                    "owned process group is live",
                    fingerprint,
                ),
                pidfds,
            )
        if members:
            return (
                _GroupObservation(
                    _GroupState.QUIESCENT,
                    "owned process group contains only zombies",
                    fingerprint,
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
        if self.identity.leader_start_time is None:
            return False, _GroupObservation(
                _GroupState.UNPROVEN,
                "process-group leader identity is unproven",
            )
        try:
            with open(
                f"/proc/{self.identity.leader_pid}/stat", encoding="utf-8"
            ) as stat_file:
                _, pgid, sid, start_time = _parse_proc_stat(stat_file.read())
        except FileNotFoundError:
            return False, None
        except (OSError, IndexError, ValueError) as exc:
            return False, _GroupObservation(
                _GroupState.UNKNOWN,
                f"could not confirm leader identity: {exc}",
            )
        if start_time != self.identity.leader_start_time:
            return False, _GroupObservation(
                _GroupState.UNPROVEN,
                "process-group leader identity is unproven",
            )
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
        for member in members:
            if member.identity not in self._known_members:
                continue
            stable, error = self._revalidate_member(member)
            if error is not None:
                return error
            if stable:
                return None

        proof_error: _GroupObservation | None = None
        for member in members:
            if member.state == "Z":
                continue
            holds_proof, error = self._member_holds_lifecycle_proof(member.pid)
            if error is not None:
                proof_error = error
                continue
            if not holds_proof:
                continue
            stable, error = self._revalidate_member(member)
            if error is not None:
                return error
            if stable:
                return None
        if proof_error is not None:
            return proof_error
        return _GroupObservation(
            _GroupState.UNPROVEN,
            "process-group continuity is unproven; refusing to signal",
        )

    def _member_holds_lifecycle_proof(
        self, pid: int
    ) -> tuple[bool, _GroupObservation | None]:
        if not self._lifecycle_proof_files:
            return False, None
        try:
            with os.scandir(f"/proc/{pid}/fd") as entries:
                for entry in entries:
                    try:
                        file_stat = entry.stat()
                    except FileNotFoundError:
                        continue
                    if (
                        file_stat.st_dev,
                        file_stat.st_ino,
                    ) in self._lifecycle_proof_files:
                        return True, None
        except FileNotFoundError:
            return False, None
        except OSError as exc:
            return False, _GroupObservation(
                _GroupState.UNKNOWN,
                f"could not inspect lifecycle proof: {exc}",
            )
        return False, None

    def _revalidate_member(
        self, member: _GroupMember
    ) -> tuple[bool, _GroupObservation | None]:
        try:
            with open(f"/proc/{member.pid}/stat", encoding="utf-8") as stat_file:
                _, pgid, sid, start_time = _parse_proc_stat(stat_file.read())
        except FileNotFoundError:
            return False, None
        except (OSError, IndexError, ValueError) as exc:
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
        self, pid: int, expected_start_time: int
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
                state == "Z"
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
    def _close_pidfds(pidfds: list[int]) -> None:
        for pidfd in pidfds:
            os.close(pidfd)

    def _close_lifecycle_proofs(self) -> None:
        for proof_fd in self._lifecycle_proof_fds:
            os.close(proof_fd)
        self._lifecycle_proof_fds.clear()


def _parse_proc_stat(stat: str) -> tuple[str, int, int, int]:
    """Return state, group, session, and start time from a Linux proc stat."""
    fields = stat[stat.rindex(")") + 2 :].split()
    return fields[0], int(fields[2]), int(fields[3]), int(fields[19])


async def launch_process(*program: str, **kwargs: Any) -> SupervisedProcess:
    """Launch ``program`` in a new Linux session and retain its group identity."""
    conflicts = {"start_new_session", "process_group", "preexec_fn"} & kwargs.keys()
    if conflicts:
        names = ", ".join(sorted(conflicts))
        raise TypeError(f"launch_process owns subprocess option(s): {names}")
    process = await asyncio.create_subprocess_exec(
        *program, start_new_session=True, **kwargs
    )
    proof_fds = tuple(
        os.dup(
            process._transport.get_pipe_transport(child_fd)  # type: ignore[attr-defined]
            .get_extra_info("pipe")
            .fileno()
        )
        for child_fd, option in ((1, "stdout"), (2, "stderr"))
        if kwargs.get(option) == asyncio.subprocess.PIPE
    )
    try:
        with open(f"/proc/{process.pid}/stat", encoding="utf-8") as stat_file:
            leader_start_time = _parse_proc_stat(stat_file.read())[3]
    except (OSError, IndexError, ValueError):
        leader_start_time = None
    identity = ProcessIdentity(
        process.pid, process.pid, process.pid, leader_start_time
    )
    try:
        pgid = os.getpgid(process.pid)
        sid = os.getsid(process.pid)
    except ProcessLookupError:
        # start_new_session ran in the child before exec; an early exit can race
        # this observational check without invalidating that launch proof.
        pass
    else:
        if pgid != process.pid or sid != process.pid:
            for proof_fd in proof_fds:
                os.close(proof_fd)
            process.kill()
            await process.wait()
            raise RuntimeError("subprocess did not enter its dedicated session")
    return SupervisedProcess(
        process,
        identity,
        _proof=_LAUNCH_PROOF,
        _lifecycle_proof_fds=proof_fds,
    )
