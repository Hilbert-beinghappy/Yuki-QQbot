"""Restart-safe polling worker consuming original accepted runs and due slots."""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import timedelta
from typing import Any

from qq_ai_bot.automation.executor import AutomationExecutor
from qq_ai_bot.automation.models import ExecutionResult, RunStatus
from qq_ai_bot.automation.repository import AutomationRepository
from qq_ai_bot.config import Settings
from qq_ai_bot.runtime.lease_heartbeat import supervise_lease
from qq_ai_bot.time.service import TimeContextService

logger = logging.getLogger(__name__)


class AutomationWorker:
    """Lease due tasks, execute each slot once, and advance periodic schedules."""

    def __init__(
        self,
        *,
        settings: Settings,
        repository: AutomationRepository,
        executor: AutomationExecutor,
        time_service: TimeContextService,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._executor = executor
        self._time = time_service
        self._worker_id = uuid.uuid4().hex
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._running: set[asyncio.Task[None]] = set()

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self.running:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._loop(), name="automation-worker")

    async def close(self) -> None:
        self._stop.set()
        if self._task is not None:
            task, self._task = self._task, None
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if self._running:
            _done, pending = await asyncio.wait(
                self._running,
                timeout=5,
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                rows = await self._repository.claim_due(
                    worker_id=uuid.uuid4().hex,
                    now=self._time.clock.now(),
                    lease_seconds=self._settings.automation_lease_seconds,
                    enabled=self._settings.automation_enabled,
                    clock=self._time.clock.now,
                )
                for row in rows:
                    task = asyncio.create_task(
                        self._process_guarded(row), name=f"automation-{row.id}"
                    )
                    self._running.add(task)
                    task.add_done_callback(self._running.discard)
            except Exception as exc:
                logger.error("automation_poll_failed category=%s", type(exc).__name__, exc_info=exc)
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self._settings.automation_poll_seconds
                )
            except TimeoutError:
                continue

    async def _process_guarded(self, automation: Any) -> None:
        """Contain unexpected task failures and release the lease for recovery."""

        worker_id = automation.claimed_by or self._worker_id

        async def renew() -> bool:
            return await self._repository.renew_claim(
                automation.id,
                worker_id,
                self._time.clock.now() + timedelta(seconds=self._settings.automation_lease_seconds),
            )

        try:
            async with supervise_lease(
                renew,
                lambda: self._repository.claim_expiry(automation.id, worker_id),
                seconds=self._settings.automation_lease_seconds,
                interval=max(1, self._settings.automation_lease_seconds / 3),
                clock=lambda: self._time.clock.now().timestamp(),
            ):
                await self._process(automation)
        except asyncio.CancelledError:
            await self._repository.release_claim(
                automation.id,
                worker_id=automation.claimed_by or self._worker_id,
            )
            raise
        except Exception as exc:
            logger.error(
                "automation_process_failed automation_id=%s category=%s",
                getattr(automation, "id", "unknown"),
                type(exc).__name__,
                exc_info=exc,
            )
            try:
                await self._repository.release_claim(
                    automation.id,
                    worker_id=automation.claimed_by or self._worker_id,
                )
            except Exception as release_exc:
                logger.error(
                    "automation_claim_release_failed automation_id=%s category=%s",
                    getattr(automation, "id", "unknown"),
                    type(release_exc).__name__,
                )

    async def _process(self, automation: Any) -> None:
        worker_id = automation.claimed_by or self._worker_id
        runs = list(await self._repository.running_runs(automation.id))
        now = self._time.clock.now()
        scheduled_for = automation.next_run_at
        if (
            self._settings.automation_enabled
            and automation.status.value == "active"
            and scheduled_for is not None
            and scheduled_for <= now
            and not any(run.scheduled_for == scheduled_for for run in runs)
        ):
            run = await self._repository.create_run(
                automation.id,
                scheduled_for=scheduled_for,
                actual_started_at=now,
                worker_id=worker_id,
                version_id=automation.version_id,
            )
            if run is not None:
                runs.append(run)
        pending = False
        for run in runs:
            accepted = await self._repository.get_for_run(run.id)
            result = (
                await self._executor.execute(accepted, run)
                if accepted is not None
                else ExecutionResult(
                    status=RunStatus.UNCERTAIN,
                    steps_completed=run.steps_completed,
                    llm_calls=run.llm_calls,
                    tool_calls=run.tool_calls,
                    messages_sent=run.messages_sent,
                    error_category="missing_original_run_version",
                    summary=run.result_summary,
                )
            )
            if result.status is RunStatus.RUNNING:
                pending = True
                continue
            recorded = await self._repository.finish_run(
                run.id,
                worker_id=worker_id,
                status=result.status,
                steps_completed=result.steps_completed,
                llm_calls=result.llm_calls,
                tool_calls=result.tool_calls,
                messages_sent=result.messages_sent,
                error_category=result.error_category,
                summary=result.summary,
                finished_at=self._time.clock.now(),
            )
            if recorded:
                logger.info(
                    "automation_run_finished automation_id=%d run_id=%d status=%s "
                    "error_category=%s messages_sent=%d",
                    automation.id,
                    run.id,
                    result.status.value,
                    result.error_category,
                    result.messages_sent,
                )
        await self._repository.release_claim(
            automation.id,
            worker_id=worker_id,
            not_before=self._time.clock.now() + timedelta(seconds=5) if pending else None,
        )
