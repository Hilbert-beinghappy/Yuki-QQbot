"""Original automation versions, admitted slots, cancellation and durable settlement."""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import event, func, insert, select, update
from tests.support.automation_runtime_helpers import FakeClock
from tests.support.automation_unified_delivery_helpers import sent, setup_run
from tests.support.runtime_execution import make_work_resumer
from tests.unit.test_work_owner_recovery import public_action

from qq_ai_bot.automation.compiler import TaskSpec
from qq_ai_bot.automation.models import AutomationStatus, RunStatus
from qq_ai_bot.automation.service import AutomationService
from qq_ai_bot.automation.worker import AutomationWorker
from qq_ai_bot.control_plane.paging import PageRequest
from qq_ai_bot.domain.conversations import ScopeType
from qq_ai_bot.domain.messages import ChatResponse, ToolCall, ToolFunction
from qq_ai_bot.domain.tool_actor import ToolActor
from qq_ai_bot.identity.canonical_repository import ensure_space
from qq_ai_bot.identity.db_models import CanonicalPersonModel, IdentityBindingModel
from qq_ai_bot.persistence.control_query import ControlQueryAdapter
from qq_ai_bot.persistence.models import (
    AutomationModel,
    AutomationRunModel,
    AutomationStepRunModel,
    AutomationVersionModel,
)
from qq_ai_bot.runtime.origin import TurnOrigin
from qq_ai_bot.runtime.subagent_repository import SubagentRepository
from qq_ai_bot.runtime.work_budget_schema import automation_budgets
from qq_ai_bot.runtime.work_repository import WorkRepository
from qq_ai_bot.runtime.work_scheduler import WorkScheduler
from qq_ai_bot.runtime.work_schema_v1 import inputs, work
from qq_ai_bot.runtime.work_wait import WorkWaitRepository
from qq_ai_bot.runtime.work_wait_schema import waits


def service(case):
    return AutomationService(
        settings=case.executor._settings,
        repository=case.repository,
        registry=case.executor._registry,
        time_service=case.chat._time,
    )


def actor(case):
    return ToolActor(
        user_id="10001",
        bot_user_id="80001",
        group_id="20001",
        origin=TurnOrigin.USER_MESSAGE,
        instruction="管理原计划",
        event_id=1,
        person_id=case.env.person,
        conversation_id=case.env.context.conversation_id,
        presence_id=case.env.presence,
    )


async def cancel_plan(case):
    from tests.support.control_plane_foundation_helpers import context

    from qq_ai_bot.control_plane import ControlCommand, ControlCommandService
    from qq_ai_bot.persistence.control_command import ControlCommandAdapter
    from qq_ai_bot.persistence.unit_of_work import state_revision

    row = await case.repository.get(case.row.id)
    decision = context("control.automation.mutate")
    return await ControlCommandService(
        ControlCommandAdapter(case.repository._database, automation=service(case))
    ).mutate_automation(
        decision,
        ControlCommand(
            request_id=decision.request_id,
            expected_revision=state_revision(row.updated_at),
            payload={"resource_id": str(row.id), "action": "cancel"},
        ),
    )


@pytest.mark.asyncio
async def test_waiting_original_slot_and_new_slot_keep_versions_dates_and_cumulative_facts(
    database, tmp_path
):
    case = await setup_run(
        database,
        tmp_path,
        delivery="none",
        mode="silent",
        trigger={"type": "interval", "seconds": 60},
        max_runs=3,
        superusers_csv="10001",
    )
    case.clock = FakeClock(case.row.next_run_at)
    case.chat._time._clock = case.clock
    case.provider._responder = lambda request: (
        ChatResponse(
            content="",
            latency_seconds=0,
            tool_calls=(
                ToolCall(
                    id="original-wait",
                    function=ToolFunction(
                        name="task_control",
                        arguments=json.dumps(
                            {
                                "action": "wait",
                                "conditions": [{"kind": "time_due", "after_seconds": 45}],
                                "wait_mode": "all",
                            }
                        ),
                    ),
                ),
            ),
        )
        if len(case.provider.requests) == 1
        else "Original internal completion"
    )
    result = await case.executor.execute(case.row, case.run)
    assert result.status is RunStatus.RUNNING
    repository = WorkRepository(database)
    async with database.sessions() as reader:
        original = dict((await reader.execute(select(work))).mappings().one())
    original_source = original["source_json"]
    replacement = case.row.script.model_dump(mode="json")
    replacement["name"] = "new definition"
    replacement["steps"][0]["arguments"]["instruction"] = "A distinct current slot"
    manager = service(case)
    case.executor._settings.superusers_csv = ""
    del case.executor._settings.__dict__["superusers"]
    changed = await manager.update(
        case.row.id, replacement, actor=actor(case), conversation_key=original["conversation_id"]
    )
    assert changed.next_run_at == case.row.next_run_at
    assert changed.authority_snapshot["permission_level"] == "user"
    case.clock.advance(1)
    assert await manager.run_now(
        case.row.id, actor=actor(case), conversation_key=original["conversation_id"]
    )
    worker = AutomationWorker(
        settings=case.executor._settings,
        repository=case.repository,
        executor=case.executor,
        time_service=case.chat._time,
    )
    claimed = await case.repository.claim_due(
        worker_id=worker._worker_id, now=case.clock.now(), lease_seconds=60
    )
    await worker._process(claimed[0])
    history = await case.repository.run_history(case.row.id)
    assert len(history) == 2
    newer = next(run for run in history if run.id != case.run.id)
    assert newer.status is RunStatus.SUCCEEDED
    assert (await case.repository.running_runs(case.row.id))[0].id == case.run.id
    after_new = await case.repository.get(case.row.id)
    assert after_new.run_count == 1 and after_new.next_run_at > case.clock.now()
    assert after_new.max_runs == 3 and after_new.status is AutomationStatus.ACTIVE
    accepted = await case.repository.get_for_run(case.run.id)
    assert accepted.script == case.row.script and accepted.version_id != newer.version_id
    assert accepted.authority_snapshot["permission_level"] == "superuser"
    case.executor._settings.superusers_csv = "10001"
    del case.executor._settings.__dict__["superusers"]
    restored_definition = await manager.update(
        case.row.id,
        case.row.script.model_dump(mode="json"),
        actor=actor(case),
        conversation_key=original["conversation_id"],
    )
    assert restored_definition.script_hash == case.row.script_hash
    assert restored_definition.version_id not in {accepted.version_id, newer.version_id}
    assert restored_definition.authority_snapshot["permission_level"] == "user"
    assert (
        await case.repository.get_for_run(case.run.id)
    ).authority_snapshot == case.row.authority_snapshot
    assert restored_definition.next_run_at == after_new.next_run_at
    assert (await repository.get(original["id"]))["source_json"] == original_source
    assert await manager.pause(
        case.row.id, actor=actor(case), conversation_key=original["conversation_id"]
    )
    case.executor._settings.automation_enabled = False
    timer = WorkWaitRepository(repository)
    binding = await timer.describe(original["id"])
    assert await timer.deliver_due(now=binding["conditions"][0]["due"]) == 1
    handlers = case.executor._registry.require("yuki.agent").handler.__self__

    async def resume(item, source):
        await handlers.resume_work(database, item, source)

    resumer = make_work_resumer(
        repository,
        ledger=case.chat._ledger,
        scopes=case.chat._conversation_scopes,
        turns=case.chat._turn_coordinator,
        router=case.env.router,
        config=case.chat._runtime_config,
        generate_self=case.chat.generate_self_initiative,
        generate_wakeup=case.chat.generate_main_agent_wakeup,
        bindings=case.chat.runtime.bindings,
    )
    resumer.services = replace(
        resumer.services, settings=case.executor._settings, resume_automation=resume
    )
    await WorkScheduler(repository, resumer.resume, chat_admission_enabled=True).drain_once()
    assert (await repository.get(original["id"]))["state"] == "completed"
    wake_clock = FakeClock(case.clock.now() + timedelta(seconds=6))
    claimed = await case.repository.claim_due(
        worker_id=worker._worker_id,
        now=wake_clock.now(),
        lease_seconds=60,
        enabled=False,
        clock=wake_clock.now,
    )
    await worker._process(claimed[0])
    final = await case.repository.get(case.row.id)
    assert final.status.value == "paused" and final.next_run_at == after_new.next_run_at
    assert final.run_count == 2 and len(await case.repository.run_history(case.row.id)) == 2
    assert final.max_runs == 3 and final.run_count < final.max_runs
    assert case.executor._settings.automation_enabled is False
    assert not sent(case.env)
    async with database.sessions() as reader:
        assert set(await reader.scalars(select(AutomationRunModel.status))) == {"succeeded"}
        assert list(await reader.scalars(select(inputs.c.state))) == ["consumed"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.UNCERTAIN])
async def test_terminal_occurrence_consumes_explicit_max_runs_once(database, tmp_path, status):
    case = await setup_run(
        database,
        tmp_path,
        strategy="static",
        mode="silent",
        trigger={"type": "interval", "seconds": 60},
        max_runs=1,
    )
    summary = {
        "result": "x" * 20000,
        "token": "redacted",
        "nested": [{"item": index} for index in range(60)],
    }
    payload = dict(
        status=status,
        steps_completed=1,
        llm_calls=2,
        tool_calls=3,
        messages_sent=0,
        error_category=None if status is RunStatus.SUCCEEDED else "original_failure",
        summary=summary,
        finished_at=case.clock.now(),
    )
    assert await case.repository.finish_run(case.run.id, **payload)
    assert not await case.repository.finish_run(case.run.id, **payload)
    current = await case.repository.get(case.row.id)
    assert (
        current.run_count == 1
        and current.next_run_at is None
        and current.status.value == "completed"
    )
    stored = (await case.repository.run_history(case.row.id))[0]
    assert stored.status is status and stored.llm_calls == 2 and stored.tool_calls == 3
    assert stored.result_summary == {**summary, "token": "[redacted]"}
    assert (
        await case.repository.create_run(
            case.row.id,
            scheduled_for=case.clock.now() + timedelta(seconds=60),
            actual_started_at=case.clock.now(),
        )
        is None
    )
    assert not case.provider.requests and not sent(case.env)


@pytest.mark.asyncio
async def test_claimed_definition_update_or_cancel_cannot_admit_an_old_preparation(
    database, tmp_path
):
    case = await setup_run(database, tmp_path, strategy="static")
    async with database.immediate_session() as writer:
        await writer.execute(
            update(AutomationModel)
            .where(AutomationModel.id == case.row.id)
            .values(next_run_at=case.clock.now())
        )
    claimed = (
        await case.repository.claim_due(
            worker_id="old-owner", now=case.clock.now(), lease_seconds=60
        )
    )[0]
    manager = service(case)
    changed = case.row.script.model_dump(mode="json")
    changed["name"] = "a replacement definition"
    await manager.update(
        case.row.id, changed, actor=actor(case), conversation_key=case.env.context.conversation_id
    )
    assert (
        await case.repository.create_run(
            case.row.id,
            scheduled_for=claimed.next_run_at,
            actual_started_at=case.clock.now(),
            worker_id="old-owner",
            version_id=claimed.version_id,
        )
        is None
    )
    assert await manager.cancel(
        case.row.id, actor=actor(case), conversation_key=case.env.context.conversation_id
    )
    assert (
        await case.repository.create_run(
            case.row.id,
            scheduled_for=claimed.next_run_at,
            actual_started_at=case.clock.now(),
            worker_id="old-owner",
            version_id=claimed.version_id,
        )
        is None
    )
    history = await case.repository.run_history(case.row.id)
    assert (
        len(history) == 1
        and history[0].id == case.run.id
        and history[0].status is RunStatus.CANCELLED
    )
    assert not sent(case.env)


@pytest.mark.asyncio
async def test_worker_reaches_every_due_plan_without_treating_read_or_send_as_model_capacity(
    database, tmp_path
):
    case = await setup_run(database, tmp_path, delivery="none", mode="silent")
    manager = service(case)
    case.chat._time._clock = FakeClock(case.clock.now())
    case.provider._responder = lambda request: ChatResponse(
        content="",
        latency_seconds=0,
        tool_calls=(
            ToolCall(
                id="original-wait",
                function=ToolFunction(
                    name="task_control",
                    arguments=json.dumps(
                        {
                            "action": "wait",
                            "conditions": [{"kind": "time_due", "after_seconds": 45}],
                        }
                    ),
                ),
            ),
        ),
    )
    heads = [case.row.id]
    assert (await case.executor.execute(case.row, case.run)).status is RunStatus.RUNNING
    for index in range(50):
        declaration = case.row.script.model_dump(mode="json")
        declaration["name"] = f"original waiting plan {index}"
        row = await manager.administer_create(
            declaration,
            owner_id=case.env.person,
            conversation_id=case.env.context.conversation_id,
            creation_source_key=f"waiting-head:{index}",
        )
        run = await case.repository.create_run(
            row.id, scheduled_for=row.next_run_at, actual_started_at=case.chat._time.clock.now()
        )
        assert (await case.executor.execute(row, run)).status is RunStatus.RUNNING
        heads.append(row.id)
    _, _, provenance = await manager._creator_context(actor(case))
    declaration = manager._compiler.compile(
        TaskSpec.model_validate(
            {
                "name": "independent static tail",
                "goal": "TAIL_ONCE",
                "strategy": "static",
                "trigger": {"type": "after", "seconds": 60},
                "delivery": {"target": "current_group"},
            }
        ),
        provenance,
        default_timezone=case.row.timezone,
    ).script
    tail = await manager.administer_create(
        declaration,
        owner_id=case.env.person,
        conversation_id=case.env.context.conversation_id,
        creation_source_key="independent-static-tail",
    )
    requests = tuple(case.provider.requests)
    async with database.sessions() as reader:
        original_work = list((await reader.execute(select(work))).mappings())
        original_budgets = list((await reader.execute(select(automation_budgets))).mappings())
    assert {row["state"] for row in original_work} == {"waiting_external"}
    case.chat._time.clock.advance(3600)
    case.executor._settings.global_llm_concurrency = 1
    delivered = asyncio.Event()
    capability = case.executor._registry.require("social.send_message")

    async def send(arguments, context):
        assert context.automation_id == tail.id
        result = await capability.handler(arguments, context)
        delivered.set()
        return result

    case.executor._registry._items["social.send_message"] = replace(capability, handler=send)
    worker = AutomationWorker(
        settings=case.executor._settings,
        repository=case.repository,
        executor=case.executor,
        time_service=case.chat._time,
    )
    await worker.start()
    try:
        await asyncio.wait_for(delivered.wait(), timeout=60)
        await asyncio.gather(*tuple(worker._running))
    finally:
        await worker.close()
    assert len(sent(case.env)) == 1
    assert sent(case.env)[0]["message"] == [{"type": "text", "data": {"text": "TAIL_ONCE"}}]
    assert (await case.repository.get(tail.id)).run_count == 1
    async with database.sessions() as reader:
        assert set(
            await reader.scalars(
                select(AutomationRunModel.status).where(AutomationRunModel.automation_id.in_(heads))
            )
        ) == {"running"}
        assert set(
            await reader.scalars(
                select(AutomationModel.run_count).where(AutomationModel.id.in_(heads))
            )
        ) == {0}
        assert list((await reader.execute(select(work))).mappings()) == original_work
        assert (
            list(
                (
                    await reader.execute(
                        select(automation_budgets).where(
                            automation_budgets.c.run_id.in_(
                                [item["run_id"] for item in original_budgets]
                            )
                        )
                    )
                ).mappings()
            )
            == original_budgets
        )
        assert await reader.scalar(select(func.count()).select_from(AutomationStepRunModel)) == 0
    assert tuple(case.provider.requests) == requests


@pytest.mark.asyncio
async def test_create_replay_reads_initial_definition_after_time_and_authority_change(
    database, tmp_path
):
    from qq_ai_bot.domain.conversations import ConversationScope

    case = await setup_run(
        database,
        tmp_path,
        strategy="static",
        trigger={
            "type": "once",
            "local_datetime": (datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
        },
    )
    manager = service(case)
    original = case.row.script.model_dump(mode="json")
    replacement = {**original, "name": "updated after original admission"}
    case.chat._time._clock = FakeClock(case.row.next_run_at + timedelta(days=1))
    await manager.update(
        case.row.id,
        replacement,
        actor=actor(case),
        conversation_key=case.env.context.conversation_id,
    )
    case.executor._settings.automation_enabled = False
    # Pure receipt consumption must not depend on the now-unregistered call.
    definition = case.executor._registry._items.pop("social.send_message")
    replay = await manager.create(
        original, actor=actor(case), conversation_key=case.env.context.conversation_id
    )
    assert replay.id == case.row.id and replay.name == replacement["name"]
    case.executor._registry._items["social.send_message"] = definition
    fresh = await case.chat._ledger._writer.append(
        scope=ConversationScope.group("80001", "20001"),
        platform_message_id="disabled-new-operation",
        sender_user_id="10001",
        direction="inbound",
        content="另建一个新自动化任务",
    )
    with pytest.raises(ValueError, match="自动化功能当前未启用"):
        await manager.create_task(
            {
                "name": "new independent admission",
                "goal": "a new target",
                "strategy": "static",
                "trigger": {"type": "after", "seconds": 60},
                "delivery": {"target": "current_group"},
            },
            actor=replace(actor(case), event_id=fresh.event.id),
            conversation_key=case.env.context.conversation_id,
        )
    async with database.sessions() as reader:
        assert set(await reader.scalars(select(AutomationModel.id))) == {case.row.id}
        assert set(await reader.scalars(select(AutomationRunModel.id))) == {case.run.id}
    assert len(await case.repository.run_history(case.row.id)) == 1
    assert not case.provider.requests and not sent(case.env)


@pytest.mark.asyncio
async def test_cancel_terminal_plan_and_root_with_active_descendants_preserves_independent_work(
    database, tmp_path
):
    case = await setup_run(database, tmp_path, delivery="none", mode="silent")
    case.provider._responder = lambda request: ChatResponse(
        content="",
        latency_seconds=0,
        tool_calls=(
            ToolCall(
                id="original-wait",
                function=ToolFunction(
                    name="task_control",
                    arguments=json.dumps(
                        {
                            "action": "wait",
                            "conditions": [{"kind": "time_due", "after_seconds": 45}],
                        }
                    ),
                ),
            ),
        ),
    )
    assert (await case.executor.execute(case.row, case.run)).status is RunStatus.RUNNING
    async with database.sessions() as reader:
        original = dict((await reader.execute(select(work))).mappings().one())
        ledger = dict((await reader.execute(select(automation_budgets))).mappings().one())
    repository = WorkRepository(database)
    lease = await repository.acquire(original["conversation_id"], original["generation"])
    child = await repository.derive(lease, original["id"], "real-child", "unfinished subgoal")
    workers = SubagentRepository(repository)
    leaf = await workers.start(lease, child["id"], "real-grandchild", {"goal": "owned run"})
    await repository.transition(lease, child["id"], child["revision"], "suspended")
    await repository.accept_control(
        lease,
        original["id"],
        {"action": "complete", "call_key": "original-completion", "result": "preserved result"},
    )
    current = await repository.get(original["id"])
    await repository.transition(lease, original["id"], current["revision"], "completed")
    await repository.release(lease)
    assert await case.repository.finish_run(
        case.run.id,
        status=RunStatus.SUCCEEDED,
        steps_completed=1,
        llm_calls=1,
        tool_calls=0,
        messages_sent=0,
        error_category=None,
        summary={},
        finished_at=case.clock.now(),
    )
    async with database.immediate_session() as writer:
        await writer.execute(
            update(AutomationRunModel)
            .where(AutomationRunModel.id == case.run.id)
            .values(created_at=case.clock.now() - timedelta(days=40))
        )
    child_source = child["source_json"]
    assert (await public_action(database, await repository.get(child["id"]), "resume")).success
    case.provider._responder = lambda request: ChatResponse(
        content="",
        latency_seconds=0,
        tool_calls=(
            ToolCall(
                id="retained-child-needs-input",
                function=ToolFunction(
                    name="task_control",
                    arguments=json.dumps({"action": "need_input", "question": "Continue?"}),
                ),
            ),
        ),
    )
    handlers = case.executor._registry.require("yuki.agent").handler.__self__

    async def resume(item, source):
        await handlers.resume_work(database, item, source)

    resumer = make_work_resumer(
        repository,
        ledger=case.chat._ledger,
        scopes=case.chat._conversation_scopes,
        turns=case.chat._turn_coordinator,
        router=case.env.router,
        config=case.chat._runtime_config,
        generate_self=case.chat.generate_self_initiative,
        generate_wakeup=case.chat.generate_main_agent_wakeup,
        bindings=case.chat.runtime.bindings,
    )
    resumer.services = replace(
        resumer.services, settings=case.executor._settings, resume_automation=resume
    )
    await WorkScheduler(repository, resumer.resume, chat_admission_enabled=True).drain_once()
    restored = await repository.get(child["id"])
    assert restored["state"] == "waiting_user" and restored["source_json"] == child_source
    assert (await repository.get(original["id"]))["state"] == "completed"
    stored_run = (await case.repository.run_history(case.row.id))[0]
    assert stored_run.id == case.run.id and stored_run.status is RunStatus.SUCCEEDED
    assert stored_run.version_id == case.run.version_id
    async with database.sessions() as reader:
        restored_ledger = dict((await reader.execute(select(automation_budgets))).mappings().one())
    assert restored_ledger["models"] == ledger["models"] + 1
    assert restored_ledger["model_limit"] == ledger["model_limit"]
    lease = await repository.acquire(original["conversation_id"], original["generation"])
    independent = await repository.accept(
        lease,
        source_key="independent-sdk",
        source={"owner": "plugin_invocation"},
        goal="independent owned work",
    )
    await repository.release(lease)
    assert await service(case).administer_transition(case.row.id, action="cancel")
    assert (await repository.get(original["id"]))["state"] == "completed"
    assert (await repository.get(child["id"]))["state"] == "cancelled"
    assert (await repository.get(leaf))["state"] == "cancelled"
    assert await repository.get(independent["id"]) == independent
    assert (await case.repository.get(case.row.id)).status.value == "completed"
    assert (await case.repository.get(case.row.id)).run_count == 1
    async with database.sessions() as reader:
        assert (
            dict((await reader.execute(select(automation_budgets))).mappings().one())
            == restored_ledger
        )
        assert set(await reader.scalars(select(waits.c.status))) == {"cancelled"}
    assert len(case.provider.requests) == 2 and not sent(case.env)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "trigger",
    [
        {"type": "after", "seconds": 60},
        {"type": "daily", "hour": 9, "minute": 0, "timezone": "UTC"},
    ],
)
async def test_paused_body_and_ineffective_timezone_update_preserves_original_date(
    database, tmp_path, trigger
):
    case = await setup_run(database, tmp_path, strategy="static", trigger=trigger)
    manager = service(case)
    assert await manager.pause(
        case.row.id, actor=actor(case), conversation_key=case.env.context.conversation_id
    )
    case.chat._time._clock = FakeClock(case.row.next_run_at + timedelta(days=1))
    replacement = case.row.script.model_dump(mode="json")
    replacement.update(name="new body in paused definition", timezone="Asia/Tokyo")
    changed = await manager.update(
        case.row.id,
        replacement,
        actor=actor(case),
        conversation_key=case.env.context.conversation_id,
    )
    assert changed.next_run_at == case.row.next_run_at and changed.status.value == "paused"
    accepted = await case.repository.get_for_run(case.run.id)
    assert accepted.script == case.row.script and accepted.version_id == case.run.version_id
    assert not case.provider.requests and not sent(case.env)


@pytest.mark.asyncio
@pytest.mark.parametrize("previous_count", [0, 1])
async def test_legacy_terminal_slot_does_not_guess_accounting_or_swallow_new_schedule(
    database, tmp_path, previous_count
):
    case = await setup_run(
        database, tmp_path, strategy="static", trigger={"type": "interval", "seconds": 60}
    )
    # The legacy two-writer path could retain this due slot either before or
    # after its aggregate count was committed. It has no per-run count receipt.
    async with database.immediate_session() as writer:
        await writer.execute(
            update(AutomationRunModel)
            .where(AutomationRunModel.id == case.run.id)
            .values(status="succeeded", finished_at=case.clock.now())
        )
        await writer.execute(
            update(AutomationModel)
            .where(AutomationModel.id == case.row.id)
            .values(run_count=previous_count)
        )
    assert (
        await case.repository.create_run(
            case.row.id, scheduled_for=case.run.scheduled_for, actual_started_at=case.clock.now()
        )
        is None
    )
    retired = await case.repository.get(case.row.id)
    assert retired.next_run_at is None and retired.run_count == previous_count
    manager = service(case)
    case.chat._time._clock = FakeClock(case.row.next_run_at + timedelta(seconds=1))
    assert await manager.run_now(
        case.row.id, actor=actor(case), conversation_key=case.env.context.conversation_id
    )
    new_date = (await case.repository.get(case.row.id)).next_run_at
    assert (
        await case.repository.create_run(
            case.row.id, scheduled_for=case.run.scheduled_for, actual_started_at=case.clock.now()
        )
        is None
    )
    assert (await case.repository.get(case.row.id)).next_run_at == new_date
    worker = AutomationWorker(
        settings=case.executor._settings,
        repository=case.repository,
        executor=case.executor,
        time_service=case.chat._time,
    )
    claimed = (
        await case.repository.claim_due(
            worker_id=worker._worker_id,
            now=case.chat._time.clock.now(),
            lease_seconds=60,
            clock=case.chat._time.clock.now,
        )
    )[0]
    await worker._process(claimed)
    final = await case.repository.get(case.row.id)
    assert final.run_count == previous_count + 1 and final.next_run_at > new_date
    assert len(await case.repository.run_history(case.row.id)) == 2 and len(sent(case.env)) == 1
    assert not case.provider.requests


@pytest.mark.asyncio
async def test_claim_lease_starts_after_competing_writer_releases(database, tmp_path):
    case = await setup_run(database, tmp_path, strategy="static")
    clock = FakeClock(case.row.next_run_at)
    waiting = asyncio.Event()

    def attempted_writer(_connection, _cursor, statement, _parameters, _context, _many):
        if statement == "BEGIN IMMEDIATE":
            waiting.set()

    async with database.immediate_session():
        event.listen(database.engine.sync_engine, "before_cursor_execute", attempted_writer)
        claim = asyncio.create_task(
            case.repository.claim_due(
                worker_id="real-writer", now=clock.now(), lease_seconds=60, clock=clock.now
            )
        )
        await asyncio.wait_for(waiting.wait(), timeout=5)
        clock.advance(120)
    try:
        claimed = await claim
    finally:
        event.remove(database.engine.sync_engine, "before_cursor_execute", attempted_writer)
    assert [item.id for item in claimed] == [case.row.id]
    expiry = await case.repository.claim_expiry(case.row.id, "real-writer")
    assert expiry == (clock.now() + timedelta(seconds=60)).timestamp()
    assert (await case.repository.run_history(case.row.id))[0].id == case.run.id
    assert not case.provider.requests and not sent(case.env)


@pytest.mark.asyncio
async def test_equivalent_directory_match_reaches_the_actual_tail(database, tmp_path):
    case = await setup_run(database, tmp_path, strategy="static")
    manager = service(case)
    requested = {
        "name": "tail target " + "full visible name " * 10,
        "goal": "only the last plan matches",
        "strategy": "static",
        "trigger": {"type": "after", "seconds": 60},
        "delivery": {"target": "current_group"},
    }
    _, _, provenance = await manager._creator_context(actor(case))
    declaration = manager._compiler.compile(
        TaskSpec.model_validate(requested), provenance, default_timezone=case.row.timezone
    ).script
    identities = {case.row.id}
    for index in range(101):
        different = declaration.model_dump(mode="json")
        different["steps"][0]["arguments"]["text"] = f"another target {index}"
        row = await manager.administer_create(
            different,
            owner_id=case.env.person,
            conversation_id=case.env.context.conversation_id,
            creation_source_key=f"directory:{index}",
        )
        identities.add(row.id)
    target = await manager.administer_create(
        declaration,
        owner_id=case.env.person,
        conversation_id=case.env.context.conversation_id,
        creation_source_key="directory:tail",
    )
    identities.add(target.id)
    matches = await manager.find_equivalent_directory_entries(requested, actor=actor(case))
    assert [entry.record.id for entry in matches] == [target.id]
    assert (
        await manager.find_equivalent_directory_entries(requested, actor=actor(case), max_runs=1)
        == ()
    )
    assert await manager.administer_transition(row.id, action="cancel")
    assert await manager.administer_transition(target.id, action="cancel")
    control = ControlQueryAdapter(database, settings=case.executor._settings, automation=manager)
    records = []
    request = PageRequest(limit=25)
    while True:
        page = await control.list_automations(request)
        records.extend(page.items)
        if page.next_cursor is None:
            break
        request = PageRequest(limit=25, cursor=page.next_cursor)
    assert {item.automation_id for item in records} == identities
    visible_target = next(item for item in records if item.automation_id == target.id)
    assert visible_target.name == requested["name"] and visible_target.status == "cancelled"
    assert (await control.read_automation(target.id)).fields["name"] == requested["name"]
    terminal = [
        item.record.id
        for offset in range(2)
        for item in await case.repository.list_directory(
            statuses=(AutomationStatus.CANCELLED,), limit=1, offset=offset
        )
    ]
    assert set(terminal) == {target.id, row.id}
    assert not case.provider.requests and not sent(case.env)


@pytest.mark.asyncio
async def test_paused_self_run_keeps_original_claim_and_social_receipt(database, tmp_path):
    case = await setup_run(database, tmp_path, strategy="static", principal="self")
    worker = AutomationWorker(
        settings=case.executor._settings,
        repository=case.repository,
        executor=case.executor,
        time_service=case.chat._time,
    )
    capability = case.executor._registry.require("social.send_message")
    entered, proceed = asyncio.Event(), asyncio.Event()

    async def original_send(arguments, context):
        entered.set()
        await proceed.wait()
        return await capability.handler(arguments, context)

    case.executor._registry._items[capability.name] = replace(capability, handler=original_send)
    claimed = (
        await case.repository.claim_due(
            worker_id=worker._worker_id, now=case.clock.now(), lease_seconds=60
        )
    )[0]
    processing = asyncio.create_task(worker._process_guarded(claimed))
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        assert await service(case).administer_transition(case.row.id, action="pause")
        paused = await case.repository.get(case.row.id)
        assert paused.status.value == "paused" and paused.claimed_by == worker._worker_id
        assert await case.repository.renew_claim(
            case.row.id, worker._worker_id, case.clock.now() + timedelta(seconds=60)
        )
        case.executor._settings.automation_enabled = False
        proceed.set()
        await processing
    finally:
        proceed.set()
        if not processing.done():
            processing.cancel()
            await asyncio.gather(processing, return_exceptions=True)
    settled = await case.repository.get(case.row.id)
    assert settled.status.value == "paused" and settled.run_count == 1
    assert settled.claimed_by is None and settled.next_run_at is None
    run = (await case.repository.run_history(case.row.id))[0]
    assert run.id == case.run.id and run.status is RunStatus.SUCCEEDED
    assert len(sent(case.env)) == 1 and not case.provider.requests
    assert (
        await case.repository.get_for_run(run.id)
    ).authority_snapshot == case.row.authority_snapshot


@pytest.mark.asyncio
async def test_management_from_another_group_uses_current_owner_account_without_promoting_grant(
    database, tmp_path
):
    case = await setup_run(database, tmp_path, strategy="static")
    original_script = case.row.script.model_dump(mode="json")
    original_script["steps"].insert(
        0,
        {
            "id": "own_memory",
            "call": "memory.get_person",
            "arguments": {"user_id": "$creator_user_id", "limit": 1},
        },
    )
    case.row = await service(case).administer_create(
        original_script,
        owner_id=case.env.person,
        conversation_id=case.env.context.conversation_id,
        creation_source_key="original-person-memory",
    )
    case.run = await case.repository.create_run(
        case.row.id, scheduled_for=case.row.next_run_at, actual_started_at=case.clock.now()
    )
    fact = await case.chat._memories.add_explicit_person("10001", "Same permanent Person fact")
    capability = case.executor._registry.require("memory.get_person")
    reads = []

    async def original_memory(arguments, context):
        result = await capability.handler(arguments, context)
        reads.append((arguments["user_id"], context.authority.actor_is_superuser, result.data))
        return result

    case.executor._registry._items[capability.name] = replace(capability, handler=original_memory)
    async with database.immediate_session() as writer:
        await writer.execute(
            update(IdentityBindingModel)
            .where(IdentityBindingModel.person_id == case.env.person)
            .values(status="disabled")
        )
        writer.add(
            IdentityBindingModel(
                id=str(uuid4()),
                person_id=case.env.person,
                platform="qq",
                external_account_id="10008",
                status="active",
                first_seen_at=case.clock.now(),
                last_seen_at=case.clock.now(),
                created_at=case.clock.now(),
                updated_at=case.clock.now(),
            )
        )
        await ensure_space(writer, "20002")
    settings = case.executor._settings
    settings.superusers_csv = "10008"
    del settings.__dict__["superusers"]
    inbound, _ = await case.chat._ledger.append(
        bot_user_id="80001",
        platform_message_id="another-group-management",
        scope_type=ScopeType.GROUP,
        group_id="20002",
        sender_user_id="10008",
        direction="inbound",
        content="update the original owned plan",
    )
    current_actor = replace(
        actor(case),
        user_id="10008",
        group_id="20002",
        conversation_id=inbound.canonical_conversation_id,
        event_id=inbound.id,
    )
    requested = {
        "name": "new metadata",
        "goal": "new body in the original group",
        "strategy": "static",
        "trigger": {"type": "after", "seconds": 60},
        "delivery": {"target": "auto"},
    }
    changed, _ = await service(case).update_task(
        case.row.id,
        requested,
        actor=current_actor,
        conversation_key=inbound.canonical_conversation_id,
    )
    assert (
        changed.creator_user_id == case.row.creator_user_id
        and changed.canonical_creator_person_id == case.env.person
    )
    assert (
        changed.canonical_target_space_id == case.env.space
        and changed.next_run_at == case.row.next_run_at
    )
    assert changed.authority_snapshot["current_group_id"] == "20001"
    assert (
        changed.authority_snapshot["canonical_conversation_id"] == case.env.context.conversation_id
    )
    assert changed.authority_snapshot["permission_level"] == "user"
    assert changed.authority_snapshot["creator_user_id"] == "10008"
    original = await case.repository.get_for_run(case.run.id)
    assert (
        original.creator_user_id == case.row.creator_user_id
        and original.authority_snapshot == case.row.authority_snapshot
    )
    assert original.script == case.row.script and original.version_id == case.run.version_id
    result = await case.executor.execute(case.row, case.run)
    assert result.status is RunStatus.SUCCEEDED
    assert reads == [("10008", False, {"memories": [{"id": fact.id, "content": fact.content}]})]
    assert len(sent(case.env)) == 1 and sent(case.env)[0]["group_id"] == 20001
    assert not case.provider.requests


@pytest.mark.asyncio
async def test_blocked_past_once_plan_resumes_as_an_explicit_new_slot(database, tmp_path):
    case = await setup_run(
        database,
        tmp_path,
        strategy="static",
        trigger={
            "type": "once",
            "local_datetime": (datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
        },
    )
    async with database.immediate_session() as writer:
        await writer.execute(
            update(CanonicalPersonModel)
            .where(CanonicalPersonModel.id == case.env.person)
            .values(enabled=False)
        )
    blocked = await case.executor.execute(case.row, case.run)
    assert blocked.status is RunStatus.BLOCKED
    assert await case.repository.finish_run(
        case.run.id,
        status=blocked.status,
        steps_completed=blocked.steps_completed,
        llm_calls=blocked.llm_calls,
        tool_calls=blocked.tool_calls,
        messages_sent=blocked.messages_sent,
        error_category=blocked.error_category,
        summary=blocked.summary,
        finished_at=case.clock.now(),
    )
    assert (await case.repository.get(case.row.id)).status.value == "blocked"
    case.chat._time._clock = FakeClock(case.row.next_run_at + timedelta(days=1))
    async with database.immediate_session() as writer:
        await writer.execute(
            update(CanonicalPersonModel)
            .where(CanonicalPersonModel.id == case.env.person)
            .values(enabled=True)
        )
    assert await service(case).resume(
        case.row.id, actor=actor(case), conversation_key=case.env.context.conversation_id
    )
    worker = AutomationWorker(
        settings=case.executor._settings,
        repository=case.repository,
        executor=case.executor,
        time_service=case.chat._time,
    )
    claimed = (
        await case.repository.claim_due(
            worker_id=worker._worker_id,
            now=case.chat._time.clock.now(),
            lease_seconds=60,
            clock=case.chat._time.clock.now,
        )
    )[0]
    await worker._process(claimed)
    history = await case.repository.run_history(case.row.id)
    assert len(history) == 2 and {run.status for run in history} == {
        RunStatus.BLOCKED,
        RunStatus.SUCCEEDED,
    }
    assert next(run for run in history if run.id == case.run.id).status is RunStatus.BLOCKED
    current = await case.repository.get(case.row.id)
    assert (
        current.script == case.row.script
        and current.run_count == 2
        and current.status.value == "completed"
    )
    assert len(sent(case.env)) == 1 and not case.provider.requests


@pytest.mark.asyncio
async def test_late_periodic_run_executes_then_advances_from_actual_completion(database, tmp_path):
    case = await setup_run(
        database, tmp_path, strategy="static", trigger={"type": "interval", "seconds": 60}
    )
    case.chat._time._clock = FakeClock(case.row.next_run_at + timedelta(hours=2))
    capability = case.executor._registry.require("social.send_message")

    async def finish_later(arguments, context):
        case.chat._time.clock.advance(120)
        return await capability.handler(arguments, context)

    case.executor._registry._items[capability.name] = replace(capability, handler=finish_later)
    worker = AutomationWorker(
        settings=case.executor._settings,
        repository=case.repository,
        executor=case.executor,
        time_service=case.chat._time,
    )
    claimed = (
        await case.repository.claim_due(
            worker_id=worker._worker_id,
            now=case.chat._time.clock.now(),
            lease_seconds=60,
            clock=case.chat._time.clock.now,
        )
    )[0]
    await worker._process(claimed)
    current = await case.repository.get(case.row.id)
    history = await case.repository.run_history(case.row.id)
    assert (
        len(history) == 1
        and history[0].id == case.run.id
        and history[0].status is RunStatus.SUCCEEDED
    )
    assert current.run_count == 1 and current.next_run_at > case.chat._time.clock.now()
    assert history[0].finished_at == case.chat._time.clock.now()
    assert len(sent(case.env)) == 1 and not case.provider.requests


@pytest.mark.asyncio
async def test_four_unknown_occurrences_do_not_seal_the_next_independent_slot(database, tmp_path):
    case = await setup_run(
        database, tmp_path, strategy="static", trigger={"type": "interval", "seconds": 60}
    )
    case.chat._time._clock = FakeClock(case.row.next_run_at)
    original_api = case.env.bot.call_api
    attempts = 0

    async def original_transport(action, **arguments):
        nonlocal attempts
        if action == "send_group_msg":
            attempts += 1
            if attempts <= 4:
                raise TimeoutError("original send receipt unavailable")
        return await original_api(action, **arguments)

    case.env.bot.call_api = original_transport
    worker = AutomationWorker(
        settings=case.executor._settings,
        repository=case.repository,
        executor=case.executor,
        time_service=case.chat._time,
    )
    original_receipts = []
    for index in range(5):
        current = await case.repository.get(case.row.id)
        case.chat._time._clock = FakeClock(current.next_run_at)
        claimed = (
            await case.repository.claim_due(
                worker_id=worker._worker_id,
                now=case.chat._time.clock.now(),
                lease_seconds=60,
                clock=case.chat._time.clock.now,
            )
        )[0]
        await worker._process(claimed)
        settled = await case.repository.get(case.row.id)
        assert settled.status.value == "active" and settled.run_count == index + 1
        assert settled.next_run_at > case.chat._time.clock.now()
        run = (await case.repository.run_history(case.row.id))[0]
        receipt = await case.env.service.receipts.find(f"automation:{run.id}", "deliver")
        if index < 4:
            assert run.status is RunStatus.UNCERTAIN and receipt.status == "uncertain"
            original_receipts.append((run.id, receipt))
        else:
            assert run.status is RunStatus.SUCCEEDED and receipt.status == "succeeded"
    for run_id, receipt in original_receipts:
        assert await case.env.service.receipts.find(f"automation:{run_id}", "deliver") == receipt
    assert attempts == 5 and len(sent(case.env)) == 1 and not case.provider.requests


@pytest.mark.asyncio
async def test_control_cancel_interrupts_inflight_native_handler_without_work_or_replay(
    database, tmp_path
):
    from qq_ai_bot.automation.work_cursor import load
    from qq_ai_bot.domain.messages import CitationOrigin, ResponseCitation
    from qq_ai_bot.llm.fake import FakeLLMProvider
    from qq_ai_bot.model_runtime.models import ModelProfile
    from qq_ai_bot.web.bridge_state import BridgeState
    from qq_ai_bot.web.gemini_bridge import GeminiSearchBridge

    case = await setup_run(database, tmp_path, strategy="static")
    case.executor._settings.runtime_work_enabled = False
    case.executor._settings.automation_lease_seconds = 3
    case.executor._settings.web_extract_max_results = 0
    native = FakeLLMProvider(
        responder=lambda request: ChatResponse(
            content="Original paid native evidence",
            latency_seconds=0,
            citations=(
                ResponseCitation(
                    url="https://example.com/original",
                    title="Original evidence",
                    origin=CitationOrigin.ANNOTATION,
                ),
            ),
        )
    )
    handlers = case.executor._registry.require("yuki.agent").handler.__self__
    handlers._web = GeminiSearchBridge(
        profile=ModelProfile(
            id="native-cancellation",
            provider="fake",
            model="fake",
            timeout_seconds=30,
            max_retries=0,
            default_temperature=0.5,
            default_max_output_tokens=32768,
        ),
        credential="synthetic",
        provider=native,
        state=BridgeState(tmp_path / "search.sqlite3"),
    )
    script = case.row.script.model_dump(mode="json")
    script["steps"].insert(
        0, {"id": "research", "call": "web.search", "arguments": {"query": "original evidence"}}
    )
    case.row = await service(case).administer_create(
        script,
        owner_id=case.env.person,
        conversation_id=case.env.context.conversation_id,
        creation_source_key="native-inflight-cancel",
    )
    case.run = await case.repository.create_run(
        case.row.id, scheduled_for=case.row.next_run_at, actual_started_at=case.clock.now()
    )
    original_call = case.env.bot.call_api
    entered, cancelled = asyncio.Event(), asyncio.Event()
    dispatched = []

    async def inflight(action, **params):
        if action == "send_group_msg":
            dispatched.append((action, params))
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
        return await original_call(action, **params)

    case.env.bot.call_api = inflight
    worker = AutomationWorker(
        settings=case.executor._settings,
        repository=case.repository,
        executor=case.executor,
        time_service=case.chat._time,
    )
    claimed = next(
        item
        for item in await case.repository.claim_due(
            worker_id=worker._worker_id,
            now=case.row.next_run_at,
            lease_seconds=3,
        )
        if item.id == case.row.id
    )
    processing = asyncio.create_task(worker._process_guarded(claimed))
    try:
        await asyncio.wait_for(entered.wait(), timeout=10)
        original_cursor = await load(database, case.run.id, case.row.script_hash)
        async with database.sessions() as reader:
            assert not list(await reader.scalars(select(work.c.id)))
            ledger = dict(
                (
                    await reader.execute(
                        select(automation_budgets).where(automation_budgets.c.run_id == case.run.id)
                    )
                )
                .mappings()
                .one()
            )
        assert ledger["models"] == len(native.requests) == 1
        assert original_cursor[1]["outputs"]["research"]["provider_summary"] == (
            "Original paid native evidence"
        )
        original_receipt = await case.env.service.receipts.find(
            f"automation:{case.run.id}", "deliver"
        )
        assert original_receipt.status == "executing"
        assert (await cancel_plan(case)).success
        await asyncio.wait_for(cancelled.wait(), timeout=10)
        await asyncio.wait_for(processing, timeout=10)
        terminal = next(
            item
            for item in await case.repository.run_history(case.row.id)
            if item.id == case.run.id
        )
        assert terminal.status is RunStatus.CANCELLED and terminal.llm_calls == 1
        assert terminal.steps_completed == original_cursor[1]["steps_completed"] == 1
        assert terminal.tool_calls == original_cursor[1]["tool_calls"] == 1
        assert terminal.messages_sent == original_cursor[1]["messages_sent"] == 0
        assert terminal.version_id == case.run.version_id
        settled = await case.repository.get(case.row.id)
        assert settled.status is AutomationStatus.CANCELLED and settled.run_count == 1
        assert settled.claimed_by is None
        assert await case.repository.claim_expiry(case.row.id, worker._worker_id) is None
        assert await load(database, case.run.id, case.row.script_hash) == original_cursor
        interrupted_receipt = await case.env.service.receipts.find(
            f"automation:{case.run.id}", "deliver"
        )
        assert interrupted_receipt.operation_id == original_receipt.operation_id
        assert interrupted_receipt.status == "uncertain"
        async with database.sessions() as reader:
            assert (
                dict(
                    (
                        await reader.execute(
                            select(automation_budgets).where(
                                automation_budgets.c.run_id == case.run.id
                            )
                        )
                    )
                    .mappings()
                    .one()
                )
                == ledger
            )
        replay = await case.executor.execute(settled, terminal)
        assert replay.status is RunStatus.CANCELLED and replay.llm_calls == 1
        assert len(native.requests) == 1 and not sent(case.env)
        assert len(dispatched) == 1
        assert (
            await case.env.service.receipts.find(f"automation:{case.run.id}", "deliver")
            == interrupted_receipt
        )
        assert not case.provider.requests
    finally:
        if not processing.done():
            processing.cancel()
            await asyncio.gather(processing, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="Supervisor requires POSIX")
async def test_control_cancel_automation_tree_stops_original_supervisor_and_retains_late_receipt(
    database, tmp_path, monkeypatch
):
    import signal
    import sys
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from tests.unit.test_work_execution_receipt_regressions import EXTERNAL

    from qq_ai_bot.sandbox.client import SandboxClient
    from qq_ai_bot.sandbox.persistent import PersistentManager
    from qq_ai_bot.sandbox.task_repository import SandboxTaskRepository
    from qq_ai_bot.services.subagent_execution import SubagentExecution
    from qq_ai_bot.workspace.store import WorkspaceStore

    case = await setup_run(database, tmp_path, delivery="none", mode="silent")
    manager = PersistentManager(
        tmp_path / "manager",
        WorkspaceStore(tmp_path / "artifacts"),
        "test-image",
        "test-network",
        "",
        tmp_path / "home",
        testing=True,
    )
    manager.ready, manager.container_id = True, "original-container"
    transport = SimpleNamespace(
        request=AsyncMock(return_value={"session_id": EXTERNAL, "running": False}),
        connect=AsyncMock(side_effect=TimeoutError("original handshake lost")),
    )
    manager.execd = transport

    async def observe(identity):
        return manager.get(identity)

    monkeypatch.setattr(manager, "wait_result", observe)
    original_run = {}

    async def receive(reader, writer):
        message = json.loads(await reader.readline())
        if message["method"] == "terminal_exec":
            result = await manager.submit(message["method"], message["args"], message["request_id"])
            original_run.update(result)
        else:
            assert message["method"] == "cancel_code_run"
            result = await manager.control(message["args"]["run_id"], "cancel")
        writer.write(json.dumps(result).encode() + b"\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    socket = tmp_path / "manager.sock"
    server = await asyncio.start_unix_server(receive, path=socket)
    tasks = SandboxTaskRepository(database)
    client = SandboxClient(socket, tasks=tasks)
    case.chat._tools.sandbox_client = client
    arguments = {"command": "printf once >> executions; sleep 300", "cwd": str(tmp_path)}
    case.provider._responder = lambda request: ChatResponse(
        content="",
        latency_seconds=0,
        tool_calls=(
            ToolCall(
                id="original-terminal" if len(case.provider.requests) == 1 else "original-run-wait",
                function=ToolFunction(
                    name="terminal_exec" if len(case.provider.requests) == 1 else "task_control",
                    arguments=json.dumps(
                        arguments
                        if len(case.provider.requests) == 1
                        else {
                            "action": "wait",
                            "conditions": [{"kind": "owned_run", "run_id": original_run["run_id"]}],
                        }
                    ),
                ),
            ),
        ),
    )
    process = None
    try:
        result = await case.executor.execute(case.row, case.run)
        assert result.status is RunStatus.RUNNING
        repository = WorkRepository(database)
        async with database.sessions() as reader:
            original = dict((await reader.execute(select(work))).mappings().one())
        task = await tasks.by_run(original_run["run_id"])
        task_source = json.loads(task.source_json)
        assert task_source["work_id"] == original["id"]
        assert task_source["automation_run_id"] == case.run.id
        assert task_source["origin"] == "scheduled_automation"
        assert json.loads(original["source_json"])["owner"] == "automation"
        lease = await repository.acquire(original["conversation_id"], original["generation"])
        child = await repository.derive(lease, original["id"], "owned-child", "unfinished child")
        independent = await repository.accept(
            lease,
            source_key="independent-sdk",
            source={"owner": "plugin_invocation"},
            goal="independent owned work",
        )
        await repository.release(lease)
        with pytest.raises(TimeoutError, match="original handshake lost"):
            await manager.launch(original_run["run_id"])
        wrapper = (
            "import sys; from pathlib import Path; "
            "from qq_ai_bot.sandbox import environment_supervisor as s; "
            "root, job = Path(sys.argv[1]), sys.argv[2]; "
            "s.Path = lambda value: root if value == '/var/lib/yuki-runtime' else Path(value); "
            "sys.argv = ['supervisor', job]; raise SystemExit(s.main())"
        )
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            wrapper,
            str(manager.runtime_root),
            original_run["run_id"],
            stdin=asyncio.subprocess.DEVNULL,
        )
        executions = tmp_path / "executions"
        async with asyncio.timeout(10):
            while True:
                started = manager.status_record(original_run["run_id"])
                if started.get("status") == "running" and executions.exists():
                    break
                await asyncio.sleep(0.01)
        assert started["supervisor_pid"] == process.pid
        os.kill(started["pid"], 0)
        async with database.sessions() as reader:
            budget = dict(
                (
                    await reader.execute(
                        select(automation_budgets).where(automation_budgets.c.run_id == case.run.id)
                    )
                )
                .mappings()
                .one()
            )
        assert (await cancel_plan(case)).success
        execution = SubagentExecution(
            repository, SubagentRepository(repository), SimpleNamespace(sandbox_client=client)
        )
        await execution.cancel_commands()
        assert (manager.state_path(original_run["run_id"]) / "cancel").is_file()
        async with asyncio.timeout(10):
            await process.wait()
        final = manager.status_record(original_run["run_id"])
        assert final["status"] == "cancelled" and final["exit_code"] == -signal.SIGTERM
        assert final["finished_at"] - final["started_at"] < 300
        assert executions.read_text() == "once"
        await manager.reconcile(manager.active()[0])
        receipt = manager.completions.pending()["events"][0]
        assert receipt["request_id"] == task.request_id
        assert receipt["run_id"] == original_run["run_id"]
        await tasks.receive(receipt)
        await tasks.receive(receipt)
        await repository.route_child_completion(task.request_id)
        await execution.cancel_commands()
        assert (await repository.get(original["id"]))["state"] == "cancelled"
        assert (await repository.get(child["id"]))["state"] == "cancelled"
        assert await repository.get(independent["id"]) == independent
        assert (await repository.get(original["id"]))["source_json"] == original["source_json"]
        settled = await case.repository.get(case.row.id)
        run = (await case.repository.run_history(case.row.id))[0]
        assert settled.status is AutomationStatus.CANCELLED and settled.run_count == 1
        assert run.id == case.run.id and run.status is RunStatus.CANCELLED
        assert run.version_id == case.run.version_id
        assert (await tasks.get(task.request_id)).status == "completed"
        async with database.sessions() as reader:
            assert set(await reader.scalars(select(waits.c.status))) == {"cancelled"}
            assert (
                dict(
                    (
                        await reader.execute(
                            select(automation_budgets).where(
                                automation_budgets.c.run_id == case.run.id
                            )
                        )
                    )
                    .mappings()
                    .one()
                )
                == budget
            )
        assert (await manager.submit("terminal_exec", arguments, task.request_id))["run_id"] == (
            original_run["run_id"]
        )
        assert not sent(case.env)
    finally:
        if process is not None and process.returncode is None:
            (manager.state_path(original_run["run_id"]) / "cancel").touch()
            async with asyncio.timeout(10):
                await process.wait()
        server.close()
        await server.wait_closed()
        await manager.close()


async def test_ordinary_directory_does_not_parse_execution_payloads(database, tmp_path):
    from pydantic import ValidationError
    from sqlalchemy import update
    from tests.support.automation_unified_delivery_helpers import setup_run

    from qq_ai_bot.automation.models import AutomationStatus
    from qq_ai_bot.persistence.models import AutomationModel

    case = await setup_run(database, tmp_path, delivery="none", mode="silent")
    async with database.immediate_session() as writer:
        await writer.execute(
            update(AutomationModel)
            .where(AutomationModel.id == case.row.id)
            .values(
                script_json="unreadable execution IR",
                required_capabilities_json="unreadable capabilities",
                authority_snapshot_json="unreadable authority",
            )
        )
    entry = await case.repository.get_directory_entry(case.row.id)
    assert entry.record.name == case.row.name and entry.creator.person_id == case.env.person
    directory = await case.repository.list_directory(statuses=(AutomationStatus.ACTIVE,))
    assert [item.record.id for item in directory] == [case.row.id]
    with pytest.raises(ValidationError):
        await case.repository.list_directory(
            statuses=(AutomationStatus.ACTIVE,), include_script=True
        )


async def test_batch_claimers_accept_disjoint_sets_and_keep_running_and_versionless_rows(
    database, tmp_path, monkeypatch
):
    case = await setup_run(database, tmp_path, delivery="none", mode="silent")
    clock = FakeClock(case.row.next_run_at)
    async with database.sessions() as reader:
        definition = await reader.get(AutomationModel, case.row.id)
        version = await reader.get(AutomationVersionModel, case.row.version_id)
        definition_values = {
            column.name: getattr(definition, column.name)
            for column in AutomationModel.__table__.columns
            if column.name != "id"
        }
        version_values = {
            column.name: getattr(version, column.name)
            for column in AutomationVersionModel.__table__.columns
            if column.name != "id"
        }
    async with database.immediate_session() as writer:
        await writer.execute(
            update(AutomationModel)
            .where(AutomationModel.id == case.row.id)
            .values(status="paused", next_run_at=None)
        )
        for identity in (2, 3, 4):
            await writer.execute(
                insert(AutomationModel).values(
                    id=identity, **{**definition_values, "next_run_at": clock.now()}
                )
            )
            if identity != 4:
                await writer.execute(
                    insert(AutomationVersionModel).values(
                        **{**version_values, "automation_id": identity}
                    )
                )
    original_writer = database.immediate_session
    both_discovered = asyncio.Event()
    arriving = 0

    @asynccontextmanager
    async def compete():
        nonlocal arriving
        arriving += 1
        if arriving == 2:
            both_discovered.set()
        await asyncio.wait_for(both_discovered.wait(), timeout=5)
        async with original_writer() as writer:
            yield writer

    monkeypatch.setattr(database, "immediate_session", compete)
    first, second = await asyncio.gather(
        *(
            case.repository.claim_due(
                worker_id=worker, now=clock.now(), lease_seconds=60, clock=clock.now
            )
            for worker in ("first-real-writer", "second-real-writer")
        )
    )
    first_ids, second_ids = {row.id for row in first}, {row.id for row in second}
    assert not first_ids & second_ids and first_ids | second_ids == {case.row.id, 2, 3, 4}
    accepted = {row.id: row for row in (*first, *second)}
    assert accepted[case.row.id].status is AutomationStatus.PAUSED
    assert accepted[4].version_id is None
    assert all(row.script == case.row.script for row in accepted.values())
    original_run_source = await case.repository.get_for_run(case.run.id)
    assert original_run_source.version_id == case.run.version_id
    assert original_run_source.script == case.row.script
    assert (await case.repository.run_history(case.row.id))[0].id == case.run.id
    assert not case.provider.requests and not sent(case.env)


@pytest.mark.parametrize("unknown_commit", [False, True])
async def test_claim_parses_only_after_successful_commit(
    database, tmp_path, monkeypatch, unknown_commit
):
    import qq_ai_bot.automation.repository as automation_repository

    case = await setup_run(database, tmp_path, delivery="none", mode="silent")
    original_writer = database.immediate_session
    original_record = automation_repository._automation_record
    committed = False
    parsed = []

    @asynccontextmanager
    async def commit_outcome():
        nonlocal committed
        async with original_writer() as writer:
            yield writer
        if unknown_commit:
            raise RuntimeError("unknown_claim_commit")
        committed = True

    def parse_accepted(row, version_id=None):
        assert committed
        parsed.append(row.id)
        return original_record(row, version_id)

    monkeypatch.setattr(database, "immediate_session", commit_outcome)
    monkeypatch.setattr(automation_repository, "_automation_record", parse_accepted)
    if unknown_commit:
        with pytest.raises(RuntimeError, match="unknown_claim_commit"):
            await case.repository.claim_due(
                worker_id="original-claim", now=case.row.next_run_at, lease_seconds=60
            )
        assert parsed == []
    else:
        accepted = await case.repository.claim_due(
            worker_id="original-claim", now=case.row.next_run_at, lease_seconds=60
        )
        assert parsed == [case.row.id] and accepted[0].script == case.row.script
    async with database.sessions() as reader:
        original = await reader.get(AutomationModel, case.row.id)
        assert original.claimed_by == "original-claim"
    assert not case.provider.requests and not sent(case.env)
