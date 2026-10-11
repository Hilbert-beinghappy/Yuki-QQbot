"""Full task declarations reach the original execution and calendar consumers."""

import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from pydantic import BaseModel, field_validator
from sqlalchemy import select
from tests.conftest import build_harness, make_settings
from tests.support.social_identity_cases import social_env

from qq_ai_bot.automation.authority import PermissionLevel
from qq_ai_bot.automation.executor import AutomationExecutor
from qq_ai_bot.automation.handlers import AutomationCapabilityHandlers
from qq_ai_bot.automation.models import (
    AutomationScript,
    DailySchedule,
    OnceSchedule,
    RetryPolicy,
    RiskClass,
    RunStatus,
)
from qq_ai_bot.automation.registry import (
    AutomationCapability,
    CapabilityResult,
    build_capability_registry,
)
from qq_ai_bot.automation.repository import AutomationRepository
from qq_ai_bot.automation.service import AutomationService
from qq_ai_bot.automation.templates import resolve_templates
from qq_ai_bot.automation.validator import AutomationValidator, CreationProvenance
from qq_ai_bot.domain.tool_actor import ToolActor
from qq_ai_bot.llm.gemini import GeminiProvider
from qq_ai_bot.persistence.models import AutomationModel, AutomationRunModel, ChatEventModel
from qq_ai_bot.runtime.origin import TurnOrigin
from qq_ai_bot.services.main_agent_contract import MainAgentContract
from qq_ai_bot.time.schedules import _resolve_local, initial_run_at, next_run_at
from qq_ai_bot.time.service import TimeContextService
from qq_ai_bot.workspace.short_state import ShortState


@pytest.fixture
async def gemini_wire():
    requests = []

    def receive(request):
        requests.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "candidates": [
                    {
                        "content": {"role": "model", "parts": [{"text": "INTERNAL_COMPLETE"}]},
                        "finishReason": "STOP",
                    }
                ]
            },
        )

    base_url = "https://wire.test/v1beta/"
    async with httpx.AsyncClient(
        base_url=base_url, transport=httpx.MockTransport(receive)
    ) as client:
        yield (
            GeminiProvider(
                base_url=base_url,
                api_key="offline-test",
                timeout_seconds=10,
                max_retries=0,
                client=client,
            ),
            requests,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("padding", [32, 4300, 5600])
async def test_long_task_persists_and_reaches_original_main_agent(
    database, tmp_path, padding, gemini_wire
):
    env = await social_env(database, tmp_path)
    provider, requests = gemini_wire
    settings = make_settings(database.url, automation_enabled=True, runtime_work_enabled=True)
    harness = build_harness(database, settings, provider)
    chat = harness.processor._chat
    contract = MainAgentContract(chat, ShortState(env.store))
    chat.runtime.runner.main_contract = contract
    handlers = AutomationCapabilityHandlers(
        settings=settings,
        main_turns=chat.runtime.main_turns,
        main_contract=contract,
        runtime_config=chat._runtime_config,
        time_service=chat._time,
        ledger=harness.ledger,
        memories=chat._memories,
        web_provider=None,
        gateway_factory=lambda context: None,
    )
    registry = build_capability_registry(handlers.mapping())
    repository = AutomationRepository(database)
    service = AutomationService(
        settings=settings, repository=repository, registry=registry, time_service=chat._time
    )
    async with database.sessions() as session:
        event = await session.scalar(select(ChatEventModel).order_by(ChatEventModel.id))
    actor = ToolActor(
        user_id="10001",
        bot_user_id="80001",
        group_id="20001",
        origin=TurnOrigin.USER_MESSAGE,
        instruction="按完整约束完成内部工作",
        event_id=event.id,
        person_id=env.person,
        conversation_id=env.context.conversation_id,
    )
    goal = "完整目标：" + "待处理资料" * 600
    constraints = [
        "占位资料，不要输出：" + "A" * padding + "\nTAIL_MARKER=YUKI_AUTO_TAIL_OK_731",
        *[f"条件 {i}" for i in range(13)],
    ]
    payload = {
        "name": "完整任务" * 40,
        "goal": goal,
        "constraints": constraints,
        "trigger": {"type": "after", "seconds": 1},
        "timezone": " UTC ",
        "context": {"scene": "none"},
        "delivery": {"target": "none"},
    }
    row, plan = await service.create_task(
        payload,
        actor=actor,
        conversation_key="group:80001:20001",
    )
    expected = {"goal": goal, "constraints": constraints, "delivery": "none"}
    assert json.loads(plan.script.steps[0].arguments["instruction"]) == expected
    async with database.sessions() as session:
        stored = await session.get(AutomationModel, row.id)
        assert (
            json.loads(json.loads(stored.script_json)["steps"][0]["arguments"]["instruction"])
            == expected
        )
    replay = await service.create(plan.script, actor=actor, conversation_key="group:80001:20001")
    assert replay.id == row.id and replay.next_run_at == row.next_run_at
    updated, plan = await service.update_task(
        row.id,
        {**payload, "name": payload["name"] + "（修改）"},
        actor=actor,
        conversation_key="group:80001:20001",
    )
    assert updated.next_run_at == row.next_run_at
    row = updated
    assert json.loads(row.script.steps[0].arguments["instruction"]) == expected
    async with database.sessions() as session:
        stored = await session.get(AutomationModel, row.id)
        instruction = json.loads(stored.script_json)["steps"][0]["arguments"]["instruction"]
        assert json.loads(instruction) == expected
    run = await repository.create_run(
        row.id, scheduled_for=row.next_run_at, actual_started_at=chat._time.clock.now()
    )
    result = await AutomationExecutor(
        settings=settings,
        registry=registry,
        repository=repository,
        time_service=chat._time,
    ).execute(row, run)
    assert result.status is RunStatus.SUCCEEDED, result
    from qq_ai_bot.prompting.serializer import strip_dynamic_prefix

    submitted = [
        json.loads(body)
        for message in requests[0]["contents"]
        for part in message["parts"]
        if message["role"] == "user"
        and "text" in part
        and (body := strip_dynamic_prefix(part["text"])).startswith('{"origin":')
    ]
    assert len(submitted) == 1
    assert json.loads(submitted[0]["instruction"]) == expected
    assert len(requests) == 1
    assert result.llm_calls == 1
    assert not [action for action, _ in env.bot.calls if action.startswith("send_")]


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [{"key": [1, 2, 3]}, [1, {"name": "资料"}], 7.5])
async def test_exact_prior_output_reaches_native_execution_once(database, tmp_path, value):
    parsed = []

    class Arguments(BaseModel):
        key: dict | list | float

        @field_validator("key")
        @classmethod
        def observe(cls, item):
            parsed.append(item)
            return item

    registry = build_capability_registry()
    call = "business." + "registered_capability" * 8
    observed = []

    async def consume(arguments, context):
        observed.append(arguments["key"])
        return CapabilityResult(data={"data": arguments["key"]})

    capability = AutomationCapability(
        name=call,
        description="consume original typed object",
        argument_model=Arguments,
        output_schema={"type": "object"},
        required_permission=PermissionLevel.USER,
        risk_class=RiskClass.READ,
        retry_policy=RetryPolicy.NONE,
        allowed_origins=frozenset({TurnOrigin.SCHEDULED_AUTOMATION}),
        handler=consume,
    )
    registry.register(capability)
    alias = "original_output" * 4
    producer = "original_producer" * 4
    script = AutomationScript.model_validate(
        {
            "version": 1,
            "name": "原对象传递",
            "schedule": {"type": "interval", "seconds": 1},
            "steps": [
                {"id": producer, "call": call, "arguments": {"key": value}, "save_as": alias},
                *[
                    {
                        "id": f"consume_{i}",
                        "call": call,
                        "arguments": {"key": f"${{{producer if i == 0 else alias}.data}}"},
                    }
                    for i in range(17)
                ],
            ],
            "limits": {
                "max_steps": 18,
                "max_llm_calls": 0,
                "max_tool_calls": 18,
                "max_messages": 0,
                "timeout_seconds": 601,
            },
        }
    )
    provenance = CreationProvenance(
        creator_user_id="10001",
        bot_user_id="80001",
        current_group_id="20001",
        permission=PermissionLevel.USER,
    )
    AutomationValidator(registry=registry).validate(script, provenance)
    assert parsed == []
    resolved = resolve_templates(
        script.steps[1].arguments, builtins={}, step_outputs={producer: {"data": value}}
    )
    assert resolved["key"] is value
    env = await social_env(database, tmp_path)
    settings = make_settings(database.url, automation_enabled=True)
    time_service = TimeContextService(database)
    repository = AutomationRepository(database)
    service = AutomationService(
        settings=settings, repository=repository, registry=registry, time_service=time_service
    )
    async with database.sessions() as session:
        event = await session.scalar(select(ChatEventModel).order_by(ChatEventModel.id))
    row = await service.create(
        script,
        actor=ToolActor(
            user_id="10001",
            bot_user_id="80001",
            group_id="20001",
            origin=TurnOrigin.USER_MESSAGE,
            instruction="按原登记能力处理对象",
            event_id=event.id,
            person_id=env.person,
            conversation_id=env.context.conversation_id,
        ),
        conversation_key="group:80001:20001",
    )
    assert row.script.limits == script.limits
    assert parsed == []
    run = await repository.create_run(
        row.id, scheduled_for=row.next_run_at, actual_started_at=time_service.clock.now()
    )
    executor = AutomationExecutor(
        settings=settings,
        registry=registry,
        repository=repository,
        time_service=time_service,
    )
    result = await executor.execute(row, run)
    assert result.status is RunStatus.SUCCEEDED, result
    assert result.tool_calls == 18 and result.llm_calls == 0
    assert observed == [value] * 18 and parsed == [value] * 18
    replay = await executor.execute(row, run)
    assert replay.status is RunStatus.SUCCEEDED and replay.tool_calls == 18
    assert observed == [value] * 18 and parsed == [value] * 18


def test_naive_once_and_local_iso_preserve_subsecond_instant():
    value = datetime(2026, 10, 11, 8, 0, 0, 750000)
    schedule = OnceSchedule(type="once", local_datetime=value, timezone=" UTC ")
    expected = value.replace(tzinfo=UTC)
    assert initial_run_at(schedule, expected.replace(microsecond=500000), "UTC") == expected
    assert _resolve_local(value, " UTC ") == expected
    assert _resolve_local(expected, "UTC") == expected


def test_daily_schedule_uses_original_iana_skipped_day():
    schedule = DailySchedule(type="daily", hour=9, minute=0, timezone="Pacific/Apia")
    after = datetime(2011, 12, 29, 20, tzinfo=UTC)
    assert next_run_at(schedule, after, "UTC") == datetime(2011, 12, 30, 19, tzinfo=UTC)


@pytest.mark.parametrize(
    ("local", "expected"),
    [
        (datetime(2026, 3, 8, 2, 30), datetime(2026, 3, 8, 7, 30, tzinfo=UTC)),
        (datetime(2026, 11, 1, 1, 30), datetime(2026, 11, 1, 5, 30, tzinfo=UTC)),
    ],
)
def test_original_iana_gap_and_first_fold(local, expected):
    assert _resolve_local(local, "America/New_York") == expected


@pytest.mark.asyncio
async def test_long_static_message_keeps_original_parent_parts_and_no_replay(database, tmp_path):
    from sqlalchemy import event as sa_event
    from tests.support.automation_unified_delivery_helpers import setup_run

    from qq_ai_bot.automation.work_cursor import load
    from qq_ai_bot.social.db_models import SocialOperationModel

    case = await setup_run(database, tmp_path, strategy="static")
    manager = AutomationService(
        settings=case.executor._settings,
        repository=case.repository,
        registry=case.executor._registry,
        time_service=case.chat._time,
    )
    body = "完整原文资料" * 2200 + "TAIL=STATIC_LONG_TEXT"
    from qq_ai_bot.domain.messages import ScopeType

    event, _ = await case.chat._ledger.append(
        bot_user_id="80001",
        platform_message_id="long-static-fixture",
        scope_type=ScopeType.GROUP,
        group_id="20001",
        sender_user_id="10001",
        direction="inbound",
        content=body,
    )
    row, plan = await manager.create_task(
        {
            "name": "long static reminder",
            "goal": body,
            "strategy": "static",
            "trigger": {"type": "after", "seconds": 60},
            "delivery": {"target": "current_group"},
        },
        actor=ToolActor(
            user_id="10001",
            bot_user_id="80001",
            group_id="20001",
            origin=TurnOrigin.USER_MESSAGE,
            event_id=event.id,
            person_id=case.env.person,
            conversation_id=case.env.context.conversation_id,
            instruction="deliver the complete text",
        ),
        conversation_key="group:80001:20001",
    )
    assert plan.script.steps[0].arguments["text"] == body
    assert plan.script.limits.max_messages is None and plan.script.limits.timeout_seconds is None
    run = await case.repository.create_run(
        row.id, scheduled_for=row.next_run_at, actual_started_at=case.clock.now()
    )
    result = await case.executor.execute(row, run)
    assert result.status is RunStatus.SUCCEEDED, result
    _, cursor = await load(database, run.id, row.script_hash)
    receipt = cursor["outputs"]["deliver"]
    parts = receipt["parts"]
    assert len(parts) > 1 and result.messages_sent == receipt["sent_messages"] == len(parts)
    assert "".join(part["delivered_text"] for part in parts) == body
    async with database.sessions() as session:
        parent = await session.get(SocialOperationModel, receipt["operation_id"])
        assert parent.planned_parts == len(parts) and parent.action == "send_message_sequence"
        assert list(
            await session.scalars(
                select(SocialOperationModel.id).where(
                    SocialOperationModel.source_turn_id == f"automation:{run.id}",
                    SocialOperationModel.action == "send_message_sequence",
                )
            )
        ) == [parent.id]
        for part in parts:
            stored = await session.get(SocialOperationModel, part["operation_id"])
            assert stored.status == "succeeded" and stored.event_id == part["event_id"]
    sent = [item for item in case.env.bot.calls if item[0] == "send_group_msg"]
    assert len(sent) == len(parts) and not case.provider.requests
    replay = await case.executor.execute(row, run)
    assert replay.status is result.status and replay.messages_sent == result.messages_sent
    assert [item for item in case.env.bot.calls if item[0] == "send_group_msg"] == sent
    settlement = {
        **{
            key: getattr(result, key)
            for key in (
                "status",
                "steps_completed",
                "llm_calls",
                "tool_calls",
                "messages_sent",
                "error_category",
                "summary",
            )
        },
        "finished_at": case.clock.now(),
    }
    interrupted = False

    def interrupt_plan_write(conn, db_cursor, statement, parameters, context, executemany):
        nonlocal interrupted
        if statement.lstrip().upper().startswith("UPDATE AUTOMATIONS "):
            interrupted = True
            raise RuntimeError("interrupt original plan settlement")

    sa_event.listen(database.engine.sync_engine, "before_cursor_execute", interrupt_plan_write)
    try:
        with pytest.raises(RuntimeError, match="interrupt original plan settlement"):
            await case.repository.finish_run(run.id, **settlement)
    finally:
        sa_event.remove(database.engine.sync_engine, "before_cursor_execute", interrupt_plan_write)
    assert interrupted
    async with database.sessions() as session:
        preserved_run = await session.get(AutomationRunModel, run.id)
        preserved_plan = await session.get(AutomationModel, row.id)
        assert preserved_run.status == "running"
        assert preserved_plan.run_count == 0
    assert (await case.repository.get(row.id)).next_run_at == row.next_run_at
    replay = await case.executor.execute(row, run)
    assert replay.messages_sent == len(parts)
    assert [item for item in case.env.bot.calls if item[0] == "send_group_msg"] == sent
    assert await case.repository.finish_run(run.id, **settlement)
    assert not await case.repository.finish_run(run.id, **settlement)
    async with database.sessions() as session:
        preserved_run = await session.get(AutomationRunModel, run.id)
        preserved_plan = await session.get(AutomationModel, row.id)
        assert preserved_run.status == "succeeded" and preserved_run.messages_sent == len(parts)
        assert preserved_plan.run_count == 1 and preserved_plan.next_run_at is None


@pytest.mark.parametrize(
    "declaration",
    [
        {"type": "after", "seconds": 2 * 365 * 86400},
        {"type": "interval", "seconds": 2 * 365 * 86400},
        {"type": "weekly", "hour": 9, "minute": 0, "weekdays": [7] * 8},
    ],
)
def test_declared_long_schedule_and_repeated_weekday_use_original_calendar(declaration):
    from pydantic import TypeAdapter

    from qq_ai_bot.automation.models import Schedule

    now = datetime(2026, 10, 11, tzinfo=UTC)
    schedule = TypeAdapter(Schedule).validate_python(declaration)
    assert initial_run_at(schedule, now, " UTC ") == (
        now + timedelta(seconds=declaration["seconds"])
        if "seconds" in declaration
        else now.replace(hour=9)
    )
