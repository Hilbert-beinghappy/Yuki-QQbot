"""#267: implicit group sources merge by quiet window; original Work input goes first."""

import asyncio
import json
from types import SimpleNamespace

from sqlalchemy import select, update
from tests.conftest import MemorySender, build_harness, make_settings
from tests.support.social_identity_cases import social_env

from qq_ai_bot.admin.models import ConversationRuntimeConfig
from qq_ai_bot.conversation.ordinary_admission_db_models import OrdinaryTurnAdmissionModel
from qq_ai_bot.conversation.scope import runtime_conversation_key
from qq_ai_bot.domain.conversations import ScopeType
from qq_ai_bot.domain.messages import InboundMessage, SenderIdentity
from qq_ai_bot.identity.db_models import CanonicalSpaceModel
from qq_ai_bot.llm.fake import FakeLLMProvider
from qq_ai_bot.runtime.work_control import WorkControl
from qq_ai_bot.services.autonomous_groups import AutonomousGroupService
from qq_ai_bot.services.turn_coordinator import ConversationTurnCoordinator


def _policy(*, debounce: float, autonomous: bool = False) -> ConversationRuntimeConfig:
    return ConversationRuntimeConfig(
        autonomous_enabled=autonomous,
        autonomous_debounce_seconds=debounce,
        autonomous_admission_threshold=100,
        autonomous_batch_limit=1,
        autonomous_presence_window_seconds=0,
        interrupt_autonomous_on_new_message=True,
    )


class _Runtime:
    def __init__(self, policy: ConversationRuntimeConfig) -> None:
        self.policy = policy

    async def snapshot(self, **_: object) -> SimpleNamespace:
        return SimpleNamespace(conversation_policy=lambda: self.policy)


def _message(event_id: int, *, group: str = "20001") -> InboundMessage:
    return InboundMessage(
        message_id=f"m{event_id}",
        event_type="message:test",
        scope_type=ScopeType.GROUP,
        sender=SenderIdentity("10001"),
        text=f"片段{event_id}",
        bot_user_id="80001",
        group_id=group,
        source_event_id=event_id,
    )


def _service(policy: ConversationRuntimeConfig) -> AutonomousGroupService:
    return AutonomousGroupService(
        chat=SimpleNamespace(),  # type: ignore[arg-type]
        admission_features=SimpleNamespace(),  # type: ignore[arg-type]
        runtime_config=_Runtime(policy),  # type: ignore[arg-type]
        turn_coordinator=ConversationTurnCoordinator(),
    )


def _key(message: InboundMessage) -> str:
    return runtime_conversation_key(identity=message.scope(), inbound=message)


async def test_worker_attempts_only_latest_source_and_ignores_older_classification() -> None:
    service = _service(_policy(debounce=0.05))
    attempts: list[int] = []

    def attempt_for(event_id: int):
        async def attempt() -> int:
            attempts.append(event_id)
            return 1

        return attempt

    first = _message(100)
    service.observe(first, ordinary_attempt=attempt_for(100))
    await asyncio.sleep(0.02)
    service.observe(_message(102), ordinary_attempt=attempt_for(102))
    # A slower classification of an older source neither wins nor extends the window.
    service.observe(_message(101), ordinary_attempt=attempt_for(101))
    service.observe(_message(102), ordinary_attempt=attempt_for(102))
    await service.wait_until_idle(_key(first))

    assert attempts == [102]


async def test_worker_window_zero_runs_ordinary_even_without_autonomy() -> None:
    service = _service(_policy(debounce=0))
    attempts: list[int] = []
    other_group: list[int] = []

    async def attempt() -> int:
        attempts.append(1)
        return 0

    async def other() -> int:
        other_group.append(1)
        return 0

    message = _message(7)
    service.observe(message, ordinary_attempt=attempt)
    service.observe(_message(8, group="20002"), ordinary_attempt=other)
    # Legacy observation without an attempt still needs autonomy and stays idle.
    service.observe(_message(9, group="20003"))
    await service.wait_until_idle(_key(message))
    await service.wait_until_idle(_key(_message(8, group="20002")))

    assert attempts == [1] and other_group == [1]
    await service.close()


async def _open_group_scene(database, tmp_path, provider, *, debounce: float):
    env = await social_env(database, tmp_path)
    async with database.sessions() as session, session.begin():
        await session.execute(
            update(CanonicalSpaceModel)
            .where(CanonicalSpaceModel.id == env.space)
            .values(require_mention=False)
        )
    harness = build_harness(
        database,
        make_settings(
            database.url,
            enabled_groups_csv="20001",
            conversation_autonomous_debounce_seconds=debounce,
        ),
        provider,
    )
    processor = harness.processor
    autonomous = AutonomousGroupService(
        chat=processor._chat,
        admission_features=SimpleNamespace(),  # type: ignore[arg-type]
        runtime_config=processor._runtime_config,
        turn_coordinator=processor._turn_coordinator,
    )
    processor._autonomous = autonomous

    def message(index: int, text: str, *, mention: bool = False) -> InboundMessage:
        return InboundMessage(
            message_id=f"open-{index}",
            event_type="message:test",
            scope_type=ScopeType.GROUP,
            sender=SenderIdentity("10001"),
            text=text,
            bot_user_id="80001",
            group_id="20001",
            mentions_bot=mention,
            conversation_id=env.context.conversation_id,
            person_id=env.person,
            presence_id=env.presence,
            space_id=env.space,
        )

    return env, harness, autonomous, message


async def _admissions(database) -> list[OrdinaryTurnAdmissionModel]:
    async with database.sessions() as session:
        return list((await session.scalars(select(OrdinaryTurnAdmissionModel))).all())


async def test_open_floor_fragments_admit_one_turn_for_latest_source(database, tmp_path):
    provider = FakeLLMProvider()
    _env, harness, autonomous, message = await _open_group_scene(
        database, tmp_path, provider, debounce=0.2
    )
    sender = MemorySender()
    fragments = [
        message(index, text) for index, text in enumerate(("我想问", "那个", "明天几点开会"))
    ]
    results = [await harness.processor.handle(fragment, sender) for fragment in fragments]
    assert [result.reason for result in results] == ["ordinary_deferred"] * 3
    assert provider.requests == [] and await _admissions(database) == []

    # Same keeper resent: no new version, no new window, no second source.
    coordinator = harness.processor._turn_coordinator
    latest = max(harness.processor._observed_human_turns)
    key = harness.processor._observed_human_turns[latest].snapshot.scope_key
    version = coordinator._states[key].version
    resent = await harness.processor.handle(fragments[-1], sender)
    assert resent.reason == "group_observed"
    assert coordinator._states[key].version == version

    await autonomous.wait_until_idle(key)
    admitted = await _admissions(database)
    assert [row.event_id for row in admitted] == [latest]
    assert len(provider.requests) >= 1
    await autonomous.close()


async def test_explicit_call_during_window_cancels_pending_open_source(database, tmp_path):
    provider = FakeLLMProvider()
    _env, harness, autonomous, message = await _open_group_scene(
        database, tmp_path, provider, debounce=0.2
    )
    sender = MemorySender()
    assert (await harness.processor.handle(message(1, "在吗"), sender)).reason == (
        "ordinary_deferred"
    )
    pending = next(iter(harness.processor._observed_human_turns.values()))
    direct = await harness.processor.handle(message(2, "帮我看看", mention=True), sender)
    assert direct.reason == "chat"
    await autonomous.wait_until_idle(pending.snapshot.scope_key)

    admitted = await _admissions(database)
    assert len(admitted) == 1
    assert admitted[0].event_id not in harness.processor._observed_human_turns
    await autonomous.close()


async def test_open_floor_rechecks_require_mention_when_window_closes(database, tmp_path):
    provider = FakeLLMProvider()
    env, harness, autonomous, message = await _open_group_scene(
        database, tmp_path, provider, debounce=0.2
    )
    assert (await harness.processor.handle(message(1, "有人吗"), MemorySender())).reason == (
        "ordinary_deferred"
    )
    pending = next(iter(harness.processor._observed_human_turns.values()))
    async with database.sessions() as session, session.begin():
        await session.execute(
            update(CanonicalSpaceModel)
            .where(CanonicalSpaceModel.id == env.space)
            .values(require_mention=True)
        )
    await autonomous.wait_until_idle(pending.snapshot.scope_key)

    assert await _admissions(database) == [] and provider.requests == []
    await autonomous.close()


class _Repository:
    def __init__(self, database: object, rows: list[dict[str, object]]) -> None:
        self.database = database
        self.rows = rows
        self.staged: list[int] = []

    async def input_images(self, _item: dict[str, object]) -> tuple[()]:
        return ()

    async def has_pending_business_inputs(self, *_args: object, **_kwargs: object) -> bool:
        return False

    async def stage(self, _lease: object, selected: list[int], _attempt: str) -> None:
        self.staged.extend(selected)


async def test_signal_with_visible_anchor_keeps_wait_payload(database) -> None:
    signal = {
        "kind": "work_signal",
        "wait_id": "wait-1",
        "reason": "signal",
        "conditions": [{"kind": "conversation", "matched": {"event_id": 100}}],
    }
    rows = [
        {
            "id": 1,
            "kind": "control",
            "event_id": 100,
            "ready": True,
            "payload_json": json.dumps({"text": json.dumps(signal), "signal": True}),
        },
        {
            "id": 2,
            "kind": "message",
            "event_id": 100,
            "ready": True,
            "payload_json": json.dumps({"text": "原文"}),
        },
    ]
    repository = _Repository(database, rows)

    class _Control(WorkControl):
        async def pending(self) -> list[dict[str, object]]:  # type: ignore[override]
            return rows

        async def reconcile_completed_children(self) -> None:
            return None

    control = _Control.__new__(_Control)
    control.repository = repository  # type: ignore[assignment]
    control.session = None
    control.current = {"id": "work-1"}
    control.lease = SimpleNamespace(conversation_id="c", generation=1)  # type: ignore[assignment]
    messages = await control.take_inputs("a1", observed_event_ids=frozenset({100}))

    assert "wait-1" in messages[0].content and "Work 信号" in messages[0].content
    assert "已在当前聊天展示" in messages[1].content
    assert repository.staged == [1, 2]


async def test_open_floor_input_reaches_original_wait_without_waiting_window(database, tmp_path):
    from qq_ai_bot.runtime.work_repository import WorkRepository
    from qq_ai_bot.runtime.work_wait import WorkWaitRepository

    provider = FakeLLMProvider()
    env, harness, autonomous, message = await _open_group_scene(
        database, tmp_path, provider, debounce=5
    )
    repository = WorkRepository(database)
    lease = await repository.acquire(env.context.conversation_id, 1)
    assert lease

    async def validate() -> None:
        assert await repository.valid(lease)

    source = {"actor_person_id": env.person, "principal_kind": "person", "origin": "user_message"}
    control = WorkControl(repository, lease, "open-floor-wait", source, validate)
    assert json.loads(
        await control.execute("task_control", {"action": "accept", "goal": "等回复"}, "accept")
    )["ok"]
    work_id = control.current["id"]
    await WorkWaitRepository(repository).register(
        lease,
        work_id=work_id,
        source=control.source,
        call_key="wait-conversation",
        mode="any",
        conditions=[{"kind": "conversation"}],
        deadline_at=None,
    )

    result = await harness.processor.handle(message(1, "好了，结果是42"), MemorySender())

    assert result.reason == "work_input_queued"
    admitted = await _admissions(database)
    assert len(admitted) == 1 and admitted[0].route == "work"
    assert admitted[0].work_id == work_id
    # Nothing was left for the quiet window to open as a second Main.
    assert autonomous._states == {} and provider.requests == []
    await autonomous.close()
