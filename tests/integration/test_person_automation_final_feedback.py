"""Automation sends explicitly; failed Work resumes without reopening its run."""

import json
from dataclasses import replace

import pytest
from sqlalchemy import select
from tests.support.automation_unified_delivery_helpers import sent, setup_run

from qq_ai_bot.automation.models import RunStatus
from qq_ai_bot.domain.messages import ChatResponse, ToolCall, ToolFunction
from qq_ai_bot.runtime.work_schema_v1 import work


@pytest.mark.parametrize(
    ("delivery", "context_profile"),
    [
        pytest.param("current_group", None, id="current_group"),
        pytest.param("self_private", None, id="self_private"),
        pytest.param("self_private", "creator_private", id="creator_private"),
    ],
)
@pytest.mark.parametrize("completion_proposed", [False, True])
async def test_person_notification_uses_explicit_target_without_courtesy_recovery(
    database, tmp_path, delivery, context_profile, completion_proposed
):
    case = await setup_run(
        database,
        tmp_path,
        delivery=delivery,
        mode="silent",
        context={"scene": context_profile, "history_limit": 1} if context_profile else None,
    )
    text = "PUBLIC_EXPLICIT_SEND"
    if context_profile == "creator_private":
        from qq_ai_bot.domain.conversations import ConversationScope

        text += "A" * 13000 + "PRIVATE_BODY_TAIL"
        await case.chat._ledger._writer.append(
            scope=ConversationScope.private("80001", "10001"),
            platform_message_id="offline-original-private-material",
            sender_user_id="10001",
            direction="inbound",
            content="ORIGINAL_CREATOR_PRIVATE_MATERIAL",
        )
    calls = 0

    def respond(request):
        nonlocal calls
        calls += 1
        if completion_proposed and calls == 1:
            return ChatResponse(
                "",
                0,
                tool_calls=(
                    ToolCall(
                        "original-completion-proposal",
                        ToolFunction("task_control", '{"action":"complete"}'),
                    ),
                ),
            )
        if calls == (2 if completion_proposed else 1):
            args = {"text": text}
            if delivery == "self_private":
                args["target"] = {"kind": "person", "subject_ref": "current_speaker"}
            return ChatResponse(
                "",
                0,
                tool_calls=(
                    ToolCall(
                        "explicit-notification", ToolFunction("send_message", json.dumps(args))
                    ),
                ),
            )
        return "INTERNAL_FINAL_MUST_NOT_AUTO_SEND"

    case.provider._responder = respond
    result = await case.executor.execute(case.row, case.run)
    assert result.status is RunStatus.SUCCEEDED, result
    assert calls == (1 if completion_proposed else 2)
    private_sequence = context_profile == "creator_private" and not completion_proposed
    assert (
        len(sent(case.env)) > 1
        if private_sequence
        else (len(sent(case.env)) == int(not completion_proposed))
    )
    actions = [action for action, _ in case.env.bot.calls if action.startswith("send_")]
    assert actions == (
        []
        if completion_proposed
        else ["send_private_msg" if delivery == "self_private" else "send_group_msg"]
        * len(sent(case.env))
    )
    if not completion_proposed:
        assert "PUBLIC_EXPLICIT_SEND" in str(sent(case.env))
    assert "INTERNAL_FINAL" not in str(sent(case.env))
    async with database.sessions() as reader:
        sources = list(await reader.scalars(select(work.c.source_json)))
    assert any(
        json.loads(source).get("owner") == "automation"
        and json.loads(source).get("delivery_target") == delivery
        and json.loads(source).get("context_profile") == (context_profile or "none")
        for source in sources
    )
    if context_profile == "creator_private":
        assert "ORIGINAL_CREATOR_PRIVATE_MATERIAL" in "\n".join(
            message.content or "" for message in case.provider.requests[0].messages
        )
        from qq_ai_bot.persistence.models import ChatEventModel
        from qq_ai_bot.social.db_models import SocialOperationModel

        async with database.sessions() as reader:
            receipts = (await reader.scalars(select(SocialOperationModel))).all()
            original_receipts = {
                receipt.id: {
                    column.name: getattr(receipt, column.name)
                    for column in receipt.__table__.columns
                }
                for receipt in receipts
            }
            if completion_proposed:
                assert not receipts
            else:
                parent = next(row for row in receipts if row.action == "send_message_sequence")
                parts = [row for row in receipts if row.action == "send_message"]
                assert parent.planned_parts == len(parts) == len(sent(case.env))
                assert result.messages_sent == len(parts)
                assert all(
                    part.status == "succeeded"
                    and part.target_kind == "person"
                    and part.target_id == case.env.person
                    and part.presence_id == case.env.presence
                    and part.event_id is not None
                    and part.platform_reference
                    for part in parts
                )
                assert all(str(payload["user_id"]) == "10001" for payload in sent(case.env))
                delivered_text = "".join(
                    segment["data"]["text"]
                    for payload in sent(case.env)
                    for segment in payload["message"]
                    if segment["type"] == "text"
                )
                assert delivered_text.replace("\n", "") == text
                assert (await reader.get(ChatEventModel, parts[-1].event_id)).content.endswith(
                    "PRIVATE_BODY_TAIL"
                )
        replay = await case.executor.execute(case.row, case.run)
        assert replay.status is RunStatus.SUCCEEDED and replay.messages_sent == result.messages_sent
        assert len(case.provider.requests) == calls and len(sent(case.env)) == len(actions)
        async with database.sessions() as reader:
            receipts = (await reader.scalars(select(SocialOperationModel))).all()
            assert {
                receipt.id: {
                    column.name: getattr(receipt, column.name)
                    for column in receipt.__table__.columns
                }
                for receipt in receipts
            } == original_receipts


@pytest.mark.parametrize("delivery", ["current_group", "self_private"])
@pytest.mark.parametrize("after_final_send", [False, True])
async def test_unsent_person_notification_final_stops_without_courtesy_correction(
    database, tmp_path, delivery, after_final_send
):
    case = await setup_run(database, tmp_path, delivery=delivery, mode="silent")
    row, run = case.row, case.run
    if after_final_send:
        from qq_ai_bot.automation.models import AutomationScript
        from qq_ai_bot.automation.service import AutomationService
        from qq_ai_bot.domain.conversations import ConversationScope
        from qq_ai_bot.domain.tool_actor import ToolActor
        from qq_ai_bot.runtime.origin import TurnOrigin

        payload = row.script.model_dump(mode="json")
        payload["name"] = "explicit send after internal final"
        payload["steps"].append(
            {
                "id": "explicit_send",
                "call": "social.send_message",
                "arguments": {
                    **(
                        {"target": {"kind": "person", "subject_ref": "current_speaker"}}
                        if delivery == "self_private"
                        else {}
                    ),
                    "text": "EXPLICIT_AFTER_INTERNAL_FINAL",
                },
            }
        )
        event = await case.chat._ledger._writer.append(
            scope=ConversationScope.group("80001", "20001"),
            platform_message_id="after-final-source",
            sender_user_id="10001",
            direction="inbound",
            content="Explicitly send after the internal task",
        )
        service = AutomationService(
            settings=case.executor._settings,
            repository=case.repository,
            registry=case.executor._registry,
            time_service=case.chat._time,
        )
        row = await service.create(
            AutomationScript.model_validate(payload),
            actor=ToolActor(
                user_id="10001",
                bot_user_id="80001",
                group_id="20001",
                origin=TurnOrigin.USER_MESSAGE,
                instruction="Explicitly send after the internal task",
                event_id=event.event.id,
                person_id=case.env.person,
                conversation_id=case.env.context.conversation_id,
            ),
            conversation_key="group:80001:20001",
        )
        run = await case.repository.create_run(
            row.id,
            scheduled_for=row.next_run_at,
            actual_started_at=case.clock.now(),
        )
    result = await case.executor.execute(row, run)
    assert result.status is RunStatus.SUCCEEDED
    assert len(case.provider.requests) == 1
    assert len(sent(case.env)) == int(after_final_send)
    assert result.messages_sent == int(after_final_send)
    if after_final_send:
        assert "EXPLICIT_AFTER_INTERNAL_FINAL" in str(sent(case.env))
        await case.executor.execute(row, run)
        assert len(case.provider.requests) == 1 and len(sent(case.env)) == 1


@pytest.mark.parametrize(
    "budget",
    [
        "segment",
        "undeclared",
        "run",
        "tool_run",
        "message_run",
        "native_run",
        "native_segment",
        "native_revoked",
    ],
)
async def test_agent_script_uses_original_work_budget_without_outer_flag(
    database, tmp_path, monkeypatch, budget
):
    from qq_ai_bot.automation.models import AutomationScript
    from qq_ai_bot.automation.service import AutomationService
    from qq_ai_bot.automation.work_cursor import load
    from qq_ai_bot.domain.conversations import ConversationScope
    from qq_ai_bot.domain.tool_actor import ToolActor
    from qq_ai_bot.runtime.origin import TurnOrigin
    from qq_ai_bot.runtime.work_budget_schema import automation_budgets
    from qq_ai_bot.web.models import WebMode, WebSearchResponse, WebSearchSource

    case = await setup_run(database, tmp_path, delivery="none", mode="silent")
    payload = case.row.script.model_dump(mode="json")
    payload["name"] = f"original {budget} budget"
    payload["limits"] = (
        {"max_llm_calls": 2}
        if budget in {"run", "native_run"}
        else {"max_tool_calls": 1}
        if budget == "tool_run"
        else {"max_messages": 0}
        if budget == "message_run"
        else {}
    )
    step = payload["steps"][0]
    step["arguments"]["max_model_requests"] = (
        1 if budget == "segment" else 2 if budget == "native_segment" else None
    )
    script = AutomationScript.model_validate(payload)
    service = AutomationService(
        settings=case.executor._settings,
        repository=case.repository,
        registry=case.executor._registry,
        time_service=case.chat._time,
    )
    event = await case.chat._ledger._writer.append(
        scope=ConversationScope.group("80001", "20001"),
        platform_message_id=f"budget-source-{budget}",
        sender_user_id="10001",
        direction="inbound",
        content="Complete this internal task with the declared budget",
    )
    row = await service.create(
        script,
        actor=ToolActor(
            user_id="10001",
            bot_user_id="80001",
            group_id="20001",
            origin=TurnOrigin.USER_MESSAGE,
            instruction="Complete this internal task with the declared budget",
            event_id=event.event.id,
            person_id=case.env.person,
            conversation_id=case.env.context.conversation_id,
        ),
        conversation_key="group:80001:20001",
    )
    run = await case.repository.create_run(
        row.id, scheduled_for=row.next_run_at, actual_started_at=case.clock.now()
    )
    monkeypatch.setattr(case.chat._settings, "runtime_work_enabled", budget == "message_run")
    monkeypatch.setattr(case.chat._settings, "web_mode", WebMode.NATIVE)
    web_requests = []

    class Web:
        async def search(self, request):
            web_requests.append(request)
            return WebSearchResponse(
                request.query,
                (
                    WebSearchSource(
                        request.query,
                        "original title",
                        f"https://example.com/{len(web_requests)}",
                        "example.com",
                        "original snippet",
                        "original content",
                    ),
                ),
                None,
                0,
            )

    case.chat._tools._web_provider = Web()
    native = budget.startswith("native_")
    native_provider = None
    bridge = None
    if native:
        from sqlalchemy import update

        from qq_ai_bot.domain.messages import CitationOrigin, ResponseCitation
        from qq_ai_bot.identity.db_models import CanonicalPersonModel
        from qq_ai_bot.llm.fake import FakeLLMProvider
        from qq_ai_bot.model_runtime.models import ModelProfile
        from qq_ai_bot.web.bridge_state import BridgeState
        from qq_ai_bot.web.gemini_bridge import GeminiSearchBridge

        class SearchProvider(FakeLLMProvider):
            async def complete(self, request):
                response = await super().complete(request)
                if budget == "native_revoked":
                    async with database.immediate_session() as writer:
                        await writer.execute(
                            update(CanonicalPersonModel)
                            .where(
                                CanonicalPersonModel.id == case.env.person,
                            )
                            .values(enabled=False)
                        )
                return response

        native_provider = SearchProvider(
            lambda _: ChatResponse(
                "Original native summary",
                0,
                citations=(
                    ResponseCitation(
                        url="https://example.com/original",
                        title="original grounding",
                        origin=CitationOrigin.ANNOTATION,
                    ),
                ),
            )
        )
        bridge = GeminiSearchBridge(
            profile=ModelProfile(
                id="original-native-search",
                provider="fake",
                model="fake",
                timeout_seconds=1,
                max_retries=0,
                default_temperature=0.5,
                default_max_output_tokens=8192,
            ),
            credential="synthetic",
            provider=native_provider,
            state=BridgeState(tmp_path / "native-search.sqlite3"),
        )
        case.chat._tools._web_provider = bridge
    calls = 0

    def respond(request):
        nonlocal calls
        calls += 1
        if native:
            return (
                ChatResponse(
                    "",
                    0,
                    tool_calls=tuple(
                        ToolCall(
                            f"original-native-{index}",
                            ToolFunction(
                                "web_search",
                                json.dumps({"query": f"original concurrent query {index}"}),
                            ),
                        )
                        for index in range(2)
                    ),
                )
                if calls == 1
                else "INTERNAL_QUIET_FINAL"
            )
        if budget == "undeclared" and calls > 37:
            return "INTERNAL_QUIET_FINAL"
        if budget == "message_run":
            return (
                ChatResponse(
                    "",
                    0,
                    tool_calls=(
                        ToolCall(
                            "original-budgeted-send",
                            ToolFunction("send_message", '{"text":"confirmed once"}'),
                        ),
                    ),
                )
                if calls == 1
                else "INTERNAL_QUIET_FINAL"
            )
        is_web = budget == "undeclared" and calls <= 4
        return ChatResponse(
            "",
            0,
            tool_calls=(
                ToolCall(
                    f"original-read-{calls}",
                    ToolFunction(
                        "web_search" if is_web else "get_my_capabilities",
                        json.dumps({"query": f"original search {calls}"})
                        if is_web
                        else json.dumps({"query": f"original capability {calls}"}),
                    ),
                ),
            ),
        )

    case.provider._responder = respond
    try:
        result = await case.executor.execute(row, run)
    finally:
        if bridge is not None:
            await bridge.close()
    if native:
        assert result.status is RunStatus.FAILED, result
        assert calls == 1 and len(native_provider.requests) == 1
        assert result.llm_calls == 2 and result.tool_calls == 2, result
        assert result.error_category == (
            "delegated_authority_revoked"
            if budget == "native_revoked"
            else "agent_segment_budget_exhausted"
            if budget == "native_segment"
            else "automation_run_total_budget_exhausted"
        ), result
    elif budget == "undeclared":
        assert result.status is RunStatus.SUCCEEDED, result
        assert result.llm_calls == calls == 38 and result.tool_calls == 37
        assert len(web_requests) == 4
        web_results = [
            json.loads(message.content)
            for request in case.provider.requests
            for message in request.messages
            if message.role == "tool"
            and message.tool_call_id
            in {"original-read-1", "original-read-2", "original-read-3", "original-read-4"}
        ]
        assert web_results and all(item["ok"] for item in web_results)
    else:
        assert result.status is not RunStatus.SUCCEEDED, result
        assert calls == result.llm_calls == (1 if budget == "segment" else 2)
        assert result.error_category == (
            "agent_segment_budget_exhausted"
            if budget == "segment"
            else "message_limit_exceeded"
            if budget == "message_run"
            else "work_total_budget_exhausted"
            if budget == "tool_run"
            else "automation_run_total_budget_exhausted"
        )
        if budget in {"run", "tool_run"}:
            before = await load(database, run.id, row.script_hash)
            await case.repository.finish_run(
                run.id,
                status=result.status,
                steps_completed=result.steps_completed,
                llm_calls=result.llm_calls,
                tool_calls=result.tool_calls,
                messages_sent=result.messages_sent,
                error_category=result.error_category,
                summary=result.summary,
                finished_at=case.clock.now(),
            )
            run = (await case.repository.run_history(row.id))[0]
            replay = await case.executor.execute(row, run)
            assert replay.status is not RunStatus.SUCCEEDED and calls == 2
            assert replay.llm_calls == result.llm_calls
            assert (await load(database, run.id, row.script_hash))[1]["llm_calls"] == before[1][
                "llm_calls"
            ]
    async with database.sessions() as reader:
        recorded = (
            (
                await reader.execute(
                    select(automation_budgets).where(automation_budgets.c.run_id == run.id)
                )
            )
            .mappings()
            .one()
        )
        assert recorded["models"] == calls + (len(native_provider.requests) if native else 0)
        if budget == "tool_run":
            assert recorded["tools"] == result.tool_calls == 1
    assert len(sent(case.env)) == int(budget == "message_run")
    if budget == "message_run":
        from qq_ai_bot.social.db_models import SocialOperationModel

        assert result.messages_sent == 1
        async with database.sessions() as reader:
            original_receipt = (await reader.scalars(select(SocialOperationModel))).one()
            assert original_receipt.status == "succeeded" and original_receipt.event_id is not None


@pytest.mark.parametrize("limit", ["segment", "run", "work", "revoked"])
async def test_automation_preparation_uses_original_budget_and_current_authority(
    database, tmp_path, monkeypatch, limit
):
    from datetime import UTC, datetime
    from uuid import uuid4

    from sqlalchemy import update

    from qq_ai_bot.automation.models import AutomationScript
    from qq_ai_bot.automation.service import AutomationService
    from qq_ai_bot.conversation.canonical_db_models import CanonicalConversationModel
    from qq_ai_bot.conversation.observation_models import ContextObservationModel
    from qq_ai_bot.domain.conversations import ConversationScope
    from qq_ai_bot.domain.tool_actor import ToolActor
    from qq_ai_bot.identity.db_models import CanonicalPersonModel
    from qq_ai_bot.runtime.origin import TurnOrigin
    from qq_ai_bot.runtime.work_budget_schema import automation_budgets

    case = await setup_run(database, tmp_path, delivery="none", mode="silent")
    settings = case.executor._settings
    settings.runtime_work_enabled = limit == "work"
    settings.context_window_tokens = 65536
    settings.context_compaction_window_tokens = 65536
    settings.llm_max_output_tokens = 1024
    settings.work_compaction_max_output_tokens = 1024
    payload = case.row.script.model_dump(mode="json")
    payload["name"] = "original preparation budget"
    payload["context"] = {"scene": "current_group", "history_limit": 1}
    payload["steps"][0]["arguments"]["context_profile"] = "current_group"
    payload["steps"][0]["arguments"]["max_model_requests"] = (
        1 if limit == "segment" else 2 if limit == "work" else None
    )
    payload["limits"] = {"max_llm_calls": 1 if limit == "run" else 2}
    event = await case.chat._ledger._writer.append(
        scope=ConversationScope.group("80001", "20001"),
        platform_message_id="original-preparation-source",
        sender_user_id="10001",
        direction="inbound",
        content="Prepare and complete this internal task",
    )
    service = AutomationService(
        settings=settings,
        repository=case.repository,
        registry=case.executor._registry,
        time_service=case.chat._time,
    )
    row = await service.create(
        AutomationScript.model_validate(payload),
        actor=ToolActor(
            user_id="10001",
            bot_user_id="80001",
            group_id="20001",
            origin=TurnOrigin.USER_MESSAGE,
            instruction="Prepare and complete this internal task",
            event_id=event.event.id,
            person_id=case.env.person,
            conversation_id=case.env.context.conversation_id,
        ),
        conversation_key="group:80001:20001",
    )
    run = await case.repository.create_run(
        row.id,
        scheduled_for=row.next_run_at,
        actual_started_at=case.clock.now(),
    )
    observation_id = str(uuid4())
    async with database.immediate_session() as session:
        conversation = await session.get(
            CanonicalConversationModel, case.env.context.conversation_id
        )
        session.add(
            ContextObservationModel(
                id=observation_id,
                conversation_id=case.env.context.conversation_id,
                generation=conversation.generation,
                actor_id="actorless",
                read_scope=json.dumps(
                    {"memory": [], "plugin_id": None, "delegation_id": None}, sort_keys=True
                ),
                source_key="work-note:" + observation_id,
                source_event_id=event.event.id,
                version=1,
                payload_json=json.dumps({"facts": "original evidence " * 9000}),
                created_at=datetime.now(UTC),
            )
        )

    def respond(request):
        if request.structured_output:
            material = json.loads(request.messages[-1].content)
            return json.dumps(
                {
                    "facts": [
                        {
                            "text": "Retain the original observation.",
                            "refs": material["source_refs"],
                        }
                    ]
                }
            )
        return "INTERNAL_QUIET_FINAL"

    case.provider._responder = respond
    if limit == "revoked":
        models = case.chat.runtime.runner._models
        execute = models.execute

        async def revoke_before_dispatch(*args, **kwargs):
            async with database.immediate_session() as session:
                await session.execute(
                    update(CanonicalPersonModel)
                    .where(
                        CanonicalPersonModel.id == case.env.person,
                    )
                    .values(enabled=False)
                )
            return await execute(*args, **kwargs)

        monkeypatch.setattr(models, "execute", revoke_before_dispatch)
    result = await case.executor.execute(row, run)
    expected_calls = 0 if limit == "revoked" else 2 if limit == "work" else 1
    assert len(case.provider.requests) == expected_calls, result
    assert [request.structured_output for request in case.provider.requests] == (
        [True, False] if limit == "work" else [] if limit == "revoked" else [True]
    )
    assert result.status is (RunStatus.SUCCEEDED if limit == "work" else RunStatus.FAILED), result
    if limit == "segment":
        assert result.error_category == "agent_segment_budget_exhausted"
    elif limit == "run":
        assert result.error_category == "automation_run_total_budget_exhausted"
    elif limit == "revoked":
        assert result.error_category == "delegated_authority_revoked"
    await case.repository.finish_run(
        run.id,
        status=result.status,
        steps_completed=result.steps_completed,
        llm_calls=result.llm_calls,
        tool_calls=result.tool_calls,
        messages_sent=result.messages_sent,
        error_category=result.error_category,
        summary=result.summary,
        finished_at=case.clock.now(),
    )
    retained = (await case.repository.run_history(row.id))[0]
    assert retained.llm_calls == expected_calls
    async with database.sessions() as reader:
        recorded = (
            (
                await reader.execute(
                    select(automation_budgets).where(
                        automation_budgets.c.run_id == run.id,
                    )
                )
            )
            .mappings()
            .one()
        )
        assert recorded["models"] == expected_calls and recorded["tools"] == 0
    replay = await case.executor.execute(row, retained)
    assert replay.status is result.status and replay.llm_calls == expected_calls
    assert len(case.provider.requests) == expected_calls and not sent(case.env)


@pytest.mark.parametrize("work_enabled", [False, True])
async def test_declared_agent_deadline_cancels_original_request_without_replay(
    database, tmp_path, monkeypatch, work_enabled
):
    from qq_ai_bot.automation.models import AutomationScript
    from qq_ai_bot.automation.service import AutomationService
    from qq_ai_bot.domain.conversations import ConversationScope
    from qq_ai_bot.domain.tool_actor import ToolActor
    from qq_ai_bot.runtime.origin import TurnOrigin
    from qq_ai_bot.runtime.work_budget_schema import automation_budgets

    case = await setup_run(database, tmp_path, delivery="none", mode="silent")
    monkeypatch.setattr(case.executor._settings, "runtime_work_enabled", work_enabled)
    payload = case.row.script.model_dump(mode="json")
    payload["name"] = "original explicitly declared deadline"
    payload["limits"]["timeout_seconds"] = 1
    event = await case.chat._ledger._writer.append(
        scope=ConversationScope.group("80001", "20001"),
        platform_message_id="original-deadline-source",
        sender_user_id="10001",
        direction="inbound",
        content="Execute within the explicitly declared deadline",
    )
    service = AutomationService(
        settings=case.executor._settings,
        repository=case.repository,
        registry=case.executor._registry,
        time_service=case.chat._time,
    )
    row = await service.create(
        AutomationScript.model_validate(payload),
        actor=ToolActor(
            user_id="10001",
            bot_user_id="80001",
            group_id="20001",
            origin=TurnOrigin.USER_MESSAGE,
            instruction="Execute within the explicitly declared deadline",
            event_id=event.event.id,
            person_id=case.env.person,
            conversation_id=case.env.context.conversation_id,
        ),
        conversation_key="group:80001:20001",
    )
    run = await case.repository.create_run(
        row.id,
        scheduled_for=row.next_run_at,
        actual_started_at=case.clock.now(),
    )
    case.provider._delay_seconds = 2
    result = await case.executor.execute(row, run)
    assert result.status is RunStatus.FAILED and result.error_category == "runtime_timeout", result
    assert len(case.provider.requests) == 1
    async with database.sessions() as reader:
        budget = (
            (
                await reader.execute(
                    select(automation_budgets).where(
                        automation_budgets.c.run_id == run.id,
                    )
                )
            )
            .mappings()
            .one()
        )
        assert budget["models"] == 1 and budget["tools"] == 0
        original_work = list((await reader.execute(select(work))).mappings())
    await case.repository.finish_run(
        run.id,
        status=result.status,
        steps_completed=result.steps_completed,
        llm_calls=result.llm_calls,
        tool_calls=result.tool_calls,
        messages_sent=result.messages_sent,
        error_category=result.error_category,
        summary=result.summary,
        finished_at=case.clock.now(),
    )
    retained = (await case.repository.run_history(row.id))[0]
    assert retained.llm_calls == 1
    case.provider._delay_seconds = 0
    replay = await case.executor.execute(row, retained)
    assert replay.status is RunStatus.FAILED and replay.llm_calls == 1
    assert len(case.provider.requests) == 1 and not sent(case.env)
    async with database.sessions() as reader:
        assert list((await reader.execute(select(work))).mappings()) == original_work


async def test_none_person_automation_nonempty_internal_final_is_silent(
    database, tmp_path, monkeypatch
):
    from datetime import timedelta

    from qq_ai_bot.automation.work_cursor import load
    from qq_ai_bot.automation.worker import AutomationWorker
    from qq_ai_bot.persistence.models import (
        AutomationModel,
        AutomationRunModel,
    )

    case = await setup_run(database, tmp_path, delivery="none", mode="silent")
    handlers = case.executor._registry.require("yuki.agent").handler.__self__
    compose = handlers._generation_composition
    caller = []

    async def capture_caller(arguments, context, **kwargs):
        caller.append((arguments, context))
        return await compose(arguments, context, **kwargs)

    monkeypatch.setattr(handlers, "_generation_composition", capture_caller)
    result = await case.executor.execute(case.row, case.run)
    assert result.status is RunStatus.SUCCEEDED
    assert len(case.provider.requests) == 1 and not sent(case.env)
    async with database.sessions() as reader:
        original = dict((await reader.execute(select(work))).mappings().one())
    original_cursor = await load(database, case.run.id, case.row.script_hash)
    monkeypatch.setattr(case.chat._settings, "runtime_work_enabled", False)
    monkeypatch.setattr(case.chat, "_history_input_budget", lambda *_args, **_kwargs: 0)
    replay = await handlers.agent(*caller[0])
    assert replay.data["text"] == json.loads(original["checkpoint_json"])["sync_result"]
    assert replay.llm_calls == replay.tool_calls == replay.messages_sent == 0
    async with database.sessions() as reader:
        assert dict((await reader.execute(select(work))).mappings().one()) == original
    assert len(case.provider.requests) == 1 and not sent(case.env)

    # The final step is durable, but its owning run has not been settled yet.
    worker = AutomationWorker(
        settings=case.executor._settings,
        repository=case.repository,
        executor=case.executor,
        time_service=case.chat._time,
    )
    claimed = await case.repository.claim_due(
        worker_id=worker._worker_id,
        now=case.row.next_run_at + timedelta(seconds=1),
        lease_seconds=60,
    )
    assert len(claimed) == 1
    await worker._process(claimed[0])
    async with database.sessions() as reader:
        run = await reader.get(AutomationRunModel, case.run.id)
        automation = await reader.get(AutomationModel, case.row.id)
        assert run.status == "succeeded" and run.error_category is None
        assert (run.steps_completed, run.llm_calls, run.tool_calls, run.messages_sent) == (
            result.steps_completed,
            result.llm_calls,
            result.tool_calls,
            result.messages_sent,
        )
        assert automation.status == "completed" and automation.run_count == 1
        assert list(await reader.scalars(select(AutomationRunModel.id))) == [case.run.id]
        assert dict((await reader.execute(select(work))).mappings().one()) == original
    assert await load(database, case.run.id, case.row.script_hash) == original_cursor
    assert len(case.provider.requests) == 1 and not sent(case.env)


@pytest.mark.parametrize(
    ("context_profile", "principal"),
    [
        ("none", "person"),
        ("none", "self"),
        ("current_group", "person"),
        ("current_group", "self"),
        ("creator_private", "person"),
    ],
)
async def test_failed_agent_releases_owner_and_explicitly_resumes_original_work(
    database, tmp_path, principal, context_profile
):
    from datetime import timedelta

    from tests.support.runtime_execution import make_work_resumer
    from tests.unit.test_work_owner_recovery import public_action

    from qq_ai_bot.automation.work_cursor import load
    from qq_ai_bot.automation.worker import AutomationWorker
    from qq_ai_bot.domain.conversations import ConversationScope
    from qq_ai_bot.llm.base import LLMAuthenticationError
    from qq_ai_bot.persistence.models import AutomationModel, AutomationRunModel
    from qq_ai_bot.runtime.work_budget_schema import automation_budgets
    from qq_ai_bot.runtime.work_repository import WorkRepository
    from qq_ai_bot.runtime.work_scheduler import WorkScheduler

    case = await setup_run(
        database,
        tmp_path,
        delivery="none",
        mode="silent",
        principal=principal,
        context={"scene": context_profile, "history_limit": 3},
        limits={"max_llm_calls": 2},
    )
    await case.chat._ledger._writer.append(
        scope=ConversationScope.group("80001", "20001"),
        platform_message_id="original-profile-history",
        sender_user_id="10001",
        direction="inbound",
        content="ORIGINAL_GROUP_CONTEXT_TAIL",
    )
    if context_profile == "creator_private":
        await case.chat._ledger._writer.append(
            scope=ConversationScope.private("80001", "10001"),
            platform_message_id="offline-original-private-restore-material",
            sender_user_id="10001",
            direction="inbound",
            content="ORIGINAL_PRIVATE_CONTEXT_TAIL",
        )
    worker = AutomationWorker(
        settings=case.executor._settings,
        repository=case.repository,
        executor=case.executor,
        time_service=case.chat._time,
    )

    async def claimed_process():
        claimed = await case.repository.claim_due(
            worker_id=worker._worker_id,
            now=case.row.next_run_at + timedelta(seconds=1),
            lease_seconds=60,
        )
        assert len(claimed) == 1 and claimed[0].id == case.row.id
        await worker._process(claimed[0])

    def unavailable(request):
        raise LLMAuthenticationError("synthetic provider authentication failure")

    case.provider._responder = unavailable
    await claimed_process()
    async with database.sessions() as reader:
        original = dict((await reader.execute(select(work))).mappings().one())
        run = await reader.get(AutomationRunModel, case.run.id)
        automation = await reader.get(AutomationModel, case.row.id)
        assert original["state"] == "failed" and original["reason"] == "LLMAuthenticationError"
        assert run.status == "failed" and run.finished_at is not None
        assert run.llm_calls == original["model_requests"] == len(case.provider.requests) == 1
        assert automation.status == "failed" and automation.run_count == 1
        assert automation.claimed_by is None and automation.claimed_until is None
        accepted = await case.repository.get_for_run(case.run.id)
        assert accepted.version_id == case.run.version_id
        assert accepted.script.limits.max_llm_calls == 2
        original_run = {column.name: getattr(run, column.name) for column in run.__table__.columns}
        original_owner = {
            column.name: getattr(automation, column.name) for column in automation.__table__.columns
        }
        original_budget = (
            (
                await reader.execute(
                    select(automation_budgets).where(automation_budgets.c.run_id == case.run.id)
                )
            )
            .mappings()
            .one()
        )
        assert original_budget["model_limit"] == 2 and original_budget["models"] == 1
    original_cursor = await load(database, case.run.id, case.row.script_hash)
    assert json.loads(original["source_json"])["context_profile"] == context_profile
    assert (
        "ORIGINAL_GROUP_CONTEXT_TAIL"
        in "\n".join(message.content or "" for message in case.provider.requests[0].messages)
    ) is (context_profile == "current_group")
    assert (
        "ORIGINAL_PRIVATE_CONTEXT_TAIL"
        in "\n".join(message.content or "" for message in case.provider.requests[0].messages)
    ) is (context_profile == "creator_private")
    assert not await case.repository.claim_due(
        worker_id=worker._worker_id,
        now=case.row.next_run_at + timedelta(seconds=2),
        lease_seconds=60,
    )
    repository = WorkRepository(database)
    resumed_control = await public_action(database, original, "resume")
    assert resumed_control.success and resumed_control.effective_state["status"] == "queued"
    if context_profile == "creator_private":
        await case.chat._ledger._writer.append(
            scope=ConversationScope.private("80001", "10001"),
            platform_message_id="offline-new-allowed-private-restore-material",
            sender_user_id="10001",
            direction="inbound",
            content="NEW_ALLOWED_PRIVATE_EVENT_TAIL",
        )
    handlers = case.executor._registry.require("yuki.agent").handler.__self__

    async def resume_automation(item, source):
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
        resumer.services,
        settings=case.executor._settings,
        resume_automation=resume_automation,
    )
    scheduler = WorkScheduler(repository, resumer.resume, chat_admission_enabled=True)
    case.provider._responder = lambda request: "Verified internal result after provider recovery."
    owner_lease = await repository.acquire(original["conversation_id"], original["generation"])
    await scheduler.drain_once()
    assert len(case.provider.requests) == 1
    await repository.release(owner_lease)
    await scheduler.drain_once()
    async with database.sessions() as reader:
        resumed = (await reader.execute(select(work))).mappings().one()
        run = await reader.get(AutomationRunModel, case.run.id)
        automation = await reader.get(AutomationModel, case.row.id)
        assert resumed["id"] == original["id"] and resumed["state"] == "completed"
        assert resumed["source_json"] == original["source_json"]
        assert (
            resumed["model_requests"]
            == original["model_requests"] + 1
            == len(case.provider.requests)
        )
        assert json.loads(resumed["source_json"])["automation_run_id"] == run.id == case.run.id
        assert {
            column.name: getattr(run, column.name) for column in run.__table__.columns
        } == original_run
        assert {
            column.name: getattr(automation, column.name) for column in automation.__table__.columns
        } == original_owner
        assert len(list(await reader.scalars(select(AutomationRunModel.id)))) == 1
        budget = (
            (
                await reader.execute(
                    select(automation_budgets).where(automation_budgets.c.run_id == case.run.id)
                )
            )
            .mappings()
            .one()
        )
        assert budget["models"] == original_budget["models"] + 1 == 2
        assert budget["tools"] == original_budget["tools"]
        assert (budget["model_limit"], budget["tool_limit"]) == (
            original_budget["model_limit"],
            original_budget["tool_limit"],
        )
    assert await load(database, case.run.id, case.row.script_hash) == original_cursor
    assert (
        "ORIGINAL_GROUP_CONTEXT_TAIL"
        in "\n".join(message.content or "" for message in case.provider.requests[-1].messages)
    ) is (context_profile == "current_group")
    if context_profile == "creator_private":
        restored_messages = "\n".join(
            message.content or "" for message in case.provider.requests[-1].messages
        )
        assert "ORIGINAL_PRIVATE_CONTEXT_TAIL" in restored_messages
        assert "NEW_ALLOWED_PRIVATE_EVENT_TAIL" in restored_messages
    assert not sent(case.env)


@pytest.mark.parametrize("principal", ["person", "self"])
@pytest.mark.parametrize("wait_kind", ["need_input", "time_due"])
async def test_public_cancel_settles_owning_run_without_error_or_new_request(
    database, tmp_path, principal, wait_kind, monkeypatch
):

    from tests.support.automation_runtime_helpers import FakeClock
    from tests.unit.test_work_owner_recovery import public_action

    from qq_ai_bot.automation.worker import AutomationWorker
    from qq_ai_bot.persistence.models import AutomationModel, AutomationRunModel

    case = await setup_run(database, tmp_path, delivery="none", mode="silent", principal=principal)
    clock = FakeClock(case.row.next_run_at)
    monkeypatch.setattr(case.chat._time, "_clock", clock)
    worker = AutomationWorker(
        settings=case.executor._settings,
        repository=case.repository,
        executor=case.executor,
        time_service=case.chat._time,
    )

    async def process():
        clock.advance(10)
        claimed = await case.repository.claim_due(
            worker_id=worker._worker_id,
            now=clock.now(),
            lease_seconds=60,
            clock=clock.now,
        )
        assert len(claimed) == 1
        await worker._process(claimed[0])

    case.provider._responder = lambda request: ChatResponse(
        "",
        0,
        tool_calls=(
            ToolCall(
                "wait-original-input",
                ToolFunction(
                    "task_control",
                    json.dumps(
                        {"action": "need_input"}
                        if wait_kind == "need_input"
                        else {
                            "action": "wait",
                            "conditions": [{"kind": "time_due", "after_seconds": 3600}],
                        }
                    ),
                ),
            ),
        ),
    )
    await process()
    async with database.sessions() as reader:
        original = dict((await reader.execute(select(work))).mappings().one())
    assert original["state"] == (
        "waiting_user" if wait_kind == "need_input" else "waiting_external"
    )
    if wait_kind == "time_due":
        from qq_ai_bot.automation.work_cursor import load
        from qq_ai_bot.runtime.work_wait_schema import waits

        async with database.sessions() as reader:
            original_run = dict(
                (await reader.execute(select(AutomationRunModel.__table__))).mappings().one()
            )
            original_owner = dict(
                (await reader.execute(select(AutomationModel.__table__))).mappings().one()
            )
            original_wait = dict((await reader.execute(select(waits))).mappings().one())
        original_owner.pop("claimed_until")
        phase, cursor = await load(database, case.run.id, case.row.script_hash)
        cursor.pop("active_seconds")
        monkeypatch.setattr(case.chat._settings, "runtime_work_enabled", False)
        monkeypatch.setattr(case.chat, "_history_input_budget", lambda *_args, **_kwargs: 0)
        # Owner polls read the original wait without preparing another model turn.
        for _ in range(2):
            await process()
            async with database.sessions() as reader:
                assert dict((await reader.execute(select(work))).mappings().one()) == original
                assert (
                    dict(
                        (await reader.execute(select(AutomationRunModel.__table__)))
                        .mappings()
                        .one()
                    )
                    == original_run
                )
                current_owner = dict(
                    (await reader.execute(select(AutomationModel.__table__))).mappings().one()
                )
                # Normal polling advances the owner backoff, retaining its business facts.
                current_owner.pop("claimed_until")
                assert current_owner == original_owner
                assert dict((await reader.execute(select(waits))).mappings().one()) == original_wait
            current_phase, current_cursor = await load(database, case.run.id, case.row.script_hash)
            assert current_phase == phase
            current_cursor.pop("active_seconds")
            assert current_cursor == cursor
        assert len(case.provider.requests) == 1
    cancelled = await public_action(database, original, "cancel")
    assert cancelled.success
    async with database.sessions() as reader:
        released = await reader.get(AutomationModel, case.row.id)
        assert released.claimed_by is None
        if wait_kind == "time_due":
            from qq_ai_bot.runtime.work_wait_schema import waits

            assert await reader.scalar(select(waits.c.status)) == "cancelled"
    await process()
    async with database.sessions() as reader:
        terminal = (await reader.execute(select(work))).mappings().one()
        run = await reader.get(AutomationRunModel, case.run.id)
        automation = await reader.get(AutomationModel, case.row.id)
        assert terminal["id"] == original["id"] and terminal["state"] == "cancelled"
        assert terminal["source_json"] == original["source_json"]
        assert terminal["model_requests"] == original["model_requests"] == 1
        assert run.status == "cancelled" and run.finished_at is not None
        assert run.error_category is None and run.llm_calls == 1
        assert automation.status == "cancelled" and automation.run_count == 1
        assert automation.claimed_by is None and automation.claimed_until is None
        assert automation.next_run_at is None
    assert len(case.provider.requests) == 1 and not sent(case.env)
