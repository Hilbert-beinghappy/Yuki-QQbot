"""Offline real plugin lifecycle/registry/Host probe; synthetic files and SQLite only."""

import asyncio
import json
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from tests.conftest import build_harness, make_settings
from tests.support.codemode_cases import effect_rows, environment, outer_call, requires_worker

from qq_ai_bot import __version__
from qq_ai_bot.capabilities.invocation import direct_invocations
from qq_ai_bot.codemode.driver import CodeModeDriver
from qq_ai_bot.domain.conversations import ScopeType
from qq_ai_bot.domain.messages import InboundMessage, SenderIdentity, ToolCall, ToolFunction
from qq_ai_bot.persistence.database import Database
from qq_ai_bot.plugin_host.capability_adapter import PluginCapabilityAdapter
from qq_ai_bot.plugin_host.discovery import PluginDiscovery
from qq_ai_bot.plugin_host.event_bus import PluginEventBus
from qq_ai_bot.plugin_host.extension_registry import ExtensionKind, ExtensionRegistry
from qq_ai_bot.plugin_host.facades import HostPluginContext, PluginFacadeServices
from qq_ai_bot.plugin_host.loader import PluginLoader
from qq_ai_bot.plugin_host.manager import PluginManager
from qq_ai_bot.plugin_host.repository import PluginInstallationRepository
from qq_ai_bot.runtime.origin import TurnOrigin
from qq_ai_bot.services.agent_runner import AgentRuntime
from qq_ai_bot.services.agent_tools import ToolRuntime
from qq_ai_bot.services.invocation_service import InvocationService
from qq_ai_bot.services.main_agent_backend import MainAgentBackend
from qq_ai_bot.services.main_agent_contract import MainAgentContract
from qq_ai_bot.workspace.short_state import ShortState
from qq_ai_bot.workspace.store import WorkspaceStore
from yuki_plugin_sdk.api import PLUGIN_API_VERSION


def write_plugin(root, version, risk, field):
    root.mkdir(parents=True, exist_ok=True)
    root.joinpath("plugin.toml").write_text(f'''id = "test.contract"
name = "Offline contract fixture"
version = "{version}.0.0"
description = "Synthetic inert audit fixture"
entrypoint = "fixture{version}:Fixture"
plugin_api = "{PLUGIN_API_VERSION}"
yuki_requires = ">=3.8"
permissions = ["tool.register"]
''')
    root.joinpath(f"fixture{version}.py").write_text(f'''
from pydantic import BaseModel, ConfigDict
from yuki_plugin_sdk.registrar import ToolMetadata, ToolRegistration
from yuki_plugin_sdk.models import RiskClass
from yuki_plugin_sdk.results import ToolResult
class Arguments(BaseModel):
    model_config = ConfigDict(extra="forbid")
    {field}: str
class Fixture:
    async def register(self, registrar):
        registrar.register_tool(ToolRegistration(
            ToolMetadata(name="inspect", description="Read an inert fixture",
                         risk=RiskClass.{risk}), Arguments, ToolResult, self.handle))
    async def start(self, context):
        self.context = context
    async def stop(self):
        pass
    async def handle(self, arguments):
        self.context.calls.append({{"version": {version}, "args": arguments.model_dump(),
                                   "declared_risk": "{risk}"}})
        if {version} == 2:
            self.context.effect_file.write_text("committed synthetic fixture")
        return ToolResult(data={{"version": {version}, "value": arguments.{field}}},
                          mutation_committed=({version} == 2))
''')


async def run(*, native_child=False):
    with tempfile.TemporaryDirectory(prefix="yuki-r3-plugin-") as d:
        base = Path(d)
        root = base / "plugins" / "test.contract"
        write_plugin(root, 1, "READ", "value")
        db = Database(f"sqlite+aiosqlite:///{base}/fixture.db")
        await db.create_schema()
        registry = ExtensionRegistry()
        installs = PluginInstallationRepository(db)
        ctx = SimpleNamespace(calls=[], effect_file=base / "inert-effect.txt")
        contexts = {}

        class AuditContext(HostPluginContext):
            pass

        def context_factory(manifest, permissions):
            context = AuditContext(
                plugin_id=manifest.id,
                approved_permissions=permissions,
                services=PluginFacadeServices(approval_revision=manifest.manifest_hash),
            )
            context.calls = ctx.calls
            context.effect_file = ctx.effect_file
            contexts[manifest.id] = context
            return context

        manager = PluginManager(
            enabled=True,
            discovery=PluginDiscovery(base / "plugins", yuki_version=__version__),
            installations=installs,
            loader=PluginLoader(),
            extensions=registry,
            event_bus=PluginEventBus(),
            context_factory=context_factory,
        )
        try:
            await manager.start()
            await manager.approve("test.contract", actor_user_id="9000")
            await manager.enable("test.contract", actor_user_id="9000")
            adapter = PluginCapabilityAdapter(
                registry=registry,
                installations=installs,
                is_running=lambda p: p in manager.running_plugin_ids,
                invocation_scope=lambda p, r, **kw: contexts[p].invocation_scope(p, r, **kw),
            )
            chat = build_harness(db, make_settings(db.url)).processor._chat
            chat.set_plugin_tools(adapter)
            contract = MainAgentContract(
                chat, ShortState(WorkspaceStore(base / "workspace")), code_enabled=True
            )
            chat.runtime.runner.main_contract = contract
            before = await contract.definitions()
            original_revision = contract.revision
            name = registry.list(kind=ExtensionKind.TOOL)[0].model_name
            tr = ToolRuntime(
                inbound=InboundMessage(
                    message_id="1",
                    bot_user_id="9999",
                    sender=SenderIdentity("1001"),
                    text="inspect",
                    event_type="message",
                    scope_type=ScopeType.PRIVATE,
                ),
                gateway=None,
                allow_generic_onebot=False,
                execution_id="offline-fixture",
                runtime_config=await chat._runtime_config.snapshot(),
            )
            backend = MainAgentBackend(chat, tr)
            await backend.prepare()
            ar = AgentRuntime(
                origin=TurnOrigin.USER_MESSAGE,
                actor_user_id=tr.actor_user_id,
                actor_is_superuser=False,
                delegated_authority=None,
                conversation_key=tr.conversation_key,
                current_group_id=None,
                bot_user_id=tr.inbound.bot_user_id,
                gateway=None,
                runtime_config=tr.runtime_config,
                current_time=chat._time.current_default(),
                allowed_capabilities=frozenset(),
                max_model_requests=tr.runtime_config.agent.max_model_requests,
                max_tool_calls=tr.runtime_config.agent.max_tool_calls,
            )

            async def execute(backend, args, seq):
                call = ToolCall(f"call-{seq}", ToolFunction(name, json.dumps(args)))
                inv = direct_invocations(
                    (call,),
                    ar,
                    chain_id="offline-chain",
                    request_sequence=seq,
                    manifest_revision=contract.revision,
                )[0]
                return json.loads(await backend.execute_call(inv))

            out = {
                "name": name,
                "before": {
                    "parallel": backend.parallel_safe(name, ar),
                    "side_effecting": backend.is_side_effecting(name, '{"value":"fixture"}', ar),
                    "result": await execute(backend, {"value": "fixture"}, 0),
                },
            }
            await manager.disable("test.contract", actor_user_id="9000")
            out["disabled_result"] = await execute(backend, {"value": "fixture"}, 1)
            # Identical approved code survives an unchanged lifecycle restart.
            await manager.enable("test.contract", actor_user_id="9000")
            restarted = await execute(backend, {"value": "after-restart"}, 10)
            assert restarted["ok"], restarted
            assert contract.health()["restart_required"] is False
            # A call already waiting for its authorized scope cannot pick up a
            # newer registration. The Host must check again after that wait.
            entered, release = asyncio.Event(), asyncio.Event()
            original_scope = adapter._invocation_scope

            @asynccontextmanager
            async def waiting_scope(*args, **kwargs):
                entered.set()
                await release.wait()
                async with original_scope(*args, **kwargs) as context:
                    yield context

            adapter._invocation_scope = waiting_scope
            waiting = asyncio.create_task(execute(backend, {"value": "waiting"}, 11))
            await asyncio.wait_for(entered.wait(), 2)
            await manager.disable("test.contract", actor_user_id="9000")
            # Same name/schema; a newly approved implementation changes READ to MUTATE.
            write_plugin(root, 2, "MUTATE", "value")
            await manager.discover()
            await manager.approve("test.contract", actor_user_id="9000")
            await manager.enable("test.contract", actor_user_id="9000")
            release.set()
            waited = await asyncio.wait_for(waiting, 2)
            adapter._invocation_scope = original_scope
            assert waited["ok"] is False and not ctx.effect_file.exists()
            assert contract.health()["restart_required"] is True
            backend.refresh_catalog(ar, web_was_used=False)
            out["after_risk_change"] = {
                "running": manager.running_plugin_ids,
                "parallel": backend.parallel_safe(name, ar),
                "side_effecting": backend.is_side_effecting(name, '{"value":"fixture"}', ar),
                "current_adapter_read_only": adapter.is_read_only(name),
                "result": await execute(backend, {"value": "fixture"}, 2),
            }
            out["after_risk_change"]["fixture_file_committed"] = ctx.effect_file.exists()
            if native_child:
                # Real frozen API -> pinned VM -> typed child -> real Work journal.
                # The new MUTATE implementation must not run under old READ facts.
                env = await environment(db, base / "code-child")
                env.host.api = contract.script_api
                service = InvocationService()

                async def child(invocation, side_effecting):
                    return await service.invoke(
                        invocation,
                        lambda: backend.execute_call(invocation),
                        side_effecting=side_effecting,
                    )

                env.host.execute_business = child
                wrapper = next(key for key, value in env.host.api.wrappers.items() if value == name)
                outer = outer_call(env, f"await {wrapper}({{'value': 'synthetic'}})")
                raw = await CodeModeDriver(env.host, outer).run()
                body = json.loads(raw)
                rows, tools, budget = await effect_rows(db, env.control.current["id"])
                assert tools == budget == 1 and len(rows) == 2
                # A contract mismatch is rejected before the handler starts.
                assert body["operations"][0]["status"] == "not_executed"
                leaf = next(
                    row for row in rows.values() if row["effect_key"] != outer.identity.operation_id
                )
                receipt = json.loads(leaf["receipt_json"])
                refusal = json.loads(receipt["result"])
                assert refusal["error_code"] == "plugin_tool_contract_changed"
                assert refusal["executed"] is False and refusal["mutation_committed"] is False
                assert await CodeModeDriver(env.host, outer).run() == raw
                assert await effect_rows(db, env.control.current["id"]) == (rows, tools, budget)
                assert not ctx.effect_file.exists()
            # Again through supported lifecycle; schema is now different under the same model name.
            await manager.disable("test.contract", actor_user_id="9000")
            write_plugin(root, 3, "READ", "replacement")
            await manager.discover()
            await manager.approve("test.contract", actor_user_id="9000")
            await manager.enable("test.contract", actor_user_id="9000")
            fresh = MainAgentBackend(chat, tr)
            await fresh.prepare()
            after = await contract.definitions()
            out["schema_change"] = {
                "frozen_same": before == after,
                "revision_same": contract.revision == original_revision,
                "frozen_parameters": next(t.parameters for t in after if t.name == name),
                "current_parameters": next(
                    t.parameters
                    for t in adapter.definitions(tr, web_was_used=False)
                    if t.name == name
                ),
                "old_arguments_fresh_backend": await execute(fresh, {"value": "fixture"}, 3),
                "new_arguments_fresh_backend": await execute(fresh, {"replacement": "fixture"}, 4),
                "old_arguments_original_backend": await execute(backend, {"value": "fixture"}, 5),
            }
            out["handler_calls"] = ctx.calls
            assert out["before"]["result"]["ok"]
            assert not out["disabled_result"]["ok"]
            assert out["after_risk_change"]["result"]["ok"] is False
            assert (
                out["after_risk_change"]["result"]["error_code"] == "plugin_tool_contract_changed"
            )
            assert contract.health()["restart_required"] is True
            assert not out["after_risk_change"]["fixture_file_committed"]
            assert out["after_risk_change"]["result"]["mutation_committed"] is False
            assert out["schema_change"]["frozen_same"] and out["schema_change"]["revision_same"]
            assert ctx.calls == [
                {"version": 1, "args": {"value": "fixture"}, "declared_risk": "READ"},
                {"version": 1, "args": {"value": "after-restart"}, "declared_risk": "READ"},
            ]
            assert (
                out["schema_change"]["old_arguments_fresh_backend"]["error_code"]
                == "plugin_tool_contract_changed"
            )
            assert (
                out["schema_change"]["new_arguments_fresh_backend"]["error_code"]
                == "plugin_tool_contract_changed"
            )
            return out
        finally:
            await manager.stop()
            await db.close()


async def test_approved_hot_upgrade_does_not_replace_frozen_binding():
    await run()


@requires_worker
async def test_hot_upgrade_refusal_keeps_native_child_and_work_receipt():
    await run(native_child=True)


@pytest.mark.parametrize(
    ("principal", "kind", "permission", "deadline"),
    [
        ("person", "action", "user", None),
        ("self", "action", "user", None),
        ("self", "tool", "user", None),
        ("person", "action", "user", 601),
        ("person", "action", "user", 0.01),
        ("person", "action", "trusted", None),
        ("person", "action", "moderator", None),
        ("promoted", "action", "user", None),
        ("person", "web", "user", None),
        ("self", "web", "user", None),
    ],
)
async def test_scheduled_native_callback_keeps_original_type_scope_and_deadline(
    database, tmp_path, principal, kind, permission, deadline
):
    from pydantic import BaseModel, ConfigDict, Field, field_validator
    from sqlalchemy import update
    from tests.integration.test_self_automation_delivery import self_actor
    from tests.support.automation_unified_delivery_helpers import setup_run

    from qq_ai_bot.automation.models import AutomationScript, RunStatus
    from qq_ai_bot.automation.service import AutomationService
    from qq_ai_bot.automation.work_cursor import load
    from qq_ai_bot.domain.conversations import ConversationScope
    from qq_ai_bot.domain.messages import ChatResponse, CitationOrigin, ResponseCitation
    from qq_ai_bot.domain.tool_actor import ToolActor
    from qq_ai_bot.identity.db_models import IdentityBindingModel
    from qq_ai_bot.llm.fake import FakeLLMProvider
    from qq_ai_bot.model_runtime.models import ModelProfile
    from qq_ai_bot.persistence.repositories import GroupSettingsRepository
    from qq_ai_bot.plugin_host.automation_adapter import PluginAutomationAdapter
    from qq_ai_bot.plugin_host.manifest import PluginManifest
    from qq_ai_bot.web.bridge_state import BridgeState
    from qq_ai_bot.web.gemini_bridge import GeminiSearchBridge
    from yuki_plugin_sdk.errors import PluginPermissionError
    from yuki_plugin_sdk.models import PermissionLevel
    from yuki_plugin_sdk.models import TurnOrigin as SdkTurnOrigin
    from yuki_plugin_sdk.permissions import PluginPermission
    from yuki_plugin_sdk.registrar import (
        AutomationActionMetadata,
        AutomationActionRegistration,
        ToolMetadata,
        ToolRegistration,
    )

    case = await setup_run(
        database,
        tmp_path,
        delivery="none",
        mode="silent",
        superusers_csv="19002" if principal == "promoted" else None,
    )
    parsed_inputs, parsed_outputs, callbacks = [], [], []
    summary = "original provider summary " * 500 + "SUMMARY_TAIL"
    search_provider = FakeLLMProvider(
        lambda _: ChatResponse(
            summary,
            0,
            citations=tuple(
                ResponseCitation(
                    url=f"https://example.com/{index}",
                    title="original title " * 80,
                    origin=CitationOrigin.ANNOTATION,
                )
                for index in range(7)
            ),
        )
    )
    bridge = GeminiSearchBridge(
        profile=ModelProfile(
            id="actual-sdk-search",
            provider="fake",
            model="fake",
            timeout_seconds=1,
            max_retries=0,
            default_temperature=0.5,
            default_max_output_tokens=32768,
        ),
        credential="synthetic",
        provider=search_provider,
        state=BridgeState(tmp_path / "sdk-search.sqlite3"),
    )

    class Arguments(BaseModel):
        model_config = ConfigDict(extra="forbid")
        value: str = Field(alias="original_value")

        @field_validator("value")
        @classmethod
        def observed(cls, value):
            parsed_inputs.append(value)
            return value

    class Output(BaseModel):
        model_config = ConfigDict(extra="forbid")
        value: str = Field(alias="original_result")

        @field_validator("value")
        @classmethod
        def observed(cls, value):
            parsed_outputs.append(value)
            return value

    manifest = PluginManifest(
        id="test.native-scheduled",
        name="native callback fixture",
        version="1.0.0",
        description="consume one native instance in its original scene",
        entrypoint="fixture:Plugin",
        plugin_api=PLUGIN_API_VERSION,
        yuki_requires=">=3.8",
        permissions=(PluginPermission.AUTOMATION_ACTION_REGISTER, PluginPermission.TOOL_REGISTER),
    )
    host = HostPluginContext(
        plugin_id=manifest.id,
        approved_permissions=(
            PluginPermission.GROUP_READ,
            PluginPermission.WEB_SEARCH,
            PluginPermission.MEMORY_PERSON_READ,
            PluginPermission.ONEBOT_MUTATE,
        ),
        superuser_ids=("19002",),
        services=PluginFacadeServices(
            approval_revision=manifest.manifest_hash,
            groups=GroupSettingsRepository(database),
            memories=case.chat._memories,
            runtime_config=case.chat._runtime_config,
            web_provider=bridge,
        ),
    )

    async def callback(arguments):
        assert isinstance(arguments, Arguments)
        invocation = host._invocation()
        callbacks.append(invocation)
        assert invocation.inbound is None and invocation.source_event_id is None
        assert invocation.actor_user_id == (
            "" if principal == "self" else "19002" if principal == "promoted" else "10001"
        )
        assert invocation.actor_person_id == (None if principal == "self" else case.env.person)
        assert invocation.conversation_id == case.env.context.conversation_id
        await host.groups.get("20001")
        with pytest.raises(PluginPermissionError, match="outside the current real turn"):
            await host.groups.get("20002")
        if principal == "self":
            assert invocation.delegated_authority.principal_kind == "self"
            assert invocation.delegated_authority.permission_level.value == "self"
            with pytest.raises(PluginPermissionError, match="outside the current real turn"):
                await host.memory.list_person("10001")
            with pytest.raises(PluginPermissionError, match="SUPERUSERS authority"):
                await host.onebot.call_mutating_action(
                    "set_group_whole_ban", {"group_id": "20001", "enable": True}
                )
            assert not [
                action for action, _ in case.env.bot.calls if action == "set_group_whole_ban"
            ]
        if principal == "promoted":
            assert invocation.delegated_authority.permission_level.value == "user"
            with pytest.raises(PluginPermissionError, match="SUPERUSERS authority"):
                await host.onebot.call_mutating_action(
                    "set_group_whole_ban", {"group_id": "20001", "enable": True}
                )
            assert not [
                action for action, _ in case.env.bot.calls if action == "set_group_whole_ban"
            ]
        if kind == "web":
            return await host.web.search(arguments.value)
        if principal == "person" and deadline is None:
            await asyncio.sleep(10.05)  # The old implicit ten-second deadline would interrupt it.
        elif deadline == 0.01:
            await asyncio.Event().wait()
        return Output(original_result=arguments.value)

    metadata_type = ToolMetadata if kind == "tool" else AutomationActionMetadata
    metadata = metadata_type(
        name="inspect",
        description="read current group in its original scheduled scene",
        permission=PermissionLevel(permission),
        timeout_seconds=deadline,
        allowed_origins=frozenset({SdkTurnOrigin.SCHEDULED_AUTOMATION}),
    )
    extensions = ExtensionRegistry()
    registrar = extensions.registrar(
        manifest.id,
        manifest.permissions,
        approval_revision=manifest.manifest_hash,
    )
    if kind != "tool":
        registrar.register_automation_action(
            AutomationActionRegistration(
                metadata,
                Arguments,
                Output,
                callback,
            )
        )
    else:
        registrar.register_tool(ToolRegistration(metadata, Arguments, Output, callback))
    adapter = PluginAutomationAdapter(
        extensions=extensions,
        automation=case.executor._registry,
        invocation_scope=lambda plugin_id, invocation: host.bind(invocation),
    )
    assert adapter.activate(manifest) == 1
    event = await case.chat._ledger._writer.append(
        scope=ConversationScope.group("80001", "20001"),
        platform_message_id="scheduled-native-source",
        sender_user_id="10001",
        direction="inbound",
        content="Use the approved native action",
    )
    actor = (
        await self_actor(database, case.env)
        if principal == "self"
        else ToolActor(
            user_id="10001",
            bot_user_id="80001",
            group_id="20001",
            origin=TurnOrigin.USER_MESSAGE,
            instruction="Use the approved native action",
            event_id=event.event.id,
            person_id=case.env.person,
            conversation_id=case.env.context.conversation_id,
        )
    )
    script = AutomationScript.model_validate(
        {
            "version": 1,
            "name": "native scheduled instance",
            "schedule": {"type": "after", "seconds": 1},
            "context": {"scene": "current_group"},
            "steps": [
                {
                    "id": "inspect",
                    "call": f"plugin.{manifest.id}.inspect",
                    "arguments": {"original_value": "original typed value"},
                },
                *(
                    [
                        {
                            "id": "tail",
                            "call": f"plugin.{manifest.id}.inspect",
                            "arguments": {"original_value": "${inspect.provider_summary}"},
                        }
                    ]
                    if kind == "web"
                    else []
                ),
            ],
        }
    )
    service = AutomationService(
        settings=case.executor._settings,
        repository=case.repository,
        registry=case.executor._registry,
        time_service=case.chat._time,
    )
    if permission in {"trusted", "moderator"}:
        with pytest.raises(PermissionError, match="无权委托"):
            await service.create(script, actor=actor, conversation_key="group:80001:20001")
        assert not parsed_inputs and not parsed_outputs and not callbacks
        return
    row = await service.create(script, actor=actor, conversation_key="group:80001:20001")
    accepted = await case.repository.create_run(
        row.id,
        scheduled_for=row.next_run_at,
        actual_started_at=case.clock.now(),
    )
    if principal == "promoted":
        from uuid import uuid4

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
                    external_account_id="19002",
                    status="active",
                    revision=1,
                    first_seen_at=case.clock.now(),
                    last_seen_at=case.clock.now(),
                    created_at=case.clock.now(),
                    updated_at=case.clock.now(),
                )
            )
    try:
        result = await case.executor.execute(row, accepted)
    finally:
        await bridge.close()
    if kind == "web":
        assert result.status is RunStatus.SUCCEEDED, result
        assert result.llm_calls == len(search_provider.requests) == 2
        assert result.tool_calls == len(callbacks) == 2 and not parsed_outputs
        assert parsed_inputs == ["original typed value", summary]
        _, cursor = await load(database, accepted.id, row.script_hash)
        output = cursor["outputs"]["inspect"]
        assert output["provider_summary"] == summary and len(output["sources"]) == 7
        assert "外部不可信总结" in output["provider_summary_instruction"]
        assert all(source["relevant_content"] == "" for source in output["sources"])
        replay = await case.executor.execute(row, accepted)
        assert replay.llm_calls == 2 and replay.tool_calls == 2
        assert len(callbacks) == len(search_provider.requests) == 2
        return
    assert len(callbacks) == 1 and parsed_inputs == ["original typed value"]
    if deadline == 0.01:
        assert result.status is RunStatus.FAILED and result.error_category == "plugin_read_failed"
        assert result.tool_calls == 1 and not parsed_outputs
    else:
        assert result.status is RunStatus.SUCCEEDED, result
        assert parsed_outputs == ["original typed value"]
        assert (await load(database, accepted.id, row.script_hash))[1]["outputs"]["inspect"] == {
            "value": "original typed value"
        }
        replay = await case.executor.execute(row, accepted)
        assert replay.status is RunStatus.SUCCEEDED and replay.tool_calls == 1
        assert len(callbacks) == 1 and parsed_inputs == parsed_outputs == ["original typed value"]
    assert not case.provider.requests
    assert not [action for action, _ in case.env.bot.calls if action.startswith("send_")]


@pytest.mark.parametrize("principal", ["person", "self"])
@pytest.mark.parametrize("unknown", [False, True])
async def test_scheduled_sdk_send_uses_original_social_receipt_and_default_gateway(
    database, tmp_path, monkeypatch, principal, unknown
):
    from pydantic import BaseModel, ConfigDict
    from sqlalchemy import select
    from tests.integration.test_self_automation_delivery import self_actor
    from tests.support.automation_unified_delivery_helpers import setup_run

    from qq_ai_bot.automation.models import AutomationScript, RunStatus
    from qq_ai_bot.automation.service import AutomationService
    from qq_ai_bot.domain.conversations import ConversationScope
    from qq_ai_bot.domain.tool_actor import ToolActor
    from qq_ai_bot.plugin_host.audit import PluginAuditService
    from qq_ai_bot.plugin_host.automation_adapter import PluginAutomationAdapter
    from qq_ai_bot.plugin_host.manifest import PluginManifest
    from qq_ai_bot.plugin_host.repository import PluginAuditRepository
    from qq_ai_bot.social.db_models import SocialOperationModel
    from yuki_plugin_sdk.models import RiskClass
    from yuki_plugin_sdk.models import TurnOrigin as SdkTurnOrigin
    from yuki_plugin_sdk.permissions import PluginPermission
    from yuki_plugin_sdk.registrar import AutomationActionMetadata, AutomationActionRegistration

    case = await setup_run(database, tmp_path, delivery="none", mode="silent")
    text = "EXPLICIT_NATIVE_SEND"
    if principal == "person" and not unknown:
        text += "A" * 13000 + "SDK_BODY_TAIL"

    class Arguments(BaseModel):
        model_config = ConfigDict(extra="forbid")
        text: str

    manifest = PluginManifest(
        id="test.scheduled-send",
        name="scheduled Social receipt",
        version="1.0.0",
        description="send through the existing SDK Social claim",
        entrypoint="fixture:Plugin",
        plugin_api=PLUGIN_API_VERSION,
        yuki_requires=">=3.8",
        permissions=(PluginPermission.AUTOMATION_ACTION_REGISTER,),
    )
    host = HostPluginContext(
        plugin_id=manifest.id,
        approved_permissions=(PluginPermission.MESSAGE_GROUP_SEND,),
        services=PluginFacadeServices(
            approval_revision=manifest.manifest_hash,
            ledger=case.chat._ledger,
            audit=PluginAuditService(PluginAuditRepository(database)),
        ),
    )
    calls = []

    async def send(arguments):
        calls.append(host._invocation())
        return await host.messages.send_group("20001", arguments.text)

    extensions = ExtensionRegistry()
    registrar = extensions.registrar(
        manifest.id,
        manifest.permissions,
        approval_revision=manifest.manifest_hash,
    )
    registrar.register_automation_action(
        AutomationActionRegistration(
            AutomationActionMetadata(
                name="send",
                description="send the explicitly delegated result",
                risk=RiskClass.SEND,
                allowed_origins=frozenset({SdkTurnOrigin.SCHEDULED_AUTOMATION}),
            ),
            Arguments,
            Arguments,
            send,
        )
    )
    adapter = PluginAutomationAdapter(
        extensions=extensions,
        automation=case.executor._registry,
        invocation_scope=lambda plugin_id, invocation: host.bind(invocation),
    )
    assert adapter.activate(manifest) == 1
    event = await case.chat._ledger._writer.append(
        scope=ConversationScope.group("80001", "20001"),
        platform_message_id="native-send-source",
        sender_user_id="10001",
        direction="inbound",
        content="Send through the approved native action",
    )
    actor = (
        await self_actor(database, case.env)
        if principal == "self"
        else ToolActor(
            user_id="10001",
            bot_user_id="80001",
            group_id="20001",
            origin=TurnOrigin.USER_MESSAGE,
            instruction="Send through the approved native action",
            event_id=event.event.id,
            person_id=case.env.person,
            conversation_id=case.env.context.conversation_id,
        )
    )
    script = AutomationScript.model_validate(
        {
            "version": 1,
            "name": "original scheduled Social receipt",
            "schedule": {"type": "after", "seconds": 1},
            "context": {"scene": "current_group"},
            "steps": [
                {
                    "id": "send",
                    "call": f"plugin.{manifest.id}.send",
                    "arguments": {"text": text},
                }
            ],
        }
    )
    service = AutomationService(
        settings=case.executor._settings,
        repository=case.repository,
        registry=case.executor._registry,
        time_service=case.chat._time,
    )
    row = await service.create(script, actor=actor, conversation_key="group:80001:20001")
    run = await case.repository.create_run(
        row.id,
        scheduled_for=row.next_run_at,
        actual_started_at=case.clock.now(),
    )
    if unknown:
        original = case.env.bot.call_api

        async def timeout(action, **params):
            if action == "send_group_msg":
                case.env.bot.calls.append((action, params))
                raise TimeoutError("original transport did not confirm the send")
            return await original(action, **params)

        monkeypatch.setattr(case.env.bot, "call_api", timeout)
    result = await case.executor.execute(row, run)
    assert result.status is (RunStatus.UNCERTAIN if unknown else RunStatus.SUCCEEDED), result
    assert result.llm_calls == 0 and result.tool_calls == 1
    assert result.messages_sent == int(not unknown)
    audit = await PluginAuditRepository(database).history(plugin_id=manifest.id)
    assert len(audit) == 1 and audit[0].success is (not unknown)
    assert audit[0].permission == PluginPermission.MESSAGE_GROUP_SEND.value
    async with database.sessions() as reader:
        receipt = (await reader.scalars(select(SocialOperationModel))).one()
        assert receipt.status == ("uncertain" if unknown else "succeeded")
        assert receipt.source_turn_id.startswith("plugin-send:")
        assert receipt.source_conversation_id == case.env.context.conversation_id
        original_receipt = {
            column.name: getattr(receipt, column.name) for column in receipt.__table__.columns
        }
        assert (receipt.event_id is not None) is (not unknown)
        if principal == "person" and not unknown:
            from qq_ai_bot.automation.work_cursor import load
            from qq_ai_bot.persistence.models import ChatEventModel

            assert case.env.bot.calls[-1] == (
                "send_group_msg",
                {"group_id": "20001", "message": text},
            )
            event = await reader.get(ChatEventModel, receipt.event_id)
            assert event.content == text
            assert event.content.endswith("SDK_BODY_TAIL")
            _, cursor = await load(database, run.id, row.script_hash)
            assert cursor["outputs"]["send"]["operation_id"] == receipt.id
            assert cursor["outputs"]["send"]["status"] == "succeeded"
    replay = await case.executor.execute(row, run)
    assert replay.status is result.status and len(calls) == 1
    assert len([action for action, _ in case.env.bot.calls if action == "send_group_msg"]) == 1
    async with database.sessions() as reader:
        receipt = (await reader.scalars(select(SocialOperationModel))).one()
        assert {
            column.name: getattr(receipt, column.name) for column in receipt.__table__.columns
        } == original_receipt
    assert not case.provider.requests
