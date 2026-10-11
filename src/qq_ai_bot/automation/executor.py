"""Execute the original DSL cursor and preserve its effect receipts and usage."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from pydantic import BaseModel
from sqlalchemy import select

from qq_ai_bot.automation.authority import (
    AuthorityContext,
    DelegatedAuthority,
    PermissionLevel,
)
from qq_ai_bot.automation.context import AutomationBindError, bind_automation_conversation
from qq_ai_bot.automation.gateway import ProactiveGatewayError
from qq_ai_bot.automation.models import (
    AutomationRecord,
    AutomationRunRecord,
    ExecutionResult,
    RetryPolicy,
    RiskClass,
    RunStatus,
    TurnOrigin,
)
from qq_ai_bot.automation.registry import (
    AutomationCapability,
    AutomationCapabilityRegistry,
    CapabilityExecutionContext,
    CapabilityResult,
)
from qq_ai_bot.automation.repository import AutomationRepository
from qq_ai_bot.automation.templates import TemplateError, resolve_templates
from qq_ai_bot.config import Settings
from qq_ai_bot.control_plane.principal import ControlPrincipal, PrincipalSource
from qq_ai_bot.conversation.canonical_db_models import CanonicalConversationModel
from qq_ai_bot.domain.identity import PersonId, PrincipalId
from qq_ai_bot.identity.db_models import (
    CanonicalPersonModel,
    CanonicalSpaceModel,
    PresenceModel,
    SpaceBindingModel,
)
from qq_ai_bot.model_runtime.dispatch_guard import model_dispatch_guard
from qq_ai_bot.runtime.principal import SELF, PrincipalRef
from qq_ai_bot.runtime.work_budget import WorkBudgetExceeded, charge_automation_run
from qq_ai_bot.time.service import TimeContextService

logger = logging.getLogger(__name__)
_RUN_USAGE_FIELDS = ("steps_completed", "llm_calls", "tool_calls", "messages_sent")


def _canonical_identity(
    record: AutomationRecord,
) -> tuple[str, str | None, str | None, str | None]:
    return (
        record.creator_kind,
        record.canonical_creator_person_id,
        record.canonical_target_person_id,
        record.canonical_target_space_id,
    )


@dataclass(frozen=True, slots=True)
class _ExecutionSnapshot:
    record: AutomationRecord
    allowed: frozenset[str]
    actor_is_superuser: bool
    actor_user_id: str


class AutomationExecutionError(RuntimeError):
    def __init__(
        self,
        category: str,
        *,
        transient: bool = False,
        uncertain: bool = False,
        llm_calls: int = 0,
        tool_calls: int = 0,
        messages_sent: int = 0,
    ) -> None:
        super().__init__(category)
        self.category = category
        self.transient = transient
        self.uncertain = uncertain
        self.llm_calls = llm_calls
        self.tool_calls = tool_calls
        self.messages_sent = messages_sent


class AutomationExecutor:
    """Run one claimed script within its creator authority and resource quotas."""

    def __init__(
        self,
        *,
        settings: Settings,
        registry: AutomationCapabilityRegistry,
        repository: AutomationRepository,
        time_service: TimeContextService,
        gateway_factory: Callable[[CapabilityExecutionContext], object] | None = None,
    ) -> None:
        self._settings = settings
        self._registry = registry
        self._repository = repository
        self._time = time_service
        self._gateway_factory = gateway_factory

    async def execute(
        self,
        automation: AutomationRecord,
        run: AutomationRunRecord,
        *,
        current_group_id: str | None = None,
    ) -> ExecutionResult:
        from qq_ai_bot.automation.work_cursor import load as load_cursor

        if run.status is not RunStatus.RUNNING:
            return ExecutionResult(
                status=run.status,
                **{field: getattr(run, field) for field in _RUN_USAGE_FIELDS},
                error_category=run.error_category,
                summary=run.result_summary,
            )
        original = await self._repository.get_for_run(run.id)
        if original is None:
            return ExecutionResult(
                status=RunStatus.UNCERTAIN,
                error_category="missing_original_run_version",
                **{field: getattr(run, field) for field in _RUN_USAGE_FIELDS},
                summary=run.result_summary,
            )
        automation = original.model_copy(update={"claimed_by": automation.claimed_by})
        phase, cursor = await load_cursor(
            self._repository._database, run.id, automation.script_hash
        )
        usage = {
            field: max(getattr(run, field), int(cursor.get(field, 0)))
            for field in _RUN_USAGE_FIELDS
        }
        if phase == "ready" and cursor.get("next_step") == len(automation.script.steps):
            result = ExecutionResult(
                status=RunStatus.SUCCEEDED,
                **usage,
                summary={"output_steps": list(cursor.get("outputs", {}))},
            )
        else:
            result = await self._execute(
                automation,
                run,
                phase=phase,
                cursor={**cursor, **usage},
                current_group_id=current_group_id,
            )
        # Preflight rejection is still the same run, including when its cursor
        # belongs to an older script. It must not erase committed usage/evidence.
        from qq_ai_bot.runtime.work_budget_schema import automation_budgets

        async with self._repository._database.sessions() as reader:
            models = await reader.scalar(
                select(automation_budgets.c.models).where(automation_budgets.c.run_id == run.id)
            )
        if models is not None:
            usage["llm_calls"] = max(usage["llm_calls"], models)
        return result.model_copy(
            update={
                **{field: max(value, getattr(result, field)) for field, value in usage.items()},
                "summary": {**run.result_summary, **result.summary},
            }
        )

    async def _execute(
        self,
        automation: AutomationRecord,
        run: AutomationRunRecord,
        *,
        phase: str,
        cursor: dict[str, Any],
        current_group_id: str | None,
    ) -> ExecutionResult:
        snapshot = await self._begin_execution(automation, run.id)
        if isinstance(snapshot, ExecutionResult):
            return snapshot
        if snapshot.record.script_hash != automation.script_hash:
            phase = "changed"
        automation = snapshot.record
        allowed = snapshot.allowed
        actor_is_superuser = snapshot.actor_is_superuser
        authority = DelegatedAuthority.model_validate(automation.authority_snapshot).model_copy(
            update={"canonical_creator_person_id": automation.canonical_creator_person_id}
        )
        if current_group_id is None:
            current_group_id = authority.current_group_id
        local = self._time.at(run.actual_started_at, automation.timezone)
        authority_context = AuthorityContext(
            origin=TurnOrigin.SCHEDULED_AUTOMATION,
            actor_user_id=snapshot.actor_user_id,
            actor_person_id=automation.canonical_creator_person_id,
            actor_is_superuser=actor_is_superuser,
            bot_user_id=automation.bot_user_id,
            principal_kind=automation.creator_kind,
            delegated_authority=authority,
            allowed_capabilities=allowed,
        )

        async def revalidate_authority(capability: str | None) -> None:
            fresh = await self._begin_execution(automation, run.id)
            if isinstance(fresh, ExecutionResult):
                raise AutomationExecutionError(
                    fresh.error_category or "delegated_authority_revoked"
                )
            if (
                fresh.record.script_hash != automation.script_hash
                or fresh.record.authority_snapshot != automation.authority_snapshot
            ):
                raise AutomationExecutionError("automation_changed")
            if fresh.actor_user_id != snapshot.actor_user_id:
                raise AutomationExecutionError("actor_identity_changed")
            if fresh.actor_is_superuser != actor_is_superuser:
                raise AutomationExecutionError("actor_permission_changed")
            if capability is not None and (
                capability not in allowed or capability not in fresh.allowed
            ):
                raise AutomationExecutionError("capability_not_delegated")

        builtins: dict[str, Any] = {
            "creator_user_id": snapshot.actor_user_id,
            "bot_user_id": automation.bot_user_id,
            "automation_id": automation.id,
            "automation_run_id": run.id,
            "scheduled_for": run.scheduled_for.isoformat(),
            "actual_started_at": run.actual_started_at.isoformat(),
            "local_time": local.local.isoformat(),
            "current_group_id": current_group_id,
        }
        outputs: dict[str, Any] = {}
        steps_completed = llm_calls = tool_calls = messages_sent = 0
        from qq_ai_bot.automation.work_cursor import save as save_cursor

        if phase == "new":
            # Legacy orphan runs have no proof that dispatch never started. Keep
            # their identity and usage; an empty replacement cursor would authorize replay.
            return ExecutionResult(
                status=RunStatus.UNCERTAIN,
                error_category="missing_initial_run_cursor",
            )
        if phase in {"dispatching", "changed"}:
            return ExecutionResult(
                status=RunStatus.UNCERTAIN, error_category="step_outcome_requires_reconciliation"
            )
        outputs = cursor.get("outputs", {})
        steps_completed = int(cursor.get("steps_completed", 0))
        llm_calls = int(cursor.get("llm_calls", 0))
        tool_calls = int(cursor.get("tool_calls", 0))
        messages_sent = int(cursor.get("messages_sent", 0))
        next_step = int(cursor.get("next_step", 0))
        active_before = float(cursor.get("active_seconds", 0))
        activated_at = time.monotonic()

        async def checkpoint(phase: str, index: int, work_id: str | None = None) -> None:
            await save_cursor(
                self._repository._database,
                run.id,
                automation.script_hash,
                phase,
                {
                    "outputs": outputs,
                    "steps_completed": steps_completed,
                    "llm_calls": llm_calls,
                    "tool_calls": tool_calls,
                    "messages_sent": messages_sent,
                    "next_step": index,
                    "work_id": work_id,
                    "active_seconds": active_before + time.monotonic() - activated_at,
                },
                expected_owner=automation.claimed_by,
            )

        web_was_used = False
        conversation_key = f"automation:{automation.id}"
        conversation_id = None
        conversation_generation = None
        try:
            async with self._repository._database.sessions() as session:
                conversation_key, conversation_id = await bind_automation_conversation(
                    session, automation
                )
                if conversation_id is not None:
                    conversation = await session.get(CanonicalConversationModel, conversation_id)
                    if conversation is None:
                        raise AutomationBindError("conversation_not_found")
                    conversation_generation = conversation.generation
        except AutomationBindError as exc:
            return ExecutionResult(
                status=RunStatus.BLOCKED,
                error_category=exc.category,
                summary={"reason": "canonical conversation hydrate failed"},
            )
        if conversation_id is None and any(
            step.arguments.get("delivery_target") in {"self_private", "current_group"}
            for step in automation.script.steps
            if step.call == "yuki.agent"
        ):
            return ExecutionResult(
                status=RunStatus.BLOCKED, error_category="work_conversation_unavailable"
            )
        dispatched = False
        completed_result: CapabilityResult | None = None
        result_usage_recorded = False
        active_definition: AutomationCapability | None = None
        active_context: CapabilityExecutionContext | None = None
        active_arguments: Any = {}
        effect_evidence: dict[str, Any] = {}

        def mark_dispatch() -> None:
            nonlocal dispatched
            dispatched = True

        async def failure_evidence() -> tuple[bool, int, dict[str, Any]]:
            """Read existing effect facts; never invoke a handler to reconcile."""
            uncertain = (
                dispatched
                and completed_result is None
                and active_definition is not None
                and active_definition.risk_class in {RiskClass.SEND, RiskClass.MUTATE}
            )
            confirmed_messages = completed_result.messages_sent if completed_result else 0
            summary: dict[str, Any] = {}
            if (
                active_context is not None
                and active_definition is not None
                and active_definition.name.startswith("social.")
            ):
                from qq_ai_bot.social.automation import inspect_dispatch_receipt

                try:
                    receipt = await inspect_dispatch_receipt(
                        self._repository._database,
                        active_definition.name,
                        active_arguments,
                        active_context,
                    )
                except Exception as exc:
                    # An unavailable read does not prove a send failed.
                    logger.warning(
                        "automation_effect_reconciliation_deferred category=%s", type(exc).__name__
                    )
                else:
                    if receipt is not None:
                        status, count, operation_id = receipt
                        uncertain = status in {"executing", "uncertain"}
                        confirmed_messages = max(confirmed_messages, count)
                        summary = {"effect_operation_id": operation_id, "effect_status": status}
            return uncertain, 0 if result_usage_recorded else confirmed_messages, summary

        try:
            async with asyncio.timeout(
                None
                if automation.script.limits.timeout_seconds is None
                else max(0, automation.script.limits.timeout_seconds - active_before)
            ):
                for index, step in enumerate(automation.script.steps):
                    if index < next_step:
                        continue
                    dispatched = False
                    completed_result = None
                    result_usage_recorded = False
                    active_definition = None
                    active_context = None
                    active_arguments = {}
                    effect_evidence = {}
                    definition = self._registry.require(step.call)
                    active_definition = definition
                    if step.call not in allowed:
                        raise AutomationExecutionError("capability_not_delegated")
                    resume_plugin = bool(
                        phase == "agent"
                        and index == next_step
                        and cursor.get("work_id")
                        and definition.provider_plugin_id is not None
                    )
                    if not resume_plugin:
                        try:
                            resolved = resolve_templates(
                                step.arguments,
                                builtins=builtins,
                                step_outputs=outputs,
                            )
                            arguments = definition.validate_arguments(resolved)
                            if (
                                isinstance(arguments, BaseModel)
                                and definition.provider_plugin_id is None
                            ):
                                arguments = arguments.model_dump()
                        except (TemplateError, ValueError) as exc:
                            raise AutomationExecutionError("runtime_argument_validation") from exc
                    context = CapabilityExecutionContext(
                        authority=authority_context,
                        automation_id=automation.id,
                        automation_run_id=run.id,
                        step_id=step.id,
                        creator_user_id=automation.creator_user_id,
                        creator_kind=automation.creator_kind,
                        bot_user_id=automation.bot_user_id,
                        current_group_id=current_group_id,
                        scheduled_for=run.scheduled_for,
                        actual_started_at=run.actual_started_at,
                        local_time=local.local,
                        timezone=automation.timezone,
                        automation_context=automation.script.context,
                        conversation_key=conversation_key,
                        web_was_used=web_was_used,
                        canonical_creator_person_id=automation.canonical_creator_person_id,
                        canonical_target_person_id=automation.canonical_target_person_id,
                        canonical_target_space_id=automation.canonical_target_space_id,
                        canonical_presence_id=automation.canonical_presence_id,
                        canonical_conversation_id=conversation_id,
                        conversation_generation=conversation_generation,
                        automation_script_hash=automation.script_hash,
                        source_step_id=step.id,
                        revalidate_authority=revalidate_authority,
                    )
                    if self._gateway_factory is not None:
                        context = replace(
                            context,
                            gateway=self._gateway_factory(context),
                        )
                    started = self._time.clock.now()
                    active_context = context
                    active_arguments = arguments if not resume_plugin else {}
                    await checkpoint(
                        "agent" if resume_plugin or step.call in {"yuki.agent"} else "dispatching",
                        index,
                        str(cursor["work_id"]) if resume_plugin else None,
                    )
                    try:
                        if (
                            not resume_plugin
                            and automation.script.uses_runtime_budget
                            and step.call
                            not in {
                                "yuki.agent",
                            }
                        ):
                            from qq_ai_bot.runtime.work_budget import (
                                WorkBudgetExceeded,
                                charge_automation_run,
                            )

                            try:
                                async with (
                                    self._repository._database.immediate_session() as budget_session
                                ):
                                    await charge_automation_run(
                                        budget_session, run.id, models=0, tools=1
                                    )
                            except WorkBudgetExceeded as exc:
                                raise AutomationExecutionError("agent_work_blocked") from exc
                        if resume_plugin:
                            from qq_ai_bot.plugin_host.automation_adapter import (
                                resume_plugin_result,
                            )

                            result = await resume_plugin_result(
                                self._repository._database,
                                str(cursor["work_id"]),
                                definition,
                                context,
                            )
                        else:
                            result = await self._execute_capability(
                                definition, arguments, context, on_dispatch=mark_dispatch
                            )
                        completed_result = result
                    except AutomationExecutionError as exc:
                        if exc.uncertain and definition.name.startswith("social."):
                            unknown, confirmed, effect_evidence = await failure_evidence()
                            exc.uncertain = unknown
                            exc.messages_sent = max(exc.messages_sent, confirmed)
                        llm_calls += exc.llm_calls
                        tool_calls += exc.tool_calls
                        messages_sent += exc.messages_sent
                        result_usage_recorded = True
                        finished = self._time.clock.now()
                        step_status = (
                            "cancelled"
                            if exc.category == "agent_work_cancelled"
                            else "uncertain"
                            if exc.uncertain
                            else "failed"
                        )
                        step_error = None if step_status == "cancelled" else exc.category
                        self._log_step(
                            automation,
                            run,
                            capability=step.call,
                            step_id=step.id,
                            started=started,
                            finished=finished,
                            status=step_status,
                            error_category=step_error,
                        )
                        raise
                    finished = self._time.clock.now()
                    if result.pending_work_id is not None:
                        llm_calls += result.llm_calls
                        tool_calls += result.tool_calls
                        messages_sent += result.messages_sent
                        result_usage_recorded = True
                        self._enforce_runtime_limits(
                            automation,
                            llm_calls=llm_calls,
                            tool_calls=tool_calls,
                            messages_sent=messages_sent,
                        )
                        await checkpoint("agent", index, result.pending_work_id)
                        return ExecutionResult(
                            status=RunStatus.RUNNING,
                            steps_completed=steps_completed,
                            llm_calls=llm_calls,
                            tool_calls=tool_calls,
                            messages_sent=messages_sent,
                            summary={"pending_work_id": result.pending_work_id},
                        )
                    self._log_step(
                        automation,
                        run,
                        capability=step.call,
                        step_id=step.id,
                        started=started,
                        finished=finished,
                        status="succeeded",
                        error_category=None,
                    )
                    outputs[step.id] = result.data
                    if step.save_as:
                        outputs[step.save_as] = result.data
                    steps_completed += 1
                    llm_calls += result.llm_calls
                    tool_calls += result.tool_calls
                    messages_sent += result.messages_sent
                    result_usage_recorded = True
                    web_was_used = web_was_used or step.call in {"web.search", "web.read_page"}
                    self._enforce_runtime_limits(
                        automation,
                        llm_calls=llm_calls,
                        tool_calls=tool_calls,
                        messages_sent=messages_sent,
                    )
                    await checkpoint("ready", index + 1)
        except TimeoutError:
            uncertain, confirmed, evidence = await failure_evidence()
            return ExecutionResult(
                status=RunStatus.UNCERTAIN if uncertain else RunStatus.FAILED,
                steps_completed=steps_completed,
                llm_calls=llm_calls
                + (
                    completed_result.llm_calls
                    if completed_result and not result_usage_recorded
                    else 0
                ),
                tool_calls=tool_calls
                + (
                    completed_result.tool_calls
                    if completed_result and not result_usage_recorded
                    else 0
                ),
                messages_sent=messages_sent + confirmed,
                error_category="runtime_timeout",
                summary=evidence,
            )
        except AutomationExecutionError as exc:
            if exc.category == "agent_work_cancelled":
                return ExecutionResult(
                    status=RunStatus.CANCELLED,
                    steps_completed=steps_completed,
                    llm_calls=llm_calls,
                    tool_calls=tool_calls,
                    messages_sent=messages_sent,
                )
            if exc.category == "conversation_activation_busy":
                return ExecutionResult(
                    status=RunStatus.RUNNING,
                    steps_completed=steps_completed,
                    llm_calls=llm_calls,
                    tool_calls=tool_calls,
                    messages_sent=messages_sent,
                    error_category=exc.category,
                )
            if exc.category in {
                "paused",
                "ambiguous",
                "none",
                "disconnected",
                "no_connection",
                "capability",
                "bot_unavailable",
                "state_mismatch",
                "target_missing",
                "operation_unavailable",
                "agent_work_blocked",
            }:
                return ExecutionResult(
                    status=RunStatus.BLOCKED,
                    steps_completed=steps_completed,
                    llm_calls=llm_calls,
                    tool_calls=tool_calls,
                    messages_sent=messages_sent,
                    error_category=exc.category,
                )
            return ExecutionResult(
                status=RunStatus.UNCERTAIN if exc.uncertain else RunStatus.FAILED,
                steps_completed=steps_completed,
                llm_calls=llm_calls,
                tool_calls=tool_calls,
                messages_sent=messages_sent,
                error_category=exc.category,
                summary=effect_evidence,
            )
        except Exception as exc:
            uncertain, confirmed, evidence = await failure_evidence()
            logger.error("automation_step_failed category=%s", type(exc).__name__)
            return ExecutionResult(
                status=RunStatus.UNCERTAIN if uncertain else RunStatus.FAILED,
                steps_completed=steps_completed,
                llm_calls=llm_calls
                + (
                    completed_result.llm_calls
                    if completed_result and not result_usage_recorded
                    else 0
                ),
                tool_calls=tool_calls
                + (
                    completed_result.tool_calls
                    if completed_result and not result_usage_recorded
                    else 0
                ),
                messages_sent=messages_sent + confirmed,
                error_category="step_recording_failed",
                summary=evidence,
            )
        return ExecutionResult(
            status=RunStatus.SUCCEEDED,
            steps_completed=steps_completed,
            llm_calls=llm_calls,
            tool_calls=tool_calls,
            messages_sent=messages_sent,
            summary={"output_steps": list(outputs)},
        )

    async def _begin_execution(
        self, claimed: AutomationRecord, run_id: int
    ) -> _ExecutionSnapshot | ExecutionResult:
        async with self._repository._database.sessions() as session:
            current = await self._repository.get_for_run(run_id, session=session)
            if current is None:
                return ExecutionResult(
                    status=RunStatus.BLOCKED,
                    error_category="automation_inactive",
                    summary={"reason": "automation row is missing"},
                )
            if current.claimed_by != claimed.claimed_by:
                return ExecutionResult(
                    status=RunStatus.BLOCKED,
                    error_category="automation_lease_lost",
                    summary={"reason": "claimed execution owner is stale"},
                )
            if _canonical_identity(current) != _canonical_identity(claimed):
                return ExecutionResult(
                    status=RunStatus.BLOCKED,
                    error_category="state_mismatch",
                    summary={"reason": "claimed canonical identity is stale"},
                )
            blocked = await self._validate_canonical_identity(session, current)
            if blocked is not None:
                return blocked
            loaded = await self._canonical_creator_principal(session, current)
            if isinstance(loaded, ExecutionResult):
                return loaded
            principal, current_permission, execution_user_id = loaded
            original_authority = DelegatedAuthority.model_validate(current.authority_snapshot)
            if (
                original_authority.permission_level is not PermissionLevel.SUPERUSER
                and current_permission is PermissionLevel.SUPERUSER
            ):
                current_permission = PermissionLevel.USER
            if current.creator_kind != "self" and (
                not isinstance(principal, ControlPrincipal)
                or not principal.authenticated
                or not principal.active
            ):
                return ExecutionResult(
                    status=RunStatus.BLOCKED,
                    error_category="delegated_authority_revoked",
                    summary={"reason": "control principal is no longer active"},
                )
            allowed = frozenset(
                item.name
                for item in self._registry.list()
                if item.permits(current_permission)
                and item.name in original_authority.granted_capabilities
            )
            return _ExecutionSnapshot(
                record=current,
                allowed=allowed,
                actor_is_superuser=current_permission is PermissionLevel.SUPERUSER,
                actor_user_id=execution_user_id,
            )

    async def _validate_canonical_identity(
        self, session: Any, automation: AutomationRecord
    ) -> ExecutionResult | None:
        person_id = automation.canonical_target_person_id
        space_id = automation.canonical_target_space_id
        if person_id and space_id:
            return ExecutionResult(
                status=RunStatus.BLOCKED,
                error_category="state_mismatch",
                summary={"reason": "canonical target is not XOR"},
            )
        if not person_id and not space_id:
            return ExecutionResult(
                status=RunStatus.BLOCKED,
                error_category="target_missing",
                summary={"reason": "automation has no canonical target"},
            )
        if person_id:
            person = await session.get(CanonicalPersonModel, person_id)
            if person is None or not person.enabled:
                return ExecutionResult(
                    status=RunStatus.BLOCKED,
                    error_category="target_disabled",
                    summary={"reason": "canonical person is missing or disabled"},
                )
        else:
            space = await session.get(CanonicalSpaceModel, space_id or "")
            if space is None or not space.enabled:
                return ExecutionResult(
                    status=RunStatus.BLOCKED,
                    error_category="target_disabled",
                    summary={"reason": "canonical space is missing or disabled"},
                )
        return None

    async def _canonical_creator_principal(
        self, session: Any, automation: AutomationRecord
    ) -> tuple[ControlPrincipal | PrincipalRef, PermissionLevel, str] | ExecutionResult:
        if automation.creator_kind == "self":
            scene = automation.authority_snapshot
            if (
                automation.canonical_creator_person_id is not None
                or automation.creator_user_id
                or not automation.canonical_target_space_id
                or automation.canonical_target_person_id is not None
                or automation.authority_snapshot.get("principal_kind") != "self"
                or scene.get("canonical_space_id") != automation.canonical_target_space_id
                or scene.get("canonical_presence_id") != automation.canonical_presence_id
            ):
                return ExecutionResult(status=RunStatus.BLOCKED, error_category="state_mismatch")
            conversation = await session.get(
                CanonicalConversationModel, scene.get("canonical_conversation_id")
            )
            presence = await session.get(PresenceModel, automation.canonical_presence_id)
            binding = await session.scalar(
                select(SpaceBindingModel).where(
                    SpaceBindingModel.space_id == automation.canonical_target_space_id,
                    SpaceBindingModel.platform == "qq",
                    SpaceBindingModel.status == "active",
                    SpaceBindingModel.external_space_id == scene.get("current_group_id"),
                )
            )
            if (
                conversation is None
                or conversation.space_id != automation.canonical_target_space_id
                or conversation.generation != scene.get("conversation_generation")
                or presence is None
                or not presence.enabled
                or presence.platform != "qq"
                or presence.external_account_id != automation.bot_user_id
                or binding is None
            ):
                return ExecutionResult(
                    status=RunStatus.BLOCKED, error_category="self_scene_changed"
                )
            return SELF, PermissionLevel.SELF, ""
        creator_id = automation.canonical_creator_person_id
        if not creator_id:
            return ExecutionResult(
                status=RunStatus.BLOCKED,
                error_category="target_missing",
                summary={"reason": "automation has no canonical creator"},
            )
        person = await session.get(CanonicalPersonModel, creator_id)
        if person is None or not person.enabled:
            return ExecutionResult(
                status=RunStatus.BLOCKED,
                error_category="delegated_authority_revoked",
                summary={"reason": "canonical creator is missing or disabled"},
            )
        from qq_ai_bot.automation.control_context import resolve_execution_identity

        try:
            execution_user_id, current_permission = await resolve_execution_identity(
                session, self._settings, owner_id=creator_id
            )
        except (PermissionError, ValueError) as exc:
            return ExecutionResult(status=RunStatus.BLOCKED, error_category=str(exc))
        person_id = PersonId.parse(creator_id)
        principal = ControlPrincipal(
            principal_id=PrincipalId.parse(person_id.text),
            person_id=person_id,
            source=PrincipalSource.QQ,
            roles=("superuser",) if current_permission is PermissionLevel.SUPERUSER else ("user",),
            granted_capabilities=(),
            authenticated=True,
            active=True,
        )
        return principal, current_permission, execution_user_id

    async def _execute_capability(
        self,
        definition: AutomationCapability,
        arguments: BaseModel | dict[str, Any],
        context: CapabilityExecutionContext,
        *,
        on_dispatch: Callable[[], None],
    ) -> CapabilityResult:
        if definition.handler is None:
            raise AutomationExecutionError("capability_handler_unavailable")
        attempts = (
            2
            if definition.retry_policy is RetryPolicy.TRANSIENT_ONCE
            and definition.risk_class is RiskClass.READ
            and definition.name not in {"yuki.agent"}
            else 1
        )
        admitted_models = 0

        async def admit_native_request() -> None:
            nonlocal admitted_models
            if context.revalidate_authority is not None:
                await context.revalidate_authority(definition.name)
            async with self._repository._database.immediate_session() as writer:
                await charge_automation_run(writer, context.automation_run_id, models=1, tools=0)
            admitted_models += 1

        for attempt in range(attempts):
            dispatched = False
            try:
                if context.revalidate_authority is not None:
                    await context.revalidate_authority(definition.name)
                dispatched = True
                on_dispatch()
                if definition.name == "yuki.agent":
                    return await definition.handler(arguments, context)
                with model_dispatch_guard(admit_native_request):
                    result = await definition.handler(arguments, context)
                return replace(result, llm_calls=max(result.llm_calls, admitted_models))
            except ProactiveGatewayError as exc:
                raise AutomationExecutionError(
                    exc.category, uncertain=exc.uncertain, llm_calls=admitted_models
                ) from exc
            except AutomationExecutionError as exc:
                exc.llm_calls = max(exc.llm_calls, admitted_models)
                if not exc.transient or attempt + 1 >= attempts:
                    raise
            except WorkBudgetExceeded as exc:
                raise AutomationExecutionError(str(exc), llm_calls=admitted_models) from exc
            except Exception as exc:
                from qq_ai_bot.runtime.work_repository import WorkConflict

                if isinstance(exc, WorkConflict) and str(exc) == "conversation_activation_busy":
                    raise AutomationExecutionError("conversation_activation_busy") from exc
                logger.error(
                    "automation_capability_failed capability=%s category=%s",
                    definition.name,
                    type(exc).__name__,
                )
                raise AutomationExecutionError(
                    "capability_execution_failed",
                    llm_calls=admitted_models,
                    uncertain=dispatched
                    and definition.risk_class in {RiskClass.SEND, RiskClass.MUTATE},
                ) from exc
        raise AutomationExecutionError("capability_failed")

    @staticmethod
    def _enforce_runtime_limits(
        automation: AutomationRecord,
        *,
        llm_calls: int,
        tool_calls: int,
        messages_sent: int,
    ) -> None:
        limits = automation.script.limits
        if limits.max_messages is not None and messages_sent > limits.max_messages:
            raise AutomationExecutionError("message_limit_exceeded")
        if automation.script.uses_runtime_budget:
            return
        if limits.max_llm_calls is not None and llm_calls > limits.max_llm_calls:
            raise AutomationExecutionError("llm_limit_exceeded")
        if limits.max_tool_calls is not None and tool_calls > limits.max_tool_calls:
            raise AutomationExecutionError("tool_limit_exceeded")

    @staticmethod
    def _log_step(
        automation: AutomationRecord,
        run: AutomationRunRecord,
        *,
        capability: str,
        step_id: str,
        started: datetime,
        finished: datetime,
        status: str,
        error_category: str | None,
    ) -> None:
        logger.info(
            "automation_step_finished automation_id=%d run_id=%d creator_user_id=%s "
            "bot_user_id=%s schedule_type=%s capability=%s step_id=%s duration_seconds=%.3f "
            "status=%s error_category=%s",
            automation.id,
            run.id,
            automation.creator_user_id,
            automation.bot_user_id,
            automation.script.schedule.type,
            capability,
            step_id,
            max(0.0, (finished - started).total_seconds()),
            status,
            error_category,
        )
