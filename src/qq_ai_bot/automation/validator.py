"""Whole-script validation, provenance checks, and stable hashing."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from qq_ai_bot.automation.authority import PermissionLevel
from qq_ai_bot.automation.models import AutomationScript
from qq_ai_bot.automation.registry import AutomationCapabilityRegistry
from qq_ai_bot.automation.templates import TemplateError, referenced_steps, validate_templates
from qq_ai_bot.time.service import validate_timezone

_LLM_CAPABILITIES = frozenset({"yuki.agent"})
_MESSAGE_CAPABILITIES = frozenset({"social.send_message"})


@dataclass(frozen=True, slots=True)
class CreationProvenance:
    creator_user_id: str
    bot_user_id: str
    current_group_id: str | None
    permission: PermissionLevel


@dataclass(frozen=True, slots=True)
class ValidatedAutomation:
    script: AutomationScript
    script_hash: str
    required_capabilities: tuple[str, ...]


class AutomationValidator:
    """Validate every executable field against immutable backend context."""

    def __init__(
        self,
        *,
        registry: AutomationCapabilityRegistry,
    ) -> None:
        self._registry = registry

    def validate(
        self,
        script: AutomationScript,
        provenance: CreationProvenance,
    ) -> ValidatedAutomation:
        script = script.model_copy(update={"timezone": validate_timezone(script.timezone)})
        schedule_timezone = getattr(script.schedule, "timezone", None)
        if schedule_timezone:
            script = script.model_copy(
                update={
                    "schedule": script.schedule.model_copy(
                        update={"timezone": validate_timezone(schedule_timezone)}
                    )
                }
            )
        if script.context.scene == "current_group" and provenance.current_group_id is None:
            raise ValueError("当前消息不是群聊，不能声明 current_group 上下文")
        if provenance.permission is PermissionLevel.SELF:
            if script.context.scene not in {"none", "current_group"}:
                raise PermissionError("SELF 自动化不能使用私人上下文")
        available_steps: set[str] = set()
        required: list[str] = []
        llm_calls = 0
        tool_calls = 0
        messages = 0
        for step in script.steps:
            definition = self._registry.require(step.call)
            if not definition.permits(provenance.permission):
                raise PermissionError(f"当前用户无权委托 capability：{step.call}")
            try:
                validate_templates(step.arguments)
            except TemplateError as exc:
                raise ValueError(str(exc)) from exc
            references = referenced_steps(step.arguments)
            missing = references - available_steps
            if missing:
                raise ValueError(f"步骤 {step.id} 引用了尚未执行的步骤：{sorted(missing)}")
            available_steps.add(step.id)
            if step.save_as:
                available_steps.add(step.save_as)
            if step.call not in required:
                required.append(step.call)
            llm_calls += int(step.call in _LLM_CAPABILITIES)
            tool_calls += 1
            messages += int(step.call in _MESSAGE_CAPABILITIES)
        if (
            not script.uses_runtime_budget
            and script.limits.max_llm_calls is not None
            and llm_calls > script.limits.max_llm_calls
        ):
            raise ValueError("脚本中的 LLM 调用数超过 limits.max_llm_calls")
        if (
            not script.uses_runtime_budget
            and script.limits.max_messages is not None
            and messages > script.limits.max_messages
        ):
            raise ValueError("脚本中的消息发送数超过 limits.max_messages")
        if (
            not script.uses_runtime_budget
            and script.limits.max_tool_calls is not None
            and tool_calls > script.limits.max_tool_calls
        ):
            raise ValueError("脚本可能使用的工具次数超过 limits.max_tool_calls")
        return ValidatedAutomation(
            script=script,
            script_hash=canonical_script_hash(script),
            required_capabilities=tuple(required),
        )


@dataclass(frozen=True, slots=True)
class CollectedSendTargets:
    kind: str | None
    raw_ids: tuple[str, ...]


def collect_send_targets(
    script: AutomationScript,
    *,
    creator_user_id: str,
    current_group_id: str | None,
) -> CollectedSendTargets:
    """Collect unique send owners. Mixed Person/Space or untrusted targets fail closed."""

    person_ids: list[str] = []
    space_ids: list[str] = []
    for step in script.steps:
        _collect_step_send_targets(
            step.call,
            step.arguments,
            person_ids,
            space_ids,
            creator_user_id=creator_user_id,
            current_group_id=current_group_id,
        )
    if person_ids and space_ids:
        raise ValueError("一条自动化不能同时发送给不同的永久主体类型")
    if not person_ids and not space_ids:
        return CollectedSendTargets(kind=None, raw_ids=())
    if person_ids:
        return CollectedSendTargets(kind="person", raw_ids=tuple(dict.fromkeys(person_ids)))
    return CollectedSendTargets(kind="space", raw_ids=tuple(dict.fromkeys(space_ids)))


def _collect_step_send_targets(
    call: str,
    arguments: dict[str, Any],
    person_ids: list[str],
    space_ids: list[str],
    *,
    creator_user_id: str,
    current_group_id: str | None,
) -> None:
    if call == "social.send_message":
        target = arguments.get("target")
        if target is None:
            if current_group_id:
                space_ids.append(current_group_id)
            else:
                person_ids.append(creator_user_id)
        elif target == {"kind": "person", "subject_ref": "current_speaker"}:
            person_ids.append(creator_user_id)
        else:
            raise ValueError("静态消息只能投递到当前群或创建者本人")
        return


def canonical_script_hash(script: AutomationScript) -> str:
    data = script.model_dump(mode="json", exclude_none=True)
    data["timezone"] = validate_timezone(data["timezone"])
    if data["schedule"].get("timezone"):
        data["schedule"]["timezone"] = validate_timezone(data["schedule"]["timezone"])
    if not script.limits.agent_budget_managed:
        data["limits"].pop("agent_budget_managed", None)
    payload = json.dumps(
        data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
