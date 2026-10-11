"""Compile model-friendly task intents into the strict automation runtime IR."""

from __future__ import annotations

import json
from typing import Literal

from qq_ai_bot.automation.models import (
    AutomationLimits,
    AutomationScript,
    AutomationStep,
    StrictModel,
)
from qq_ai_bot.automation.task_spec import (
    TaskDelivery as TaskDelivery,
)
from qq_ai_bot.automation.task_spec import (
    TaskSpec as TaskSpec,
)
from qq_ai_bot.automation.task_spec import (
    TaskStrategy as TaskStrategy,
)
from qq_ai_bot.automation.validator import CreationProvenance


class ExecutionPlan(StrictModel):
    """Backend-generated plan; only the contained script is persisted and executed."""

    strategy: Literal["static", "agentic"]
    script: AutomationScript


class AutomationCompiler:
    """Deterministically lower a TaskSpec into the already-audited DSL."""

    def compile(
        self,
        task: TaskSpec,
        provenance: CreationProvenance,
        *,
        default_timezone: str,
    ) -> ExecutionPlan:
        strategy = self._resolve_strategy(task)
        timezone = task.timezone or default_timezone
        delivery = self._resolve_delivery(task.delivery, provenance)
        steps: tuple[AutomationStep, ...]

        if strategy == "static":
            if delivery is None:
                raise ValueError("static 任务必须声明消息投递目标")
            steps = (self._delivery_step(delivery, task.delivery.text or task.goal),)
            limits = AutomationLimits()
        else:
            execute = AutomationStep(
                id="execute",
                call="yuki.agent",
                arguments={
                    "instruction": self._instruction(task, delivery),
                    "context_profile": task.context.scene,
                    "delivery_target": delivery or "none",
                },
                save_as="result",
            )
            steps = (execute,)
            limits = AutomationLimits(agent_budget_managed=True)

        script = AutomationScript(
            version=1,
            name=task.name,
            timezone=timezone,
            schedule=task.trigger,
            context=task.context,
            steps=steps,
            limits=limits,
        )
        return ExecutionPlan(
            strategy=strategy,
            script=script,
        )

    @staticmethod
    def _resolve_strategy(task: TaskSpec) -> str:
        if task.strategy is TaskStrategy.AUTO:
            return "agentic"
        return task.strategy.value

    @staticmethod
    def _resolve_delivery(
        delivery: TaskDelivery,
        provenance: CreationProvenance,
    ) -> Literal["self_private", "current_group"] | None:
        target = delivery.target
        if target == "none":
            return None
        if target == "auto":
            return "current_group" if provenance.current_group_id else "self_private"
        if target == "current_group" and provenance.current_group_id is None:
            raise ValueError("当前消息不是群聊，不能把任务结果投递到 current_group")
        return target

    @staticmethod
    def _delivery_step(
        target: Literal["self_private", "current_group"],
        text: str,
        *,
        step_id: str = "deliver",
    ) -> AutomationStep:
        return AutomationStep(
            id=step_id,
            call="social.send_message",
            arguments=(
                {"text": text}
                if target == "current_group"
                else {"text": text, "target": {"kind": "person", "subject_ref": "current_speaker"}}
            ),
        )

    @staticmethod
    def _instruction(
        task: TaskSpec, delivery: Literal["self_private", "current_group"] | None
    ) -> str:
        payload = {
            "goal": task.goal,
            "constraints": task.constraints,
            "delivery": delivery or "none",
        }
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
