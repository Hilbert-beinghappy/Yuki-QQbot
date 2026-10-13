"""Truncated structured responses never purchase a validation repair request."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel

from qq_ai_bot.domain.messages import ChatResponse, ModelResponseStatus, ToolCall, ToolFunction
from qq_ai_bot.model_runtime.models import ModelTask, StructuredOutputMode
from qq_ai_bot.model_runtime.structured import StructuredTaskError, StructuredTaskRunner


class Result(BaseModel):
    value: str


@pytest.mark.parametrize("mode", list(StructuredOutputMode))
@pytest.mark.parametrize("payload", ['{"value":"complete"}', '{"value":'])
async def test_incomplete_structured_output_does_not_retry(mode, payload):
    response = ChatResponse(
        payload,
        0,
        status=ModelResponseStatus.INCOMPLETE,
        tool_calls=(ToolCall("emit", ToolFunction("emit_result", payload)),)
        if mode is StructuredOutputMode.FUNCTION_TOOL
        else (),
    )
    models = SimpleNamespace(
        model_name=lambda task: "fake", execute=AsyncMock(return_value=response)
    )
    with pytest.raises(StructuredTaskError) as failed:
        await StructuredTaskRunner(models).run(
            task=ModelTask.UTILITY_STRUCTURED,
            instruction="Return the requested result.",
            structured_input={"value": "requested"},
            output_model=Result,
            mode=mode,
            validation_retries=1,
        )
    assert failed.value.reason_code == "output_budget_exhausted"
    assert failed.value.response is response
    assert failed.value.attempts == 1
    models.execute.assert_awaited_once()
