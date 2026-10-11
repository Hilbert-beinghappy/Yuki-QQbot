"""Project original control requests and existing domain runs without executing them."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qq_ai_bot.control_plane.operations import (
    OperationKind,
    OperationRef,
    OperationStatus,
    StateEpoch,
    _sanitize_error_category,
)
from qq_ai_bot.control_plane.problems import Problem, ProblemCode
from qq_ai_bot.control_plane.query_types import ControlQueryError
from qq_ai_bot.conversation.canonical_db_models import ControlCommandReceiptModel
from qq_ai_bot.domain.identity import PrincipalId, RequestId
from qq_ai_bot.memory.dream.db_models import MemoryDreamRunModel
from qq_ai_bot.memory.dream.models import DreamRun
from qq_ai_bot.memory.rebuild.models import MemoryRebuildRun
from qq_ai_bot.persistence.database import Database
from qq_ai_bot.persistence.models import MemoryRebuildRunModel


def _op_ref(
    operation_id: str,
    status: OperationStatus,
    *,
    created_at: datetime,
    updated_at: datetime,
    error_category: str | None = None,
) -> OperationRef:
    if status is OperationStatus.FAILED:
        try:
            error_category = _sanitize_error_category(error_category)
        except (TypeError, ValueError):
            error_category = "operation_failed"
    else:
        # Earlier diagnostics remain in the domain record, not the current outcome.
        error_category = None
    return OperationRef(
        operation_id=operation_id,
        status=status,
        progress=1.0
        if status in {OperationStatus.SUCCEEDED, OperationStatus.FAILED, OperationStatus.CANCELLED}
        else None,
        state_epoch=StateEpoch.V2,
        error_category=error_category,
        created_at=created_at,
        updated_at=updated_at,
    )


def _rebuild_status(value: str) -> OperationStatus:
    mapping = {
        "planned": OperationStatus.QUEUED,
        "extracting": OperationStatus.RUNNING,
        "extraction_paused": OperationStatus.BLOCKED,
        "review": OperationStatus.WAITING,
        "committing": OperationStatus.RUNNING,
        "commit_paused": OperationStatus.BLOCKED,
        "completed": OperationStatus.SUCCEEDED,
        "cancelled": OperationStatus.CANCELLED,
        "failed": OperationStatus.FAILED,
    }
    return mapping[value]


def _dream_status(value: str) -> OperationStatus:
    mapping = {
        "planned": OperationStatus.QUEUED,
        "running": OperationStatus.RUNNING,
        "partial_failed": OperationStatus.FAILED,
        "completed": OperationStatus.SUCCEEDED,
        "cancelled": OperationStatus.CANCELLED,
        "rolling_back": OperationStatus.RUNNING,
        "rolled_back": OperationStatus.CANCELLED,
    }
    return mapping[value]


def rebuild_operation(run: MemoryRebuildRun) -> OperationRef:
    return run_operation(
        OperationKind.REBUILD,
        run.public_id,
        run.status.value,
        created_at=run.created_at,
        updated_at=run.updated_at,
        error_category=run.error_category,
    )


def dream_operation(run: DreamRun) -> OperationRef:
    return run_operation(
        OperationKind.DREAM,
        run.public_id,
        run.status.value,
        created_at=run.created_at,
        updated_at=run.updated_at,
        error_category=run.error_category,
    )


def run_operation(
    kind: OperationKind,
    public_id: str,
    status: str,
    *,
    created_at: datetime,
    updated_at: datetime,
    error_category: str | None,
) -> OperationRef:
    return _op_ref(
        f"{kind.value}:{public_id}",
        _rebuild_status(status) if kind is OperationKind.REBUILD else _dream_status(status),
        created_at=created_at.replace(tzinfo=UTC)
        if created_at.tzinfo is None
        else created_at.astimezone(UTC),
        updated_at=updated_at.replace(tzinfo=UTC)
        if updated_at.tzinfo is None
        else updated_at.astimezone(UTC),
        error_category=error_category
        or ("rebuild_failed" if kind is OperationKind.REBUILD else "partial_failed"),
    )


async def read_operation(
    database: Database, session: AsyncSession, operation_id: str
) -> OperationRef:
    del database
    parts = operation_id.split(":") if type(operation_id) is str else []
    try:
        if len(parts) == 3 and parts[0] == "control":
            principal = PrincipalId.parse(parts[1])
            request = RequestId.parse(parts[2])
            row = await session.scalar(
                select(ControlCommandReceiptModel).where(
                    ControlCommandReceiptModel.principal_id == principal.text,
                    ControlCommandReceiptModel.request_id == request.text,
                )
            )
            if row is not None:
                from qq_ai_bot.persistence.control_external import control_operation

                return control_operation(row)
        elif len(parts) == 2 and parts[0] in {"rebuild", "dream"}:
            public_id = RequestId.parse(parts[1]).text
            kind = OperationKind(parts[0])
            model = MemoryRebuildRunModel if kind is OperationKind.REBUILD else MemoryDreamRunModel
            run = (
                await session.execute(
                    select(
                        model.public_id,
                        model.status,
                        model.created_at,
                        model.updated_at,
                        model.error_category,
                    ).where(model.public_id == public_id)
                )
            ).one_or_none()
            if run is not None:
                return run_operation(
                    kind,
                    run.public_id,
                    run.status,
                    created_at=run.created_at,
                    updated_at=run.updated_at,
                    error_category=run.error_category,
                )
        else:
            raise ValueError("invalid operation ID")
    except (TypeError, ValueError) as exc:
        raise ControlQueryError(Problem(ProblemCode.VALIDATION_ERROR)) from exc
    raise ControlQueryError(Problem(ProblemCode.NOT_FOUND))
