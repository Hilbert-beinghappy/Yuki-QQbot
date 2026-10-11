"""Deterministic Memory V2 retrieval target resolution."""

from __future__ import annotations

from pydantic import ValidationError
from sqlalchemy import text

from qq_ai_bot.domain.conversations import ScopeType
from qq_ai_bot.domain.messages import InboundMessage
from qq_ai_bot.memory.enums import (
    MemoryScopeType,
    MemoryTargetRole,
    SelfMemoryVisibility,
)
from qq_ai_bot.memory.errors import MemoryRetrievalError
from qq_ai_bot.memory.models import MemoryEntityTarget
from qq_ai_bot.memory.read_scope import MemoryReadScopeResolver
from qq_ai_bot.persistence.repositories import PeopleRepository


class MemoryTargetResolver:
    """Resolve only backend-authenticated current, mention, and reply identities."""

    def __init__(self, people: PeopleRepository) -> None:
        self._people = people
        self._read_scopes = MemoryReadScopeResolver(people.database)

    async def resolve(
        self,
        inbound: InboundMessage,
        *,
        include_self: bool = False,
    ) -> tuple[MemoryEntityTarget, ...]:
        user_id = inbound.sender.user_id
        try:
            targets = []
            if include_self:
                is_private = inbound.scope_type is ScopeType.PRIVATE or inbound.group_id is None
                targets.append(
                    MemoryEntityTarget(
                        role=MemoryTargetRole.CURRENT_SELF,
                        scope_type=MemoryScopeType.SELF,
                        visibility_type=(
                            SelfMemoryVisibility.PRIVATE
                            if is_private
                            else SelfMemoryVisibility.GROUP
                        ),
                        visibility_user_id=user_id if is_private else None,
                        visibility_group_id=None if is_private else inbound.group_id,
                        block_id="current_self",
                    )
                )
            is_group = inbound.scope_type is ScopeType.GROUP and inbound.group_id is not None
            group_id = inbound.group_id if is_group else None
            async with self._people.database.sessions() as session, session.begin():
                await session.execute(text("BEGIN"))
                owners = await self._people.person_reference_map(
                    tuple(
                        item
                        for item in (*inbound.mentioned_user_ids, inbound.reply_sender_user_id)
                        if item
                    )
                    if is_group
                    else (),
                    speaker_user_id=user_id,
                    bot_user_id=inbound.bot_user_id,
                    session=session,
                )
                representatives: dict[str, str] = {}
                for account, person_id in owners.items():
                    representatives.setdefault(person_id, account)
                candidates = tuple(representatives.values())
                scopes = await self._read_scopes.people(
                    user_id,
                    (user_id, *candidates),
                    group_id=group_id,
                    include_person_groups=is_group,
                    restrict_to_group=False,
                    person_ids=owners,
                    session=session,
                )
            current = scopes.get(user_id)
            if current is not None:
                targets.extend(
                    target
                    if target.scope_type is MemoryScopeType.PERSON
                    else target.model_copy(update={"block_id": "current_person_in_group"})
                    for target in current.targets
                )
                if group_id is not None and any(
                    target.scope_type is MemoryScopeType.PERSON_GROUP for target in current.targets
                ):
                    targets.append(
                        MemoryEntityTarget(
                            scope_type=MemoryScopeType.GROUP,
                            group_id=group_id,
                            role=MemoryTargetRole.CURRENT_GROUP,
                            block_id="current_group",
                        )
                    )
            for candidate in candidates:
                resolved = scopes.get(candidate)
                if resolved is not None:
                    targets.extend(resolved.targets)
        except ValidationError as exc:
            raise MemoryRetrievalError("memory_target_invalid") from exc
        return tuple(targets)
