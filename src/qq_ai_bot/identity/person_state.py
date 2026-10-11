"""Canonical Person, Space, alias, and membership state.

Transport-facing APIs accept QQ identifiers, resolve them through Bindings, and
store ownership exclusively as canonical UUIDs.  External identifiers never act
as database ownership keys in this module.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import func, literal, literal_column, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql.elements import ColumnElement

from qq_ai_bot.identity.canonical_repository import (
    external_id,
    find_identity_binding,
    find_space_binding,
)
from qq_ai_bot.identity.db_models import (
    CanonicalPersonModel,
    CanonicalSpaceModel,
    IdentityBindingModel,
    SpaceBindingModel,
)
from qq_ai_bot.identity.errors import CanonicalIdentityError
from qq_ai_bot.persistence.models import MembershipModel, PersonAliasModel
from qq_ai_bot.persistence.repository_records import GroupSetting, PrivateUserSetting


@dataclass(frozen=True, slots=True)
class CanonicalMemberNameProjection:
    user_id: str
    nickname: str
    group_card: str
    aliases: tuple[str, ...]


async def _live_person_binding(
    session: AsyncSession,
    user_id: str,
    *,
    allow_disabled: bool = False,
    required: bool = False,
) -> tuple[IdentityBindingModel, CanonicalPersonModel] | None:
    binding = await find_identity_binding(session, external_id(user_id))
    if binding is None:
        if required:
            raise CanonicalIdentityError("unclassified")
        return None
    if binding.status != "active":
        if required:
            raise CanonicalIdentityError("canonical_owner_disabled")
        return None
    person = await session.get(CanonicalPersonModel, binding.person_id)
    if person is None:
        raise CanonicalIdentityError("unclassified")
    if not person.enabled and not allow_disabled:
        if required:
            raise CanonicalIdentityError("canonical_owner_disabled")
        return None
    return binding, person


async def _live_space_binding(
    session: AsyncSession,
    group_id: str,
    *,
    required: bool = False,
) -> tuple[SpaceBindingModel, CanonicalSpaceModel] | None:
    binding = await find_space_binding(session, external_id(group_id))
    if binding is None:
        if required:
            raise CanonicalIdentityError("no_space_binding")
        return None
    if binding.status != "active":
        if required:
            raise CanonicalIdentityError("canonical_owner_disabled")
        return None
    space = await session.get(CanonicalSpaceModel, binding.space_id)
    if space is None:
        raise CanonicalIdentityError("unclassified")
    return binding, space


def _nickname_value() -> ColumnElement[str]:
    alias = (
        select(PersonAliasModel.alias)
        .where(
            PersonAliasModel.canonical_person_id == IdentityBindingModel.person_id,
            PersonAliasModel.canonical_space_id.is_(None),
            PersonAliasModel.alias_type == "nickname",
        )
        .order_by(PersonAliasModel.last_seen_at.desc(), PersonAliasModel.id.desc())
        .limit(1)
        .correlate(IdentityBindingModel)
        .scalar_subquery()
    )
    other = aliased(IdentityBindingModel)
    fallback = (
        select(other.display_name)
        .where(
            other.person_id == IdentityBindingModel.person_id,
            other.status == "active",
            other.display_name != "",
        )
        .order_by(other.external_account_id.desc(), other.id.desc())
        .limit(1)
        .correlate(IdentityBindingModel)
        .scalar_subquery()
    )
    return func.coalesce(
        func.nullif(alias, ""),
        func.nullif(IdentityBindingModel.display_name, ""),
        fallback,
        "",
    )


async def _find_alias(
    session: AsyncSession,
    *,
    person_id: str,
    space_id: str | None,
    alias: str,
) -> PersonAliasModel | None:
    conditions = [
        PersonAliasModel.canonical_person_id == person_id,
        PersonAliasModel.alias == alias[:128],
    ]
    conditions.append(
        func.coalesce(PersonAliasModel.canonical_space_id, literal_column("''")) == (space_id or "")
    )
    return (await session.scalars(select(PersonAliasModel).where(*conditions))).first()


def _stage_alias(
    session: AsyncSession,
    row: PersonAliasModel | None,
    *,
    person_id: str,
    space_id: str | None,
    alias: str,
    alias_type: str,
    now: datetime,
) -> None:
    if row is None:
        session.add(
            PersonAliasModel(
                canonical_person_id=person_id,
                canonical_space_id=space_id,
                alias=alias[:128],
                alias_type=alias_type,
                first_seen_at=now,
                last_seen_at=now,
            )
        )
        return
    row.alias_type = alias_type
    row.last_seen_at = now


async def observe_canonical_person(
    session: AsyncSession,
    *,
    binding: IdentityBindingModel,
    nickname: str,
    group_id: str | None,
    group_card: str,
    group_name: str,
    nickname_known: bool,
    group_card_known: bool,
    now: datetime,
) -> None:
    if binding.status != "active":
        raise CanonicalIdentityError("canonical_owner_disabled")
    person = await session.get(CanonicalPersonModel, binding.person_id)
    if person is None:
        raise CanonicalIdentityError("unclassified")
    if not person.enabled:
        raise CanonicalIdentityError("canonical_owner_disabled")
    nickname_alias = (
        await _find_alias(session, person_id=binding.person_id, space_id=None, alias=nickname)
        if nickname
        else None
    )
    space_binding = None
    space = None
    membership = None
    card_alias = None
    if group_id is not None:
        state = await _live_space_binding(session, group_id, required=True)
        assert state is not None
        space_binding, space = state
        membership = await session.get(MembershipModel, (binding.person_id, space_binding.space_id))
        if group_card:
            card_alias = await _find_alias(
                session,
                person_id=binding.person_id,
                space_id=space_binding.space_id,
                alias=group_card,
            )

    # All owner, alias and membership reads precede staged writes.
    # The repository's short immediate transaction serializes first observation.
    binding.last_seen_at = now
    if nickname_known and nickname and binding.display_name != nickname[:128]:
        binding.display_name = nickname[:128]
        binding.updated_at = now
        binding.revision += 1
    if nickname:
        _stage_alias(
            session,
            nickname_alias,
            person_id=binding.person_id,
            space_id=None,
            alias=nickname,
            alias_type="nickname",
            now=now,
        )
    if group_id is None:
        return
    assert space_binding is not None and space is not None
    space_binding.last_seen_at = now
    if group_name:
        if space.name != group_name[:128]:
            space.name = group_name[:128]
            space.updated_at = now
            space.revision += 1
        if space_binding.display_name != group_name[:128]:
            space_binding.display_name = group_name[:128]
            space_binding.updated_at = now
            space_binding.revision += 1
    if membership is None:
        membership = MembershipModel(
            canonical_person_id=binding.person_id,
            canonical_space_id=space_binding.space_id,
            group_card=group_card[:128] if group_card_known else "",
            first_seen_at=now,
            last_seen_at=now,
        )
        session.add(membership)
    else:
        if group_card_known:
            membership.group_card = group_card[:128]
        membership.last_seen_at = now
    if group_card:
        _stage_alias(
            session,
            card_alias,
            person_id=binding.person_id,
            space_id=space_binding.space_id,
            alias=group_card,
            alias_type="group_card",
            now=now,
        )


async def load_canonical_profile(
    session: AsyncSession,
    *,
    user_id: str,
    group_id: str | None,
    include_nickname: bool = True,
) -> tuple[str, str] | None:
    return (
        await load_canonical_profiles(
            session, (user_id,), group_id=group_id, include_nickname=include_nickname
        )
    ).get(user_id)


async def load_canonical_profiles(
    session: AsyncSession,
    user_ids: tuple[str, ...],
    *,
    group_id: str | None,
    include_nickname: bool = True,
    person_ids: Mapping[str, str] | None = None,
    members_only: bool = False,
) -> dict[str, tuple[str, str]]:
    """Read requested profiles, reusing canonical owners from this read snapshot."""
    accounts = {raw: external_id(raw) for raw in user_ids}
    if person_ids is not None:
        accounts = {raw: account for raw, account in accounts.items() if account in person_ids}
    if not accounts:
        return {}
    query = select(
        IdentityBindingModel.external_account_id,
        IdentityBindingModel.person_id,
        (CanonicalPersonModel.enabled if person_ids is None else literal(True).label("enabled")),
        (_nickname_value() if include_nickname else literal("")).label("nickname"),
    ).where(
        IdentityBindingModel.platform == "qq",
        IdentityBindingModel.status == "active",
    )
    if person_ids is None:
        values = func.json_each(json.dumps(tuple(dict.fromkeys(accounts.values())))).table_valued(
            "value"
        )
        query = query.outerjoin(
            CanonicalPersonModel, CanonicalPersonModel.id == IdentityBindingModel.person_id
        ).where(IdentityBindingModel.external_account_id.in_(select(values.c.value)))
    else:
        values = func.json_each(
            json.dumps(tuple((account, person_ids[account]) for account in accounts.values()))
        ).table_valued("value")
        query = query.where(
            tuple_(IdentityBindingModel.external_account_id, IdentityBindingModel.person_id).in_(
                select(
                    func.json_extract(values.c.value, "$[0]"),
                    func.json_extract(values.c.value, "$[1]"),
                )
            )
        )
    if group_id is not None and members_only:
        state = await _live_space_binding(session, group_id, required=True)
        assert state is not None
        space_binding, _space = state
        query = query.join(
            MembershipModel,
            (MembershipModel.canonical_person_id == IdentityBindingModel.person_id)
            & (MembershipModel.canonical_space_id == space_binding.space_id),
        ).add_columns(MembershipModel.group_card)
    rows = (await session.execute(query)).all()
    people = {}
    for row in rows:
        if row.enabled is None:
            raise CanonicalIdentityError("unclassified")
        if row.enabled:
            people[row.external_account_id] = row
    if not people:
        return {}
    cards: dict[str, str] = {}
    if group_id is not None and members_only:
        cards = {row.person_id: row.group_card for row in people.values()}
    elif group_id is not None:
        state = await _live_space_binding(session, group_id)
        if state is None:
            return {}
        space_binding, _space = state
        person_values = func.json_each(
            json.dumps(tuple({row.person_id for row in people.values()}))
        ).table_valued("value")
        cards = {
            person_id: card
            for person_id, card in (
                await session.execute(
                    select(MembershipModel.canonical_person_id, MembershipModel.group_card).where(
                        MembershipModel.canonical_person_id.in_(select(person_values.c.value)),
                        MembershipModel.canonical_space_id == space_binding.space_id,
                    )
                )
            ).all()
        }
    return {
        raw: (
            str(people[account].nickname) if include_nickname else "",
            cards.get(people[account].person_id, ""),
        )
        for raw, account in accounts.items()
        if account in people
    }


async def load_canonical_aliases(
    session: AsyncSession,
    user_id: str,
    *,
    limit: int,
) -> tuple[str, ...]:
    state = await _live_person_binding(session, user_id)
    if state is None:
        return ()
    binding, _person = state
    values = (
        await session.scalars(
            select(PersonAliasModel.alias)
            .where(PersonAliasModel.canonical_person_id == binding.person_id)
            .order_by(PersonAliasModel.last_seen_at.desc(), PersonAliasModel.id.desc())
            .limit(max(1, limit))
        )
    ).all()
    return tuple(dict.fromkeys(str(value) for value in values))


async def load_canonical_membership_count(session: AsyncSession, user_id: str) -> int:
    state = await _live_person_binding(session, user_id)
    if state is None:
        return 0
    binding, _person = state
    value = await session.scalar(
        select(func.count())
        .select_from(MembershipModel)
        .where(MembershipModel.canonical_person_id == binding.person_id)
    )
    return int(value or 0)


async def load_canonical_members_in_group(
    session: AsyncSession,
    user_ids: tuple[str, ...],
    group_id: str,
    *,
    person_ids: Mapping[str, str] | None = None,
) -> frozenset[str]:
    state = await _live_space_binding(session, group_id, required=True)
    assert state is not None
    space_binding, _space = state
    if person_ids is not None:
        values = func.json_each(json.dumps(tuple(dict.fromkeys(person_ids.values())))).table_valued(
            "value"
        )
        members = set(
            await session.scalars(
                select(MembershipModel.canonical_person_id).where(
                    MembershipModel.canonical_person_id.in_(select(values.c.value)),
                    MembershipModel.canonical_space_id == space_binding.space_id,
                )
            )
        )
        return frozenset(user for user in user_ids if person_ids.get(user) in members)
    accounts = {raw: external_id(raw) for raw in user_ids}
    values = func.json_each(json.dumps(tuple(dict.fromkeys(accounts.values())))).table_valued(
        "value"
    )
    rows = (
        await session.execute(
            select(IdentityBindingModel.external_account_id, CanonicalPersonModel.enabled)
            .outerjoin(
                CanonicalPersonModel, CanonicalPersonModel.id == IdentityBindingModel.person_id
            )
            .outerjoin(
                MembershipModel,
                (MembershipModel.canonical_person_id == IdentityBindingModel.person_id)
                & (MembershipModel.canonical_space_id == space_binding.space_id),
            )
            .where(
                IdentityBindingModel.platform == "qq",
                IdentityBindingModel.external_account_id.in_(select(values.c.value)),
                IdentityBindingModel.status == "active",
            )
            .add_columns(MembershipModel.canonical_person_id)
        )
    ).all()
    matched = set()
    for account, enabled, member in rows:
        if enabled is None:
            raise CanonicalIdentityError("unclassified")
        if enabled and member is not None:
            matched.add(account)
    return frozenset(raw for raw, account in accounts.items() if account in matched)


async def load_canonical_group_member_name_projections(
    session: AsyncSession,
    group_id: str,
) -> tuple[CanonicalMemberNameProjection, ...]:
    state = await _live_space_binding(session, group_id, required=True)
    assert state is not None
    space_binding, _space = state
    memberships = {
        person_id: card
        for person_id, card in (
            await session.execute(
                select(MembershipModel.canonical_person_id, MembershipModel.group_card)
                .join(
                    CanonicalPersonModel,
                    CanonicalPersonModel.id == MembershipModel.canonical_person_id,
                )
                .where(
                    MembershipModel.canonical_space_id == space_binding.space_id,
                    CanonicalPersonModel.enabled.is_(True),
                )
            )
        ).all()
    }
    if not memberships:
        return ()
    values = func.json_each(json.dumps(tuple(memberships))).table_valued("value")
    selected = select(values.c.value)
    bindings = (
        await session.execute(
            select(
                IdentityBindingModel.person_id,
                IdentityBindingModel.external_account_id,
                IdentityBindingModel.display_name,
            )
            .where(
                IdentityBindingModel.person_id.in_(selected),
                IdentityBindingModel.status == "active",
            )
            .order_by(
                IdentityBindingModel.person_id,
                IdentityBindingModel.external_account_id,
                IdentityBindingModel.id,
            )
        )
    ).all()
    representatives: dict[str, tuple[str, str]] = {}
    fallback_names: dict[str, str] = {}
    for person_id, account, display_name in bindings:
        representatives.setdefault(person_id, (account, display_name))
        if display_name:
            fallback_names[person_id] = display_name
    aliases: dict[str, dict[str, None]] = {}
    nicknames: dict[str, str] = {}
    rows = (
        await session.execute(
            select(
                PersonAliasModel.canonical_person_id,
                PersonAliasModel.canonical_space_id,
                PersonAliasModel.alias,
                PersonAliasModel.alias_type,
            )
            .where(
                PersonAliasModel.canonical_person_id.in_(selected),
                (PersonAliasModel.canonical_space_id.is_(None))
                | (PersonAliasModel.canonical_space_id == space_binding.space_id),
            )
            .order_by(
                PersonAliasModel.canonical_person_id,
                PersonAliasModel.last_seen_at.desc(),
                PersonAliasModel.id.desc(),
            )
        )
    ).all()
    for person_id, alias_space, alias, alias_type in rows:
        aliases.setdefault(person_id, {}).setdefault(alias, None)
        if alias_space is None and alias_type == "nickname":
            nicknames.setdefault(person_id, alias)
    result: list[CanonicalMemberNameProjection] = []
    for person_id, (representative_id, display_name) in representatives.items():
        result.append(
            CanonicalMemberNameProjection(
                user_id=representative_id,
                nickname=nicknames.get(person_id)
                or display_name
                or fallback_names.get(person_id, ""),
                group_card=memberships[person_id],
                aliases=tuple(aliases.get(person_id, {})),
            )
        )
    return tuple(sorted(result, key=lambda item: item.user_id))


async def load_canonical_group_members_by_exact_name(
    session: AsyncSession,
    name: str,
    group_id: str,
) -> tuple[str, ...]:
    normalized = name.strip()
    if not normalized:
        return ()
    result = []
    for projection in await load_canonical_group_member_name_projections(session, group_id):
        if normalized in {projection.nickname, projection.group_card, *projection.aliases}:
            result.append(projection.user_id)
    return tuple(sorted(result))


async def observe_canonical_space(
    session: AsyncSession,
    group_id: str,
    *,
    name: str,
    now: datetime,
) -> GroupSetting:
    state = await _live_space_binding(session, group_id, required=True)
    assert state is not None
    binding, space = state
    binding.last_seen_at = now
    if name and (binding.display_name != name[:128] or space.name != name[:128]):
        binding.display_name = name[:128]
        binding.updated_at = now
        binding.revision += 1
        space.name = name[:128]
        space.updated_at = now
        space.revision += 1
    return _group_setting(group_id, space)


async def load_canonical_group(
    session: AsyncSession,
    group_id: str,
) -> GroupSetting | None:
    state = await _live_space_binding(session, group_id)
    if state is None:
        return None
    _binding, space = state
    return _group_setting(group_id, space)


async def set_canonical_space_flags(
    session: AsyncSession,
    group_id: str,
    *,
    enabled: bool | None = None,
    autonomous_enabled: bool | None = None,
    now: datetime,
) -> GroupSetting:
    state = await _live_space_binding(session, group_id, required=True)
    assert state is not None
    _binding, space = state
    if enabled is not None:
        space.enabled = enabled
    if autonomous_enabled is not None:
        space.autonomous_enabled = autonomous_enabled
    space.updated_at = now
    space.revision += 1
    return _group_setting(group_id, space)


def _group_setting(group_id: str, space: CanonicalSpaceModel) -> GroupSetting:
    return GroupSetting(
        group_id=external_id(group_id),
        enabled=bool(space.enabled),
        require_mention=bool(space.require_mention),
        autonomous_enabled=bool(space.autonomous_enabled),
        name=space.name,
    )


async def set_canonical_person_enabled(
    session: AsyncSession,
    user_id: str,
    enabled: bool,
    *,
    now: datetime,
) -> PrivateUserSetting:
    state = await _live_person_binding(session, user_id, allow_disabled=True, required=True)
    assert state is not None
    _binding, person = state
    person.enabled = enabled
    person.updated_at = now
    person.revision += 1
    return PrivateUserSetting(user_id=external_id(user_id), enabled=enabled)


async def load_canonical_person_enabled(
    session: AsyncSession,
    user_id: str,
) -> PrivateUserSetting | None:
    state = await _live_person_binding(session, user_id, allow_disabled=True)
    if state is None:
        return None
    _binding, person = state
    return PrivateUserSetting(user_id=external_id(user_id), enabled=bool(person.enabled))
