"""Message wording lives in isolated messaging tables, not runtime fallbacks."""

from unittest.mock import AsyncMock

import pytest

from lnbits.extensions.cyberherd.tests.test_database_operations import (
    audit_db as _audit_db,
)
from lnbits.extensions.cyberherd_messaging import crud, migrations, services

audit_db = _audit_db
# All schema identifiers come from the disposable database fixture.
# ruff: noqa: S608


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def messaging_db(audit_db, monkeypatch):
    class IsolatedMessagingDatabase:
        references_schema = audit_db.references_schema

        async def execute(self, sql, values=None):
            return await audit_db.execute(
                sql.replace("cyberherd_messaging.", self.references_schema),
                values or {},
            )

        async def fetchone(self, sql, values=None):
            return await audit_db.fetchone(
                sql.replace("cyberherd_messaging.", self.references_schema),
                values or {},
            )

        async def fetchall(self, sql, values=None):
            return await audit_db.fetchall(
                sql.replace("cyberherd_messaging.", self.references_schema),
                values or {},
            )

    database = IsolatedMessagingDatabase()
    identity = (
        "INTEGER PRIMARY KEY AUTOINCREMENT"
        if audit_db.type == "SQLITE"
        else "BIGSERIAL PRIMARY KEY"
    )
    await database.execute(
        f"""CREATE TABLE {database.references_schema}message_templates (
        id {identity}, user_id TEXT NOT NULL, category TEXT NOT NULL, key TEXT NOT NULL,
        content TEXT NOT NULL, reply_relay TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, UNIQUE(user_id,category,key))"""
    )
    await database.execute(f"""CREATE TABLE {database.references_schema}user_settings (
        user_id TEXT NOT NULL,key TEXT NOT NULL,value TEXT NOT NULL,
        PRIMARY KEY(user_id,key))""")
    monkeypatch.setattr(crud, "db", database)
    return database


async def insert_template(database, category, key, content, owner="owner"):
    await database.execute(
        f"""INSERT INTO {database.references_schema}message_templates
        (user_id,category,key,content) VALUES (:owner,:category,:key,:content)""",
        {"owner": owner, "category": category, "key": key, "content": content},
    )


@pytest.mark.anyio
async def test_migration_seeds_variants_and_preserves_existing_categories(messaging_db):
    await insert_template(
        messaging_db,
        "cyber_herd_treats",
        "0",
        "Our custom paid template {name}: {new_amount} sats",
    )
    await insert_template(messaging_db, "cyber_herd", "0", "Our custom welcome {name}")
    await insert_template(
        messaging_db, "feeder_trigger", "0", "Our custom feeder {new_amount}"
    )
    await insert_template(
        messaging_db,
        "cyber_herd_treats_skipped",
        "0",
        "Our previous skipped template {name}",
    )
    await migrations.m005_seed_member_message_templates(messaging_db)
    await migrations.m005_seed_member_message_templates(messaging_db)
    paid = await crud.get_message_templates("owner", "cyber_herd_treats")
    skipped = await crud.get_message_templates("owner", "cyber_herd_no_treats")
    assert next(row.content for row in paid if row.key == "0").startswith(
        "Our custom paid"
    )
    assert len([row for row in paid if row.key.startswith("treat_")]) == 6
    assert len(skipped) == 7
    assert next(row.content for row in skipped if row.key == "legacy_0").startswith(
        "Our previous skipped"
    )
    assert not await crud.get_message_templates("owner", "feeder_trigger_dict")
    assert not await crud.get_message_templates("owner", "cyber_herd_join")
    assert not await crud.get_message_templates("other-owner", "cyber_herd_no_treats")


@pytest.mark.anyio
async def test_all_variants_render_from_separate_stored_categories(
    messaging_db, monkeypatch
):
    await crud.ensure_default_templates("owner")
    pubkey = "a" * 64
    outputs = set()
    for index in range(6):
        monkeypatch.setattr(
            "random.choice",
            lambda items, index=index: next(
                row for row in items if row.key == str(index)
            ),
        )
        content = await services.render_member_message(
            "cyber_herd_treats_skipped", {"name": "Alice"}, pubkey, "owner"
        )
        assert "Alice" in content and "nostr:npub" in content
        assert "received" not in content and "3 points" not in content
        outputs.add(content)
    assert len(outputs) == 6
    paid = await crud.get_message_templates("owner", "cyber_herd_treats")
    assert all("too small" not in row.content for row in paid)
    for index in range(6):
        monkeypatch.setattr(
            "random.choice",
            lambda items, index=index: next(
                row for row in items if row.key == f"treat_{index}"
            ),
        )
        content = await services.render_member_message(
            "cyber_herd_treats", {"name": "Alice", "new_amount": 42}, pubkey, "owner"
        )
        assert "42 sats" in content and "nostr:npub" in content


@pytest.mark.anyio
async def test_database_edits_control_hint_summary_and_membership(
    messaging_db, monkeypatch
):
    await crud.ensure_default_templates("owner")
    for category, content in (
        ("cyber_herd_no_treats", "Edited no treat for {name}"),
        ("repost_hint", "Edited repost hint: 3 points"),
        ("feeder_batch_summary", "Edited summary {feedings}/{payout_cycles}"),
        ("kind_6_repost", "Edited repost join for {name}"),
    ):
        await messaging_db.execute(
            f"UPDATE {messaging_db.references_schema}message_templates "
            "SET content=:content "
            "WHERE user_id='owner' AND category=:category AND key='0'",
            {"category": category, "content": content},
        )
    monkeypatch.setattr("random.choice", lambda items: items[0])
    content = await services.render_member_message(
        "cyber_herd_treats_skipped", {"name": "Alice"}, "a" * 64, "owner", True
    )
    assert content.startswith("Edited no treat") and content.endswith(
        "Edited repost hint: 3 points"
    )
    member = await services.render_member_message(
        "kind_6_repost", {"name": "Alice"}, "a" * 64, "owner"
    )
    assert "Edited repost join" in member
    feeder = await services.render_feeder_message(
        {"new_amount": 3456, "feedings": 3, "payout_cycles": 3}, "owner"
    )
    assert feeder.endswith("Edited summary 3/3")


@pytest.mark.anyio
async def test_template_failure_does_not_use_hardcoded_fallback(
    messaging_db, monkeypatch
):
    await crud.ensure_default_templates("owner")
    monkeypatch.setattr(
        crud,
        "get_message_templates",
        AsyncMock(side_effect=RuntimeError("database unavailable")),
    )
    with pytest.raises(RuntimeError, match="database unavailable"):
        await services.render_member_message(
            "cyber_herd_treats_skipped", {"name": "Alice"}, "a" * 64, "owner"
        )
    with pytest.raises(RuntimeError, match="database unavailable"):
        await services.render_member_message(
            "new_member", {"name": "Alice"}, "a" * 64, "owner"
        )


@pytest.mark.anyio
async def test_deleted_variants_are_not_reseeded(messaging_db):
    await crud.ensure_default_templates("owner")
    await messaging_db.execute(
        f"DELETE FROM {messaging_db.references_schema}message_templates "
        "WHERE user_id='owner' AND category='cyber_herd_no_treats' AND key='0'"
    )
    await crud.ensure_default_templates("owner")
    remaining = await crud.get_message_templates("owner", "cyber_herd_no_treats")
    assert len(remaining) == 5 and not any(row.key == "0" for row in remaining)


@pytest.mark.anyio
async def test_partial_seed_failure_is_retryable_and_preserves_custom_rows(
    messaging_db, monkeypatch
):
    await insert_template(
        messaging_db, "cyber_herd_treats", "0", "Our custom paid message"
    )
    execute = messaging_db.execute

    async def interrupted(sql, values=None):
        if (
            values
            and values.get("category") == "cyber_herd_no_treats"
            and values.get("key") == "2"
        ):
            raise RuntimeError("interrupted seed")
        return await execute(sql, values)

    monkeypatch.setattr(messaging_db, "execute", interrupted)
    with pytest.raises(RuntimeError, match="interrupted seed"):
        await crud.ensure_default_templates("owner")
    marker = await messaging_db.fetchone(
        f"SELECT * FROM {messaging_db.references_schema}user_settings "
        "WHERE user_id='owner' AND key='member_message_templates_v1'"
    )
    assert marker is None
    monkeypatch.setattr(messaging_db, "execute", execute)
    await crud.ensure_default_templates("owner")
    assert len(await crud.get_message_templates("owner", "cyber_herd_no_treats")) == 6
    paid = await crud.get_message_templates("owner", "cyber_herd_treats")
    assert (
        next(row.content for row in paid if row.key == "0") == "Our custom paid message"
    )


@pytest.mark.anyio
async def test_breakdown_upgrade_preserves_edits_and_deletions(messaging_db):
    await insert_template(messaging_db, "cyber_herd_treats", "custom", "Custom treats")
    await messaging_db.execute(
        f"INSERT INTO {messaging_db.references_schema}user_settings "
        "(user_id,key,value) "
        "VALUES ('owner','member_message_templates_v1','1')"
    )
    await migrations.m006_seed_treat_breakdown_template(messaging_db)
    templates = await crud.get_message_templates("owner", "cyber_herd_treat_breakdown")
    assert len(templates) == 1 and "{total_points}" in templates[0].content
    await crud.update_message_template(
        "owner", "cyber_herd_treat_breakdown", "0", "Custom {total_points}"
    )
    await migrations.m006_seed_treat_breakdown_template(messaging_db)
    assert (await crud.get_message_templates("owner", "cyber_herd_treat_breakdown"))[
        0
    ].content == "Custom {total_points}"
    await crud.delete_message_template("owner", "cyber_herd_treat_breakdown", "0")
    await crud.ensure_default_templates("owner")
    assert not await crud.get_message_templates("owner", "cyber_herd_treat_breakdown")
    assert len(await crud.get_message_templates("owner", "cyber_herd_treats")) == 1
