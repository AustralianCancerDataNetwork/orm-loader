from __future__ import annotations

import sqlalchemy as sa

from oa_configurator import Role
from oa_configurator.testing import isolated_test_schema
from orm_loader.mappers.materialised_view_mixin import MaterializedViewMixin, refresh_all_mvs


class _PrimaryRoleMV(MaterializedViewMixin):
    __mv_name__ = "mv_primary_role_test"
    __mv_select__ = sa.select(sa.literal(1).label("n"))


class _VocabRoleMV(MaterializedViewMixin):
    __mv_name__ = "mv_vocab_role_test"
    __mv_select__ = sa.select(sa.literal(2).label("n"))
    __mv_role__ = Role.VOCAB


def test_refresh_all_mvs_resolves_each_views_own_role(pg_db) -> None:
    engine = pg_db.connection.engine

    with isolated_test_schema(engine, prefix="mv_primary") as primary_schema, \
         isolated_test_schema(engine, prefix="mv_vocab") as vocab_schema:
        scoped = engine.execution_options(
            schema_translate_map={Role.PRIMARY.value: primary_schema, Role.VOCAB.value: vocab_schema}
        )
        with scoped.begin() as conn:
            _PrimaryRoleMV.create_mv(conn)
            _VocabRoleMV.create_mv(conn)
            refresh_all_mvs(conn, [_PrimaryRoleMV, _VocabRoleMV])

        with engine.connect() as conn:
            inspector = sa.inspect(conn)
            assert inspector.has_table("mv_primary_role_test", schema=primary_schema)
            assert not inspector.has_table("mv_primary_role_test", schema=vocab_schema)
            assert inspector.has_table("mv_vocab_role_test", schema=vocab_schema)
            assert not inspector.has_table("mv_vocab_role_test", schema=primary_schema)
