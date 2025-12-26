from __future__ import annotations

from alembic import context
from flask import current_app
from sqlalchemy import engine_from_config, pool

config = context.config

def get_url() -> str:
    app = current_app._get_current_object()
    return str(app.config.get('SQLALCHEMY_DATABASE_URI') or '')

def get_metadata():
    app = current_app._get_current_object()
    ext = app.extensions.get('migrate')
    db = getattr(ext, 'db', None)
    return getattr(db, 'metadata', None)


def run_migrations_offline() -> None:
    url = get_url()
    context.configure(
        url=url,
        target_metadata=get_metadata(),
        literal_binds=True,
        compare_type=True,
        render_as_batch=('sqlite' in (url or '').lower()),
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section) or {}
    configuration['sqlalchemy.url'] = get_url()

    connectable = engine_from_config(
        configuration,
        prefix='sqlalchemy.',
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=get_metadata(),
            compare_type=True,
            render_as_batch=('sqlite' in (get_url() or '').lower()),
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
