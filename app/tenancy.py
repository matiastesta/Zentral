import re

from flask import g, request, session

from app import db
from app.models import Company


def _host_subdomain(hostname: str | None) -> str | None:
    raw = str(hostname or '').strip().lower()
    raw = re.sub(r':\d+$', '', raw)
    if not raw or raw == 'localhost' or raw == '127.0.0.1':
        return None
    parts = [p for p in raw.split('.') if p]
    if len(parts) < 3:
        return None
    sub = parts[0]
    if sub in {'www'}:
        return None
    return sub


def resolve_company_slug() -> str | None:
    qp = str(request.args.get('company') or '').strip().lower()
    if qp:
        return qp
    return _host_subdomain(request.host)


def resolve_company() -> Company | None:
    slug = resolve_company_slug()
    if not slug:
        return None
    return db.session.query(Company).filter(Company.slug == slug).first()


def effective_company_id() -> str | None:
    is_admin = bool(session.get('auth_is_zentral_admin') == '1')
    if is_admin:
        imp = session.get('impersonate_company_id')
        return str(imp) if imp else None

    cid = session.get('auth_company_id')
    if cid:
        return str(cid)

    c = resolve_company()
    return str(c.id) if c else None


def ensure_request_context() -> None:
    if hasattr(g, 'company'):
        return

    if getattr(g, '_ensuring_request_context', False):
        return

    g._ensuring_request_context = True

    company = None
    company_id = effective_company_id()

    # Set placeholders early so any nested DB access (triggering do_orm_execute) can
    # safely short-circuit on hasattr(g, 'company').
    g.company_id = company_id
    g.company = None

    try:
        is_admin = bool(session.get('auth_is_zentral_admin') == '1')
        if is_admin:
            imp = session.get('impersonate_company_id')
            if imp:
                company = db.session.get(Company, str(imp))
        if company is None:
            company = resolve_company()

        g.company = company
    finally:
        g._ensuring_request_context = False


def is_impersonating() -> bool:
    return bool(session.get('impersonate_company_id'))
