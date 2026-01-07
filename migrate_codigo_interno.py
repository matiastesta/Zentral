import argparse
import json
import os
from datetime import datetime

from sqlalchemy.orm import joinedload

from app import create_app, db
from app.models import Product


def _normalize_company_id(raw: str) -> str:
    return str(raw or '').strip()


def _codigo_prefix_from(name: str, category_name: str) -> str:
    # Reuse backend logic from inventory module for consistency.
    from app.inventory.routes import _codigo_prefix_from as _inv_prefix  # noqa: WPS433

    return _inv_prefix(name, category_name)


def _generate_codigo_interno(company_id: str, name: str, category_name: str, used: set) -> str:
    # Reuse backend logic from inventory module for consistency.
    from app.inventory.routes import _generate_codigo_interno as _inv_gen  # noqa: WPS433

    return _inv_gen(company_id, name, category_name, used=used)


def migrate_company(company_id: str) -> dict:
    cid = _normalize_company_id(company_id)
    if not cid:
        return {'company_id': cid, 'ok': False, 'error': 'invalid_company_id'}

    rows = (
        db.session.query(Product)
        .options(joinedload(Product.category))
        .filter(Product.company_id == cid)
        .order_by(Product.id.asc())
        .all()
    )

    # Two-phase update to avoid collisions with legacy codes:
    # 1) clear internal_code
    # 2) assign new NNNCCC##
    changes = []
    used = set()

    for p in (rows or []):
        before = str(getattr(p, 'internal_code', '') or '').strip()
        if before:
            changes.append({'id': int(p.id), 'name': str(p.name or ''), 'before': before, 'after': None})
        p.internal_code = None

    db.session.flush()

    updated = 0
    errors = []
    final_changes = []

    for p in (rows or []):
        try:
            cat_name = ''
            try:
                cat_name = str(getattr(getattr(p, 'category', None), 'name', '') or '').strip()
            except Exception:
                cat_name = ''
            if not cat_name:
                cat_name = 'GEN'

            next_code = _generate_codigo_interno(cid, str(getattr(p, 'name', '') or ''), cat_name, used)
            if not next_code or len(next_code) != 8:
                raise ValueError('generation_failed')

            before = ''
            for c in changes:
                if c.get('id') == int(p.id):
                    before = str(c.get('before') or '')
                    break

            p.internal_code = next_code
            updated += 1
            final_changes.append({'id': int(p.id), 'name': str(p.name or ''), 'before': before, 'after': next_code})
        except Exception as e:
            errors.append({'id': int(getattr(p, 'id', 0) or 0), 'error': str(e)})

    db.session.flush()

    return {
        'company_id': cid,
        'ok': len(errors) == 0,
        'total': len(rows or []),
        'updated': updated,
        'errors': errors,
        'changes': final_changes,
    }


def rollback_from_log(log_path: str) -> dict:
    pth = str(log_path or '').strip()
    if not pth or not os.path.exists(pth):
        return {'ok': False, 'error': 'log_not_found'}

    with open(pth, 'r', encoding='utf-8') as f:
        payload = json.load(f)

    items = payload.get('companies') if isinstance(payload, dict) else None
    if not isinstance(items, list):
        return {'ok': False, 'error': 'invalid_log_format'}

    restored = 0
    errors = []

    for comp in items:
        changes = (comp or {}).get('changes')
        if not isinstance(changes, list) or not changes:
            continue

        ids = [int(c.get('id') or 0) for c in changes if int(c.get('id') or 0) > 0]
        if not ids:
            continue

        rows = db.session.query(Product).filter(Product.id.in_(ids)).all()
        by_id = {int(r.id): r for r in (rows or [])}

        # Two-phase restore to avoid unique conflicts.
        for pid in ids:
            r = by_id.get(int(pid))
            if r:
                r.internal_code = None
        db.session.flush()

        for c in changes:
            pid = int(c.get('id') or 0)
            before = str(c.get('before') or '').strip() or None
            r = by_id.get(pid)
            if not r:
                continue
            try:
                r.internal_code = before
                restored += 1
            except Exception as e:
                errors.append({'id': pid, 'error': str(e)})

        db.session.flush()

    return {'ok': len(errors) == 0, 'restored': restored, 'errors': errors}


def main() -> int:
    parser = argparse.ArgumentParser(description='Migrate codigo_interno to NNNCCC## (8 chars).')
    parser.add_argument('--company-id', default='', help='Migrate only this company_id (default: all companies).')
    parser.add_argument('--dry-run', action='store_true', help='Do not commit changes.')
    parser.add_argument('--output', default='', help='Path to write JSON log file.')
    parser.add_argument('--rollback', default='', help='Rollback using a previously generated JSON log file.')

    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        if args.rollback:
            res = rollback_from_log(args.rollback)
            if args.dry_run:
                db.session.rollback()
            else:
                if res.get('ok'):
                    db.session.commit()
                else:
                    db.session.rollback()
            print(json.dumps(res, ensure_ascii=False, indent=2))
            return 0 if res.get('ok') else 2

        company_ids = []
        if str(args.company_id or '').strip():
            company_ids = [str(args.company_id).strip()]
        else:
            company_ids = [str(cid or '').strip() for (cid,) in db.session.query(Product.company_id).distinct().all()]
            company_ids = [c for c in company_ids if c]

        companies = []
        for cid in company_ids:
            companies.append(migrate_company(cid))

        ok = all(bool(c.get('ok')) for c in companies)

        out = {
            'ok': ok,
            'dry_run': bool(args.dry_run),
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'companies': companies,
        }

        if args.dry_run:
            db.session.rollback()
        else:
            if ok:
                db.session.commit()
            else:
                db.session.rollback()

        out_path = str(args.output or '').strip()
        if not out_path:
            ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            out_path = f'codigo_interno_migration_{ts}.json'

        try:
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(out, f, ensure_ascii=False, indent=2)
            out['log_path'] = out_path
        except Exception as e:
            out['log_write_error'] = str(e)

        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0 if ok else 2


if __name__ == '__main__':
    raise SystemExit(main())
