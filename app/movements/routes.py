from datetime import date as dt_date

from flask import jsonify, render_template, request
from flask_login import login_required

from app import db
from app.models import CashCount
from app.permissions import module_required, module_required_any
from app.movements import bp


def _company_id() -> str:
    from flask_login import current_user
    return str(getattr(current_user, 'company_id', '') or '').strip()


@bp.route('/')
@bp.route('/index')
@login_required
@module_required('movements')
def index():
    """Historial de movimientos (dummy)."""
    return render_template('movements/index.html', title='Movimientos')


@bp.get('/api/cash-counts')
@login_required
@module_required_any('movements', 'dashboard')
def list_cash_counts():
    raw_from = (request.args.get('from') or '').strip()
    raw_to = (request.args.get('to') or '').strip()

    cid = _company_id()
    if not cid:
        return jsonify({'ok': True, 'items': []})

    def parse_iso(s):
        try:
            return dt_date.fromisoformat(s) if s else None
        except Exception:
            return None

    d_from = parse_iso(raw_from)
    d_to = parse_iso(raw_to)

    q = db.session.query(CashCount).filter(CashCount.company_id == cid)
    if d_from:
        q = q.filter(CashCount.count_date >= d_from)
    if d_to:
        q = q.filter(CashCount.count_date <= d_to)
    q = q.order_by(CashCount.count_date.desc())

    rows = q.all()
    items = []
    for r in rows:
        items.append({
            'date': r.count_date.isoformat(),
            'employee_name': r.employee_name,
            'opening_amount': r.opening_amount,
            'cash_day_amount': r.cash_day_amount,
            'closing_amount': r.closing_amount,
            'difference_amount': r.difference_amount,
        })

    return jsonify({'ok': True, 'items': items})
