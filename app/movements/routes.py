from datetime import date as dt_date

from flask import jsonify, render_template, request
from flask_login import login_required
from flask_login import current_user

from app import db
from app.models import CalendarEvent, CashCount, CashWithdrawal, User
from app.permissions import module_required, module_required_any
from app.movements import bp


def _company_id() -> str:
    try:
        from flask import g
        return str(getattr(g, 'company_id', '') or '').strip()
    except Exception:
        return ''


@bp.route('/')
@bp.route('/index')
@login_required
@module_required('movements')
def index():
    """Historial de movimientos (dummy)."""
    return render_template('movements/index.html', title='Movimientos')


@bp.get('/api/cash-counts')
@login_required
@module_required('movements')
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


def _is_admin_user() -> bool:
    try:
        return bool(
            getattr(current_user, 'is_master', False)
            or (str(getattr(current_user, 'role', '') or '').strip() in {'admin', 'company_admin', 'zentral_admin'})
        )
    except Exception:
        return False


def _can_withdraw_cash() -> bool:
    try:
        if getattr(current_user, 'is_master', False):
            return True
        if getattr(current_user, 'can', None) and (current_user.can('cash_withdrawals') or current_user.can('can_cash_withdrawal')):
            return True
    except Exception:
        return False
    return False


def _parse_date_iso(s: str | None) -> dt_date | None:
    raw = (s or '').strip()
    if not raw:
        return None
    try:
        return dt_date.fromisoformat(raw)
    except Exception:
        return None


def _num(v) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


@bp.get('/api/lookups/users')
@login_required
@module_required('movements')
def lookup_users_api():
    cid = _company_id()
    if not cid:
        return jsonify({'ok': True, 'items': []})

    q = (request.args.get('q') or '').strip().lower()
    try:
        limit = int(request.args.get('limit') or 80)
    except Exception:
        limit = 80
    if limit <= 0:
        limit = 80
    if limit > 200:
        limit = 200

    query = db.session.query(User).filter(User.company_id == cid).filter(User.active.is_(True))
    if q:
        like = f"%{q}%"
        query = query.filter(
            (User.username.ilike(like))
            | (User.display_name.ilike(like))
            | (User.email.ilike(like))
        )
    rows = query.order_by(User.display_name.asc().nullslast(), User.username.asc()).limit(limit).all()

    def _label(u: User) -> str:
        return (str(getattr(u, 'display_name', '') or '').strip() or str(getattr(u, 'username', '') or '').strip() or str(getattr(u, 'id', '') or '').strip())

    return jsonify({
        'ok': True,
        'items': [{'id': int(u.id), 'label': _label(u)} for u in (rows or [])],
    })


def _withdrawals_total(cid: str, d: dt_date) -> float:
    try:
        rows = (
            db.session.query(db.func.coalesce(db.func.sum(CashWithdrawal.monto), 0.0))
            .filter(CashWithdrawal.company_id == cid)
            .filter(CashWithdrawal.fecha_imputacion == d)
            .scalar()
        )
        return _num(rows)
    except Exception:
        return 0.0


@bp.get('/api/cash-withdrawals')
@login_required
@module_required('movements')
def list_cash_withdrawals_api():
    raw_date = (request.args.get('date') or '').strip()
    raw_from = (request.args.get('from') or '').strip()
    raw_to = (request.args.get('to') or '').strip()

    cid = _company_id()
    if not cid:
        return jsonify({'ok': True, 'items': []})

    d = _parse_date_iso(raw_date)
    d_from = _parse_date_iso(raw_from)
    d_to = _parse_date_iso(raw_to)

    q = db.session.query(CashWithdrawal).filter(CashWithdrawal.company_id == cid)
    if d:
        q = q.filter(CashWithdrawal.fecha_imputacion == d)
    else:
        if d_from:
            q = q.filter(CashWithdrawal.fecha_imputacion >= d_from)
        if d_to:
            q = q.filter(CashWithdrawal.fecha_imputacion <= d_to)
    q = q.order_by(CashWithdrawal.fecha_imputacion.desc(), CashWithdrawal.id.desc())

    rows = q.limit(5000).all()
    user_ids = set()
    for r in (rows or []):
        try:
            if r.usuario_registro_id:
                user_ids.add(int(r.usuario_registro_id))
            if r.usuario_responsable_id:
                user_ids.add(int(r.usuario_responsable_id))
        except Exception:
            pass

    users_map = {}
    if user_ids:
        try:
            for u in db.session.query(User).filter(User.id.in_(list(user_ids))).all():
                users_map[int(u.id)] = (getattr(u, 'display_name', None) or getattr(u, 'username', None) or str(u.id))
        except Exception:
            users_map = {}

    items = []
    for r in (rows or []):
        items.append({
            'id': int(getattr(r, 'id', 0) or 0),
            'fecha_imputacion': (r.fecha_imputacion.isoformat() if r.fecha_imputacion else None),
            'fecha_registro': (r.fecha_registro.isoformat() if r.fecha_registro else None),
            'monto': _num(r.monto),
            'nota': str(getattr(r, 'nota', '') or ''),
            'usuario_registro_id': (int(r.usuario_registro_id) if r.usuario_registro_id else None),
            'usuario_registro': users_map.get(int(r.usuario_registro_id)) if r.usuario_registro_id else None,
            'usuario_responsable_id': (int(r.usuario_responsable_id) if r.usuario_responsable_id else None),
            'usuario_responsable': users_map.get(int(r.usuario_responsable_id)) if r.usuario_responsable_id else None,
        })

    return jsonify({'ok': True, 'items': items})


@bp.post('/api/cash-withdrawals')
@login_required
@module_required('movements')
def create_cash_withdrawal_api():
    if not _can_withdraw_cash():
        return jsonify({'ok': False, 'error': 'forbidden'}), 403

    payload = request.get_json(silent=True) or {}
    d = _parse_date_iso(str(payload.get('fecha_imputacion') or payload.get('date') or ''))
    if not d:
        d = dt_date.today()

    today = dt_date.today()
    if (not _is_admin_user()) and d != today:
        return jsonify({'ok': False, 'error': 'forbidden_date', 'message': 'Solo admin puede imputar a fechas anteriores.'}), 403

    amount = _num(payload.get('monto') or payload.get('amount'))
    if amount <= 0.0:
        return jsonify({'ok': False, 'error': 'invalid_amount', 'message': 'El monto debe ser mayor a 0.'}), 400

    note = str(payload.get('nota') or payload.get('note') or '').strip()
    if d != today and not note:
        return jsonify({'ok': False, 'error': 'note_required', 'message': 'La nota es obligatoria para retiros retroactivos.'}), 400

    responsible_id = payload.get('usuario_responsable_id') or payload.get('responsible_user_id')
    try:
        responsible_id = int(responsible_id) if responsible_id is not None and str(responsible_id).strip() else None
    except Exception:
        responsible_id = None

    if not responsible_id:
        try:
            responsible_id = int(getattr(current_user, 'id', 0) or 0) or None
        except Exception:
            responsible_id = None

    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    if responsible_id:
        try:
            uresp = db.session.query(User).filter(User.company_id == cid, User.id == int(responsible_id)).first()
            if not uresp or not bool(getattr(uresp, 'active', True)):
                return jsonify({'ok': False, 'error': 'invalid_responsible', 'message': 'Responsable inválido.'}), 400
        except Exception:
            return jsonify({'ok': False, 'error': 'invalid_responsible', 'message': 'Responsable inválido.'}), 400

    cash_row = db.session.query(CashCount).filter(CashCount.company_id == cid, CashCount.count_date == d).first()
    opening = _num(getattr(cash_row, 'opening_amount', 0.0) if cash_row else 0.0)
    cash_day = _num(getattr(cash_row, 'cash_day_amount', 0.0) if cash_row else 0.0)

    already = _withdrawals_total(cid, d)
    available = opening + cash_day - already
    if amount - available > 0.00001:
        return jsonify({'ok': False, 'error': 'insufficient_cash', 'message': 'El retiro supera el efectivo disponible proyectado del día.'}), 400

    row = CashWithdrawal(company_id=cid)
    row.fecha_imputacion = d
    row.monto = amount
    row.nota = note or None
    try:
        row.usuario_registro_id = int(getattr(current_user, 'id', 0) or 0) or None
    except Exception:
        row.usuario_registro_id = None
    row.usuario_responsable_id = responsible_id

    db.session.add(row)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        try:
            from flask import current_app
            current_app.logger.exception('create_cash_withdrawal_api failed')
        except Exception:
            pass
        return jsonify({'ok': False, 'error': 'internal_server_error', 'message': 'No se pudo guardar el retiro.'}), 500

    try:
        label_user = str(getattr(current_user, 'display_name', '') or getattr(current_user, 'username', '') or '').strip()
        desc_parts = [
            'Retiro de efectivo: $' + f"{float(amount or 0.0):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'),
        ]
        if responsible_id:
            nm = users_map.get(int(responsible_id)) if 'users_map' in locals() else None
            if nm:
                desc_parts.append('Responsable: ' + str(nm))
        elif label_user:
            desc_parts.append('Responsable: ' + label_user)

        ev = CalendarEvent(
            company_id=cid,
            title='Retiro de efectivo',
            description='\n'.join(desc_parts),
            event_date=d,
            priority='media',
            color='slate',
            source_module='caja',
            event_type='retiro_efectivo',
            is_system=False,
            created_by_user_id=int(getattr(current_user, 'id', 0) or 0) or None,
            assigned_user_id=(responsible_id or None),
        )
        db.session.add(ev)
        db.session.commit()
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass

    return jsonify({'ok': True, 'item': {'id': int(row.id), 'fecha_imputacion': row.fecha_imputacion.isoformat(), 'monto': row.monto}})


@bp.delete('/api/cash-withdrawals/<int:withdrawal_id>')
@login_required
@module_required('movements')
def delete_cash_withdrawal_api(withdrawal_id: int):
    if not _can_withdraw_cash():
        return jsonify({'ok': False, 'error': 'forbidden'}), 403

    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    row = db.session.query(CashWithdrawal).filter(CashWithdrawal.company_id == cid, CashWithdrawal.id == int(withdrawal_id)).first()
    if not row:
        return jsonify({'ok': True, 'deleted': True})

    d = getattr(row, 'fecha_imputacion', None)
    if d and (not _is_admin_user()) and d != dt_date.today():
        return jsonify({'ok': False, 'error': 'forbidden_date', 'message': 'Solo admin puede eliminar retiros retroactivos.'}), 403

    try:
        db.session.query(CalendarEvent)
        db.session.query(CalendarEvent).filter(
            CalendarEvent.company_id == cid,
            CalendarEvent.source_module == 'caja',
            CalendarEvent.event_type == 'retiro_efectivo',
            CalendarEvent.event_date == d,
            CalendarEvent.description.isnot(None),
        )
    except Exception:
        pass

    db.session.delete(row)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400

    try:
        db.session.query(CalendarEvent).filter(
            CalendarEvent.company_id == cid,
            CalendarEvent.source_module == 'caja',
            CalendarEvent.event_type == 'retiro_efectivo',
            CalendarEvent.event_date == d,
        ).delete(synchronize_session=False)
        db.session.commit()
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass

    return jsonify({'ok': True, 'deleted': True})
