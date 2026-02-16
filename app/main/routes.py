from datetime import date, timedelta

from datetime import datetime

from flask import jsonify, render_template, request, current_app
from flask_login import login_required, current_user
from sqlalchemy.exc import ProgrammingError, OperationalError
from sqlalchemy import inspect, text

from app import db
from app.models import CalendarEvent, CalendarUserConfig, CashWithdrawal, User, Installment, InstallmentPlan
from app.permissions import module_required, module_required_any
from app.main import bp


_CASH_WITHDRAWALS_TABLE_MISSING_LOGGED = False
_CASH_WITHDRAWALS_TABLE_ENSURED = False


def _is_cash_withdrawals_missing_error(e: Exception) -> bool:
    try:
        msg = str(getattr(e, 'orig', '') or e)
    except Exception:
        msg = ''
    msg_low = msg.lower()
    return ('cash_withdrawals' in msg_low) and (('does not exist' in msg_low) or ('undefinedtable' in msg_low))


def _ensure_cash_withdrawals_table() -> bool:
    """Best-effort failsafe for prod when migrations weren't applied.

    Returns True if the table exists or was created, False otherwise.
    """
    global _CASH_WITHDRAWALS_TABLE_ENSURED
    if _CASH_WITHDRAWALS_TABLE_ENSURED:
        return True
    try:
        engine = db.engine
        insp = inspect(engine)
        if insp.has_table('cash_withdrawals'):
            _CASH_WITHDRAWALS_TABLE_ENSURED = True
            return True

        ddl = """
        CREATE TABLE IF NOT EXISTS cash_withdrawals (
            id SERIAL PRIMARY KEY,
            company_id VARCHAR(36) NOT NULL,
            fecha_imputacion DATE NOT NULL,
            fecha_registro TIMESTAMP NOT NULL,
            monto DOUBLE PRECISION NOT NULL DEFAULT 0,
            nota TEXT NULL,
            usuario_registro_id INTEGER NULL,
            usuario_responsable_id INTEGER NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
        """
        idx1 = "CREATE INDEX IF NOT EXISTS ix_cash_withdrawals_company_id ON cash_withdrawals(company_id);"
        idx2 = "CREATE INDEX IF NOT EXISTS ix_cash_withdrawals_fecha_imputacion ON cash_withdrawals(fecha_imputacion);"
        idx3 = "CREATE INDEX IF NOT EXISTS ix_cash_withdrawals_company_imputacion ON cash_withdrawals(company_id, fecha_imputacion);"
        with engine.begin() as conn:
            conn.execute(text(ddl))
            conn.execute(text(idx1))
            conn.execute(text(idx2))
            conn.execute(text(idx3))

        _CASH_WITHDRAWALS_TABLE_ENSURED = True
        try:
            current_app.logger.warning('Applied failsafe: created cash_withdrawals table (migrations were missing)')
        except Exception:
            pass
        return True
    except Exception:
        return False


def _company_id() -> str:
    try:
        from flask import g
        return str(getattr(g, 'company_id', '') or '').strip()
    except Exception:
        return ''


def _can_cash_withdrawal() -> bool:
    try:
        if getattr(current_user, 'is_master', False):
            return True
        if getattr(current_user, 'can', None) and (current_user.can('can_cash_withdrawal') or current_user.can('cash_withdrawals')):
            return True
    except Exception:
        return False
    return False


def _parse_date_iso(raw: str | None):
    s = str(raw or '').strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except Exception:
        return None


def _num(v) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _parse_amount_ars(raw) -> float | None:
    try:
        s = str(raw or '').strip()
    except Exception:
        s = ''
    if not s:
        return None
    s = s.replace(' ', '').replace('$', '')
    # Remove thousands separators like 10.000 -> 10000
    try:
        import re
        s = re.sub(r'\.(?=\d{3}(?:\D|$))', '', s)
    except Exception:
        pass
    s = s.replace(',', '.')
    cleaned = []
    for ch in s:
        if ch.isdigit() or ch in {'.', '-'}:
            cleaned.append(ch)
    s2 = ''.join(cleaned)
    if not s2:
        return None
    try:
        val = float(s2)
    except Exception:
        return None
    if not (val > 0):
        return None
    return val


@bp.get('/api/lookups/users')
@login_required
@module_required_any('sales', 'movements', 'settings')
def lookup_users_root_api():
    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'unauthorized', 'message': 'No autorizado o sesión expirada.'}), 401

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

    return jsonify({'ok': True, 'items': [{'id': int(u.id), 'label': _label(u)} for u in (rows or [])]})


@bp.get('/api/cash-withdrawals')
@login_required
@module_required_any('sales', 'movements')
def list_cash_withdrawals_root_api():
    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'unauthorized', 'message': 'No autorizado o sesión expirada.'}), 401

    d_from = _parse_date_iso(request.args.get('date_from') or request.args.get('from'))
    d_to = _parse_date_iso(request.args.get('date_to') or request.args.get('to'))
    d = _parse_date_iso(request.args.get('date'))

    q = db.session.query(CashWithdrawal).filter(CashWithdrawal.company_id == cid)
    if d:
        q = q.filter(CashWithdrawal.fecha_imputacion == d)
    else:
        if d_from:
            q = q.filter(CashWithdrawal.fecha_imputacion >= d_from)
        if d_to:
            q = q.filter(CashWithdrawal.fecha_imputacion <= d_to)
    q = q.order_by(CashWithdrawal.fecha_imputacion.desc(), CashWithdrawal.id.desc())

    try:
        rows = q.limit(5000).all()
    except (ProgrammingError, OperationalError) as e:
        if _is_cash_withdrawals_missing_error(e):
            global _CASH_WITHDRAWALS_TABLE_MISSING_LOGGED
            if not _CASH_WITHDRAWALS_TABLE_MISSING_LOGGED:
                _CASH_WITHDRAWALS_TABLE_MISSING_LOGGED = True
                try:
                    current_app.logger.warning('cash_withdrawals table missing; attempting failsafe create')
                except Exception:
                    pass
            if _ensure_cash_withdrawals_table():
                rows = q.limit(5000).all()
            else:
                return jsonify({'ok': False, 'error': 'service_unavailable', 'details': 'cash_withdrawals_table_missing'}), 503
        else:
            raise
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

    return jsonify({
        'ok': True,
        'items': [
            {
                'id': int(r.id),
                'date_imputation': (r.fecha_imputacion.isoformat() if r.fecha_imputacion else None),
                'amount': _num(r.monto),
                'responsible_user_id': (int(r.usuario_responsable_id) if r.usuario_responsable_id else None),
                'responsible': users_map.get(int(r.usuario_responsable_id)) if r.usuario_responsable_id else None,
                'note': str(getattr(r, 'nota', '') or ''),
                'created_by_user_id': (int(r.usuario_registro_id) if r.usuario_registro_id else None),
                'created_by': users_map.get(int(r.usuario_registro_id)) if r.usuario_registro_id else None,
                'created_at': (r.fecha_registro.isoformat() if r.fecha_registro else None),
            }
            for r in (rows or [])
        ],
    })


@bp.post('/api/cash-withdrawals')
@login_required
@module_required_any('sales', 'movements')
def create_cash_withdrawal_root_api():
    if not _can_cash_withdrawal():
        return jsonify({'ok': False, 'error': 'forbidden', 'message': 'No tenés permisos.'}), 403

    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'unauthorized', 'message': 'No autorizado o sesión expirada.'}), 401

    payload = request.get_json(silent=True) or {}

    raw_date = payload.get('date_imputation') or payload.get('fecha_imputacion') or payload.get('date')
    if raw_date is None or str(raw_date).strip() == '':
        return jsonify({'ok': False, 'error': 'validation_error', 'details': 'date_required', 'message': 'Seleccioná la fecha de imputación.'}), 400
    d = _parse_date_iso(str(raw_date))
    if not d:
        return jsonify({'ok': False, 'error': 'validation_error', 'details': 'date_invalid', 'message': 'Fecha de imputación inválida.'}), 400

    amount = _parse_amount_ars(payload.get('amount') if 'amount' in payload else payload.get('monto'))
    if amount is None:
        return jsonify({'ok': False, 'error': 'validation_error', 'details': 'amount_invalid', 'message': 'El monto debe ser mayor a 0.'}), 400

    responsible_id = payload.get('responsible_user_id') or payload.get('usuario_responsable_id')
    try:
        responsible_id = int(responsible_id) if responsible_id is not None and str(responsible_id).strip() else None
    except Exception:
        responsible_id = None
    if not responsible_id:
        try:
            responsible_id = int(getattr(current_user, 'id', 0) or 0) or None
        except Exception:
            responsible_id = None

    if not responsible_id:
        return jsonify({'ok': False, 'error': 'validation_error', 'details': 'responsible_required', 'message': 'Seleccioná un responsable.'}), 400

    note = str(payload.get('note') or payload.get('nota') or '').strip() or None

    if responsible_id:
        try:
            uresp = db.session.query(User).filter(User.company_id == cid, User.id == int(responsible_id)).first()
            if not uresp or not bool(getattr(uresp, 'active', True)):
                return jsonify({'ok': False, 'error': 'validation_error', 'details': 'responsible_invalid', 'message': 'Responsable inválido.'}), 400
        except Exception:
            return jsonify({'ok': False, 'error': 'validation_error', 'details': 'responsible_invalid', 'message': 'Responsable inválido.'}), 400

    row = CashWithdrawal(company_id=cid)
    row.fecha_imputacion = d
    row.monto = amount
    row.nota = note
    try:
        now_dt = datetime.utcnow()
        row.fecha_registro = now_dt
        row.created_at = now_dt
        row.updated_at = now_dt
    except Exception:
        pass
    try:
        row.usuario_registro_id = int(getattr(current_user, 'id', 0) or 0) or None
    except Exception:
        row.usuario_registro_id = None
    row.usuario_responsable_id = responsible_id

    db.session.add(row)
    try:
        db.session.commit()
    except (ProgrammingError, OperationalError) as e:
        try:
            db.session.rollback()
        except Exception:
            pass
        msg = str(getattr(e, 'orig', '') or e)
        if _is_cash_withdrawals_missing_error(e):
            global _CASH_WITHDRAWALS_TABLE_MISSING_LOGGED
            if not _CASH_WITHDRAWALS_TABLE_MISSING_LOGGED:
                _CASH_WITHDRAWALS_TABLE_MISSING_LOGGED = True
                try:
                    current_app.logger.warning('cash_withdrawals table missing; attempting failsafe create')
                except Exception:
                    pass
            if _ensure_cash_withdrawals_table():
                try:
                    db.session.add(row)
                    db.session.commit()
                except Exception:
                    try:
                        db.session.rollback()
                    except Exception:
                        pass
                    return jsonify({'ok': False, 'error': 'internal_server_error', 'message': 'No se pudo guardar el retiro.'}), 500
            else:
                return jsonify({'ok': False, 'error': 'service_unavailable', 'details': 'cash_withdrawals_table_missing', 'message': 'Sistema en mantenimiento. Intentá nuevamente en unos minutos.'}), 503
        try:
            current_app.logger.warning('create_cash_withdrawal_root_api db error: %s', msg)
        except Exception:
            pass
        return jsonify({'ok': False, 'error': 'internal_server_error', 'message': 'No se pudo guardar el retiro.'}), 500
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
        try:
            current_app.logger.exception('create_cash_withdrawal_root_api failed')
        except Exception:
            pass
        return jsonify({'ok': False, 'error': 'internal_server_error', 'message': 'No se pudo guardar el retiro.'}), 500

    # Calendar note (informative)
    try:
        responsible_label = ''
        try:
            if responsible_id:
                uresp = db.session.query(User).filter(User.id == int(responsible_id)).first()
                responsible_label = str(getattr(uresp, 'display_name', '') or getattr(uresp, 'username', '') or '').strip()
        except Exception:
            responsible_label = ''
        created_by_label = str(getattr(current_user, 'display_name', '') or getattr(current_user, 'username', '') or '').strip()
        money = f"{float(amount or 0.0):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        details = []
        if responsible_label:
            details.append('Responsable: ' + responsible_label)
        if note:
            details.append('Nota: ' + note)
        if created_by_label:
            details.append('Registrado por: ' + created_by_label)
        ev = CalendarEvent(
            company_id=cid,
            title=f"Retiro de caja: ${money}",
            description='\n'.join(details) if details else None,
            event_date=d,
            priority='media',
            color='slate',
            source_module='caja',
            event_type='retiro_efectivo',
            is_system=False,
            created_by_user_id=int(getattr(current_user, 'id', 0) or 0) or None,
            assigned_user_id=(responsible_id or None),
            status='open',
        )
        db.session.add(ev)
        db.session.commit()
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass

    return jsonify({
        'ok': True,
        'withdrawal': {
            'id': int(row.id),
            'date_imputation': row.fecha_imputacion.isoformat(),
            'amount': _num(row.monto),
            'responsible_user_id': (int(row.usuario_responsable_id) if row.usuario_responsable_id else None),
            'note': str(getattr(row, 'nota', '') or ''),
            'created_by_user_id': (int(row.usuario_registro_id) if row.usuario_registro_id else None),
            'created_at': (row.fecha_registro.isoformat() if row.fecha_registro else None),
        }
    })


def _has_active_installments(company_id: str) -> bool:
    cid = str(company_id or '').strip()
    if not cid:
        return False
    try:
        row = (
            db.session.query(Installment.id)
            .join(InstallmentPlan, Installment.plan_id == InstallmentPlan.id)
            .filter(Installment.company_id == cid)
            .filter(InstallmentPlan.company_id == cid)
            .filter(db.func.lower(InstallmentPlan.status) == 'activo')
            .filter(db.func.lower(Installment.status) != 'pagada')
            .limit(1)
            .first()
        )
        return bool(row is not None)
    except Exception:
        return False


@bp.get('/api/customers/has_active_installments')
@login_required
@module_required_any('settings', 'customers', 'sales')
def has_active_installments_api():
    cid = _company_id()
    return jsonify({'has_active_installments': bool(_has_active_installments(cid))})


@bp.route('/')
@bp.route('/index')
@login_required
@module_required('dashboard')
def index():
    """Dashboard temporal sin acceso a base de datos.

    Muestra métricas en cero y listas vacías hasta que se implemente
    una nueva capa de persistencia.
    """
    today_revenue = 0
    weekly_revenue = 0
    monthly_revenue = 0
    recent_sales = []
    low_stock_products = []

    upcoming_calendar_events = []
    if getattr(current_user, 'can', None) and current_user.can('calendar'):
        cid = _company_id()
        if not cid:
            return render_template(
                'main/index.html',
                title='Dashboard',
                today_revenue=today_revenue,
                weekly_revenue=weekly_revenue,
                monthly_revenue=monthly_revenue,
                recent_sales=recent_sales,
                low_stock_products=low_stock_products,
                upcoming_calendar_events=upcoming_calendar_events,
            )
        cfg = None
        if cid:
            cfg = (
                db.session.query(CalendarUserConfig)
                .filter(CalendarUserConfig.company_id == cid, CalendarUserConfig.user_id == current_user.id)
                .first()
            )
        cfg_data = cfg.get_config() if cfg else {}
        dashboard_enabled = True
        if isinstance(cfg_data, dict):
            dashboard_enabled = bool(cfg_data.get('dashboard_integration', True))

        if dashboard_enabled:
            today = date.today()
            horizon = today + timedelta(days=365)

            q = db.session.query(CalendarEvent)
            q = q.filter(CalendarEvent.status != 'done')
            q = q.filter(CalendarEvent.event_date <= horizon)
            if cid:
                q = q.filter(CalendarEvent.company_id == cid)
            q = q.filter((CalendarEvent.assigned_user_id.is_(None)) | (CalendarEvent.assigned_user_id == current_user.id))

            events = list(q.order_by(CalendarEvent.event_date.asc(), CalendarEvent.id.asc()).limit(50).all())

            events.sort(key=lambda ev: (ev.event_date, getattr(ev, 'id', 0) or 0))

            def is_critical(ev: CalendarEvent) -> bool:
                pr = (ev.priority or '').lower()
                if pr in {'alta', 'critica'}:
                    return True
                if ev.event_date < today:
                    return True
                return False

            for ev in events:
                if not is_critical(ev):
                    continue
                upcoming_calendar_events.append({
                    'id': ev.id,
                    'title': ev.title,
                    'date': ev.event_date,
                    'priority': ev.priority,
                    'color': ev.color,
                    'overdue': bool(ev.event_date < today),
                })
                if len(upcoming_calendar_events) >= 6:
                    break

    return render_template(
        'main/index.html',
        title='Dashboard',
        today_revenue=today_revenue,
        weekly_revenue=weekly_revenue,
        monthly_revenue=monthly_revenue,
        recent_sales=recent_sales,
        low_stock_products=low_stock_products,
        upcoming_calendar_events=upcoming_calendar_events,
    )
