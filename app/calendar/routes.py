import calendar as py_calendar
from datetime import date, datetime, timedelta
import json

from flask import render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user
from sqlalchemy import func, text
from sqlalchemy.exc import IntegrityError

from app import db
from app.calendar import bp
from app.models import CalendarEvent, CalendarUserConfig, CashCount, Customer, Employee, Expense, InventoryLot, Product, Sale
from app.permissions import module_required


def _default_calendar_config():
    return {
        "views": ["mensual", "semanal", "diaria", "lista"],
        "event_sources": {
            "clientes": {
                "cumpleanos": True,
                "deuda_vencida": True,
                "deuda_critica": True,
            },
            "proveedores": {
                "deuda_vencida": True,
                "proximo_vencimiento": True,
            },
            "inventario": {
                "stock_critico": True,
                "reposicion": True,
            },
            "movimientos": {
                "arqueo_pendiente": True,
                "diferencias_caja": True,
            },
            "empleados": {
                "cumpleanos": True,
            },
            "manual": {
                "notas_avisos": True,
            },
        },
        "dashboard_integration": True,
    }


def _normalize_event_type(source_module: str, event_type: str) -> str:
    sm = (source_module or '').strip().lower()
    et = (event_type or '').strip().lower()
    if sm == 'clientes' and et == 'deudas':
        return 'deuda_vencida'
    if sm == 'movimientos' and et == 'arqueo_caja':
        return 'arqueo_pendiente'
    if sm == 'movimientos' and et == 'vencimientos_financieros':
        return 'sin_cerrar'
    if sm == 'manual' and et in {'nota', 'notas'}:
        return 'notas_avisos'
    if sm == 'empleados' and et == 'avisos':
        return 'recordatorios_internos'
    return et


def _load_meta(meta_json: str | None) -> dict:
    raw = meta_json if isinstance(meta_json, str) else ''
    if not raw:
        return {}
    try:
        out = json.loads(raw)
        return out if isinstance(out, dict) else {}
    except Exception:
        return {}


def _company_id() -> str:
    try:
        from flask import g
        return str(getattr(g, 'company_id', '') or '').strip()
    except Exception:
        return ''


def _get_user_config():
    uid = int(getattr(current_user, 'id', 0) or 0)
    cid = _company_id()
    if not uid:
        return None

    # This function must be idempotent.
    # Some existing SQLite DBs may still have a legacy UNIQUE(user_id) constraint
    # (instead of UNIQUE(company_id, user_id)), so inserting blindly can crash.
    # Always try to reuse the existing row for the user before attempting an insert.

    with db.session.no_autoflush:
        try:
            row = db.session.execute(
                text('SELECT id, company_id FROM calendar_user_config WHERE user_id = :uid LIMIT 1'),
                {'uid': uid},
                execution_options={'_sqlite_tenant_guard_applied': True},
            ).fetchone()
        except Exception:
            row = None

    existing_id = None
    existing_cid = ''
    if row:
        try:
            existing_id = int(row[0])
        except Exception:
            existing_id = None
        try:
            existing_cid = str(row[1] or '').strip()
        except Exception:
            existing_cid = ''

    if existing_id is not None:
        if cid and existing_cid != cid:
            try:
                db.session.execute(
                    text('UPDATE calendar_user_config SET company_id = :cid WHERE user_id = :uid'),
                    {'cid': cid, 'uid': uid},
                    execution_options={'_sqlite_tenant_guard_applied': True},
                )
                db.session.commit()
            except Exception:
                try:
                    db.session.rollback()
                except Exception:
                    pass

        with db.session.no_autoflush:
            cfg = db.session.get(CalendarUserConfig, existing_id)
            if not cfg:
                cfg = (
                    db.session.query(CalendarUserConfig)
                    .execution_options(_sqlite_tenant_guard_applied=True)
                    .filter(CalendarUserConfig.user_id == uid)
                    .first()
                )
        if cfg:
            return cfg

    # No existing row: create a new one.
    cfg = CalendarUserConfig(user_id=uid)
    # Keep company_id non-null even for users without a tenant (e.g. zentral_admin).
    cfg.company_id = cid or ''
    cfg.set_config(_default_calendar_config())
    db.session.add(cfg)
    try:
        db.session.commit()
        return cfg
    except IntegrityError:
        # Another request/user session already created it (or legacy UNIQUE(user_id)).
        try:
            db.session.rollback()
        except Exception:
            pass
        with db.session.no_autoflush:
            cfg2 = (
                db.session.query(CalendarUserConfig)
                .execution_options(_sqlite_tenant_guard_applied=True)
                .filter(CalendarUserConfig.user_id == uid)
                .first()
            )
        if cfg2 and cid:
            try:
                db.session.execute(
                    text('UPDATE calendar_user_config SET company_id = :cid WHERE user_id = :uid'),
                    {'cid': cid, 'uid': uid},
                    execution_options={'_sqlite_tenant_guard_applied': True},
                )
                db.session.commit()
            except Exception:
                try:
                    db.session.rollback()
                except Exception:
                    pass
        if cfg2:
            return cfg2

        # Last resort: return a non-persisted config to avoid breaking the calendar page.
        fallback = CalendarUserConfig(user_id=uid, company_id=(cid or ''))
        fallback.set_config(_default_calendar_config())
        return fallback


def _month_bounds(year: int, month: int):
    start = date(year, month, 1)
    last_day = py_calendar.monthrange(year, month)[1]
    end = date(year, month, last_day)
    return start, end


def _trim_trailing_empty_weeks(weeks: list[list[date]], month: int):
    if not weeks:
        return weeks
    while len(weeks) > 4:
        last_week = weeks[-1]
        if any(d.month == month for d in last_week):
            break
        weeks.pop()
    return weeks


def _priority_color(priority: str, color: str | None):
    if color:
        return color
    p = (priority or "").lower()
    if p in {"alta", "critica"}:
        return "red"
    if p in {"media"}:
        return "yellow"
    return "green"


def _sanitize_priority(v: str | None) -> str:
    p = (v or 'media').strip().lower()
    if p not in {'baja', 'media', 'alta', 'critica'}:
        return 'media'
    return p


def _sanitize_source_module(v: str | None) -> str:
    raw = (v or '').strip().lower()
    allowed = {
        'manual',
        'clientes',
        'proveedores',
        'inventario',
        'movimientos',
        'empleados',
    }
    return raw if raw in allowed else 'manual'


def _is_source_enabled(cfg_data: dict, source_module: str, event_type: str) -> bool:
    sm = (source_module or '').strip().lower() or 'manual'
    et = _normalize_event_type(sm, event_type)

    # UX cleanup: removed/noisy legacy modules & items
    if sm in {'ventas', 'configuracion', 'sistema'}:
        return False
    if sm == 'clientes' and et == 'inactivos':
        return False
    if sm == 'empleados' and et == 'licencias':
        return False

    if not isinstance(cfg_data, dict):
        cfg_data = _default_calendar_config()
    sources = cfg_data.get('event_sources')
    if not isinstance(sources, dict):
        sources = _default_calendar_config().get('event_sources')
    src = sources.get(sm)
    if not isinstance(src, dict):
        return False
    if sm == 'manual':
        return bool(src.get('notas_avisos', True))
    return bool(src.get(et, True))


def _get_system_events(cfg_data: dict, start: date, end: date):
    cid = _company_id()
    if not cid:
        return []

    today = date.today()
    out: list[CalendarEvent] = []

    def _fmt_money(v: float) -> str:
        try:
            return f"{float(v or 0.0):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        except Exception:
            return '0,00'

    def _add(*, title: str, description: str | None, d: date, priority: str, source_module: str, event_type: str):
        out.append(
            CalendarEvent(
                company_id=cid,
                title=title,
                description=description,
                event_date=d,
                priority=priority,
                color=_priority_color(priority, None),
                source_module=source_module,
                event_type=event_type,
                is_system=True,
                assigned_user_id=None,
                created_by_user_id=None,
                status='open',
            )
        )

    # Clientes · Cumpleaños
    if _is_source_enabled(cfg_data, 'clientes', 'cumpleanos'):
        rows = (
            db.session.query(Customer)
            .filter(Customer.company_id == cid)
            .filter(Customer.birthday.isnot(None))
            .limit(5000)
            .all()
        )
        for c in rows:
            b = getattr(c, 'birthday', None)
            if not b:
                continue
            nm = (str(getattr(c, 'name', '') or '').strip() or (str(getattr(c, 'first_name', '') or '').strip() + ' ' + str(getattr(c, 'last_name', '') or '').strip()).strip())
            nm = nm or 'Cliente'
            for y in {start.year, end.year}:
                try:
                    d = date(y, b.month, b.day)
                except Exception:
                    continue
                if start <= d <= end:
                    _add(title='Cumpleaños: ' + nm, description=None, d=d, priority='baja', source_module='clientes', event_type='cumpleanos')

    # Empleados · Cumpleaños
    if _is_source_enabled(cfg_data, 'empleados', 'cumpleanos'):
        rows = (
            db.session.query(Employee)
            .filter(Employee.company_id == cid)
            .filter(Employee.birth_date.isnot(None))
            .limit(5000)
            .all()
        )
        for e in rows:
            b = getattr(e, 'birth_date', None)
            if not b:
                continue
            nm = (str(getattr(e, 'name', '') or '').strip() or (str(getattr(e, 'first_name', '') or '').strip() + ' ' + str(getattr(e, 'last_name', '') or '').strip()).strip())
            nm = nm or 'Empleado'
            for y in {start.year, end.year}:
                try:
                    d = date(y, b.month, b.day)
                except Exception:
                    continue
                if start <= d <= end:
                    _add(title='Cumpleaños: ' + nm, description=None, d=d, priority='baja', source_module='empleados', event_type='cumpleanos')

    # Clientes · Deuda vencida / crítica (heurística: +30/+60 días desde venta con saldo)
    wants_v = _is_source_enabled(cfg_data, 'clientes', 'deuda_vencida')
    wants_c = _is_source_enabled(cfg_data, 'clientes', 'deuda_critica')
    if wants_v or wants_c:
        lookback = start - timedelta(days=120)
        rows = (
            db.session.query(Sale)
            .filter(Sale.company_id == cid)
            .filter(Sale.sale_type == 'Venta')
            .filter(Sale.status != 'Reemplazada')
            .filter(Sale.due_amount > 0)
            .filter(Sale.sale_date >= lookback)
            .filter(Sale.sale_date <= end)
            .all()
        )
        vencida_map: dict[tuple[str, date], float] = {}
        critica_map: dict[tuple[str, date], float] = {}
        for s in rows:
            sd = getattr(s, 'sale_date', None)
            if not sd:
                continue
            cust = str(getattr(s, 'customer_name', '') or '').strip() or str(getattr(s, 'customer_id', '') or '').strip() or 'Cliente'
            due = float(getattr(s, 'due_amount', 0.0) or 0.0)
            d30 = sd + timedelta(days=30)
            d60 = sd + timedelta(days=60)
            if start <= d30 <= end:
                vencida_map[(cust, d30)] = vencida_map.get((cust, d30), 0.0) + due
            if start <= d60 <= end:
                critica_map[(cust, d60)] = critica_map.get((cust, d60), 0.0) + due

        if wants_v:
            for (cust, d30), amt in vencida_map.items():
                _add(
                    title='Deuda vencida: ' + cust,
                    description='Saldo: $' + _fmt_money(amt),
                    d=d30,
                    priority='media',
                    source_module='clientes',
                    event_type='deuda_vencida',
                )
        if wants_c:
            for (cust, d60), amt in critica_map.items():
                _add(
                    title='Deuda crítica: ' + cust,
                    description='Saldo: $' + _fmt_money(amt),
                    d=d60,
                    priority='alta',
                    source_module='clientes',
                    event_type='deuda_critica',
                )

    # Proveedores · Cuenta corriente (desde meta_json.supplier_cc.schedule)
    wants_past = _is_source_enabled(cfg_data, 'proveedores', 'deuda_vencida')
    wants_next = _is_source_enabled(cfg_data, 'proveedores', 'proximo_vencimiento')
    if wants_past or wants_next:
        rows = (
            db.session.query(Expense)
            .filter(Expense.company_id == cid)
            .order_by(Expense.expense_date.desc())
            .limit(5000)
            .all()
        )
        by_due: dict[tuple[str, date, str], float] = {}
        for r in rows:
            meta = _load_meta(getattr(r, 'meta_json', None))
            if meta.get('supplier_cc_payment') is True:
                continue
            cc = meta.get('supplier_cc')
            if not isinstance(cc, dict) or not cc.get('enabled'):
                continue
            schedule = cc.get('schedule')
            if not isinstance(schedule, list):
                continue
            sid = str(getattr(r, 'supplier_id', '') or '').strip()
            sname = str(getattr(r, 'supplier_name', '') or '').strip()
            supp = sname or sid or 'Proveedor'
            for inst in schedule:
                if not isinstance(inst, dict):
                    continue
                if str(inst.get('status') or '').strip() != 'open':
                    continue
                due_s = str(inst.get('due_date') or '').strip()
                rem = float(inst.get('remaining') or 0.0)
                if not due_s or rem <= 0:
                    continue
                try:
                    due_d = date.fromisoformat(due_s)
                except Exception:
                    continue
                if not (start <= due_d <= end):
                    continue
                kind = 'deuda_vencida' if due_d < today else 'proximo_vencimiento'
                by_due[(supp, due_d, kind)] = by_due.get((supp, due_d, kind), 0.0) + rem

        for (supp, due_d, kind), amt in by_due.items():
            if kind == 'deuda_vencida' and not wants_past:
                continue
            if kind == 'proximo_vencimiento' and not wants_next:
                continue
            pr = 'media' if kind == 'deuda_vencida' else 'baja'
            label = 'Deuda vencida' if kind == 'deuda_vencida' else 'Próximo vencimiento'
            _add(
                title=label + ': ' + supp,
                description='Monto: $' + _fmt_money(amt),
                d=due_d,
                priority=pr,
                source_module='proveedores',
                event_type=kind,
            )

    # Inventario · Stock crítico (resumen en hoy)
    if _is_source_enabled(cfg_data, 'inventario', 'stock_critico') and start <= today <= end:
        stock_subq = (
            db.session.query(
                InventoryLot.product_id.label('pid'),
                func.coalesce(func.sum(InventoryLot.qty_available), 0.0).label('stock'),
            )
            .filter(InventoryLot.company_id == cid)
            .group_by(InventoryLot.product_id)
            .subquery()
        )

        ps = (
            db.session.query(
                Product.id,
                Product.min_stock,
                Product.reorder_point,
                func.coalesce(stock_subq.c.stock, 0.0).label('stock'),
            )
            .outerjoin(stock_subq, stock_subq.c.pid == Product.id)
            .filter(Product.company_id == cid)
            .filter(Product.active.is_(True))
            .limit(5000)
            .all()
        )
        critical = 0
        needs_restock = 0
        for pid, min_stock_v, reorder_point_v, stock_v in (ps or []):
            stock = float(stock_v or 0.0)
            min_stock = float(min_stock_v or 0.0)
            reorder_point = float(reorder_point_v or 0.0)
            is_crit = (min_stock > 0 and stock <= min_stock) or (min_stock <= 0 and stock <= 0)
            if is_crit:
                critical += 1
            if reorder_point > 0 and stock <= reorder_point:
                needs_restock += 1
        if critical > 0:
            _add(
                title='Stock crítico: ' + str(critical) + ' productos',
                description='Revisá mínimos y reposición.',
                d=today,
                priority='critica',
                source_module='inventario',
                event_type='stock_critico',
            )

        if needs_restock > 0 and _is_source_enabled(cfg_data, 'inventario', 'reposicion'):
            _add(
                title='Reposición sugerida: ' + str(needs_restock) + ' productos',
                description='Revisá punto de reposición.',
                d=today,
                priority='alta',
                source_module='inventario',
                event_type='reposicion',
            )

    # Movimientos · Arqueo pendiente
    if _is_source_enabled(cfg_data, 'movimientos', 'arqueo_pendiente') and start <= today <= end:
        has_today = (
            db.session.query(CashCount.id)
            .filter(CashCount.company_id == cid)
            .filter(CashCount.count_date == today)
            .first()
        )
        if not has_today:
            _add(
                title='Arqueo pendiente',
                description='Registrá el arqueo del día.',
                d=today,
                priority='alta',
                source_module='movimientos',
                event_type='arqueo_pendiente',
            )

    # Movimientos · Diferencias de caja
    if _is_source_enabled(cfg_data, 'movimientos', 'diferencias_caja'):
        rows = (
            db.session.query(CashCount)
            .filter(CashCount.company_id == cid)
            .filter(CashCount.count_date >= start)
            .filter(CashCount.count_date <= end)
            .all()
        )
        for r in rows:
            d = getattr(r, 'count_date', None)
            if not d:
                continue
            diff = float(getattr(r, 'difference_amount', 0.0) or 0.0)
            if abs(diff) < 0.01:
                continue
            _add(
                title='Diferencia de caja',
                description='Diferencia: $' + _fmt_money(diff),
                d=d,
                priority='alta',
                source_module='movimientos',
                event_type='diferencias_caja',
            )

    return out


@bp.route('/', methods=['GET', 'POST'])
@bp.route('/index', methods=['GET', 'POST'])
@login_required
@module_required('calendar')
def index():
    cfg = _get_user_config()
    cfg_data = cfg.get_config()
    cid = _company_id()

    if request.method == 'POST':
        action = (request.form.get('action') or '').strip()

        if action == 'create_manual_event':
            title = (request.form.get('title') or '').strip()
            desc = (request.form.get('description') or '').strip()
            dt = (request.form.get('date') or '').strip()
            priority = _sanitize_priority(request.form.get('priority'))
            source_module = _sanitize_source_module(request.form.get('source_module'))

            if not cid:
                flash('Empresa inválida.', 'error')
                return redirect(url_for('calendar.index'))

            if not title or not dt:
                flash('Completá título y fecha.', 'error')
                return redirect(url_for('calendar.index'))

            try:
                d = datetime.strptime(dt, '%Y-%m-%d').date()
            except Exception:
                flash('Fecha inválida.', 'error')
                return redirect(url_for('calendar.index'))

            ev = CalendarEvent(
                company_id=cid,
                title=title,
                description=desc or None,
                event_date=d,
                priority=priority,
                color=_priority_color(priority, None),
                source_module=source_module,
                event_type='nota',
                is_system=False,
                assigned_user_id=None,
                created_by_user_id=current_user.id,
                status='open',
            )
            db.session.add(ev)
            db.session.commit()
            flash('Aviso creado.', 'success')
            return redirect(url_for('calendar.index', year=d.year, month=d.month))

        if action == 'update_manual_event':
            eid = request.form.get('event_id')
            ev = db.session.get(CalendarEvent, int(eid)) if eid and str(eid).isdigit() else None
            if not ev or ev.is_system:
                flash('Aviso inválido.', 'error')
                return redirect(url_for('calendar.index'))

            if cid and str(getattr(ev, 'company_id', '') or '') != cid:
                flash('Aviso inválido.', 'error')
                return redirect(url_for('calendar.index'))

            if ev.created_by_user_id != current_user.id and not getattr(current_user, 'is_master', False) and getattr(current_user, 'role', '') != 'admin':
                flash('No tenés permisos para editar este aviso.', 'error')
                return redirect(url_for('calendar.index'))

            title = (request.form.get('title') or '').strip()
            desc = (request.form.get('description') or '').strip()
            dt = (request.form.get('date') or '').strip()
            priority = _sanitize_priority(request.form.get('priority'))
            status = (request.form.get('status') or 'open').strip().lower()
            source_module = _sanitize_source_module(request.form.get('source_module'))

            if not title or not dt:
                flash('Completá título y fecha.', 'error')
                return redirect(url_for('calendar.index'))

            try:
                d = datetime.strptime(dt, '%Y-%m-%d').date()
            except Exception:
                flash('Fecha inválida.', 'error')
                return redirect(url_for('calendar.index'))

            ev.title = title
            ev.description = desc or None
            ev.event_date = d
            ev.priority = priority
            ev.color = _priority_color(priority, None)
            ev.source_module = source_module
            ev.status = status if status in {'open', 'done'} else 'open'
            db.session.commit()
            flash('Aviso actualizado.', 'success')
            return redirect(url_for('calendar.index', year=d.year, month=d.month))

        if action == 'delete_manual_event':
            eid = request.form.get('event_id')
            ev = db.session.get(CalendarEvent, int(eid)) if eid and str(eid).isdigit() else None
            if not ev or ev.is_system:
                flash('Aviso inválido.', 'error')
                return redirect(url_for('calendar.index'))

            if cid and str(getattr(ev, 'company_id', '') or '') != cid:
                flash('Aviso inválido.', 'error')
                return redirect(url_for('calendar.index'))

            if ev.created_by_user_id != current_user.id and not getattr(current_user, 'is_master', False) and getattr(current_user, 'role', '') != 'admin':
                flash('No tenés permisos para eliminar este aviso.', 'error')
                return redirect(url_for('calendar.index'))

            db.session.delete(ev)
            db.session.commit()
            flash('Aviso eliminado.', 'success')
            return redirect(url_for('calendar.index'))

        if action == 'save_calendar_config':
            cfg_data = cfg.get_config()
            sources = cfg_data.get('event_sources') if isinstance(cfg_data, dict) else None
            if not isinstance(sources, dict):
                sources = _default_calendar_config().get('event_sources')

            default_sources = _default_calendar_config().get('event_sources') or {}
            if isinstance(default_sources, dict):
                # Drop unknown/legacy modules so config can't reintroduce removed sources.
                sources = {k: (sources.get(k) if isinstance(sources.get(k), dict) else {}) for k in default_sources.keys()}

            def _set(path, value):
                cur = sources
                for p in path[:-1]:
                    cur = cur.setdefault(p, {})
                cur[path[-1]] = bool(value)

            _set(['clientes', 'cumpleanos'], request.form.get('src_clientes_cumpleanos') == 'on')
            _set(['clientes', 'deuda_vencida'], request.form.get('src_clientes_deuda_vencida') == 'on')
            _set(['clientes', 'deuda_critica'], request.form.get('src_clientes_deuda_critica') == 'on')

            _set(['proveedores', 'deuda_vencida'], request.form.get('src_proveedores_deuda_vencida') == 'on')
            _set(['proveedores', 'proximo_vencimiento'], request.form.get('src_proveedores_proximo_vencimiento') == 'on')

            _set(['inventario', 'stock_critico'], request.form.get('src_inventario_stock_critico') == 'on')
            _set(['inventario', 'reposicion'], request.form.get('src_inventario_reposicion') == 'on')

            _set(['movimientos', 'arqueo_pendiente'], request.form.get('src_movimientos_arqueo_pendiente') == 'on')
            _set(['movimientos', 'diferencias_caja'], request.form.get('src_movimientos_diferencias_caja') == 'on')

            _set(['empleados', 'cumpleanos'], request.form.get('src_empleados_cumpleanos') == 'on')

            _set(['manual', 'notas_avisos'], request.form.get('src_manual_notas_avisos') == 'on')

            cfg_data['event_sources'] = sources

            cfg_data['dashboard_integration'] = (request.form.get('calendar_dashboard_integration') == 'on')

            cfg.set_config(cfg_data)
            db.session.commit()
            flash('Configuración guardada.', 'success')
            return redirect(url_for('calendar.index'))

    view = (request.args.get('view') or 'month').strip().lower()
    range_mode = (request.args.get('range') or 'month').strip().lower()
    today = date.today()

    try:
        ref_year = int(request.args.get('year') or today.year)
    except Exception:
        ref_year = today.year
    try:
        ref_month = int(request.args.get('month') or today.month)
    except Exception:
        ref_month = today.month
    try:
        ref_day = int(request.args.get('day') or today.day)
    except Exception:
        ref_day = today.day

    try:
        ref_date = date(ref_year, ref_month, ref_day)
    except Exception:
        ref_date = today

    if view == 'list':
        if range_mode == 'day':
            start = ref_date
            end = ref_date
        elif range_mode == 'week':
            start = ref_date - timedelta(days=ref_date.weekday())
            end = start + timedelta(days=6)
        else:
            start, end = _month_bounds(ref_date.year, ref_date.month)
            range_mode = 'month'
        year = start.year
        month = start.month
    else:
        year = ref_date.year
        month = ref_date.month
        start, end = _month_bounds(year, month)

    events = []

    q = db.session.query(CalendarEvent).filter(CalendarEvent.event_date >= start, CalendarEvent.event_date <= end)
    if cid:
        q = q.filter(CalendarEvent.company_id == cid)
    q = q.filter((CalendarEvent.assigned_user_id.is_(None)) | (CalendarEvent.assigned_user_id == current_user.id))
    db_events = q.order_by(CalendarEvent.event_date.asc(), CalendarEvent.id.asc()).all()
    for ev in db_events:
        if _is_source_enabled(cfg_data, ev.source_module, ev.event_type):
            events.append(ev)

    sys_events = _get_system_events(cfg_data, start, end)
    for ev in sys_events:
        if _is_source_enabled(cfg_data, ev.source_module, ev.event_type):
            events.append(ev)
    events.sort(key=lambda ev: (ev.event_date, getattr(ev, 'id', 0) or 0))

    events_by_day = {}
    for ev in events:
        events_by_day.setdefault(ev.event_date.isoformat(), []).append(ev)

    cal = py_calendar.Calendar(firstweekday=0)
    weeks = []
    for week in cal.monthdatescalendar(year, month):
        weeks.append([{ 'date': d, 'in_month': (d.month == month), 'events': events_by_day.get(d.isoformat(), []) } for d in week])

    raw_weeks = [[cell['date'] for cell in w] for w in weeks]
    raw_weeks = _trim_trailing_empty_weeks(raw_weeks, month)
    weeks = []
    for w in raw_weeks:
        weeks.append([{ 'date': d, 'in_month': (d.month == month), 'events': events_by_day.get(d.isoformat(), []) } for d in w])

    if view == 'list':
        list_events = []
        for ev in events:
            vencido = bool(ev.status != 'done' and ev.event_date < today)
            list_events.append({'event': ev, 'overdue': vencido})
        list_events.sort(key=lambda x: (x['event'].event_date, getattr(x['event'], 'id', 0) or 0))

        groups = []
        for row in list_events:
            d = row['event'].event_date
            if not groups or groups[-1]['date'] != d:
                groups.append({'date': d, 'items': []})
            groups[-1]['items'].append(row)

        week_start = None
        if range_mode == 'day':
            prev_date = ref_date - timedelta(days=1)
            next_date = ref_date + timedelta(days=1)
        elif range_mode == 'week':
            week_start = ref_date - timedelta(days=ref_date.weekday())
            prev_date = ref_date - timedelta(days=7)
            next_date = ref_date + timedelta(days=7)
        else:
            prev_y, prev_m = ref_date.year, ref_date.month - 1
            if prev_m < 1:
                prev_m = 12
                prev_y -= 1
            next_y, next_m = ref_date.year, ref_date.month + 1
            if next_m > 12:
                next_m = 1
                next_y += 1
            prev_date = date(prev_y, prev_m, 1)
            next_date = date(next_y, next_m, 1)

        return render_template(
            'calendar/index.html',
            title='Calendario',
            view='list',
            year=year,
            month=month,
            today=today,
            weeks=weeks,
            events=groups,
            cfg=cfg_data,
            range_mode=range_mode,
            range_ref=ref_date,
            range_week_start=week_start,
            range_start=start,
            range_end=end,
            prev_date=prev_date,
            next_date=next_date,
        )

    return render_template(
        'calendar/index.html',
        title='Calendario',
        view='month',
        year=year,
        month=month,
        today=today,
        weeks=weeks,
        events=[],
        cfg=cfg_data,
    )
