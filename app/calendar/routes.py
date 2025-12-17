import calendar as py_calendar
from datetime import date, datetime, timedelta

from flask import render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user

from app import db
from app.calendar import bp
from app.models import CalendarEvent, CalendarUserConfig
from app.permissions import module_required


def _default_calendar_config():
    return {
        "views": ["mensual", "semanal", "diaria", "lista"],
        "event_sources": {
            "clientes": {"cumpleanos": True, "deudas": True, "inactivos": False},
            "empleados": {"cumpleanos": True, "avisos": True, "licencias": True},
            "ventas": {"vencimientos": True, "pagos_pactados": True},
            "movimientos": {"vencimientos_financieros": True, "arqueo_caja": True},
            "inventario": {"recordatorios": True},
            "gastos": {"recordatorios": True},
            "proveedores": {"recordatorios": True},
            "reportes": {"recordatorios": True},
            "configuracion": {"recordatorios": True},
            "sistema": {"cierres": True, "mantenimientos": True},
            "manual": {"notas": True},
        },
        "alerts": {
            "enabled": True,
            "rules": [
                {"days": 10, "level": "media"},
                {"days": 15, "level": "alta"},
                {"days": 30, "level": "critica"},
            ],
            "dashboard_integration": True,
        },
        "event_priority": {
            "default": {
                "baja": {"color": "green"},
                "media": {"color": "yellow"},
                "alta": {"color": "red"},
            },
            "user_editable": True,
        },
    }


def _get_user_config():
    cfg = db.session.query(CalendarUserConfig).filter_by(user_id=current_user.id).first()
    if cfg:
        return cfg
    cfg = CalendarUserConfig(user_id=current_user.id)
    cfg.set_config(_default_calendar_config())
    db.session.add(cfg)
    db.session.commit()
    return cfg


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


def _sanitize_source_module(v: str | None) -> str:
    raw = (v or '').strip().lower()
    allowed = {
        'manual',
        'clientes',
        'empleados',
        'ventas',
        'movimientos',
        'inventario',
        'gastos',
        'proveedores',
        'reportes',
        'configuracion',
        'sistema',
    }
    return raw if raw in allowed else 'manual'


def _is_source_enabled(cfg_data: dict, source_module: str, event_type: str) -> bool:
    if not isinstance(cfg_data, dict):
        return True
    sources = cfg_data.get('event_sources')
    if not isinstance(sources, dict):
        return True
    src = sources.get(source_module)
    if not isinstance(src, dict):
        return True
    if source_module == 'manual':
        return bool(src.get('notas', True))
    if source_module == 'sistema':
        if event_type == 'cierres':
            return bool(src.get('cierres', True))
        if event_type == 'mantenimientos':
            return bool(src.get('mantenimientos', True))
        return True
    return bool(src.get(event_type, True))


def _get_system_events(cfg_data: dict, start: date, end: date):
    return []


@bp.route('/', methods=['GET', 'POST'])
@bp.route('/index', methods=['GET', 'POST'])
@login_required
@module_required('calendar')
def index():
    cfg = _get_user_config()
    cfg_data = cfg.get_config()

    if request.method == 'POST':
        action = (request.form.get('action') or '').strip()

        if action == 'create_manual_event':
            title = (request.form.get('title') or '').strip()
            desc = (request.form.get('description') or '').strip()
            dt = (request.form.get('date') or '').strip()
            priority = (request.form.get('priority') or 'media').strip().lower()
            source_module = _sanitize_source_module(request.form.get('source_module'))

            if not title or not dt:
                flash('Completá título y fecha.', 'error')
                return redirect(url_for('calendar.index'))

            try:
                d = datetime.strptime(dt, '%Y-%m-%d').date()
            except Exception:
                flash('Fecha inválida.', 'error')
                return redirect(url_for('calendar.index'))

            ev = CalendarEvent(
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

            if ev.created_by_user_id != current_user.id and not getattr(current_user, 'is_master', False) and getattr(current_user, 'role', '') != 'admin':
                flash('No tenés permisos para editar este aviso.', 'error')
                return redirect(url_for('calendar.index'))

            title = (request.form.get('title') or '').strip()
            desc = (request.form.get('description') or '').strip()
            dt = (request.form.get('date') or '').strip()
            priority = (request.form.get('priority') or 'media').strip().lower()
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

            def _set(path, value):
                cur = sources
                for p in path[:-1]:
                    cur = cur.setdefault(p, {})
                cur[path[-1]] = bool(value)

            _set(['clientes', 'cumpleanos'], request.form.get('src_clientes_cumpleanos') == 'on')
            _set(['clientes', 'deudas'], request.form.get('src_clientes_deudas') == 'on')
            _set(['clientes', 'inactivos'], request.form.get('src_clientes_inactivos') == 'on')
            _set(['empleados', 'cumpleanos'], request.form.get('src_empleados_cumpleanos') == 'on')
            _set(['empleados', 'avisos'], request.form.get('src_empleados_avisos') == 'on')
            _set(['empleados', 'licencias'], request.form.get('src_empleados_licencias') == 'on')
            _set(['ventas', 'vencimientos'], request.form.get('src_ventas_vencimientos') == 'on')
            _set(['ventas', 'pagos_pactados'], request.form.get('src_ventas_pagos') == 'on')
            _set(['movimientos', 'vencimientos_financieros'], request.form.get('src_mov_vencimientos') == 'on')
            _set(['movimientos', 'arqueo_caja'], request.form.get('src_mov_arqueo') == 'on')
            _set(['inventario', 'recordatorios'], request.form.get('src_inventario_recordatorios') == 'on')
            _set(['gastos', 'recordatorios'], request.form.get('src_gastos_recordatorios') == 'on')
            _set(['proveedores', 'recordatorios'], request.form.get('src_proveedores_recordatorios') == 'on')
            _set(['reportes', 'recordatorios'], request.form.get('src_reportes_recordatorios') == 'on')
            _set(['configuracion', 'recordatorios'], request.form.get('src_config_recordatorios') == 'on')
            _set(['sistema', 'cierres'], request.form.get('src_sys_cierres') == 'on')
            _set(['sistema', 'mantenimientos'], request.form.get('src_sys_mant') == 'on')
            _set(['manual', 'notas'], request.form.get('src_manual_notas') == 'on')

            cfg_data['event_sources'] = sources

            alerts = cfg_data.get('alerts') if isinstance(cfg_data, dict) else None
            if not isinstance(alerts, dict):
                alerts = _default_calendar_config().get('alerts')
            alerts['enabled'] = (request.form.get('alerts_enabled') == 'on')
            alerts['dashboard_integration'] = (request.form.get('alerts_dashboard') == 'on')
            cfg_data['alerts'] = alerts

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
    q = q.filter((CalendarEvent.assigned_user_id.is_(None)) | (CalendarEvent.assigned_user_id == current_user.id))
    db_events = q.order_by(CalendarEvent.event_date.asc(), CalendarEvent.id.asc()).all()
    for ev in db_events:
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
