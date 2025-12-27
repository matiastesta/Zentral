from datetime import date, timedelta

from flask import render_template
from flask_login import login_required, current_user

from app import db
from app.models import CalendarEvent, CalendarUserConfig
from app.permissions import module_required
from app.main import bp


def _company_id() -> str:
    return str(getattr(current_user, 'company_id', '') or '').strip()


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
