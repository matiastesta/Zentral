from datetime import date as dt_date
from datetime import datetime
from io import BytesIO
from uuid import uuid4
import json
import os
import time

from flask import g, jsonify, render_template, request, current_app, send_file, url_for, redirect
from flask_login import login_required
from werkzeug.utils import secure_filename

from app import db
from app.files.storage import upload_to_r2_and_create_asset
from app.models import FileAsset
from app.models import Expense, ExpenseCategory, BusinessSettings, Employee, Supplier
from app.permissions import module_required, module_required_any
from app.expenses import bp


def _parse_date_iso(raw, default=None):
    s = str(raw or '').strip()
    if not s:
        return default
    try:
        return dt_date.fromisoformat(s)
    except Exception:
        return default


def _serialize_expense(row: Expense):
    meta_obj = {}
    if row.meta_json:
        try:
            meta_obj = json.loads(row.meta_json) if isinstance(row.meta_json, str) else {}
        except Exception:
            meta_obj = {}
    if not isinstance(meta_obj, dict):
        meta_obj = {}

    comprobante = meta_obj.get('comprobante')
    custom_fields = meta_obj.get('custom_fields')
    origin_ref = meta_obj.get('origin_ref')

    return {
        'id': row.id,
        'fecha': row.expense_date.isoformat() if row.expense_date else '',
        'categoria': row.category or '',
        'valor': row.amount,
        'nota': row.note or (row.description or ''),
        'forma_pago': row.payment_method or 'Efectivo',
        'supplier_id': row.supplier_id or '',
        'supplier_name': row.supplier_name or '',
        'type': row.expense_type or '',
        'frequency': row.frequency or '',
        'employee_id': row.employee_id or '',
        'employee_name': row.employee_name or '',
        'period_from': row.period_from.isoformat() if row.period_from else '',
        'period_to': row.period_to.isoformat() if row.period_to else '',
        'meta': meta_obj,
        'meta_json': row.meta_json or '',
        'comprobante': comprobante if isinstance(comprobante, list) else [],
        'custom_fields': custom_fields if isinstance(custom_fields, list) else [],
        'origin_ref': origin_ref if isinstance(origin_ref, dict) else {},
        'origin': row.origin or 'manual',
        'created_at': int(row.created_at.timestamp() * 1000) if getattr(row, 'created_at', None) else 0,
    }


def _get_expense_upload_dir(expense_id: str):
    eid = str(expense_id or '').strip() or 'unknown'
    base = os.path.join(current_app.instance_path, 'uploads', 'expenses', eid)
    os.makedirs(base, exist_ok=True)
    return base


def _load_meta_obj(row: Expense):
    meta_obj = {}
    if row and row.meta_json:
        try:
            meta_obj = json.loads(row.meta_json) if isinstance(row.meta_json, str) else {}
        except Exception:
            meta_obj = {}
    if not isinstance(meta_obj, dict):
        meta_obj = {}
    return meta_obj


def _save_meta_obj(row: Expense, meta_obj: dict):
    try:
        row.meta_json = json.dumps(meta_obj or {}, ensure_ascii=False)
    except Exception:
        row.meta_json = None


def _format_currency_ars(amount):
    try:
        n = float(amount or 0)
    except Exception:
        n = 0.0
    s = f"{n:,.2f}"
    s = s.replace(',', 'X').replace('.', ',').replace('X', '.')
    return '$' + s


@bp.route("/")
@bp.route("/index")
@login_required
@module_required_any('expenses', 'dashboard')
def index():
    """Listado básico de gastos."""
    return render_template("expenses/list.html", title="Gastos")


@bp.route("/new")
@login_required
@module_required('expenses')
def new():
    return render_template("expenses/new.html", title="Nuevo gasto", today=dt_date.today().isoformat())


def _num(v, default=0.0):
    try:
        if v is None or v == '':
            return default
        return float(v)
    except Exception:
        return default


def _supplier_is_inventory_supplier(supplier_id: str) -> bool:
    sid = str(supplier_id or '').strip()
    if not sid:
        return False
    row = db.session.get(Supplier, sid)
    if not row:
        return False
    try:
        cid = str(getattr(g, 'company_id', '') or '').strip()
        if cid and str(getattr(row, 'company_id', '') or '') != cid:
            return False
    except Exception:
        return False
    has_inventory_flag = False
    try:
        raw_meta = str(getattr(row, 'meta_json', '') or '').strip()
        if raw_meta:
            meta_obj = json.loads(raw_meta) if isinstance(raw_meta, str) else {}
            if isinstance(meta_obj, dict) and meta_obj.get('inventory_supplier') is True:
                has_inventory_flag = True
    except Exception:
        pass

    raw = str(getattr(row, 'categories_json', '') or '').strip()
    try:
        cats = json.loads(raw) if raw else []
        cats = cats if isinstance(cats, list) else []
    except Exception:
        cats = []

    norm = [str(x or '').strip().lower() for x in cats if str(x or '').strip()]
    has_inventory_legacy = 'inventario' in norm
    non_inventory = [x for x in norm if x != 'inventario']

    has_inventory = has_inventory_flag or has_inventory_legacy
    return bool(has_inventory and (len(non_inventory) == 0))


def _payload_uses_inventory_supplier(payload: dict) -> bool:
    d = payload if isinstance(payload, dict) else {}
    origin = str(d.get('origin') or '').strip().lower()
    if origin == 'inventory':
        return False
    sid = str(d.get('supplier_id') or '').strip()
    if not sid:
        return False
    return _supplier_is_inventory_supplier(sid)


def _apply_expense_payload(row: Expense, payload: dict):
    existing_meta_obj = _load_meta_obj(row)

    row.expense_date = _parse_date_iso(payload.get('fecha') or payload.get('expense_date'), row.expense_date or dt_date.today())
    row.payment_method = str(payload.get('forma_pago') or payload.get('payment_method') or row.payment_method or 'Efectivo').strip() or 'Efectivo'
    row.amount = _num(payload.get('valor') if payload.get('valor') is not None else payload.get('amount'), 0.0)
    row.category = str(payload.get('categoria') or payload.get('category') or '').strip() or None

    row.note = str(payload.get('nota') or payload.get('note') or '').strip() or None
    row.description = str(payload.get('description') or '').strip() or None

    row.supplier_id = str(payload.get('supplier_id') or '').strip() or None
    row.supplier_name = str(payload.get('supplier_name') or payload.get('proveedor') or '').strip() or None

    row.expense_type = str(payload.get('type') or payload.get('expense_type') or '').strip() or None
    row.frequency = str(payload.get('frequency') or '').strip() or None

    row.employee_id = str(payload.get('employee_id') or '').strip() or None
    row.employee_name = str(payload.get('employee_name') or '').strip() or None

    row.period_from = _parse_date_iso(payload.get('period_from') or payload.get('periodo_desde'), None)
    row.period_to = _parse_date_iso(payload.get('period_to') or payload.get('periodo_hasta'), None)

    origin = str(payload.get('origin') or row.origin or 'manual').strip().lower()
    row.origin = 'inventory' if origin == 'inventory' else 'manual'

    meta_obj = existing_meta_obj if isinstance(existing_meta_obj, dict) else {}

    meta = payload.get('meta')
    if meta is not None and isinstance(meta, dict):
        for k, v in meta.items():
            meta_obj[k] = v

    raw_meta_json = payload.get('meta_json')
    if raw_meta_json is not None:
        try:
            parsed = json.loads(str(raw_meta_json))
            if isinstance(parsed, dict):
                for k, v in parsed.items():
                    meta_obj[k] = v
        except Exception:
            current_app.logger.exception('Failed to parse meta_json for expense payload')

    if isinstance(payload.get('comprobante'), list):
        meta_obj['comprobante'] = payload.get('comprobante')
    if isinstance(payload.get('custom_fields'), list):
        meta_obj['custom_fields'] = payload.get('custom_fields')
    if isinstance(payload.get('origin_ref'), dict):
        meta_obj['origin_ref'] = payload.get('origin_ref')

    _save_meta_obj(row, meta_obj)


def _serialize_expense_category(row: ExpenseCategory):
    return {
        'id': row.id,
        'name': row.name or '',
    }


def _apply_expense_category_payload(row: ExpenseCategory, payload: dict):
    row.name = str(payload.get('name') or payload.get('categoria') or payload.get('category') or row.name or '').strip() or row.name


@bp.get('/api/expenses')
@login_required
@module_required_any('expenses', 'dashboard')
def list_expenses_api():
    raw_from = (request.args.get('from') or '').strip()
    raw_to = (request.args.get('to') or '').strip()
    category = (request.args.get('category') or '').strip()
    limit = int(request.args.get('limit') or 5000)
    if limit <= 0 or limit > 10000:
        limit = 5000

    d_from = _parse_date_iso(raw_from, None)
    d_to = _parse_date_iso(raw_to, None)

    cid = str(getattr(g, 'company_id', '') or '').strip()
    if not cid:
        return jsonify({'ok': True, 'items': []})

    q = db.session.query(Expense).filter(Expense.company_id == cid)
    if d_from:
        q = q.filter(Expense.expense_date >= d_from)
    if d_to:
        q = q.filter(Expense.expense_date <= d_to)
    if category:
        q = q.filter(Expense.category == category)

    rows = q.order_by(Expense.expense_date.desc()).limit(limit).all()
    return jsonify({'ok': True, 'items': [_serialize_expense(r) for r in rows]})


@bp.get('/api/expenses/<expense_id>')
@login_required
@module_required('expenses')
def get_expense_api(expense_id):
    eid = str(expense_id or '').strip()
    cid = str(getattr(g, 'company_id', '') or '').strip()
    if not cid:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    row = db.session.query(Expense).filter(Expense.company_id == cid, Expense.id == eid).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    return jsonify({'ok': True, 'item': _serialize_expense(row)})


@bp.post('/api/expenses')
@login_required
@module_required('expenses')
def create_expense_api():
    payload = request.get_json(silent=True) or {}
    cid = str(getattr(g, 'company_id', '') or '').strip()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400
    if _payload_uses_inventory_supplier(payload):
        return jsonify({'ok': False, 'error': 'inventory_supplier_forbidden'}), 400
    eid = str(payload.get('id') or '').strip() or uuid4().hex

    row = db.session.query(Expense).filter(Expense.company_id == cid, Expense.id == eid).first()
    if row:
        return jsonify({'ok': False, 'error': 'already_exists'}), 400

    row = Expense(id=eid, company_id=cid, expense_date=dt_date.today())
    _apply_expense_payload(row, payload)
    db.session.add(row)

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'item': _serialize_expense(row)})


@bp.put('/api/expenses/<expense_id>')
@login_required
@module_required('expenses')
def update_expense_api(expense_id):
    eid = str(expense_id or '').strip()
    cid = str(getattr(g, 'company_id', '') or '').strip()
    if not cid:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    row = db.session.query(Expense).filter(Expense.company_id == cid, Expense.id == eid).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    payload = request.get_json(silent=True) or {}
    if _payload_uses_inventory_supplier(payload):
        return jsonify({'ok': False, 'error': 'inventory_supplier_forbidden'}), 400
    _apply_expense_payload(row, payload)

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'item': _serialize_expense(row)})


@bp.delete('/api/expenses/<expense_id>')
@login_required
@module_required('expenses')
def delete_expense_api(expense_id):
    eid = str(expense_id or '').strip()
    cid = str(getattr(g, 'company_id', '') or '').strip()
    if not cid:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    row = db.session.query(Expense).filter(Expense.company_id == cid, Expense.id == eid).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    try:
        db.session.delete(row)
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True})


@bp.post('/api/expenses/<expense_id>/attachments')
@login_required
@module_required('expenses')
def upload_expense_attachments_api(expense_id):
    eid = str(expense_id or '').strip()
    cid = str(getattr(g, 'company_id', '') or '').strip()
    if not cid:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    row = db.session.query(Expense).filter(Expense.company_id == cid, Expense.id == eid).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    files = []
    try:
        files = list(request.files.getlist('file') or [])
        if not files:
            files = list(request.files.getlist('files') or [])
    except Exception:
        current_app.logger.exception('Failed to get files from request')
        files = []

    files = [f for f in files if f and getattr(f, 'filename', '')]
    if not files:
        return jsonify({'ok': False, 'error': 'no_file'}), 400

    meta_obj = _load_meta_obj(row)
    comp = meta_obj.get('comprobante')
    comp_list = comp if isinstance(comp, list) else []

    out = []
    for f in files:
        original_name = str(f.filename or '').strip() or 'archivo'
        try:
            asset = upload_to_r2_and_create_asset(
                company_id=cid,
                file_storage=f,
                entity_type='expense',
                entity_id=eid,
                key_prefix='expenses/attachments',
            )
        except Exception:
            current_app.logger.exception('Failed to upload expense attachment to R2')
            continue

        mime = str(getattr(f, 'mimetype', '') or '').strip()
        url = url_for('files.download_file_api', file_id=asset.id)

        item = {
            'id': asset.id,
            'file_id': asset.id,
            'name': original_name,
            'size': int(getattr(asset, 'size_bytes', 0) or 0),
            'type': mime,
            'url': url,
            'created_at': int(time.time() * 1000),
        }
        comp_list.append(item)
        out.append(item)

    meta_obj['comprobante'] = comp_list
    _save_meta_obj(row, meta_obj)

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400

    return jsonify({'ok': True, 'items': out, 'item': _serialize_expense(row)})


@bp.get('/api/expenses/<expense_id>/attachments/<attachment_id>')
@login_required
@module_required('expenses')
def download_expense_attachment_api(expense_id, attachment_id):
    eid = str(expense_id or '').strip()
    aid = str(attachment_id or '').strip()
    cid = str(getattr(g, 'company_id', '') or '').strip()
    if not cid:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    row = db.session.query(Expense).filter(Expense.company_id == cid, Expense.id == eid).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    meta_obj = _load_meta_obj(row)
    comp = meta_obj.get('comprobante')
    comp_list = comp if isinstance(comp, list) else []
    item = None
    for x in comp_list:
        if not isinstance(x, dict):
            continue
        if str(x.get('id') or '').strip() == aid:
            item = x
            break
    if not item:
        return jsonify({'ok': False, 'error': 'attachment_not_found'}), 404

    fid = str(item.get('file_id') or item.get('id') or '').strip()
    if fid:
        row_asset = db.session.query(FileAsset).filter(FileAsset.company_id == cid, FileAsset.id == fid).first()
        if row_asset:
            return redirect(url_for('files.download_file_api', file_id=fid), code=302)

    stored_name = str(item.get('stored_name') or '').strip()
    if not stored_name:
        return jsonify({'ok': False, 'error': 'attachment_missing_path'}), 404

    upload_dir = _get_expense_upload_dir(eid)
    abs_path = os.path.abspath(os.path.join(upload_dir, stored_name))
    abs_base = os.path.abspath(upload_dir)
    if not abs_path.startswith(abs_base):
        return jsonify({'ok': False, 'error': 'invalid_path'}), 400
    if not os.path.exists(abs_path):
        return jsonify({'ok': False, 'error': 'file_not_found'}), 404

    download_name = str(item.get('name') or 'archivo')
    mime = str(item.get('type') or '').strip() or None
    return send_file(abs_path, as_attachment=True, download_name=download_name, mimetype=mime)


@bp.delete('/api/expenses/<expense_id>/attachments/<attachment_id>')
@login_required
@module_required('expenses')
def delete_expense_attachment_api(expense_id, attachment_id):
    eid = str(expense_id or '').strip()
    aid = str(attachment_id or '').strip()
    row = db.session.get(Expense, eid)
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    meta_obj = _load_meta_obj(row)
    comp = meta_obj.get('comprobante')
    comp_list = comp if isinstance(comp, list) else []
    keep = []
    removed = None
    for x in comp_list:
        if not isinstance(x, dict):
            continue
        if str(x.get('id') or '').strip() == aid:
            removed = x
            continue
        keep.append(x)
    if removed is None:
        return jsonify({'ok': False, 'error': 'attachment_not_found'}), 404

    fid = str(removed.get('file_id') or removed.get('id') or '').strip()
    if fid:
        try:
            fa = db.session.query(FileAsset).filter(FileAsset.company_id == str(getattr(row, 'company_id', '') or ''), FileAsset.id == fid).first()
            if fa:
                fa.status = 'deleted'
        except Exception:
            current_app.logger.exception('Failed to mark FileAsset as deleted')

    stored_name = str(removed.get('stored_name') or '').strip()
    if stored_name:
        upload_dir = _get_expense_upload_dir(eid)
        abs_path = os.path.abspath(os.path.join(upload_dir, stored_name))
        abs_base = os.path.abspath(upload_dir)
        if abs_path.startswith(abs_base) and os.path.exists(abs_path):
            try:
                os.remove(abs_path)
            except Exception:
                current_app.logger.exception('Failed to remove expense attachment file')

        try:
            if os.path.isdir(abs_base) and not os.listdir(abs_base):
                os.rmdir(abs_base)
        except Exception:
            current_app.logger.exception('Failed to cleanup empty expense upload directory')

    meta_obj['comprobante'] = keep
    _save_meta_obj(row, meta_obj)

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400

    return jsonify({'ok': True, 'item': _serialize_expense(row)})


@bp.get('/api/expenses/<expense_id>/remito.pdf')
@login_required
@module_required('expenses')
def download_expense_remito_pdf_api(expense_id):
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import mm
        from reportlab.pdfgen import canvas
    except Exception:
        return jsonify({'ok': False, 'error': 'reportlab_missing'}), 400

    eid = str(expense_id or '').strip()
    row = db.session.get(Expense, eid)
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    bs = None
    try:
        bs = BusinessSettings.get_for_company(getattr(g, 'company_id', None))
    except Exception:
        bs = None

    business_name = (getattr(bs, 'name', None) or '').strip() or 'Negocio'
    business_address = (getattr(bs, 'address', None) or '').strip()
    business_email = (getattr(bs, 'email', None) or '').strip()
    business_phone = (getattr(bs, 'phone', None) or '').strip()

    meta_obj = _load_meta_obj(row)

    is_payroll = False
    try:
        ccat = str(row.category or '').lower()
        is_payroll = ('nómina' in ccat) or ('nomina' in ccat)
    except Exception:
        current_app.logger.exception('Failed to determine if expense is payroll')
        is_payroll = False

    supplier_name = (row.supplier_name or '').strip() or '—'
    employee_name = (row.employee_name or '').strip()
    if is_payroll and not employee_name:
        try:
            if row.employee_id:
                emp = db.session.get(Employee, str(row.employee_id))
                if emp:
                    fn = str(getattr(emp, 'first_name', '') or '').strip()
                    ln = str(getattr(emp, 'last_name', '') or '').strip()
                    employee_name = (fn + ' ' + ln).strip() or employee_name
        except Exception:
            current_app.logger.exception('Failed to resolve employee name for payroll expense')
    employee_name = employee_name or '—'

    payroll_concept = ''
    try:
        payroll_concept = str(meta_obj.get('payroll_payment_type') or '').strip()
    except Exception:
        current_app.logger.exception('Failed to get payroll payment type')
    if is_payroll and not payroll_concept:
        payroll_concept = '—'

    period_from = row.period_from.isoformat() if row.period_from else ''
    period_to = row.period_to.isoformat() if row.period_to else ''

    payee_name = employee_name if is_payroll else supplier_name
    category = (row.category or '').strip() or '—'
    payment_method = (row.payment_method or '').strip() or '—'
    note = (row.note or row.description or '').strip() or '—'
    expense_date = row.expense_date.isoformat() if row.expense_date else ''
    amount = _format_currency_ars(row.amount)

    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4
    margin = 18 * mm
    y = height - margin

    # Header
    c.setFont('Helvetica-Bold', 16)
    c.drawString(margin, y, 'Remito: Pago a "' + payee_name + '"')

    # Logo del negocio (esquina superior derecha)
    top_y = y
    logo_w = 16 * mm
    logo_h = 16 * mm
    logo_x = width - margin - logo_w
    logo_y = top_y - logo_h + 6
    try:
        if bs and getattr(bs, 'logo_filename', None):
            logo_path = os.path.join(current_app.static_folder, 'uploads', str(bs.logo_filename))
            if os.path.exists(logo_path):
                c.drawImage(logo_path, logo_x, logo_y, width=logo_w, height=logo_h, preserveAspectRatio=True, mask='auto')
    except Exception:
        current_app.logger.exception('Failed to render business logo in remito PDF')

    y -= 11 * mm

    # Detalles de contacto
    c.setFont('Helvetica-Bold', 10)
    c.drawString(margin, y, 'Detalles de contacto de ' + business_name + ':')
    y -= 5 * mm
    c.setFont('Helvetica', 9)
    contact_line = 'Dirección: ' + (business_address or '—') + ' · Teléfono: ' + (business_phone or '—') + ' · Mail: ' + (business_email or '—')
    c.drawString(margin, y, contact_line)
    y -= 6 * mm

    c.line(margin, y, width - margin, y)
    y -= 8 * mm

    # Info blocks
    value_x = margin + 45 * mm
    line_h = 7 * mm

    c.setFont('Helvetica-Bold', 10)
    c.drawString(margin, y, 'Empleado' if is_payroll else 'Proveedor')
    c.setFont('Helvetica', 10)
    c.drawString(value_x, y, payee_name)
    y -= line_h

    if is_payroll:
        c.setFont('Helvetica-Bold', 10)
        c.drawString(margin, y, 'Concepto de pago')
        c.setFont('Helvetica', 10)
        c.drawString(value_x, y, payroll_concept)
        y -= line_h

        if period_from or period_to:
            c.setFont('Helvetica-Bold', 10)
            c.drawString(margin, y, 'Período')
            c.setFont('Helvetica', 10)
            pr = (period_from or '—') + ' a ' + (period_to or '—')
            c.drawString(value_x, y, pr)
            y -= line_h

    c.setFont('Helvetica-Bold', 10)
    c.drawString(margin, y, 'Fecha de pago' if is_payroll else 'Fecha')
    c.setFont('Helvetica', 10)
    c.drawString(value_x, y, expense_date or '—')
    y -= line_h

    c.setFont('Helvetica-Bold', 10)
    c.drawString(margin, y, 'Forma de pago')
    c.setFont('Helvetica', 10)
    c.drawString(value_x, y, payment_method)
    y -= 10 * mm

    c.setFont('Helvetica-Bold', 10)
    c.drawString(margin, y, 'Detalle del gasto')
    y -= 6 * mm

    # Table header
    table_x = margin
    table_w = width - 2 * margin
    col_cat = 55 * mm
    col_amt = 35 * mm
    col_note = table_w - col_cat - col_amt

    c.setFont('Helvetica-Bold', 9)
    c.drawString(table_x, y, 'Categoría')
    c.drawString(table_x + col_cat + col_note - 2 * mm, y, 'Monto total')
    c.drawString(table_x + col_cat, y, 'Observación')
    y -= 3 * mm
    c.line(table_x, y, table_x + table_w, y)
    y -= 6 * mm

    # Row
    c.setFont('Helvetica', 9)
    c.drawString(table_x, y, category)
    c.drawString(table_x + col_cat, y, (note[:140] + ('…' if len(note) > 140 else '')))
    c.drawRightString(table_x + col_cat + col_note + col_amt, y, amount)
    y -= 10 * mm

    c.line(table_x, y, table_x + table_w, y)

    c.setFont('Helvetica', 8)
    c.drawString(margin, 14 * mm, 'Remito ID: ' + eid)

    c.showPage()
    c.save()
    buf.seek(0)

    filename = f"remito_gasto_{eid[:8]}.pdf"
    return send_file(buf, mimetype='application/pdf', as_attachment=True, download_name=filename)


@bp.post('/api/expenses/bulk')
@login_required
@module_required('expenses')
def upsert_expenses_bulk():
    payload = request.get_json(silent=True) or {}
    items = payload.get('items')
    items_list = items if isinstance(items, list) else []

    out = []
    for it in items_list:
        d = it if isinstance(it, dict) else {}
        if _payload_uses_inventory_supplier(d):
            return jsonify({'ok': False, 'error': 'inventory_supplier_forbidden'}), 400
        eid = str(d.get('id') or '').strip() or uuid4().hex
        row = db.session.get(Expense, eid)
        if not row:
            row = Expense(id=eid, expense_date=dt_date.today())
            db.session.add(row)
        _apply_expense_payload(row, d)
        out.append(row)

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'items': [_serialize_expense(r) for r in out]})


@bp.get('/api/categories')
@login_required
@module_required_any('expenses', 'suppliers')
def list_expense_categories_api():
    q = (request.args.get('q') or '').strip().lower()
    limit = int(request.args.get('limit') or 5000)
    if limit <= 0 or limit > 10000:
        limit = 5000
    query = db.session.query(ExpenseCategory)
    if q:
        like = f"%{q}%"
        query = query.filter(ExpenseCategory.name.ilike(like))
    rows = query.order_by(ExpenseCategory.name.asc()).limit(limit).all()
    items = []
    for r in rows:
        if str(getattr(r, 'name', '') or '').strip().lower() == 'inventario':
            continue
        items.append(_serialize_expense_category(r))
    return jsonify({'ok': True, 'items': items})


@bp.get('/api/categories/<category_id>')
@login_required
@module_required('expenses')
def get_expense_category_api(category_id):
    cid = str(category_id or '').strip()
    row = db.session.get(ExpenseCategory, cid)
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    return jsonify({'ok': True, 'item': _serialize_expense_category(row)})


@bp.post('/api/categories')
@login_required
@module_required_any('expenses', 'suppliers')
def create_expense_category_api():
    payload = request.get_json(silent=True) or {}
    cid = str(payload.get('id') or '').strip() or uuid4().hex
    row = db.session.get(ExpenseCategory, cid)
    if row:
        return jsonify({'ok': False, 'error': 'already_exists'}), 400
    name = str(payload.get('name') or payload.get('categoria') or '').strip() or 'Categoría'
    if name.strip().lower() == 'inventario':
        return jsonify({'ok': False, 'error': 'reserved_category', 'message': 'La categoría “Inventario” está reservada para el módulo de Inventario.'}), 400
    row = ExpenseCategory(id=cid, name=name)
    _apply_expense_category_payload(row, payload)
    db.session.add(row)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'item': _serialize_expense_category(row)})


@bp.put('/api/categories/<category_id>')
@login_required
@module_required_any('expenses', 'suppliers')
def update_expense_category_api(category_id):
    cid = str(category_id or '').strip()
    row = db.session.get(ExpenseCategory, cid)
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    payload = request.get_json(silent=True) or {}
    next_name = str(payload.get('name') or payload.get('categoria') or payload.get('category') or row.name or '').strip()
    if next_name.strip().lower() == 'inventario':
        return jsonify({'ok': False, 'error': 'reserved_category', 'message': 'La categoría “Inventario” está reservada para el módulo de Inventario.'}), 400
    _apply_expense_category_payload(row, payload)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'item': _serialize_expense_category(row)})


@bp.delete('/api/categories/<category_id>')
@login_required
@module_required_any('expenses', 'suppliers')
def delete_expense_category_api(category_id):
    cid = str(category_id or '').strip()
    row = db.session.get(ExpenseCategory, cid)
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    name = str(getattr(row, 'name', '') or '').strip()
    if name.lower() == 'inventario':
        return jsonify({'ok': False, 'error': 'reserved_category', 'message': 'La categoría “Inventario” está reservada para el módulo de Inventario.'}), 400

    company_id = str(getattr(g, 'company_id', '') or '').strip()
    used_count = 0
    if company_id and name:
        try:
            sup_rows = (
                db.session.query(Supplier)
                .filter(Supplier.company_id == company_id, Supplier.status == 'Active')
                .limit(5000)
                .all()
            )
        except Exception:
            sup_rows = []
        target = name.lower()
        for s in sup_rows:
            raw_cats = str(getattr(s, 'categories_json', '') or '').strip()
            try:
                cats = json.loads(raw_cats) if raw_cats else []
                cats = cats if isinstance(cats, list) else []
            except Exception:
                cats = []
            norm = [str(x or '').strip().lower() for x in cats if str(x or '').strip()]
            if target in norm:
                used_count += 1
    if used_count > 0:
        return jsonify({
            'ok': False,
            'error': 'category_in_use',
            'count': used_count,
            'message': f'Esta categoría está siendo utilizada por {used_count} proveedor(es) activo(s).'
        }), 400
    try:
        db.session.delete(row)
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True})


@bp.post('/api/categories/bulk')
@login_required
@module_required_any('expenses', 'suppliers')
def upsert_expense_categories_bulk():
    payload = request.get_json(silent=True) or {}
    items = payload.get('items')
    items_list = items if isinstance(items, list) else []
    out = []
    for it in items_list:
        d = it if isinstance(it, dict) else {}
        name = str(d.get('name') or d.get('categoria') or 'Categoría').strip() or 'Categoría'
        if name.strip().lower() == 'inventario':
            return jsonify({'ok': False, 'error': 'reserved_category', 'message': 'La categoría “Inventario” está reservada para el módulo de Inventario.'}), 400
        cid = str(d.get('id') or '').strip() or uuid4().hex
        row = db.session.get(ExpenseCategory, cid)
        if not row:
            row = ExpenseCategory(id=cid, name=name)
            db.session.add(row)
        _apply_expense_category_payload(row, d)
        out.append(row)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400
    return jsonify({'ok': True, 'items': [_serialize_expense_category(r) for r in out]})
