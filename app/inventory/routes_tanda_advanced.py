"""
Endpoints avanzados para gestión de tandas:
- GET /api/tandas/editable-data - Obtener datos en formato editable
- GET /api/tandas/validate-modification - Validar si se puede modificar
- GET /api/tandas/summary - Resumen para modal de confirmación
- POST /api/tandas/update - Actualizar tanda existente
"""

from datetime import datetime
from flask import jsonify, request, g
from flask_login import login_required

from app import db
from app.models import InventoryLot, InventoryMovement, Product
from app.permissions import module_required
from app.tenancy import ensure_request_context
from app.inventory import bp
from app.inventory.tanda_endpoints import (
    _company_id,
    get_tanda_editable_data_by_received_at,
    validate_tanda_modification,
    get_tanda_summary
)


@bp.get('/api/tandas/editable-data')
@login_required
@module_required('inventory')
def get_tanda_editable_data():
    """
    Obtiene datos de una tanda en formato editable (similar a Excel).
    Query params: received_at (ISO datetime string)
    """
    try:
        ensure_request_context()
    except Exception:
        pass
    
    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400
    
    received_at_str = request.args.get('received_at')
    if not received_at_str:
        return jsonify({'ok': False, 'error': 'received_at_required'}), 400
    
    try:
        from dateutil import parser
        received_at = parser.isoparse(received_at_str)
    except Exception:
        return jsonify({'ok': False, 'error': 'invalid_date'}), 400
    
    rows = get_tanda_editable_data_by_received_at(cid, received_at)
    
    if not rows:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    
    # Agregar Z para indicar UTC explícitamente
    received_at_utc = received_at.isoformat()
    if not received_at_utc.endswith('Z') and '+' not in received_at_utc[-6:]:
        received_at_utc += 'Z'
    
    return jsonify({
        'ok': True,
        'received_at': received_at_utc,
        'rows': rows,
        'total_rows': len(rows)
    })


@bp.get('/api/tandas/validate-modification')
@login_required
@module_required('inventory')
def validate_tanda_modification_endpoint():
    """
    Valida si una tanda puede modificarse.
    Query params: received_at (ISO datetime string)
    """
    try:
        ensure_request_context()
    except Exception:
        pass
    
    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400
    
    received_at_str = request.args.get('received_at')
    if not received_at_str:
        return jsonify({'ok': False, 'error': 'received_at_required'}), 400
    
    try:
        from dateutil import parser
        received_at = parser.isoparse(received_at_str)
    except Exception:
        return jsonify({'ok': False, 'error': 'invalid_date'}), 400
    
    puede_modificar, razones, lotes_bloqueados = validate_tanda_modification(cid, received_at)
    
    return jsonify({
        'ok': True,
        'puede_modificar': puede_modificar,
        'razones': razones,
        'lotes_bloqueados': lotes_bloqueados
    })


@bp.get('/api/tandas/summary')
@login_required
@module_required('inventory')
def get_tanda_summary_endpoint():
    """
    Obtiene resumen de una tanda para mostrar en modal de confirmación.
    Query params: received_at (ISO datetime string)
    """
    try:
        ensure_request_context()
    except Exception:
        pass
    
    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400
    
    received_at_str = request.args.get('received_at')
    if not received_at_str:
        return jsonify({'ok': False, 'error': 'received_at_required'}), 400
    
    try:
        from dateutil import parser
        received_at = parser.isoparse(received_at_str)
    except Exception:
        return jsonify({'ok': False, 'error': 'invalid_date'}), 400
    
    summary = get_tanda_summary(cid, received_at)
    
    if not summary:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    
    # Agregar validación de modificación
    puede_modificar, razones, lotes_bloqueados = validate_tanda_modification(cid, received_at)
    summary['puede_modificar'] = puede_modificar
    summary['razones_no_modificable'] = razones
    summary['lotes_bloqueados'] = lotes_bloqueados
    
    return jsonify({
        'ok': True,
        'summary': summary
    })
