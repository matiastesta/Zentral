"""
Endpoints adicionales para gestión avanzada de tandas:
- Obtener datos editables de una tanda
- Validar si una tanda es modificable
- Preparar datos para re-edición estilo Excel
"""

from datetime import datetime
from flask import jsonify, request, g
from flask_login import login_required
from sqlalchemy import func

from app import db
from app.models import InventoryLot, InventoryMovement, Product, Category, Supplier
from app.permissions import module_required
from app.tenancy import ensure_request_context


def _company_id() -> str:
    try:
        return str(getattr(g, 'company_id', '') or '').strip()
    except Exception:
        return ''


def get_tanda_editable_data_by_received_at(cid: str, received_at: datetime):
    """
    Obtiene todos los datos de una tanda en formato editable (tipo Excel).
    Retorna filas con todos los campos necesarios para edición.
    """
    lotes = (
        db.session.query(InventoryLot)
        .join(Product)
        .filter(InventoryLot.company_id == cid)
        .filter(Product.company_id == cid)
        .filter(Product.deleted_at.is_(None))
        .filter(InventoryLot.received_at == received_at)
        .order_by(InventoryLot.id)
        .all()
    )
    
    rows = []
    for lote in lotes:
        producto = lote.product
        categoria = db.session.get(Category, producto.category_id) if producto.category_id else None
        
        # Validar si el lote puede modificarse
        tiene_movimientos = db.session.query(InventoryMovement.id).filter(
            InventoryMovement.lot_id == lote.id,
            InventoryMovement.type.in_(['sale', 'adjustment', 'consume'])
        ).first() is not None
        
        row = {
            'lot_id': lote.id,
            'nombre': producto.name or '',
            'categoria': categoria.name if categoria else '',
            'codigo_interno': producto.internal_code or '',
            'barcode': producto.barcode or '',
            'precio_lista': float(producto.sale_price or 0),
            'descripcion': producto.description or '',
            'cantidad': float(lote.qty_initial or 0),
            'costo_unitario': float(lote.unit_cost or 0),
            'proveedor': lote.supplier_name or '',
            'supplier_id': lote.supplier_id or '',
            'vencimiento': lote.expiration_date.isoformat() if lote.expiration_date else '',
            'nota_lote': lote.note or '',
            'stock_minimo': float(producto.min_stock or 0),
            'punto_pedido': float(producto.reorder_point or 0),
            'product_id': producto.id,
            'modificable': not tiene_movimientos,
            'razon_no_modificable': 'Lote con movimientos posteriores (ventas/ajustes)' if tiene_movimientos else None
        }
        rows.append(row)
    
    return rows


def validate_tanda_modification(cid: str, received_at: datetime):
    """
    Valida si una tanda completa puede modificarse.
    Retorna (puede_modificar, razones)
    """
    lotes = (
        db.session.query(InventoryLot)
        .filter(InventoryLot.company_id == cid)
        .filter(InventoryLot.received_at == received_at)
        .all()
    )
    
    if not lotes:
        return False, ['Tanda no encontrada']
    
    lotes_bloqueados = []
    razones = []
    
    for lote in lotes:
        # Verificar ventas
        tiene_ventas = db.session.query(InventoryMovement.id).filter(
            InventoryMovement.lot_id == lote.id,
            InventoryMovement.type == 'sale'
        ).first() is not None
        
        # Verificar ajustes
        tiene_ajustes = db.session.query(InventoryMovement.id).filter(
            InventoryMovement.lot_id == lote.id,
            InventoryMovement.type == 'lot_adjust'
        ).first() is not None
        
        # Verificar consumos
        tiene_consumos = db.session.query(InventoryMovement.id).filter(
            InventoryMovement.lot_id == lote.id,
            InventoryMovement.type == 'consume'
        ).first() is not None
        
        if tiene_ventas or tiene_ajustes or tiene_consumos:
            lotes_bloqueados.append(lote.id)
            if tiene_ventas:
                razones.append(f'Lote {lote.id}: tiene ventas registradas')
            if tiene_ajustes:
                razones.append(f'Lote {lote.id}: tiene ajustes posteriores')
            if tiene_consumos:
                razones.append(f'Lote {lote.id}: tiene consumos registrados')
    
    puede_modificar_completo = len(lotes_bloqueados) == 0
    
    return puede_modificar_completo, razones, lotes_bloqueados


def get_tanda_summary(cid: str, received_at: datetime):
    """
    Obtiene resumen de una tanda para mostrar en modal de confirmación.
    """
    lotes = (
        db.session.query(InventoryLot)
        .join(Product)
        .filter(InventoryLot.company_id == cid)
        .filter(Product.company_id == cid)
        .filter(Product.deleted_at.is_(None))
        .filter(InventoryLot.received_at == received_at)
        .all()
    )
    
    if not lotes:
        return None
    
    cantidad_productos = len(lotes)
    cantidad_unidades = sum(float(lote.qty_initial or 0) for lote in lotes)
    
    # Proveedores únicos
    proveedores = list(set(
        lote.supplier_name for lote in lotes 
        if lote.supplier_name
    ))
    
    # Inferir tipo de origen
    primer_lote = lotes[0]
    nota_lower = str(primer_lote.note or '').lower()
    
    if 'excel' in nota_lower or 'importa' in nota_lower:
        origen_tipo = 'excel'
        origen_texto = 'Importación por Excel'
        origen_icon = '📥'
    elif cantidad_productos == 1:
        origen_tipo = 'manual_item'
        origen_texto = 'Creado manualmente'
        origen_icon = '✏️'
    elif cantidad_productos > 1:
        origen_tipo = 'manual_lote'
        origen_texto = 'Carga por lote'
        origen_icon = '📦'
    else:
        origen_tipo = 'unknown'
        origen_texto = 'Origen desconocido'
        origen_icon = '📁'
    
    # Agregar Z para indicar UTC explícitamente
    received_at_utc = received_at.isoformat()
    if not received_at_utc.endswith('Z') and '+' not in received_at_utc[-6:]:
        received_at_utc += 'Z'
    
    return {
        'received_at': received_at_utc,
        'fecha_str': received_at.strftime('%d/%m/%Y %H:%M:%S'),
        'cantidad_productos': cantidad_productos,
        'cantidad_unidades': cantidad_unidades,
        'proveedores': proveedores,
        'origen_tipo': origen_tipo,
        'origen_texto': origen_texto,
        'origen_icon': origen_icon,
        'lotes_ids': [lote.id for lote in lotes]
    }
