"""
Endpoint para actualizar tandas con reclasificación producto→lote
Maneja cambios en lotes existentes y conversión de productos nuevos a lotes
"""

from datetime import datetime
from flask import jsonify, request, current_app, g
from flask_login import login_required
from sqlalchemy import or_

from app import db
from app.models import InventoryLot, InventoryMovement, Product, Category
from app.permissions import module_required
from app.tenancy import ensure_request_context
from app.inventory import bp


def _company_id() -> str:
    try:
        return str(getattr(g, 'company_id', '') or '').strip()
    except Exception:
        return ''


@bp.post('/api/tandas/update')
@login_required
@module_required('inventory')
def update_tanda():
    """
    Actualiza una tanda completa con posibilidad de reclasificación producto→lote.
    
    Body esperado:
    {
        "received_at": "ISO datetime",
        "changes": [
            {
                "lot_id": 123,
                "product_id": 456,
                "nombre": "...",
                "categoria": "...",
                "codigo_interno": "...",
                "proveedor": "...",
                "cantidad": 50.0,
                "costo_unitario": 1500.0,
                "vencimiento": "2025-12-31",
                "reclassify_to_product_id": 789,  # Si se debe reclasificar
                "is_reclassification": true
            }
        ]
    }
    """
    try:
        ensure_request_context()
    except Exception:
        pass
    
    cid = _company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400
    
    payload = request.get_json(silent=True) or {}
    received_at_str = payload.get('received_at')
    changes = payload.get('changes', [])
    
    if not received_at_str:
        return jsonify({'ok': False, 'error': 'received_at_required'}), 400
    
    if not changes:
        return jsonify({'ok': False, 'error': 'no_changes'}), 400
    
    # Parsear received_at
    try:
        from dateutil import parser
        received_at = parser.isoparse(received_at_str)
    except Exception:
        return jsonify({'ok': False, 'error': 'invalid_date'}), 400
    
    # Obtener todos los lotes de esta tanda
    lotes = (
        db.session.query(InventoryLot)
        .filter(InventoryLot.company_id == cid)
        .filter(InventoryLot.received_at == received_at)
        .all()
    )
    
    if not lotes:
        return jsonify({'ok': False, 'error': 'not_found'}), 404
    
    # Validar que ningún lote tenga movimientos posteriores si se intenta reclasificar
    for change in changes:
        if not change.get('is_reclassification'):
            continue
        
        lot_id = change.get('lot_id')
        lote = db.session.get(InventoryLot, lot_id)
        if not lote:
            continue
        
        # Verificar movimientos
        tiene_movimientos = db.session.query(InventoryMovement.id).filter(
            InventoryMovement.lot_id == lote.id,
            InventoryMovement.type.in_(['sale', 'lot_adjust', 'consume'])
        ).first() is not None
        
        if tiene_movimientos:
            return jsonify({
                'ok': False,
                'error': 'has_movements',
                'message': f'Lote {lot_id} tiene movimientos posteriores y no puede reclasificarse'
            }), 400
    
    # Procesar cambios dentro de transacción
    try:
        productos_huerfanos = []  # Productos que quedaron sin lotes
        
        for change in changes:
            lot_id = change.get('lot_id')
            lote = db.session.get(InventoryLot, lot_id)
            
            if not lote or str(lote.company_id) != cid:
                continue
            
            old_product_id = lote.product_id
            codigo_interno_nuevo = change.get('codigo_interno', '').strip() if change.get('codigo_interno') else None
            
            # REGLA CRÍTICA: Determinar si código interno cambió
            producto_original = db.session.get(Product, old_product_id)
            codigo_interno_original = producto_original.internal_code if producto_original else None
            
            codigo_cambio = (codigo_interno_nuevo != codigo_interno_original)
            
            # Caso 1: Código interno cambió a uno EXISTENTE → Reasociar lote
            if codigo_cambio and change.get('is_reclassification') and change.get('reclassify_to_product_id'):
                target_product_id = change['reclassify_to_product_id']
                target_product = db.session.get(Product, target_product_id)
                
                if not target_product or str(target_product.company_id) != cid:
                    return jsonify({
                        'ok': False,
                        'error': 'target_product_not_found'
                    }), 400
                
                # Reasociar lote al producto existente
                lote.product_id = target_product_id
                
                # Actualizar movimientos asociados
                db.session.query(InventoryMovement).filter(
                    InventoryMovement.lot_id == lote.id
                ).update({
                    InventoryMovement.product_id: target_product_id
                }, synchronize_session=False)
                
                # Marcar producto viejo como potencialmente huérfano
                if old_product_id and old_product_id != target_product_id:
                    productos_huerfanos.append(old_product_id)
            
            # Caso 2: Código interno cambió a uno NUEVO → Crear producto nuevo o revivir huérfano
            elif codigo_cambio and codigo_interno_nuevo:
                # Buscar producto INCLUYENDO soft-deleted (deleted_at IS NOT NULL)
                existe_producto = db.session.query(Product).filter(
                    Product.company_id == cid,
                    Product.internal_code == codigo_interno_nuevo
                ).first()
                
                if existe_producto and existe_producto.deleted_at is not None:
                    # CASO ESPECIAL: Producto existe pero estaba soft-deleted → REVIVIR
                    existe_producto.deleted_at = None
                    existe_producto.active = True
                    
                    # Reasociar lote al producto revivido
                    lote.product_id = existe_producto.id
                    
                    # Actualizar movimientos asociados
                    db.session.query(InventoryMovement).filter(
                        InventoryMovement.lot_id == lote.id
                    ).update({
                        InventoryMovement.product_id: existe_producto.id
                    }, synchronize_session=False)
                    
                    # Marcar producto viejo como potencialmente huérfano
                    productos_huerfanos.append(old_product_id)
                    
                elif not existe_producto:
                    # Crear nuevo producto con los datos editados
                    nombre_nuevo = change.get('nombre', producto_original.name if producto_original else 'Producto')
                    categoria_nombre = change.get('categoria', '')
                    
                    # Buscar o crear categoría
                    categoria_id = None
                    if categoria_nombre:
                        categoria = db.session.query(Category).filter(
                            Category.company_id == cid,
                            Category.name == categoria_nombre.strip()
                        ).first()
                        if not categoria:
                            categoria = Category(company_id=cid, name=categoria_nombre.strip())
                            db.session.add(categoria)
                            db.session.flush()
                        categoria_id = categoria.id
                    
                    # Crear producto nuevo copiando TODAS las características del original
                    producto_nuevo = Product(
                        company_id=cid,
                        name=nombre_nuevo,
                        internal_code=codigo_interno_nuevo,
                        category_id=categoria_id,
                        # Copiar características del producto original
                        unit_name=producto_original.unit_name if producto_original else 'Unidad',
                        sale_price=producto_original.sale_price if producto_original else 0.0,
                        description=producto_original.description if producto_original else None,
                        costo_unitario_referencia=producto_original.costo_unitario_referencia if producto_original else None,
                        method=producto_original.method if producto_original else 'FIFO',
                        min_stock=producto_original.min_stock if producto_original else 0.0,
                        reorder_point=producto_original.reorder_point if producto_original else 0.0,
                        uses_lots=True,
                        stock_ilimitado=producto_original.stock_ilimitado if producto_original else False,
                        # Copiar imagen si existe
                        image_filename=producto_original.image_filename if producto_original else None,
                        image_file_id=producto_original.image_file_id if producto_original else None
                    )
                    db.session.add(producto_nuevo)
                    db.session.flush()
                    
                    # Actualizar datos del lote ANTES de reasociar
                    if 'proveedor' in change:
                        lote.supplier_name = change['proveedor']
                    
                    # Reasociar lote al producto nuevo
                    lote.product_id = producto_nuevo.id
                    
                    # Actualizar movimientos asociados
                    db.session.query(InventoryMovement).filter(
                        InventoryMovement.lot_id == lote.id
                    ).update({
                        InventoryMovement.product_id: producto_nuevo.id
                    }, synchronize_session=False)
                    
                    # Marcar producto viejo como potencialmente huérfano
                    productos_huerfanos.append(old_product_id)
                else:
                    # Si el código nuevo ya existe, reasociar al existente
                    lote.product_id = existe_producto.id
                    
                    db.session.query(InventoryMovement).filter(
                        InventoryMovement.lot_id == lote.id
                    ).update({
                        InventoryMovement.product_id: existe_producto.id
                    }, synchronize_session=False)
                    
                    productos_huerfanos.append(old_product_id)
            
            # Caso 3: Código interno NO cambió → Solo actualizar datos del lote (NO del producto)
            else:
                # IMPORTANTE: NO modificar el producto maestro
                # Solo actualizar datos específicos del lote
                if 'proveedor' in change:
                    lote.supplier_name = change['proveedor']
                
                if 'cantidad' in change:
                    cantidad_nueva = float(change['cantidad'])
                    if cantidad_nueva != float(lote.qty_initial or 0):
                        # Actualizar cantidad
                        lote.qty_initial = cantidad_nueva
                        lote.qty_available = cantidad_nueva
                        
                        # Actualizar movimiento de ingreso
                        db.session.query(InventoryMovement).filter(
                            InventoryMovement.lot_id == lote.id,
                            InventoryMovement.type == 'purchase'
                        ).update({
                            InventoryMovement.qty_delta: cantidad_nueva,
                            InventoryMovement.total_cost: cantidad_nueva * float(lote.unit_cost or 0)
                        }, synchronize_session=False)
                
                if 'costo_unitario' in change:
                    costo_nuevo = float(change['costo_unitario'])
                    if costo_nuevo != float(lote.unit_cost or 0):
                        lote.unit_cost = costo_nuevo
                        
                        # Actualizar movimiento de ingreso
                        db.session.query(InventoryMovement).filter(
                            InventoryMovement.lot_id == lote.id,
                            InventoryMovement.type == 'purchase'
                        ).update({
                            InventoryMovement.unit_cost: costo_nuevo,
                            InventoryMovement.total_cost: float(lote.qty_initial or 0) * costo_nuevo
                        }, synchronize_session=False)
                
                if 'vencimiento' in change and change['vencimiento']:
                    try:
                        from datetime import date as dt_date
                        venc_str = change['vencimiento']
                        if isinstance(venc_str, str) and venc_str:
                            parts = venc_str.split('-')
                            if len(parts) == 3:
                                lote.expiration_date = dt_date(int(parts[0]), int(parts[1]), int(parts[2]))
                    except Exception:
                        pass
        
        # Limpiar productos huérfanos (que quedaron sin lotes)
        for prod_id in set(productos_huerfanos):
            producto = db.session.get(Product, prod_id)
            if not producto:
                continue
            
            # Verificar si tiene lotes
            tiene_lotes = db.session.query(InventoryLot.id).filter(
                InventoryLot.product_id == prod_id
            ).first() is not None
            
            # Verificar si tiene movimientos directos
            tiene_movimientos = db.session.query(InventoryMovement.id).filter(
                InventoryMovement.product_id == prod_id
            ).first() is not None
            
            # Si no tiene lotes ni movimientos, marcarlo como eliminado (soft delete)
            if not tiene_lotes and not tiene_movimientos:
                # Soft delete para poder revivir después si es necesario
                producto.deleted_at = datetime.utcnow()
                producto.active = False
                # NO eliminar físicamente para mantener trazabilidad y permitir "revivir"
        
        db.session.commit()
        
        return jsonify({
            'ok': True,
            'message': 'Tanda actualizada correctamente',
            'cleaned_products': len(set(productos_huerfanos))
        })
        
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception('Error updating tanda')
        return jsonify({
            'ok': False,
            'error': 'db_error',
            'detail': str(e)
        }), 500
