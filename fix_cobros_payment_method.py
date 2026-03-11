#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script de migración para corregir payment_method de CobroCC/CobroCuota históricos.

Este script identifica cobros que tienen payment_method='Efectivo' pero que en realidad
fueron por otro medio (transferencia, débito, crédito) y los corrige basándose en la
nota del cobro.

Uso:
    FLASK_APP=app flask shell < fix_cobros_payment_method.py
"""

from app import db
from app.models import Sale, SalePayment
from sqlalchemy import and_

def fix_cobros_payment_method():
    """Corrige payment_method de CobroCC/CobroCuota sin SalePayment."""
    
    # Buscar todos los CobroCC/CobroCuota que:
    # 1. No tengan SalePayment
    # 2. Tengan payment_method que no sea confiable
    
    cobros = (
        db.session.query(Sale)
        .outerjoin(SalePayment, and_(SalePayment.sale_id == Sale.id))
        .filter(Sale.sale_type.in_(['CobroCC', 'CobroCuota']))
        .filter(Sale.status == 'Completada')
        .filter(SalePayment.id.is_(None))  # Sin SalePayment
        .all()
    )
    
    print(f"Encontrados {len(cobros)} cobros sin SalePayment")
    
    updated = 0
    for cobro in cobros:
        ticket = getattr(cobro, 'ticket', '')
        notes = str(getattr(cobro, 'notes', '') or '').lower()
        current_pm = str(getattr(cobro, 'payment_method', '') or '').strip()
        total = float(getattr(cobro, 'total', 0.0) or 0.0)
        
        # Inferir el método real basándose en patrones comunes
        real_method = None
        
        if 'transfer' in notes or 'transf' in notes:
            real_method = 'transfer'
        elif 'debito' in notes or 'débito' in notes or 'debit' in notes:
            real_method = 'debit'
        elif 'credito' in notes or 'crédito' in notes or 'credit' in notes:
            real_method = 'credit'
        elif 'mercadopago' in notes or 'mp' in notes or 'wallet' in notes:
            real_method = 'mercadopago'
        elif 'efectivo' in notes or 'cash' in notes:
            real_method = 'cash'
        
        # Si no podemos inferirlo y tiene "Efectivo", dejarlo pero crear SalePayment
        if not real_method:
            if current_pm and 'efectiv' in current_pm.lower():
                real_method = 'cash'
            else:
                # Si no sabemos, marcarlo como desconocido para revisión manual
                real_method = 'unknown'
        
        # Actualizar payment_method
        if real_method and real_method != 'unknown':
            cobro.payment_method = real_method
            
            # Crear SalePayment correspondiente
            try:
                sp = SalePayment(
                    company_id=getattr(cobro, 'company_id', ''),
                    sale_id=int(getattr(cobro, 'id', 0)),
                    method=real_method,
                    amount=total,
                )
                db.session.add(sp)
                updated += 1
                print(f"  ✓ {ticket}: {current_pm} → {real_method} (${total:,.2f})")
            except Exception as e:
                print(f"  ✗ {ticket}: Error al crear SalePayment: {e}")
        else:
            print(f"  ? {ticket}: No se pudo inferir el método ({current_pm})")
    
    if updated > 0:
        confirm = input(f"\n¿Confirmar actualización de {updated} cobros? (s/N): ")
        if confirm.lower() == 's':
            db.session.commit()
            print(f"✓ Actualizados {updated} cobros correctamente")
        else:
            db.session.rollback()
            print("✗ Operación cancelada")
    else:
        print("No hay cobros para actualizar")

if __name__ == '__main__':
    fix_cobros_payment_method()
