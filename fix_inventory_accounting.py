#!/usr/bin/env python3
"""
Script para corregir problemas contables históricos del inventario:
1. Eliminar gastos operativos con categoría "Inventario" (origen=inventory)
2. Corregir horas 00:00:00 en received_at de lotes usando hora real de created_at
"""

import sys
from datetime import datetime, time
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent))

from app import create_app, db
from app.models import Expense, InventoryLot, InventoryMovement

def fix_inventory_expenses():
    """Eliminar gastos operativos generados incorrectamente desde inventario"""
    print("\n=== ELIMINANDO GASTOS OPERATIVOS DE INVENTARIO ===")
    
    # Buscar gastos con categoría "Inventario" y origin="inventory"
    gastos_inventario = db.session.query(Expense).filter(
        Expense.category == 'Inventario'
    ).all()
    
    count_origin = 0
    count_categoria = 0
    
    for gasto in gastos_inventario:
        origen = str(getattr(gasto, 'origin', '') or '').strip().lower()
        categoria = str(getattr(gasto, 'category', '') or '').strip()
        
        if origen == 'inventory' or categoria == 'Inventario':
            print(f"  - Eliminando gasto ID {gasto.id}: {categoria} | origen={origen} | valor=${gasto.amount}")
            db.session.delete(gasto)
            if origen == 'inventory':
                count_origin += 1
            else:
                count_categoria += 1
    
    db.session.commit()
    print(f"\n✓ Eliminados {count_origin + count_categoria} gastos operativos de inventario")
    print(f"  - Por origin='inventory': {count_origin}")
    print(f"  - Por categoría 'Inventario': {count_categoria}")
    
    return count_origin + count_categoria


def fix_received_at_times():
    """Corregir horas 00:00:00 en received_at usando created_at como referencia"""
    print("\n=== CORRIGIENDO HORAS EN RECEIVED_AT ===")
    
    lotes = db.session.query(InventoryLot).all()
    count_fixed = 0
    
    for lote in lotes:
        received_at = lote.received_at
        created_at = lote.created_at
        
        # Si received_at tiene hora 00:00:00 pero created_at tiene hora real
        if received_at and created_at:
            if received_at.time() == time(0, 0, 0) and created_at.time() != time(0, 0, 0):
                # Usar la hora de created_at
                nueva_hora = datetime.combine(received_at.date(), created_at.time())
                print(f"  - Lote {lote.id}: {received_at} → {nueva_hora}")
                lote.received_at = nueva_hora
                count_fixed += 1
    
    db.session.commit()
    print(f"\n✓ Corregidas {count_fixed} fechas de ingreso con hora real")
    
    return count_fixed


def main():
    app = create_app()
    
    with app.app_context():
        print("=" * 60)
        print("CORRECCIÓN CONTABLE DE INVENTARIO")
        print("=" * 60)
        
        # 1. Eliminar gastos operativos incorrectos
        gastos_eliminados = fix_inventory_expenses()
        
        # 2. Corregir horas en received_at
        horas_corregidas = fix_received_at_times()
        
        print("\n" + "=" * 60)
        print("RESUMEN FINAL")
        print("=" * 60)
        print(f"✓ Gastos operativos eliminados: {gastos_eliminados}")
        print(f"✓ Horas corregidas en received_at: {horas_corregidas}")
        print("\nInventario ahora impacta solo como activo, no como gasto operativo.")
        print("=" * 60)


if __name__ == '__main__':
    main()
