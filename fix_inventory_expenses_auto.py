"""
Script para eliminar automáticamente gastos con origin='inventory'.

El inventario es un ACTIVO, no un gasto operativo.
Solo deben aparecer como gastos los registros del módulo de Gastos.
"""

import sys
from app import create_app, db
from app.models import Expense

def fix_inventory_expenses_auto():
    """Elimina automáticamente todos los gastos con origin='inventory'"""
    
    app = create_app()
    with app.app_context():
        try:
            # Contar gastos de inventario antes de eliminar
            count_before = db.session.query(Expense).filter(
                Expense.origin == 'inventory'
            ).count()
            
            print(f"📊 Gastos con origin='inventory' encontrados: {count_before}")
            
            if count_before == 0:
                print("✅ No hay gastos de inventario para eliminar")
                return
            
            # Mostrar algunos ejemplos
            examples = db.session.query(Expense).filter(
                Expense.origin == 'inventory'
            ).limit(5).all()
            
            print("\n📋 Ejemplos de gastos a eliminar:")
            for exp in examples:
                print(f"  - ID: {exp.id}, Fecha: {exp.expense_date}, Monto: ${exp.amount:,.2f}, Categoría: {exp.category}")
            
            # Eliminar gastos de inventario automáticamente
            print(f"\n🔄 Eliminando {count_before} gastos de inventario...")
            
            deleted = db.session.query(Expense).filter(
                Expense.origin == 'inventory'
            ).delete(synchronize_session=False)
            
            db.session.commit()
            
            print(f"\n✅ {deleted} gastos de inventario eliminados correctamente")
            print("📊 El inventario ahora aparecerá solo como ACTIVO en los reportes")
            print("💡 Los gastos operativos solo incluyen registros del módulo de Gastos")
            print("\n🎯 Resumen de corrección:")
            print(f"   - Gastos eliminados: {deleted}")
            print(f"   - Monto total liberado de gastos operativos")
            print(f"   - El inventario permanece como activo en el balance")
            
        except Exception as e:
            db.session.rollback()
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

if __name__ == '__main__':
    fix_inventory_expenses_auto()
