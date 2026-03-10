"""
Script de migración retroactiva para agrupar ingresos existentes en tandas de carga.

Este script analiza los lotes e ingresos de inventario existentes y los agrupa
en tandas basándose en heurísticas de tiempo, usuario y origen.
"""

from app import create_app, db
from app.models import TandaCarga, InventoryLot, InventoryMovement, User
from datetime import datetime, timedelta
from sqlalchemy import func
import sys

def generar_identificador_tanda(fecha, secuencia):
    """Genera un identificador único para la tanda."""
    fecha_str = fecha.strftime('%Y%m%d')
    return f'TANDA-{fecha_str}-{secuencia:03d}'


def agrupar_lotes_en_tandas(dry_run=True):
    """
    Agrupa lotes de inventario existentes en tandas.
    
    Criterios de agrupación:
    - Mismo company_id
    - Mismo received_at (fecha de ingreso EXACTA)
    
    Este criterio es ideal porque las importaciones Excel generan
    exactamente el mismo timestamp para todos los items de la misma carga.
    """
    app = create_app()
    with app.app_context():
        print("=" * 80)
        print("MIGRACIÓN RETROACTIVA DE TANDAS DE CARGA")
        print("=" * 80)
        print(f"Modo: {'DRY RUN (sin cambios)' if dry_run else 'COMMIT (aplicar cambios)'}")
        print()
        
        # Obtener todos los lotes que no tienen tanda asignada
        lotes_sin_tanda = (
            db.session.query(InventoryLot)
            .filter(InventoryLot.tanda_carga_id.is_(None))
            .order_by(InventoryLot.company_id, InventoryLot.received_at)
            .all()
        )
        
        print(f"Total de lotes sin tanda asignada: {len(lotes_sin_tanda)}")
        print()
        
        if not lotes_sin_tanda:
            print("No hay lotes para procesar.")
            return
        
        # Agrupar por company_id + received_at EXACTO
        lotes_por_grupo = {}
        lotes_sin_company = 0
        for lote in lotes_sin_tanda:
            company_id = lote.company_id
            if not company_id:
                lotes_sin_company += 1
                continue
            
            # Usar received_at exacto como clave de agrupación
            received_at = lote.received_at
            key = (company_id, received_at)
            
            if key not in lotes_por_grupo:
                lotes_por_grupo[key] = []
            lotes_por_grupo[key].append(lote)
        
        if lotes_sin_company > 0:
            print(f"⚠ Se omitieron {lotes_sin_company} lotes sin company_id")
        
        print(f"Agrupados en {len(lotes_por_grupo)} tandas únicas (por fecha_ingreso exacta)")
        print()
        
        tandas_creadas = 0
        lotes_asignados = 0
        movimientos_asignados = 0
        
        # Ordenar grupos por fecha para secuenciar identificadores
        grupos_ordenados = sorted(lotes_por_grupo.items(), key=lambda x: x[0][1])
        
        secuencia_global = 1
        for (company_id, received_at), grupo_lotes in grupos_ordenados:
            if not grupo_lotes:
                continue
            
            # Detectar tipo de origen
            tipo_origen = 'historico_reconstruido'
            for lote in grupo_lotes:
                nota = (lote.note or '').lower()
                if 'excel' in nota or 'importa' in nota:
                    tipo_origen = 'excel'
                    break
            
            # Calcular estadísticas
            cantidad_items = len(grupo_lotes)
            cantidad_total_unidades = sum(lote.qty_initial for lote in grupo_lotes)
            
            # Generar identificador basado en received_at
            fecha_hora = received_at
            identificador = generar_identificador_tanda(fecha_hora, secuencia_global)
            
            observacion = f'Migración retroactiva: {cantidad_items} lotes agrupados del {fecha_hora.strftime("%d/%m/%Y %H:%M:%S")}'
            
            if not dry_run:
                tanda = TandaCarga(
                    company_id=company_id,
                    identificador=identificador,
                    tipo_origen=tipo_origen,
                    fecha_hora_creacion=fecha_hora,
                    user_id=None,  # No tenemos forma de saber el usuario original
                    cantidad_items=cantidad_items,
                    cantidad_total_unidades=cantidad_total_unidades,
                    observacion=observacion,
                    estado='activa',
                    created_at=fecha_hora,
                    updated_at=datetime.utcnow()
                )
                db.session.add(tanda)
                db.session.flush()
                
                # Asignar lotes a la tanda
                for lote in grupo_lotes:
                    lote.tanda_carga_id = tanda.id
                    lotes_asignados += 1
                
                # Asignar movimientos relacionados
                for lote in grupo_lotes:
                    movimientos = (
                        db.session.query(InventoryMovement)
                        .filter(
                            InventoryMovement.lot_id == lote.id,
                            InventoryMovement.tanda_carga_id.is_(None),
                            InventoryMovement.type == 'purchase'
                        )
                        .all()
                    )
                    for mov in movimientos:
                        mov.tanda_carga_id = tanda.id
                        movimientos_asignados += 1
                
                tandas_creadas += 1
                
                print(f"✓ Tanda {identificador}: {cantidad_items} lotes, {cantidad_total_unidades:.2f} unidades ({tipo_origen}) - {fecha_hora.strftime('%d/%m/%Y %H:%M:%S')}")
            else:
                print(f"[DRY RUN] Tanda {identificador}: {cantidad_items} lotes, {cantidad_total_unidades:.2f} unidades ({tipo_origen}) - {fecha_hora.strftime('%d/%m/%Y %H:%M:%S')}")
                tandas_creadas += 1
                lotes_asignados += cantidad_items
            
            secuencia_global += 1
        
        print()
        print("=" * 80)
        print("RESUMEN")
        print("=" * 80)
        print(f"Tandas creadas: {tandas_creadas}")
        print(f"Lotes asignados: {lotes_asignados}")
        print(f"Movimientos asignados: {movimientos_asignados}")
        
        if not dry_run:
            try:
                db.session.commit()
                print()
                print("✓ Cambios aplicados correctamente")
            except Exception as e:
                db.session.rollback()
                print()
                print(f"✗ Error al aplicar cambios: {e}")
                raise
        else:
            print()
            print("Esto es un DRY RUN. No se aplicaron cambios.")
            print("Ejecuta con --commit para aplicar los cambios.")


def revertir_migracion(dry_run=True):
    """Revierte la migración retroactiva, eliminando todas las tandas."""
    app = create_app()
    with app.app_context():
        print("=" * 80)
        print("REVERTIR MIGRACIÓN DE TANDAS")
        print("=" * 80)
        print(f"Modo: {'DRY RUN (sin cambios)' if dry_run else 'COMMIT (aplicar cambios)'}")
        print()
        
        tandas = db.session.query(TandaCarga).all()
        print(f"Tandas encontradas: {len(tandas)}")
        
        if not dry_run:
            # Desasignar lotes
            db.session.query(InventoryLot).update({InventoryLot.tanda_carga_id: None})
            
            # Desasignar movimientos
            db.session.query(InventoryMovement).update({InventoryMovement.tanda_carga_id: None})
            
            # Eliminar tandas
            db.session.query(TandaCarga).delete()
            
            db.session.commit()
            print("✓ Migración revertida correctamente")
        else:
            print("Esto es un DRY RUN. Ejecuta con --commit para revertir.")


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Migración retroactiva de tandas de carga')
    parser.add_argument('--commit', action='store_true', help='Aplicar cambios (sin esto es dry-run)')
    parser.add_argument('--revert', action='store_true', help='Revertir migración')
    
    args = parser.parse_args()
    
    if args.revert:
        revertir_migracion(dry_run=not args.commit)
    else:
        agrupar_lotes_en_tandas(dry_run=not args.commit)
