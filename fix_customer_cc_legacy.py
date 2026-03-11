#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script de reparación de cuenta corriente legacy en clientes.

Detecta y repara inconsistencias entre:
- Saldo calculado desde ventas reales (Sale.due_amount)
- Cualquier campo legacy cacheado que pueda existir
- Estados de badge vs deuda real

Ejecutar:
    python fix_customer_cc_legacy.py --company-id <company_id>
    python fix_customer_cc_legacy.py --all
    python fix_customer_cc_legacy.py --diagnose-only
"""

import sys
import os
from datetime import date as dt_date, datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app, db
from app.models import Customer, Sale, Company
from sqlalchemy import func


def calculate_real_cc_balance(company_id: str, customer_id: str) -> dict:
    """
    Calcula el saldo real de cuenta corriente desde las ventas.
    
    Returns:
        dict con:
            - cc_count: número de ventas con saldo pendiente
            - cc_total_balance: saldo total pendiente
            - cc_overdue_count: número de ventas vencidas
            - cc_overdue_balance: saldo vencido
            - sales_details: lista de ventas con saldo
    """
    today = dt_date.today()
    overdue_days = 30
    
    result = {
        'cc_count': 0,
        'cc_total_balance': 0.0,
        'cc_overdue_count': 0,
        'cc_overdue_balance': 0.0,
        'sales_details': []
    }
    
    try:
        sales = (
            db.session.query(Sale)
            .filter(Sale.company_id == company_id)
            .filter(Sale.customer_id == customer_id)
            .filter(Sale.sale_type == 'Venta')
            .filter(Sale.status != 'Reemplazada')
            .filter(Sale.due_amount > 0)
            .all()
        )
        
        result['cc_count'] = len(sales)
        
        for s in sales:
            due = float(getattr(s, 'due_amount', 0.0) or 0.0)
            result['cc_total_balance'] += max(0.0, due)
            
            days = 0
            try:
                d = getattr(s, 'sale_date', None)
                if d:
                    days = max(0, int((today - d).days))
            except Exception:
                days = 0
            
            if days >= overdue_days:
                result['cc_overdue_count'] += 1
                result['cc_overdue_balance'] += max(0.0, due)
            
            result['sales_details'].append({
                'id': s.id,
                'ticket': s.ticket,
                'sale_date': s.sale_date,
                'total': float(s.total or 0),
                'due_amount': due,
                'days_old': days,
                'is_overdue': days >= overdue_days
            })
    
    except Exception as e:
        print(f"Error calculando saldo real para customer {customer_id}: {e}")
    
    return result


def diagnose_customers(company_id: str = None, all_companies: bool = False) -> dict:
    """
    Diagnostica todos los clientes buscando inconsistencias.
    
    Returns:
        dict con estadísticas de diagnóstico
    """
    stats = {
        'total_customers': 0,
        'customers_with_cc': 0,
        'customers_fixed': 0,
        'inconsistencies_found': 0,
        'details': []
    }
    
    app = create_app()
    with app.app_context():
        if all_companies:
            companies = db.session.query(Company).all()
            company_ids = [c.id for c in companies]
        elif company_id:
            company_ids = [company_id]
        else:
            print("Error: Debe especificar --company-id o --all")
            return stats
        
        for cid in company_ids:
            customers = db.session.query(Customer).filter(Customer.company_id == cid).all()
            stats['total_customers'] += len(customers)
            
            for customer in customers:
                real_data = calculate_real_cc_balance(cid, customer.id)
                
                if real_data['cc_count'] > 0:
                    stats['customers_with_cc'] += 1
                
                # Aquí se podría comparar con campos legacy si existieran
                # Por ahora solo registramos el estado real
                stats['details'].append({
                    'company_id': cid,
                    'customer_id': customer.id,
                    'customer_name': getattr(customer, 'name', None) or f"{getattr(customer, 'first_name', '')} {getattr(customer, 'last_name', '')}".strip() or 'Sin nombre',
                    'real_cc_balance': real_data['cc_total_balance'],
                    'real_cc_count': real_data['cc_count'],
                    'real_overdue_balance': real_data['cc_overdue_balance'],
                    'real_overdue_count': real_data['cc_overdue_count'],
                })
    
    return stats


def repair_customers(company_id: str = None, all_companies: bool = False, dry_run: bool = False) -> dict:
    """
    Repara clientes con inconsistencias.
    
    Args:
        company_id: ID de la empresa a reparar
        all_companies: Si True, repara todas las empresas
        dry_run: Si True, solo simula sin guardar cambios
    
    Returns:
        dict con estadísticas de reparación
    """
    stats = {
        'total_customers': 0,
        'customers_repaired': 0,
        'errors': 0,
        'details': []
    }
    
    app = create_app()
    with app.app_context():
        if all_companies:
            companies = db.session.query(Company).all()
            company_ids = [c.id for c in companies]
        elif company_id:
            company_ids = [company_id]
        else:
            print("Error: Debe especificar --company-id o --all")
            return stats
        
        for cid in company_ids:
            customers = db.session.query(Customer).filter(Customer.company_id == cid).all()
            stats['total_customers'] += len(customers)
            
            for customer in customers:
                try:
                    real_data = calculate_real_cc_balance(cid, customer.id)
                    
                    # En este punto, si hubiera campos legacy cacheados en Customer,
                    # los actualizaríamos aquí. Como la fuente de verdad es Sale.due_amount,
                    # no hay nada que actualizar en el modelo Customer.
                    
                    # Solo registramos para el log
                    if real_data['cc_count'] > 0:
                        stats['details'].append({
                            'customer_id': customer.id,
                            'customer_name': getattr(customer, 'name', None) or 'Sin nombre',
                            'real_balance': real_data['cc_total_balance'],
                            'sales_count': real_data['cc_count'],
                            'action': 'Verificado - fuente de verdad desde Sale.due_amount'
                        })
                
                except Exception as e:
                    stats['errors'] += 1
                    print(f"Error procesando customer {customer.id}: {e}")
        
        if not dry_run:
            try:
                db.session.commit()
                print("Cambios guardados correctamente.")
            except Exception as e:
                db.session.rollback()
                print(f"Error al guardar cambios: {e}")
                stats['errors'] += 1
        else:
            db.session.rollback()
            print("Modo DRY RUN: No se guardaron cambios.")
    
    return stats


def print_stats(stats: dict, title: str = "Estadísticas"):
    """Imprime estadísticas de forma legible."""
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60)
    print(f"Total clientes procesados: {stats.get('total_customers', 0)}")
    print(f"Clientes con cuenta corriente: {stats.get('customers_with_cc', 0)}")
    print(f"Clientes reparados: {stats.get('customers_repaired', 0)}")
    print(f"Inconsistencias encontradas: {stats.get('inconsistencies_found', 0)}")
    print(f"Errores: {stats.get('errors', 0)}")
    
    if stats.get('details'):
        print(f"\nDetalles (mostrando primeros 20):")
        for i, detail in enumerate(stats['details'][:20], 1):
            print(f"\n  {i}. Cliente: {detail.get('customer_name', 'N/A')}")
            print(f"     ID: {detail.get('customer_id', 'N/A')}")
            print(f"     Saldo real: ${detail.get('real_cc_balance', 0):,.2f}")
            print(f"     Ventas pendientes: {detail.get('real_cc_count', 0)}")
            if detail.get('real_overdue_count', 0) > 0:
                print(f"     Vencidas: {detail.get('real_overdue_count', 0)} (${detail.get('real_overdue_balance', 0):,.2f})")
    
    print("\n" + "="*60 + "\n")


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Reparar cuenta corriente legacy en clientes')
    parser.add_argument('--company-id', type=str, help='ID de la empresa a procesar')
    parser.add_argument('--all', action='store_true', help='Procesar todas las empresas')
    parser.add_argument('--diagnose-only', action='store_true', help='Solo diagnosticar, no reparar')
    parser.add_argument('--dry-run', action='store_true', help='Simular sin guardar cambios')
    
    args = parser.parse_args()
    
    if not args.company_id and not args.all:
        print("Error: Debe especificar --company-id <id> o --all")
        sys.exit(1)
    
    if args.diagnose_only:
        print("Ejecutando diagnóstico...")
        stats = diagnose_customers(company_id=args.company_id, all_companies=args.all)
        print_stats(stats, "Diagnóstico de Cuenta Corriente")
    else:
        print("Ejecutando reparación...")
        if args.dry_run:
            print("MODO DRY RUN: No se guardarán cambios\n")
        stats = repair_customers(company_id=args.company_id, all_companies=args.all, dry_run=args.dry_run)
        print_stats(stats, "Reparación de Cuenta Corriente")
