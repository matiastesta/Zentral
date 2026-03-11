#!/usr/bin/env python3
"""
Script de diagnóstico y reparación para clientes con múltiples cuentas corrientes.

Problema:
- Clientes tienen múltiples tickets CC pendientes pero solo se muestra uno
- Legajo/modal/métricas inconsistentes

Solución:
- Recalcular TODOS los clientes desde ventas reales (Sale.due_amount)
- Detectar inconsistencias automáticamente
- Generar reporte completo

Uso:
    python fix_multiple_cc_accounts.py --all --diagnose-only
    python fix_multiple_cc_accounts.py --company-id <id> --customer-id <id>
    python fix_multiple_cc_accounts.py --all --fix
"""

import sys
import os
import argparse
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app import create_app, db
from app.models import Company, Customer, Sale
from sqlalchemy import func


def diagnose_customer(customer, company_id, verbose=False):
    """
    Diagnostica la cuenta corriente de un cliente específico.
    
    Returns:
        dict con información del diagnóstico
    """
    cid = str(customer.id or '').strip()
    customer_name = (
        getattr(customer, 'name', None) 
        or f"{getattr(customer, 'first_name', '')} {getattr(customer, 'last_name', '')}".strip() 
        or 'Sin nombre'
    )
    
    # Obtener TODAS las ventas CC pendientes
    sales_with_debt = (
        db.session.query(Sale)
        .filter(Sale.company_id == company_id)
        .filter(Sale.customer_id == cid)
        .filter(Sale.sale_type == 'Venta')
        .filter(Sale.status != 'Reemplazada')
        .filter(Sale.status != 'Anulado')
        .filter(Sale.due_amount > 0)
        .order_by(Sale.sale_date.asc())
        .all()
    )
    
    # Calcular saldo real desde tickets
    tickets = []
    total_cc_balance = 0.0
    
    for sale in sales_with_debt:
        due = float(getattr(sale, 'due_amount', 0.0) or 0.0)
        total = float(getattr(sale, 'total', 0.0) or 0.0)
        paid = max(0.0, total - due)
        
        if due > 0.001:
            total_cc_balance += due
            tickets.append({
                'sale_id': str(sale.id),
                'ticket': str(getattr(sale, 'ticket', '') or ''),
                'sale_date': str(getattr(sale, 'sale_date', '') or ''),
                'total': round(total, 2),
                'paid': round(paid, 2),
                'due_amount': round(due, 2)
            })
    
    # Obtener todas las ventas (incluyendo pagadas)
    all_sales = (
        db.session.query(Sale)
        .filter(Sale.company_id == company_id)
        .filter(Sale.customer_id == cid)
        .filter(Sale.sale_type == 'Venta')
        .filter(Sale.status != 'Reemplazada')
        .filter(Sale.status != 'Anulado')
        .all()
    )
    
    total_purchases = len([s for s in all_sales if float(getattr(s, 'total', 0.0) or 0.0) > 0])
    total_amount = sum([float(getattr(s, 'total', 0.0) or 0.0) for s in all_sales])
    
    return {
        'customer_id': cid,
        'customer_name': customer_name,
        'total_purchases': total_purchases,
        'total_amount': round(total_amount, 2),
        'cc_balance': round(total_cc_balance, 2),
        'pending_tickets_count': len(tickets),
        'pending_tickets': tickets,
        'has_multiple_cc': len(tickets) > 1,
        'needs_attention': len(tickets) > 1  # Múltiples CC = caso a revisar
    }


def diagnose_all_customers(company_id, verbose=False):
    """
    Diagnostica TODOS los clientes de una empresa.
    
    Returns:
        dict con resumen y casos problemáticos
    """
    customers = db.session.query(Customer).filter(Customer.company_id == company_id).all()
    
    results = []
    stats = {
        'total_customers': len(customers),
        'customers_with_debt': 0,
        'customers_with_multiple_cc': 0,
        'total_debt_amount': 0.0
    }
    
    for customer in customers:
        diagnosis = diagnose_customer(customer, company_id, verbose=verbose)
        
        if diagnosis['cc_balance'] > 0.001:
            stats['customers_with_debt'] += 1
            stats['total_debt_amount'] += diagnosis['cc_balance']
            results.append(diagnosis)
            
            if diagnosis['has_multiple_cc']:
                stats['customers_with_multiple_cc'] += 1
                if verbose:
                    print(f"⚠️  {diagnosis['customer_name']}: {diagnosis['pending_tickets_count']} tickets CC pendientes, total ${diagnosis['cc_balance']}")
    
    return {
        'stats': stats,
        'customers_with_debt': results
    }


def print_diagnosis(diagnosis):
    """Imprime el diagnóstico de forma legible."""
    stats = diagnosis['stats']
    customers = diagnosis['customers_with_debt']
    
    print("\n" + "="*80)
    print("DIAGNÓSTICO DE CUENTAS CORRIENTES")
    print("="*80)
    print(f"\nTotal de clientes analizados: {stats['total_customers']}")
    print(f"Clientes con deuda: {stats['customers_with_debt']}")
    print(f"Clientes con MÚLTIPLES tickets CC: {stats['customers_with_multiple_cc']}")
    print(f"Deuda total del sistema: ${stats['total_debt_amount']:,.2f}")
    
    # Casos con múltiples CC
    multiple_cc = [c for c in customers if c['has_multiple_cc']]
    
    if multiple_cc:
        print("\n" + "-"*80)
        print("CLIENTES CON MÚLTIPLES TICKETS CC PENDIENTES (Requieren atención)")
        print("-"*80)
        
        for c in multiple_cc:
            print(f"\n👤 {c['customer_name']}")
            print(f"   ID: {c['customer_id']}")
            print(f"   Tickets pendientes: {c['pending_tickets_count']}")
            print(f"   Saldo total CC: ${c['cc_balance']:,.2f}")
            print(f"   Compras totales: {c['total_purchases']}")
            print(f"   Monto histórico: ${c['total_amount']:,.2f}")
            
            print(f"\n   Detalle de tickets:")
            for ticket in c['pending_tickets']:
                print(f"     • Ticket {ticket['ticket']} — {ticket['sale_date']}")
                print(f"       Total: ${ticket['total']:,.2f} | Pagado: ${ticket['paid']:,.2f} | Adeuda: ${ticket['due_amount']:,.2f}")
    
    # Top 10 deudores
    print("\n" + "-"*80)
    print("TOP 10 DEUDORES")
    print("-"*80)
    
    top_debtors = sorted(customers, key=lambda x: x['cc_balance'], reverse=True)[:10]
    
    for i, c in enumerate(top_debtors, 1):
        multi_flag = "⚠️ " if c['has_multiple_cc'] else "   "
        print(f"{i:2d}. {multi_flag}{c['customer_name']}: ${c['cc_balance']:,.2f} ({c['pending_tickets_count']} tickets)")


def main():
    parser = argparse.ArgumentParser(
        description='Diagnóstico y reparación de cuentas corrientes múltiples'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Diagnosticar todos los clientes de todas las empresas'
    )
    parser.add_argument(
        '--company-id',
        type=str,
        help='ID de la empresa a diagnosticar'
    )
    parser.add_argument(
        '--customer-id',
        type=str,
        help='ID del cliente específico a diagnosticar'
    )
    parser.add_argument(
        '--diagnose-only',
        action='store_true',
        help='Solo diagnosticar, no reparar'
    )
    parser.add_argument(
        '--fix',
        action='store_true',
        help='Aplicar reparaciones (NO IMPLEMENTADO - usar endpoints backend)'
    )
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Modo verbose'
    )
    
    args = parser.parse_args()
    
    # Crear app context
    app = create_app()
    
    with app.app_context():
        try:
            if args.fix:
                print("⚠️  MODO FIX NO IMPLEMENTADO")
                print("Las reparaciones deben hacerse a través del endpoint backend:")
                print("  POST /api/customers/<customer_id>/recalculate-cc")
                return 1
            
            if args.customer_id and args.company_id:
                # Diagnosticar cliente específico
                customer = db.session.query(Customer).filter(
                    Customer.company_id == args.company_id,
                    Customer.id == args.customer_id
                ).first()
                
                if not customer:
                    print(f"❌ Cliente {args.customer_id} no encontrado en empresa {args.company_id}")
                    return 1
                
                diagnosis = diagnose_customer(customer, args.company_id, verbose=True)
                
                print("\n" + "="*80)
                print(f"DIAGNÓSTICO: {diagnosis['customer_name']}")
                print("="*80)
                print(f"ID: {diagnosis['customer_id']}")
                print(f"Compras totales: {diagnosis['total_purchases']}")
                print(f"Monto histórico: ${diagnosis['total_amount']:,.2f}")
                print(f"Saldo CC actual: ${diagnosis['cc_balance']:,.2f}")
                print(f"Tickets CC pendientes: {diagnosis['pending_tickets_count']}")
                
                if diagnosis['pending_tickets']:
                    print("\nDetalle de tickets:")
                    for ticket in diagnosis['pending_tickets']:
                        print(f"  • Ticket {ticket['ticket']} — {ticket['sale_date']}")
                        print(f"    Total: ${ticket['total']:,.2f} | Pagado: ${ticket['paid']:,.2f} | Adeuda: ${ticket['due_amount']:,.2f}")
                
                if diagnosis['has_multiple_cc']:
                    print("\n⚠️  ATENCIÓN: Este cliente tiene MÚLTIPLES tickets CC pendientes")
                
                return 0
            
            elif args.company_id:
                # Diagnosticar todos los clientes de una empresa
                print(f"Diagnosticando empresa {args.company_id}...")
                diagnosis = diagnose_all_customers(args.company_id, verbose=args.verbose)
                print_diagnosis(diagnosis)
                return 0
            
            elif args.all:
                # Diagnosticar todas las empresas
                companies = db.session.query(Company).all()
                
                print(f"\nDiagnosticando {len(companies)} empresas...")
                
                for company in companies:
                    company_name = getattr(company, 'name', 'Sin nombre')
                    company_id = str(company.id)
                    
                    print(f"\n{'='*80}")
                    print(f"EMPRESA: {company_name} (ID: {company_id})")
                    print(f"{'='*80}")
                    
                    diagnosis = diagnose_all_customers(company_id, verbose=args.verbose)
                    print_diagnosis(diagnosis)
                
                return 0
            
            else:
                parser.print_help()
                return 1
        
        except Exception as e:
            print(f"\n❌ ERROR: {e}")
            import traceback
            traceback.print_exc()
            return 1


if __name__ == '__main__':
    sys.exit(main())
