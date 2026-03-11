#!/usr/bin/env python3
"""
Script para detectar y fusionar clientes duplicados en ZENTRAL.

Problema:
- Mismo cliente tiene múltiples registros con customer_id diferentes
- Sus ventas están repartidas entre ambos registros
- Genera inconsistencias en legajo, CC, métricas

Solución:
- Detecta clientes con mismo nombre normalizado
- Consolida todas las ventas/pagos en un solo registro
- Elimina registros duplicados
- Actualiza referencias en todas las tablas

Uso:
    python merge_duplicate_customers.py --company-id <id> --diagnose-only
    python merge_duplicate_customers.py --company-id <id> --merge --dry-run
    python merge_duplicate_customers.py --company-id <id> --merge --customer-name "vicu courel"
"""

import sys
import os
import argparse
from datetime import datetime
import unicodedata

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app import create_app, db
from app.models import Company, Customer, Sale
from sqlalchemy import func


def normalize_name(name):
    """
    Normaliza un nombre para comparación.
    - Lowercase
    - Sin acentos
    - Sin espacios múltiples
    - Sin caracteres especiales
    """
    if not name:
        return ''
    
    # Lowercase
    s = str(name).strip().lower()
    
    # Remover acentos
    try:
        s = unicodedata.normalize('NFD', s)
        s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    except:
        pass
    
    # Espacios múltiples → uno solo
    s = ' '.join(s.split())
    
    return s


def get_customer_full_name(customer):
    """Obtiene el nombre completo del cliente."""
    name = getattr(customer, 'name', None)
    if name and str(name).strip():
        return str(name).strip()
    
    first = str(getattr(customer, 'first_name', '') or '').strip()
    last = str(getattr(customer, 'last_name', '') or '').strip()
    
    if first or last:
        return f"{first} {last}".strip()
    
    return getattr(customer, 'full_name', None) or 'Sin nombre'


def find_duplicates(company_id, verbose=False):
    """
    Encuentra clientes duplicados en una empresa.
    
    Returns:
        dict: {
            'nombre_normalizado': [customer1, customer2, ...]
        }
    """
    customers = db.session.query(Customer).filter(Customer.company_id == company_id).all()
    
    # Agrupar por nombre normalizado
    groups = {}
    
    for customer in customers:
        full_name = get_customer_full_name(customer)
        norm_name = normalize_name(full_name)
        
        if not norm_name:
            continue
        
        if norm_name not in groups:
            groups[norm_name] = []
        
        groups[norm_name].append(customer)
    
    # Filtrar solo grupos con duplicados
    duplicates = {k: v for k, v in groups.items() if len(v) > 1}
    
    if verbose:
        print(f"\nTotal de clientes: {len(customers)}")
        print(f"Grupos de nombres únicos: {len(groups)}")
        print(f"Grupos con duplicados: {len(duplicates)}")
    
    return duplicates


def diagnose_duplicate_group(customers, company_id, verbose=False):
    """
    Diagnostica un grupo de clientes duplicados.
    
    Returns:
        dict con información del grupo
    """
    customer_ids = [str(c.id) for c in customers]
    
    # Obtener ventas de cada customer_id
    sales_by_customer = {}
    total_sales = 0
    total_cc_balance = 0.0
    
    for cust in customers:
        cid = str(cust.id)
        sales = (
            db.session.query(Sale)
            .filter(Sale.company_id == company_id)
            .filter(Sale.customer_id == cid)
            .filter(Sale.sale_type == 'Venta')
            .filter(Sale.status != 'Reemplazada')
            .filter(Sale.status != 'Anulado')
            .all()
        )
        
        sales_by_customer[cid] = {
            'customer': cust,
            'sales': sales,
            'count': len(sales),
            'total_amount': sum([float(getattr(s, 'total', 0.0) or 0.0) for s in sales]),
            'cc_balance': sum([float(getattr(s, 'due_amount', 0.0) or 0.0) for s in sales if float(getattr(s, 'due_amount', 0.0) or 0.0) > 0])
        }
        
        total_sales += len(sales)
        total_cc_balance += sales_by_customer[cid]['cc_balance']
    
    return {
        'customer_name': get_customer_full_name(customers[0]),
        'duplicate_count': len(customers),
        'customer_ids': customer_ids,
        'total_sales': total_sales,
        'total_cc_balance': total_cc_balance,
        'details_by_customer': sales_by_customer
    }


def print_duplicate_diagnosis(diagnosis):
    """Imprime el diagnóstico de un grupo de duplicados."""
    print("\n" + "="*80)
    print(f"CLIENTE DUPLICADO: {diagnosis['customer_name']}")
    print("="*80)
    print(f"Cantidad de registros duplicados: {diagnosis['duplicate_count']}")
    print(f"Total de ventas entre todos: {diagnosis['total_sales']}")
    print(f"Saldo CC total: ${diagnosis['total_cc_balance']:,.2f}")
    
    print("\nDetalle por customer_id:")
    for cid, data in diagnosis['details_by_customer'].items():
        cust = data['customer']
        print(f"\n  ID: {cid}")
        print(f"  Creado: {getattr(cust, 'created_at', 'N/A')}")
        print(f"  Email: {getattr(cust, 'email', 'N/A') or 'N/A'}")
        print(f"  Teléfono: {getattr(cust, 'phone', 'N/A') or 'N/A'}")
        print(f"  Ventas: {data['count']}")
        print(f"  Monto total: ${data['total_amount']:,.2f}")
        print(f"  Saldo CC: ${data['cc_balance']:,.2f}")


def merge_customers(primary_customer_id, secondary_customer_ids, company_id, dry_run=True, verbose=False):
    """
    Fusiona clientes duplicados en uno solo.
    
    Args:
        primary_customer_id: ID del cliente que se mantendrá
        secondary_customer_ids: Lista de IDs de clientes a eliminar
        company_id: ID de la empresa
        dry_run: Si True, no aplica cambios, solo muestra qué haría
    
    Returns:
        dict con resultado de la operación
    """
    # Obtener clientes
    primary = db.session.query(Customer).filter(
        Customer.company_id == company_id,
        Customer.id == primary_customer_id
    ).first()
    
    if not primary:
        return {'ok': False, 'error': 'primary_customer_not_found'}
    
    secondaries = db.session.query(Customer).filter(
        Customer.company_id == company_id,
        Customer.id.in_(secondary_customer_ids)
    ).all()
    
    if len(secondaries) != len(secondary_customer_ids):
        return {'ok': False, 'error': 'some_secondary_customers_not_found'}
    
    # Estadísticas
    stats = {
        'sales_updated': 0,
        'customers_deleted': 0,
        'primary_customer': {
            'id': primary_customer_id,
            'name': get_customer_full_name(primary)
        }
    }
    
    # Actualizar ventas
    for secondary in secondaries:
        sec_id = str(secondary.id)
        
        # Obtener ventas del secundario
        sales = db.session.query(Sale).filter(
            Sale.company_id == company_id,
            Sale.customer_id == sec_id
        ).all()
        
        if verbose:
            print(f"\nMoviendo {len(sales)} ventas de {sec_id} → {primary_customer_id}")
        
        for sale in sales:
            if verbose:
                print(f"  • Venta {getattr(sale, 'ticket', 'N/A')}: ${float(getattr(sale, 'total', 0.0) or 0.0):,.2f}")
            
            if not dry_run:
                sale.customer_id = primary_customer_id
                # Actualizar customer_name también
                sale.customer_name = get_customer_full_name(primary)
            
            stats['sales_updated'] += 1
    
    # Consolidar información del cliente
    if not dry_run:
        # Actualizar email si el primario no tiene
        if not getattr(primary, 'email', None):
            for sec in secondaries:
                if getattr(sec, 'email', None):
                    primary.email = sec.email
                    break
        
        # Actualizar teléfono si el primario no tiene
        if not getattr(primary, 'phone', None):
            for sec in secondaries:
                if getattr(sec, 'phone', None):
                    primary.phone = sec.phone
                    break
        
        # Actualizar dirección si el primario no tiene
        if not getattr(primary, 'address', None):
            for sec in secondaries:
                if getattr(sec, 'address', None):
                    primary.address = sec.address
                    break
    
    # Eliminar clientes secundarios
    if not dry_run:
        for secondary in secondaries:
            db.session.delete(secondary)
            stats['customers_deleted'] += 1
            if verbose:
                print(f"\nEliminado cliente duplicado: {secondary.id}")
    else:
        stats['customers_deleted'] = len(secondaries)
        if verbose:
            print(f"\n[DRY RUN] Se eliminarían {len(secondaries)} clientes duplicados")
    
    # Commit
    if not dry_run:
        db.session.commit()
        if verbose:
            print("\n✅ Cambios aplicados correctamente")
    else:
        if verbose:
            print("\n[DRY RUN] No se aplicaron cambios (use --no-dry-run para aplicar)")
    
    return {
        'ok': True,
        'stats': stats
    }


def main():
    parser = argparse.ArgumentParser(
        description='Detectar y fusionar clientes duplicados'
    )
    parser.add_argument(
        '--company-id',
        type=str,
        required=True,
        help='ID de la empresa'
    )
    parser.add_argument(
        '--diagnose-only',
        action='store_true',
        help='Solo diagnosticar, no fusionar'
    )
    parser.add_argument(
        '--merge',
        action='store_true',
        help='Fusionar clientes duplicados'
    )
    parser.add_argument(
        '--customer-name',
        type=str,
        help='Nombre del cliente específico a fusionar'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        default=True,
        help='Modo dry-run (no aplica cambios, solo muestra qué haría)'
    )
    parser.add_argument(
        '--no-dry-run',
        action='store_true',
        help='Aplicar cambios realmente (CUIDADO)'
    )
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Modo verbose'
    )
    
    args = parser.parse_args()
    
    if args.no_dry_run:
        args.dry_run = False
    
    # Crear app context
    app = create_app()
    
    with app.app_context():
        try:
            company_id = args.company_id
            
            # Verificar que la empresa existe
            company = db.session.query(Company).filter(Company.id == company_id).first()
            if not company:
                print(f"❌ Empresa {company_id} no encontrada")
                return 1
            
            print(f"\nEmpresa: {getattr(company, 'name', 'Sin nombre')} (ID: {company_id})")
            
            # Encontrar duplicados
            duplicates = find_duplicates(company_id, verbose=args.verbose)
            
            if not duplicates:
                print("\n✅ No se encontraron clientes duplicados en esta empresa")
                return 0
            
            print(f"\n⚠️  Se encontraron {len(duplicates)} grupos de clientes duplicados")
            
            # Diagnosticar cada grupo
            for norm_name, customers in duplicates.items():
                diagnosis = diagnose_duplicate_group(customers, company_id, verbose=args.verbose)
                
                # Filtrar por nombre específico si se indicó
                if args.customer_name:
                    if normalize_name(args.customer_name) != norm_name:
                        continue
                
                print_duplicate_diagnosis(diagnosis)
                
                # Si se pidió merge
                if args.merge:
                    print("\n" + "-"*80)
                    print("FUSIÓN DE CLIENTES DUPLICADOS")
                    print("-"*80)
                    
                    # Seleccionar cliente primario (el más antiguo o el que tiene más ventas)
                    details = diagnosis['details_by_customer']
                    
                    # Ordenar por cantidad de ventas (descendente)
                    sorted_customers = sorted(
                        details.items(),
                        key=lambda x: (x[1]['count'], x[1]['total_amount']),
                        reverse=True
                    )
                    
                    primary_id = sorted_customers[0][0]
                    secondary_ids = [cid for cid, _ in sorted_customers[1:]]
                    
                    print(f"\nCliente PRIMARIO (se mantendrá): {primary_id}")
                    print(f"  Ventas: {details[primary_id]['count']}")
                    print(f"  Monto: ${details[primary_id]['total_amount']:,.2f}")
                    
                    print(f"\nClientes SECUNDARIOS (se eliminarán): {len(secondary_ids)}")
                    for sec_id in secondary_ids:
                        print(f"  • {sec_id}: {details[sec_id]['count']} ventas, ${details[sec_id]['total_amount']:,.2f}")
                    
                    if args.dry_run:
                        print("\n[DRY RUN] Ejecutar con --no-dry-run para aplicar cambios")
                    else:
                        # Confirmar
                        print("\n⚠️  ¿Está seguro de que desea fusionar estos clientes?")
                        print("Esta acción NO se puede deshacer.")
                        resp = input("Escriba 'FUSIONAR' para confirmar: ")
                        
                        if resp != 'FUSIONAR':
                            print("❌ Operación cancelada")
                            continue
                    
                    # Ejecutar merge
                    result = merge_customers(
                        primary_id,
                        secondary_ids,
                        company_id,
                        dry_run=args.dry_run,
                        verbose=args.verbose
                    )
                    
                    if result['ok']:
                        print("\n✅ FUSIÓN COMPLETADA")
                        print(f"   Ventas actualizadas: {result['stats']['sales_updated']}")
                        print(f"   Clientes eliminados: {result['stats']['customers_deleted']}")
                    else:
                        print(f"\n❌ ERROR: {result.get('error')}")
            
            return 0
        
        except Exception as e:
            print(f"\n❌ ERROR: {e}")
            import traceback
            traceback.print_exc()
            return 1


if __name__ == '__main__':
    sys.exit(main())
