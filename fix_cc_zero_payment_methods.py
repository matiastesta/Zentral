#!/usr/bin/env python3
import argparse
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app import create_app, db
from app.models import Sale, SalePayment, Company


def has_real_payment_amount(value) -> bool:
    try:
        return float(value or 0.0) > 0.0001
    except Exception:
        return False


def run(company_id: str | None, dry_run: bool, verbose: bool) -> int:
    q = db.session.query(Sale).filter(Sale.sale_type == 'Venta')
    if company_id:
        q = q.filter(Sale.company_id == company_id)

    rows = (
        q.filter(Sale.on_account.is_(True))
        .filter((Sale.is_installments.is_(False)) | (Sale.is_installments.is_(None)))
        .filter(Sale.paid_amount <= 0.0001)
        .all()
    )

    companies_seen = set()
    affected_sales = 0
    payment_rows_deleted = 0

    for sale in rows:
        companies_seen.add(str(getattr(sale, 'company_id', '') or '').strip())
        sale_id = int(getattr(sale, 'id', 0) or 0)
        payment_method = str(getattr(sale, 'payment_method', '') or '').strip()

        sp_rows = (
            db.session.query(SalePayment)
            .filter(SalePayment.company_id == sale.company_id, SalePayment.sale_id == sale_id)
            .all()
        )
        fake_payment_rows = [sp for sp in sp_rows if not has_real_payment_amount(getattr(sp, 'amount', 0.0))]
        has_any_payment_rows = bool(sp_rows)
        needs_fix = bool(payment_method) or has_any_payment_rows
        if not needs_fix:
            continue

        affected_sales += 1
        if verbose:
            print(
                f"[FIX] company={sale.company_id} ticket={getattr(sale, 'ticket', '')} "
                f"customer={getattr(sale, 'customer_name', '')} paid={getattr(sale, 'paid_amount', 0.0)} "
                f"due={getattr(sale, 'due_amount', 0.0)} method={payment_method!r} payments={len(sp_rows)}"
            )

        if dry_run:
            payment_rows_deleted += len(sp_rows)
            continue

        sale.payment_method = None
        for sp in sp_rows:
            db.session.delete(sp)
            payment_rows_deleted += 1

    if dry_run:
        print(f"[DRY RUN] Empresas analizadas: {len(companies_seen) or (1 if company_id else 0)}")
        print(f"[DRY RUN] Ventas a corregir: {affected_sales}")
        print(f"[DRY RUN] Registros sale_payment a eliminar: {payment_rows_deleted}")
        return 0

    db.session.commit()
    print(f"Empresas afectadas: {len(companies_seen) or (1 if company_id else 0)}")
    print(f"Ventas corregidas: {affected_sales}")
    print(f"Registros sale_payment eliminados: {payment_rows_deleted}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description='Corrige ventas CC sin pago real que quedaron con medio de pago.')
    parser.add_argument('--company-id', type=str, default='', help='ID de empresa a corregir. Si se omite, procesa todas.')
    parser.add_argument('--apply', action='store_true', help='Aplica los cambios. Sin este flag corre en dry-run.')
    parser.add_argument('--verbose', action='store_true', help='Muestra detalle por ticket.')
    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        if args.company_id:
            company = db.session.query(Company).filter(Company.id == args.company_id).first()
            if not company:
                print('Empresa no encontrada.')
                return 1
        return run(args.company_id or None, dry_run=(not args.apply), verbose=args.verbose)


if __name__ == '__main__':
    raise SystemExit(main())
