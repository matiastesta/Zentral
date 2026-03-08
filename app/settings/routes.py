import os
import re

from flask import g, render_template, request, redirect, url_for, flash, current_app
from flask_login import login_required
from werkzeug.utils import secure_filename

from app import db
from app.files.storage import upload_to_r2_and_create_asset
from app.models import BusinessSettings, Company, Installment, InstallmentPlan
from app.permissions import module_required
from app.settings import bp


def _parse_shift_hhmm(raw: str):
    s = str(raw or '').strip()
    if not re.match(r'^\d{2}:\d{2}$', s):
        return None
    try:
        hh = int(s[:2])
        mm = int(s[3:5])
    except Exception:
        return None
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        return None
    return (hh * 60) + mm


@bp.route("/", methods=["GET", "POST"])
@bp.route("/business", methods=["GET", "POST"])
@login_required
@module_required('settings')
def business_settings():
    """Configuración básica del negocio (dummy)."""
    if request.method == 'POST':
        action = (request.form.get('action') or '').strip()
        if action == 'save_business':
            bs = BusinessSettings.get_for_company(g.company_id)
            prev_installments_enabled = bool(getattr(bs, 'habilitar_sistema_cuotas', False))
            bs.name = (request.form.get('business_name') or '').strip() or bs.name
            try:
                c = db.session.get(Company, str(getattr(g, 'company_id', '') or '').strip())
                if c and str(getattr(bs, 'name', '') or '').strip() and (str(getattr(c, 'name', '') or '').strip() != str(bs.name or '').strip()):
                    c.name = str(bs.name or '').strip()
                    db.session.add(c)
            except Exception:
                pass
            ind = (request.form.get('business_industry') or '').strip() or None
            if ind == 'Otro':
                other = (request.form.get('business_industry_other') or '').strip()
                bs.industry = other or 'Otro'
            else:
                bs.industry = ind
            bs.email = (request.form.get('business_email') or '').strip() or None
            bs.phone = (request.form.get('business_phone') or '').strip() or None
            bs.address = (request.form.get('business_address') or '').strip() or None

            raw_color = (request.form.get('primary_color') or '').strip()
            if raw_color and raw_color.startswith('#') and len(raw_color) in {4, 7}:
                bs.primary_color = raw_color
            elif not raw_color:
                bs.primary_color = None

            def _f_range(name, default=None, mn=0.6, mx=1.6):
                raw = (request.form.get(name) or '').strip().replace(',', '.')
                if raw == '':
                    return default
                try:
                    v = float(raw)
                except Exception:
                    return default
                if v < mn:
                    v = mn
                if v > mx:
                    v = mx
                return v

            bs.background_brightness = _f_range('background_brightness', default=1.0)
            bs.background_contrast = _f_range('background_contrast', default=1.0)

            installments_enabled = prev_installments_enabled
            try:
                installments_enabled = str(request.form.get('habilitar_sistema_cuotas') or '').strip().lower() in {'1', 'true', 'yes', 'on'}
            except Exception:
                installments_enabled = prev_installments_enabled

            if prev_installments_enabled and (not installments_enabled):
                try:
                    row = (
                        db.session.query(Installment.id)
                        .join(InstallmentPlan, Installment.plan_id == InstallmentPlan.id)
                        .filter(Installment.company_id == g.company_id)
                        .filter(InstallmentPlan.company_id == g.company_id)
                        .filter(db.func.lower(InstallmentPlan.status) == 'activo')
                        .filter(db.func.lower(Installment.status) != 'pagada')
                        .limit(1)
                        .first()
                    )
                    if row is not None:
                        installments_enabled = True
                        flash('No se puede deshabilitar el sistema de cuotas porque existen cuotas activas pendientes.', 'error')
                except Exception:
                    installments_enabled = prev_installments_enabled

            bs.habilitar_sistema_cuotas = bool(installments_enabled)

            try:
                bs.habilitar_doble_turno_arqueo = str(request.form.get('habilitar_doble_turno_arqueo') or '').strip().lower() in {'1', 'true', 'yes', 'on'}
            except Exception:
                bs.habilitar_doble_turno_arqueo = bool(getattr(bs, 'habilitar_doble_turno_arqueo', False))

            t1_desde = str(request.form.get('arqueo_turno_1_desde') or getattr(bs, 'arqueo_turno_1_desde', '08:00') or '08:00').strip()
            t1_hasta = str(request.form.get('arqueo_turno_1_hasta') or getattr(bs, 'arqueo_turno_1_hasta', '16:00') or '16:00').strip()
            t2_desde = str(request.form.get('arqueo_turno_2_desde') or getattr(bs, 'arqueo_turno_2_desde', '16:00') or '16:00').strip()
            t2_hasta = str(request.form.get('arqueo_turno_2_hasta') or getattr(bs, 'arqueo_turno_2_hasta', '08:00') or '08:00').strip()

            if bool(bs.habilitar_doble_turno_arqueo):
                m1d = _parse_shift_hhmm(t1_desde)
                m1h = _parse_shift_hhmm(t1_hasta)
                m2d = _parse_shift_hhmm(t2_desde)
                m2h = _parse_shift_hhmm(t2_hasta)
                if None in {m1d, m1h, m2d, m2h}:
                    flash('Los horarios de doble turno deben tener formato HH:MM válido.', 'error')
                    return redirect(url_for('settings.business_settings'))
                if m1d == m1h or m2d == m2h:
                    flash('Cada turno debe tener una duración mayor a 0 minutos.', 'error')
                    return redirect(url_for('settings.business_settings'))
                if not (m1h == m2d and m2h == m1d):
                    flash('Los dos turnos deben cubrir las 24 horas sin huecos ni superposiciones. Configurá el fin de un turno exactamente igual al inicio del otro.', 'error')
                    return redirect(url_for('settings.business_settings'))

            bs.arqueo_turno_1_desde = t1_desde
            bs.arqueo_turno_1_hasta = t1_hasta
            bs.arqueo_turno_2_desde = t2_desde
            bs.arqueo_turno_2_hasta = t2_hasta

            f = request.files.get('business_logo')
            if f and getattr(f, 'filename', ''):
                filename = secure_filename(f.filename)
                _, ext = os.path.splitext(filename.lower())
                allowed = set((current_app.config.get('ALLOWED_EXTENSIONS') or set()))
                if allowed and ext.lstrip('.') not in allowed:
                    flash('Formato de logo no permitido.', 'error')
                    return redirect(url_for('settings.business_settings'))

                try:
                    asset = upload_to_r2_and_create_asset(
                        company_id=str(getattr(g, 'company_id', '') or '').strip(),
                        file_storage=f,
                        entity_type='business_logo',
                        entity_id=str(getattr(g, 'company_id', '') or '').strip(),
                        key_prefix='business/logo',
                    )
                    bs.logo_file_id = asset.id
                    bs.logo_filename = None
                except Exception:
                    current_app.logger.exception('Failed to upload business logo to R2')
                    flash('No se pudo subir el logo. Intentá nuevamente.', 'error')
                    return redirect(url_for('settings.business_settings'))

            bg = request.files.get('background_image')
            if bg and getattr(bg, 'filename', ''):
                filename = secure_filename(bg.filename)
                _, ext = os.path.splitext(filename.lower())
                if ext != '.png':
                    flash('La imagen de fondo debe ser PNG.', 'error')
                    return redirect(url_for('settings.business_settings'))

                try:
                    asset = upload_to_r2_and_create_asset(
                        company_id=str(getattr(g, 'company_id', '') or '').strip(),
                        file_storage=bg,
                        entity_type='business_background',
                        entity_id=str(getattr(g, 'company_id', '') or '').strip(),
                        key_prefix='business/background',
                    )
                    bs.background_file_id = asset.id
                    bs.background_image_filename = None
                except Exception:
                    current_app.logger.exception('Failed to upload business background to R2')
                    flash('No se pudo subir la imagen de fondo. Intentá nuevamente.', 'error')
                    return redirect(url_for('settings.business_settings'))

            db.session.add(bs)
            db.session.commit()
            flash('Datos del negocio guardados.', 'success')
            return redirect(url_for('settings.business_settings'))

    business = BusinessSettings.get_for_company(g.company_id)
    return render_template("settings/business.html", title="Configuración del negocio", business=business)
