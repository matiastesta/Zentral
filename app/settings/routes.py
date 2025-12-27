import os

from flask import g, render_template, request, redirect, url_for, flash, current_app
from flask_login import login_required
from werkzeug.utils import secure_filename

from app import db
from app.models import BusinessSettings
from app.permissions import module_required
from app.settings import bp


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
            bs.name = (request.form.get('business_name') or '').strip() or bs.name
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

            f = request.files.get('business_logo')
            if f and getattr(f, 'filename', ''):
                filename = secure_filename(f.filename)
                _, ext = os.path.splitext(filename.lower())
                allowed = set((current_app.config.get('ALLOWED_EXTENSIONS') or set()))
                if allowed and ext.lstrip('.') not in allowed:
                    flash('Formato de logo no permitido.', 'error')
                    return redirect(url_for('settings.business_settings'))
                folder = current_app.config.get('UPLOAD_FOLDER')
                if folder:
                    os.makedirs(folder, exist_ok=True)
                    final_name = 'business_logo' + ext
                    path = os.path.join(folder, final_name)
                    f.save(path)
                    bs.logo_filename = final_name

            bg = request.files.get('background_image')
            if bg and getattr(bg, 'filename', ''):
                filename = secure_filename(bg.filename)
                _, ext = os.path.splitext(filename.lower())
                if ext != '.png':
                    flash('La imagen de fondo debe ser PNG.', 'error')
                    return redirect(url_for('settings.business_settings'))
                folder = current_app.config.get('UPLOAD_FOLDER')
                if folder:
                    os.makedirs(folder, exist_ok=True)
                    final_name = 'business_background' + ext
                    path = os.path.join(folder, final_name)
                    bg.save(path)
                    bs.background_image_filename = final_name

            db.session.add(bs)
            db.session.commit()
            flash('Datos del negocio guardados.', 'success')
            return redirect(url_for('settings.business_settings'))

    business = BusinessSettings.get_for_company(g.company_id)
    return render_template("settings/business.html", title="Configuración del negocio", business=business)
