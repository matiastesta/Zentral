from functools import wraps

from flask import abort, redirect, url_for
from flask_login import current_user

from app.tenancy import is_impersonating


def module_required(module_name: str):
    def decorator(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            if not current_user.is_authenticated:
                abort(401)
            if getattr(current_user, 'role', '') == 'zentral_admin':
                if is_impersonating():
                    return fn(*args, **kwargs)
                return redirect(url_for('superadmin.index'))
            if not getattr(current_user, 'can', None):
                abort(403)
            try:
                allowed = bool(current_user.can(module_name))
            except Exception:
                allowed = False
            if not allowed:
                abort(403)
            return fn(*args, **kwargs)
        return wrapped
    return decorator


def module_required_any(*module_names: str):
    def decorator(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            if not current_user.is_authenticated:
                abort(401)
            if getattr(current_user, 'role', '') == 'zentral_admin':
                if is_impersonating():
                    return fn(*args, **kwargs)
                return redirect(url_for('superadmin.index'))
            if not getattr(current_user, 'can', None):
                abort(403)

            names = [str(n or '').strip() for n in (module_names or [])]
            names = [n for n in names if n]
            if not names:
                abort(403)

            allowed = False
            for n in names:
                try:
                    if current_user.can(n):
                        allowed = True
                        break
                except Exception:
                    continue

            if not allowed:
                abort(403)
            return fn(*args, **kwargs)
        return wrapped
    return decorator
