from functools import wraps

from flask import abort
from flask_login import current_user


def module_required(module_name: str):
    def decorator(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            if not current_user.is_authenticated:
                abort(401)
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
