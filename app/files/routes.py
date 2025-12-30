import os
import time
from uuid import uuid4

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError
from flask import current_app, g, jsonify, redirect, request, session
from flask_login import login_required
from flask_login import current_user
from sqlalchemy import text
from werkzeug.utils import secure_filename

from app import db
from app.files import bp
from app.models import BusinessSettings, FileAsset


def _require_company_id() -> str | None:
    try:
        cid = str(getattr(g, 'company_id', '') or '').strip()
        return cid or None
    except Exception:
        return None


def _r2_client():
    endpoint = str(current_app.config.get('R2_ENDPOINT_URL') or '').strip()
    access_key = str(current_app.config.get('R2_ACCESS_KEY_ID') or '').strip()
    secret_key = str(current_app.config.get('R2_SECRET_ACCESS_KEY') or '').strip()
    region = str(current_app.config.get('R2_REGION') or 'auto').strip() or 'auto'

    if not endpoint or not access_key or not secret_key:
        raise RuntimeError('R2 not configured')

    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=BotoConfig(signature_version='s3v4', s3={'addressing_style': 'path'}),
    )


def _r2_bucket() -> str:
    bucket = str(current_app.config.get('R2_BUCKET') or '').strip()
    if not bucket:
        raise RuntimeError('R2 bucket not configured')
    return bucket


def _r2_key_for_company(cid: str, *, original_name: str) -> str:
    safe = secure_filename(original_name or '')
    if not safe:
        safe = 'archivo'
    ext = os.path.splitext(safe)[1].lower()
    base = os.path.splitext(safe)[0]
    if not base:
        base = 'archivo'

    token = uuid4().hex
    return f"companies/{cid}/{token}_{base}{ext}"


@bp.get('/api/debug/context')
@login_required
def debug_context_api():
    try:
        cid = str(getattr(g, 'company_id', '') or '').strip()
    except Exception:
        cid = ''

    try:
        user_id = getattr(current_user, 'id', None) if getattr(current_user, 'is_authenticated', False) else None
        role = str(getattr(current_user, 'role', '') or '')
    except Exception:
        user_id = None
        role = ''

    return jsonify({
        'ok': True,
        'company_id': cid,
        'host': str(getattr(request, 'host', '') or ''),
        'path': str(getattr(request, 'path', '') or ''),
        'auth_is_zentral_admin': str(session.get('auth_is_zentral_admin') or ''),
        'auth_company_id': str(session.get('auth_company_id') or ''),
        'impersonate_company_id': str(session.get('impersonate_company_id') or ''),
        'user_id': user_id,
        'role': role,
    })


@bp.get('/api/debug/db_settings')
@login_required
def debug_db_settings_api():
    try:
        row = db.session.execute(
            text(
                """
                SELECT
                    current_setting('app.company_slug', true) AS company_slug,
                    current_setting('app.current_company_id', true) AS current_company_id,
                    current_setting('app.is_zentral_admin', true) AS is_zentral_admin,
                    current_setting('app.is_login', true) AS is_login,
                    current_setting('app.login_email', true) AS login_email
                """
            )
        ).mappings().first()
    except Exception as e:
        try:
            current_app.logger.exception('Failed to read db_settings')
        except Exception:
            pass
        return jsonify({'ok': False, 'error': 'db_error', 'detail': str(e)[:200]}), 400

    payload = {
        'ok': True,
        'db_company_slug': str((row or {}).get('company_slug') or ''),
        'db_current_company_id': str((row or {}).get('current_company_id') or ''),
        'db_is_zentral_admin': str((row or {}).get('is_zentral_admin') or ''),
        'db_is_login': str((row or {}).get('is_login') or ''),
        'db_login_email': str((row or {}).get('login_email') or ''),
    }
    return jsonify(payload)


@bp.get('/api/debug/business')
@login_required
def debug_business_api():
    try:
        cid = str(getattr(g, 'company_id', '') or '').strip()
    except Exception:
        cid = ''
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    bs = None
    try:
        bs = db.session.query(BusinessSettings).filter(BusinessSettings.company_id == cid).first()
    except Exception:
        bs = None

    logo_id = str(getattr(bs, 'logo_file_id', '') or '').strip() if bs else ''
    bg_id = str(getattr(bs, 'background_file_id', '') or '').strip() if bs else ''

    logo_asset = None
    bg_asset = None
    try:
        if logo_id:
            logo_asset = db.session.query(FileAsset.id).filter(FileAsset.company_id == cid, FileAsset.id == logo_id).first()
    except Exception:
        logo_asset = None
    try:
        if bg_id:
            bg_asset = db.session.query(FileAsset.id).filter(FileAsset.company_id == cid, FileAsset.id == bg_id).first()
    except Exception:
        bg_asset = None

    return jsonify({
        'ok': True,
        'company_id': cid,
        'business_exists': bool(bs is not None),
        'business': {
            'name': str(getattr(bs, 'name', '') or '') if bs else '',
            'primary_color': str(getattr(bs, 'primary_color', '') or '') if bs else '',
            'logo_file_id': logo_id,
            'background_file_id': bg_id,
            'background_brightness': getattr(bs, 'background_brightness', None) if bs else None,
            'background_contrast': getattr(bs, 'background_contrast', None) if bs else None,
        },
        'assets': {
            'logo_asset_exists': bool(logo_asset is not None) if logo_id else False,
            'background_asset_exists': bool(bg_asset is not None) if bg_id else False,
        }
    })


@bp.post('/api/files')
@login_required
def upload_file_api():
    cid = _require_company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'no_company'}), 400

    f = request.files.get('file') or request.files.get('files')
    if not f or not getattr(f, 'filename', ''):
        return jsonify({'ok': False, 'error': 'file_required'}), 400

    original_name = str(f.filename or '').strip() or 'archivo'
    content_type = str(getattr(f, 'mimetype', '') or '').strip() or None

    entity_type = (request.form.get('entity_type') or '').strip() or None
    entity_id = (request.form.get('entity_id') or '').strip() or None

    object_key = _r2_key_for_company(cid, original_name=original_name)
    bucket = _r2_bucket()

    try:
        body = f.stream
        extra = {}
        if content_type:
            extra['ContentType'] = content_type
        _r2_client().put_object(Bucket=bucket, Key=object_key, Body=body, **extra)
    except Exception as e:
        current_app.logger.exception('Failed to upload file to R2')
        detail = ''
        try:
            if isinstance(e, ClientError):
                er = (e.response or {}).get('Error') or {}
                code = str(er.get('Code') or '').strip()
                msg = str(er.get('Message') or '').strip()
                detail = (code + (': ' if code and msg else '') + msg).strip()
            elif isinstance(e, BotoCoreError):
                detail = str(e).strip()
            else:
                detail = str(e).strip()
        except Exception:
            detail = ''
        payload = {'ok': False, 'error': 'upload_failed'}
        if detail:
            payload['detail'] = detail[:300]
        return jsonify(payload), 400

    size_bytes = 0
    try:
        size_bytes = int(getattr(f, 'content_length', None) or 0)
    except Exception:
        size_bytes = 0

    asset = FileAsset(
        id=uuid4().hex,
        company_id=cid,
        entity_type=entity_type,
        entity_id=entity_id,
        storage_provider='r2',
        bucket=bucket,
        object_key=object_key,
        original_name=original_name,
        content_type=content_type,
        size_bytes=size_bytes,
        status='active',
    )

    db.session.add(asset)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify({'ok': False, 'error': 'db_error'}), 400

    return jsonify({
        'ok': True,
        'item': {
            'id': asset.id,
            'name': asset.original_name or '',
            'type': asset.content_type or '',
            'size': int(asset.size_bytes or 0),
            'created_at': int(time.time() * 1000),
            'entity_type': asset.entity_type or '',
            'entity_id': asset.entity_id or '',
            'download_url': f"/api/files/{asset.id}/download",
        },
    })


@bp.get('/api/files')
@login_required
def list_files_api():
    cid = _require_company_id()
    if not cid:
        return jsonify({'ok': True, 'items': []})

    entity_type = (request.args.get('entity_type') or '').strip()
    entity_id = (request.args.get('entity_id') or '').strip()

    q = db.session.query(FileAsset).filter(FileAsset.company_id == cid)
    if entity_type:
        q = q.filter(FileAsset.entity_type == entity_type)
    if entity_id:
        q = q.filter(FileAsset.entity_id == entity_id)

    rows = q.order_by(FileAsset.created_at.desc()).limit(500).all()
    return jsonify({
        'ok': True,
        'items': [
            {
                'id': r.id,
                'name': r.original_name or '',
                'type': r.content_type or '',
                'size': int(r.size_bytes or 0),
                'entity_type': r.entity_type or '',
                'entity_id': r.entity_id or '',
                'status': r.status or '',
                'download_url': f"/api/files/{r.id}/download",
            }
            for r in rows
        ],
    })


@bp.get('/api/files/<file_id>/download')
@login_required
def download_file_api(file_id: str):
    cid = _require_company_id()
    if not cid:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    fid = str(file_id or '').strip()
    row = db.session.query(FileAsset).filter(FileAsset.company_id == cid, FileAsset.id == fid).first()
    if not row:
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    if (row.status or 'active') != 'active':
        return jsonify({'ok': False, 'error': 'not_found'}), 404

    try:
        expires = int(current_app.config.get('R2_PRESIGNED_EXPIRES_SECONDS') or 120)
    except Exception:
        expires = 120
    expires = max(30, min(expires, 3600))

    try:
        url = _r2_client().generate_presigned_url(
            'get_object',
            Params={'Bucket': row.bucket, 'Key': row.object_key},
            ExpiresIn=expires,
        )
    except Exception:
        current_app.logger.exception('Failed to generate presigned URL')
        return jsonify({'ok': False, 'error': 'download_failed'}), 400

    wants_json = False
    try:
        wants_json = str(request.args.get('json') or '').strip() in {'1', 'true', 'yes'}
    except Exception:
        wants_json = False

    if wants_json:
        return jsonify({'ok': True, 'url': url, 'expires_seconds': expires})

    return redirect(url, code=302)
