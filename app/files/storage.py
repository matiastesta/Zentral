import os
import time
from uuid import uuid4

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError
from flask import current_app
from werkzeug.utils import secure_filename

from app import db
from app.models import FileAsset


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


def _r2_key_for_company(cid: str, *, original_name: str, prefix: str) -> str:
    safe = secure_filename(original_name or '')
    if not safe:
        safe = 'archivo'
    ext = os.path.splitext(safe)[1].lower()
    base = os.path.splitext(safe)[0]
    if not base:
        base = 'archivo'

    token = uuid4().hex
    p = str(prefix or '').strip().strip('/')
    if p:
        return f"companies/{cid}/{p}/{token}_{base}{ext}"
    return f"companies/{cid}/{token}_{base}{ext}"


def upload_to_r2_and_create_asset(
    *,
    company_id: str,
    file_storage,
    entity_type: str | None = None,
    entity_id: str | None = None,
    key_prefix: str = '',
) -> FileAsset:
    cid = str(company_id or '').strip()
    if not cid:
        raise RuntimeError('no_company')

    original_name = str(getattr(file_storage, 'filename', '') or '').strip() or 'archivo'
    content_type = str(getattr(file_storage, 'mimetype', '') or '').strip() or None

    object_key = _r2_key_for_company(cid, original_name=original_name, prefix=key_prefix)
    bucket = _r2_bucket()

    try:
        body = getattr(file_storage, 'stream', None)
        if body is None:
            raise RuntimeError('missing_stream')
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
        raise RuntimeError(detail or 'upload_failed')

    size_bytes = 0
    try:
        size_bytes = int(getattr(file_storage, 'content_length', None) or 0)
    except Exception:
        size_bytes = 0

    asset = FileAsset(
        id=uuid4().hex,
        company_id=cid,
        entity_type=str(entity_type or '').strip() or None,
        entity_id=str(entity_id or '').strip() or None,
        storage_provider='r2',
        bucket=bucket,
        object_key=object_key,
        original_name=original_name,
        content_type=content_type,
        size_bytes=size_bytes,
        status='active',
    )

    db.session.add(asset)
    db.session.flush()

    try:
        now_ms = int(time.time() * 1000)
    except Exception:
        now_ms = 0
    setattr(asset, '_created_at_ms', now_ms)

    return asset
