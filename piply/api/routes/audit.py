"""Administrator-only audit history queries."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from piply.api.auth import get_service, require_admin
from piply.core.audit import parse_audit_since

router = APIRouter(tags=["audit"])


@router.get("/api/audit", response_model=list[dict[str, object]])
def list_audit(
    request: Request,
    actor: str | None = Query(None, max_length=200),
    action: str | None = Query(None, max_length=200),
    target_type: str | None = Query(None, max_length=200),
    target_id: str | None = Query(None, max_length=200),
    since: str | None = Query(None, max_length=100),
    before_id: int | None = Query(None, ge=1),
    limit: int = Query(50, ge=1, le=500),
) -> list[dict[str, object]]:
    """Read action history; no API can edit or delete individual events."""
    require_admin(request)
    try:
        moment = parse_audit_since(since)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return get_service(request).store.list_audit_events(
        actor=actor,
        action=action,
        target_type=target_type,
        target_id=target_id,
        since=moment,
        before_id=before_id,
        limit=limit,
    )
