import secrets

from fastapi import Header, HTTPException, status

from backend.config import get_settings


def verify_api_token(
    authorization: str | None = Header(default=None),
) -> None:
    settings = get_settings()

    if not settings.API_AUTH_ENABLED:
        if settings.APP_ENV == "production":
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="API authentication cannot be disabled in production.",
            )
        return

    configured_token = settings.API_ACCESS_TOKEN.get_secret_value()

    if not configured_token:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="API access token is not configured.",
        )

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    supplied_token = authorization.removeprefix("Bearer ").strip()

    if not supplied_token or not secrets.compare_digest(
        supplied_token,
        configured_token,
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API access token.",
            headers={"WWW-Authenticate": "Bearer"},
        )
