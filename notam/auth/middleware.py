# notam/auth/middleware.py
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional, Dict, Any
import jwt
from datetime import datetime, timezone
from .config import supabase_auth  # keep if you have config.py exporting supabase_auth

# Required auth
security = HTTPBearer()
# Optional auth (won't 401 if header is missing)
optional_security = HTTPBearer(auto_error=False)


class AuthUser:
    def __init__(self, user_data: Dict[str, Any]):
        self.id = user_data.get("sub")
        self.email = user_data.get("email")
        self.role = user_data.get("role", "user")
        self.exp = user_data.get("exp")
        self.access_token = user_data.get("token")  # preserved below
        self.raw_data = user_data

    def is_token_expired(self) -> bool:
        if not self.exp:
            return True
        return datetime.fromtimestamp(self.exp, tz=timezone.utc) < datetime.now(timezone.utc)


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> AuthUser:
    """
    Validate JWT token and extract user information
    """
    try:
        token = credentials.credentials

        # Verify the token with Supabase (signature & revocation handled server-side)
        response = supabase_auth.get_client().auth.get_user(token)
        if not response.user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication token",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Decode JWT for claims (signature already verified by Supabase)
        decoded_token = jwt.decode(token, options={"verify_signature": False})
        decoded_token["token"] = token  # keep caller token for downstream service calls

        user = AuthUser(decoded_token)

        if user.is_token_expired():
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has expired",
                headers={"WWW-Authenticate": "Bearer"},
            )

        return user

    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Authentication failed: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def get_optional_user(credentials: Optional[HTTPAuthorizationCredentials] = Depends(optional_security)) -> Optional[AuthUser]:
    """
    Optional authentication - returns None if no token provided
    """
    if not credentials:
        return None
    try:
        return await get_current_user(credentials)
    except HTTPException:
        return None


def require_role(required_role: str):
    """
    Dependency for role-based access control
    """
    async def role_checker(current_user: AuthUser = Depends(get_current_user)):
        if current_user.role != required_role and current_user.role != "admin":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Insufficient permissions. Required role: {required_role}"
            )
        return current_user

    return role_checker
