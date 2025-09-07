# notam/auth/routes.py
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials
from .service import auth_service
from .middleware import get_current_user, security, AuthUser
from .models import (
    UserSignUp, UserSignIn, PasswordReset, PasswordUpdate,
    AuthResponse, TokenResponse, UserProfile, UserUpdate, TokenRefresh
)

router = APIRouter(prefix="/auth", tags=["Authentication"])

@router.post("/signup", response_model=AuthResponse)
async def sign_up(user_data: UserSignUp):
    """Register a new user account"""
    return await auth_service.sign_up(user_data)

@router.post("/signin", response_model=AuthResponse)
async def sign_in(credentials: UserSignIn):
    """Sign in with email and password"""
    return await auth_service.sign_in(credentials)

@router.post("/signout", response_model=AuthResponse)
async def sign_out(current_user: AuthUser = Depends(get_current_user)):
    """Sign out current user"""
    return await auth_service.sign_out(current_user.access_token)

@router.post("/refresh", response_model=TokenResponse)
async def refresh_token(payload: TokenRefresh):
    """Refresh access token using refresh token"""
    return await auth_service.refresh_token(payload.refresh_token)

@router.post("/reset-password", response_model=AuthResponse)
async def reset_password(reset_data: PasswordReset):
    """Send password reset email"""
    return await auth_service.reset_password(reset_data)

@router.post("/update-password", response_model=AuthResponse)
async def update_password(
    password_data: PasswordUpdate,
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Update user password (requires current authentication)"""
    return await auth_service.update_password(password_data, credentials.credentials)

@router.get("/profile", response_model=UserProfile)
async def get_profile(current_user: AuthUser = Depends(get_current_user)):
    """Get current user profile"""
    return await auth_service.get_user_profile(current_user.access_token)

@router.put("/profile", response_model=AuthResponse)
async def update_profile(
    profile_data: UserUpdate,
    current_user: AuthUser = Depends(get_current_user)
):
    """Update user profile information (placeholder)"""
    return AuthResponse(
        success=True,
        message="Profile update functionality not yet implemented"
    )

@router.get("/verify-token")
async def verify_token(current_user: AuthUser = Depends(get_current_user)):
    """Verify if the current token is valid"""
    return {
        "valid": True,
        "user_id": current_user.id,
        "email": current_user.email,
        "role": current_user.role,
        "expires_at": current_user.exp
    }
