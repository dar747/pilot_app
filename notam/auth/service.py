# notam/auth/service.py
import requests
import os
from typing import Optional
from fastapi import HTTPException, status
from supabase import Client
from .config import supabase_auth  # keep if you have config.py exporting supabase_auth
from .models import (
    UserSignUp, UserSignIn, PasswordReset, PasswordUpdate,
    TokenResponse, UserProfile, AuthResponse
)

class AuthService:
    def __init__(self):
        self.client: Client = supabase_auth.get_client()

    async def sign_up(self, user_data: UserSignUp) -> AuthResponse:
        """Register a new user"""
        try:
            response = self.client.auth.sign_up({
                "email": user_data.email,
                "password": user_data.password,
                "options": {
                    "data": {
                        "full_name": user_data.full_name,
                        "organization": user_data.organization
                    }
                }
            })

            if response.user:
                user_profile = UserProfile(
                    id=response.user.id,
                    email=response.user.email,
                    full_name=user_data.full_name,
                    organization=user_data.organization,
                    role="user",
                    email_confirmed=bool(response.user.email_confirmed_at),
                    created_at=response.user.created_at,
                    last_sign_in=response.user.last_sign_in_at
                )

                tokens = None
                if response.session:
                    tokens = TokenResponse(
                        access_token=response.session.access_token,
                        expires_in=response.session.expires_in,
                        refresh_token=response.session.refresh_token
                    )

                return AuthResponse(
                    success=True,
                    message="User registered successfully. Please check your email for verification.",
                    user=user_profile,
                    tokens=tokens
                )

            return AuthResponse(success=False, message="Registration failed")

        except Exception as e:
            error_msg = str(e)
            if "already been registered" in error_msg:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="User with this email already exists"
                )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Registration failed: {error_msg}"
            )

    async def sign_in(self, credentials: UserSignIn) -> AuthResponse:
        """Authenticate user and return tokens"""
        try:
            response = self.client.auth.sign_in_with_password({
                "email": credentials.email,
                "password": credentials.password
            })

            if response.user and response.session:
                user_profile = UserProfile(
                    id=response.user.id,
                    email=response.user.email,
                    full_name=response.user.user_metadata.get("full_name"),
                    organization=response.user.user_metadata.get("organization"),
                    role=response.user.user_metadata.get("role", "user"),
                    email_confirmed=bool(response.user.email_confirmed_at),
                    created_at=response.user.created_at,
                    last_sign_in=response.user.last_sign_in_at
                )

                tokens = TokenResponse(
                    access_token=response.session.access_token,
                    expires_in=response.session.expires_in,
                    refresh_token=response.session.refresh_token
                )

                return AuthResponse(
                    success=True,
                    message="Login successful",
                    user=user_profile,
                    tokens=tokens
                )

            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid email or password"
            )

        except Exception as e:
            error_msg = str(e)
            if "Invalid login credentials" in error_msg:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid email or password"
                )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Login failed: {error_msg}"
            )

    async def sign_out(self, access_token: str, refresh_token: Optional[str] = None) -> AuthResponse:
        """Sign out user and invalidate token"""
        try:
            # Scope client to caller's session before sign-out
            self.client.auth.set_session(access_token, refresh_token)
            self.client.auth.sign_out()
            return AuthResponse(success=True, message="Logged out successfully")
        except Exception as e:
            return AuthResponse(success=False, message=f"Logout failed: {str(e)}")

    async def refresh_token(self, refresh_token: str) -> TokenResponse:
        """Refresh access token using a refresh token"""
        try:
            # Attach refresh token and refresh session (adjust to SDK version if needed)
            self.client.auth.set_session(access_token="", refresh_token=refresh_token)
            response = self.client.auth.refresh_session()

            if response.session:
                return TokenResponse(
                    access_token=response.session.access_token,
                    expires_in=response.session.expires_in,
                    refresh_token=response.session.refresh_token
                )

            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token refresh failed"
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Token refresh failed: {str(e)}"
            )

    async def reset_password(self, reset_data: PasswordReset) -> AuthResponse:
        """Send password reset email"""
        try:
            self.client.auth.reset_password_email(reset_data.email)
            return AuthResponse(
                success=True,
                message="Password reset email sent. Please check your inbox."
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Password reset failed: {str(e)}"
            )

    # Quick fix - just replace the update_password method in notam/auth/service.py

    async def update_password(self, password_data: PasswordUpdate, access_token: str) -> AuthResponse:
        """Update user password - FIXED VERSION"""
        try:
            # Validate token first
            user_response = self.client.auth.get_user(access_token)
            if not user_response.user:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid or expired token"
                )

            # Use direct REST API call instead of problematic set_session


            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "apikey": os.getenv("SUPABASE_ANON_KEY")
            }

            data = {"password": password_data.password}

            response = requests.put(
                f"{os.getenv('SUPABASE_URL')}/auth/v1/user",
                headers=headers,
                json=data,
                timeout=30
            )

            if response.status_code == 200:
                return AuthResponse(success=True, message="Password updated successfully")
            else:
                try:
                    error_detail = response.json().get("error_description", "Password update failed")
                except:
                    error_detail = f"HTTP {response.status_code}: Password update failed"

                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=error_detail
                )

        except HTTPException:
            raise
        except requests.exceptions.RequestException as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Network error: {str(e)}"
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Password update failed: {str(e)}"
            )

    async def get_user_profile(self, access_token: str) -> UserProfile:
        """Get current user profile"""
        try:
            response = self.client.auth.get_user(access_token)
            if response.user:
                return UserProfile(
                    id=response.user.id,
                    email=response.user.email,
                    full_name=response.user.user_metadata.get("full_name"),
                    organization=response.user.user_metadata.get("organization"),
                    role=response.user.user_metadata.get("role", "user"),
                    email_confirmed=bool(response.user.email_confirmed_at),
                    created_at=response.user.created_at,
                    last_sign_in=response.user.last_sign_in_at
                )

            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to get user profile: {str(e)}"
            )

# Global service instance
auth_service = AuthService()
