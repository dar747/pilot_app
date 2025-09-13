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
from notam.db import SessionLocal, PasswordResetCode  # Add this line
from datetime import datetime, timezone, timedelta    # Add this line
import random
import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import random
from datetime import datetime, timezone, timedelta


class AuthService:
    def __init__(self):
        self.client: Client = supabase_auth.get_client()
    # ADD THIS METHOD RIGHT HERE ⬇️

    def get_admin_client(self):
        """Get Supabase client with admin privileges"""
        from supabase import create_client
        return create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_SERVICE_ROLE_KEY")  # Admin key
        )

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
            # Only set session if we have both tokens, otherwise just sign out
            if refresh_token:
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

    async def update_password(self, password_data: PasswordUpdate, access_token: str) -> AuthResponse:
        """Update password for logged-in user"""
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

    async def send_reset_email(self, email: str, code: str):
        """Send 6-digit code via Gmail"""
        try:
            msg = MIMEMultipart()
            msg['From'] = os.getenv("SMTP_EMAIL")
            msg['To'] = email
            msg['Subject'] = "Your Pilot App Password Reset Code"

            body = f"""
    Hello,

    Your password reset code is: {code}

    This code will expire in 15 minutes.

    If you didn't request this password reset, please ignore this email.

    Best regards,
    Pilot App Team
            """

            msg.attach(MIMEText(body, 'plain'))

            await aiosmtplib.send(
                msg,
                hostname="smtp.gmail.com",
                port=587,
                start_tls=True,
                username=os.getenv("SMTP_EMAIL"),
                password=os.getenv("SMTP_PASSWORD")
            )

            print(f"📧 Reset code email sent to {email}")

        except Exception as e:
            print(f"❌ Email failed: {e}")
            raise

    async def reset_password(self, reset_data: PasswordReset) -> AuthResponse:
        """Send 6-digit reset code to email"""
        try:
            # Generate 6-digit code
            code = f"{random.randint(100000, 999999)}"
            expires_at = datetime.now(timezone.utc) + timedelta(minutes=15)

            # Store in database
            session = SessionLocal()
            try:
                session.query(PasswordResetCode).filter_by(email=reset_data.email).delete()

                reset_code = PasswordResetCode(
                    email=reset_data.email,
                    code=code,
                    expires_at=expires_at
                )
                session.add(reset_code)
                session.commit()

                # SEND REAL EMAIL 📧
                await self.send_reset_email(reset_data.email, code)

                return AuthResponse(
                    success=True,
                    message="6-digit reset code sent to your email"
                )

            finally:
                session.close()

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Password reset failed: {str(e)}"
            )

    async def verify_reset_code(self, email: str, code: str, new_password: str) -> AuthResponse:
        """Verify code and reset password directly in app"""
        try:
            session = SessionLocal()
            try:
                # Find the code
                stored_code = session.query(PasswordResetCode).filter_by(email=email).first()

                if not stored_code:
                    raise HTTPException(status_code=400, detail="No reset code found for this email")

                # Check if expired
                if stored_code.expires_at < datetime.now(timezone.utc):
                    session.delete(stored_code)
                    session.commit()
                    raise HTTPException(status_code=400, detail="Reset code has expired")

                # Check if code matches
                if stored_code.code != code:
                    raise HTTPException(status_code=400, detail="Invalid reset code")

                # Code is valid - update password using admin client
                try:
                    admin_client = self.get_admin_client()

                    # CORRECTED: Use the right method names for your SDK version
                    users_response = admin_client.auth.admin.list_users()
                    target_user = None

                    # Look through the users list to find matching email
                    if hasattr(users_response, 'users'):
                        users = users_response.users
                    elif isinstance(users_response, list):
                        users = users_response
                    else:
                        users = []

                    for user in users:
                        if user.email == email:
                            target_user = user
                            break

                    if not target_user:
                        raise HTTPException(status_code=400, detail="User not found")

                    # Update password using admin privileges
                    update_response = admin_client.auth.admin.update_user_by_id(
                        target_user.id,
                        {"password": new_password}
                    )

                    # Delete the used code
                    session.delete(stored_code)
                    session.commit()

                    return AuthResponse(
                        success=True,
                        message="Password reset successfully! You can now login with your new password."
                    )

                except Exception as supabase_error:
                    print(f"Supabase admin error: {supabase_error}")
                    # Print more details for debugging
                    print(f"Error type: {type(supabase_error)}")
                    raise HTTPException(status_code=400, detail=f"Failed to update password: {str(supabase_error)}")

            finally:
                session.close()

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Code verification failed: {str(e)}"
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
