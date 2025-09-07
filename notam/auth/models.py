# notam/auth/models.py
from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime

class UserSignUp(BaseModel):
    email: EmailStr
    password: str
    full_name: Optional[str] = None
    organization: Optional[str] = None

class UserSignIn(BaseModel):
    email: EmailStr
    password: str

class PasswordReset(BaseModel):
    email: EmailStr

class PasswordUpdate(BaseModel):
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    refresh_token: Optional[str] = None

class TokenRefresh(BaseModel):
    refresh_token: str

class UserProfile(BaseModel):
    id: str
    email: str
    full_name: Optional[str] = None
    organization: Optional[str] = None
    role: str = "user"
    email_confirmed: bool = False
    created_at: datetime
    last_sign_in: Optional[datetime] = None

class UserUpdate(BaseModel):
    full_name: Optional[str] = None
    organization: Optional[str] = None

class AuthResponse(BaseModel):
    success: bool
    message: str
    user: Optional[UserProfile] = None
    tokens: Optional[TokenResponse] = None
