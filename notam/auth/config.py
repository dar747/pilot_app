import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()


class SupabaseAuth:
    def __init__(self):
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_ANON_KEY")

        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL and SUPABASE_ANON_KEY must be set")

        self.client: Client = create_client(self.url, self.key)

    def get_client(self) -> Client:
        return self.client


supabase_auth = SupabaseAuth()