"""
app package initializer.

Loads environment variables from .env files for the application:
- Tries to load app/.env first (local to this package)
- If not found, loads .env from the project root

This ensures environment variables are available for all app modules.
"""

import os
from dotenv import load_dotenv

app_env_path = os.path.join(os.path.dirname(__file__), ".env")
root_env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env"))
if os.path.exists(app_env_path):
    load_dotenv(app_env_path, override=True)
else:
    load_dotenv(root_env_path, override=True)
