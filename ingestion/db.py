import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------

load_dotenv()

ENV_VAR = ['DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT', 'DB_NAME']
MISSING_VAR = []
for var in ENV_VAR:
    value = os.getenv(var)
    if not value or not value.strip():
        MISSING_VAR.append(var)

if MISSING_VAR:
    raise ValueError(
        f"Missing or blank env variables: {', '.join(MISSING_VAR)}")

# ------------------------------------------------------------
# Database
# ------------------------------------------------------------
DB_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)


def get_engine():
    return create_engine(DB_URL)
