import os
import sys

os.environ.setdefault("JWT_SECRET", "test-secret")
os.environ.setdefault("SEED_DEMO", "false")
if "DATABASE_URL" not in os.environ:
    os.environ["DATABASE_URL"] = "sqlite:///" + os.path.join(os.path.dirname(__file__), "test.db")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
