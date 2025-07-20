"""
SQLAlchemy dialect for Cloudflare D1 Serverless SQLite Database.

This dialect provides connectivity to Cloudflare D1 databases via the REST API.
"""

from .dialect import CloudflareD1Dialect

__version__ = "0.1.0"
__all__ = ["CloudflareD1Dialect"]
