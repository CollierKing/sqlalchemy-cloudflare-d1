#!/usr/bin/env python3
"""
Example usage of the SQLAlchemy Cloudflare D1 dialect.

This script demonstrates how to use the dialect to connect to a D1 database
and perform basic operations.

Note: You need to replace the placeholder values with your actual Cloudflare
credentials and database information.
"""

import os
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    MetaData,
    Table,
    select,
    text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Example connection - replace with your actual credentials
# You can set these as environment variables for security
ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID", "your_account_id_here")
API_TOKEN = os.getenv("CF_API_TOKEN", "your_api_token_here")
DATABASE_ID = os.getenv("CF_DATABASE_ID", "your_database_id_here")


def create_d1_engine():
    """Create a SQLAlchemy engine for D1."""
    connection_string = f"cloudflare_d1://{ACCOUNT_ID}:{API_TOKEN}@{DATABASE_ID}"
    return create_engine(connection_string, echo=True)


def example_orm_usage():
    """Example using SQLAlchemy ORM."""
    print("=== SQLAlchemy ORM Example ===")

    # Create engine and base
    engine = create_d1_engine()
    Base = declarative_base()

    # Define a model
    class User(Base):
        __tablename__ = "users"

        id = Column(Integer, primary_key=True)
        name = Column(String(50), nullable=False)
        email = Column(String(100), nullable=False)

        def __repr__(self):
            return f"<User(id={self.id}, name='{self.name}', email='{self.email}')>"

    # Create tables (this would execute CREATE TABLE IF NOT EXISTS)
    Base.metadata.create_all(engine)

    # Create session
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Add some users
        users = [
            User(name="Alice", email="alice@example.com"),
            User(name="Bob", email="bob@example.com"),
            User(name="Charlie", email="charlie@example.com"),
        ]

        for user in users:
            session.add(user)
        session.commit()

        # Query users
        all_users = session.query(User).all()
        print(f"Found {len(all_users)} users:")
        for user in all_users:
            print(f"  {user}")

        # Query with filter
        alice = session.query(User).filter(User.name == "Alice").first()
        if alice:
            print(f"Found Alice: {alice}")

    except Exception as e:
        print(f"Error in ORM example: {e}")
        session.rollback()
    finally:
        session.close()


def example_core_usage():
    """Example using SQLAlchemy Core."""
    print("\n=== SQLAlchemy Core Example ===")

    engine = create_d1_engine()
    metadata = MetaData()

    # Define table
    products = Table(
        "products",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(100), nullable=False),
        Column("price", Integer, nullable=False),  # Price in cents
        Column("category", String(50)),
    )

    # Create table
    metadata.create_all(engine)

    with engine.connect() as conn:
        try:
            # Insert data
            conn.execute(
                products.insert().values(
                    [
                        {"name": "Laptop", "price": 99999, "category": "Electronics"},
                        {"name": "Coffee Mug", "price": 1299, "category": "Kitchen"},
                        {"name": "Book", "price": 1999, "category": "Books"},
                    ]
                )
            )

            # Query data
            result = conn.execute(select(products))
            print("Products:")
            for row in result:
                price_dollars = row.price / 100
                print(f"  {row.name}: ${price_dollars:.2f} ({row.category})")

            # Query with conditions
            expensive_items = conn.execute(
                select(products).where(products.c.price > 1500)
            )
            print("\nExpensive items (>$15):")
            for row in expensive_items:
                price_dollars = row.price / 100
                print(f"  {row.name}: ${price_dollars:.2f}")

        except Exception as e:
            print(f"Error in Core example: {e}")


def example_raw_sql():
    """Example using raw SQL."""
    print("\n=== Raw SQL Example ===")

    engine = create_d1_engine()

    with engine.connect() as conn:
        try:
            # Query database schema
            result = conn.execute(
                text("""
                SELECT name, type FROM sqlite_master
                WHERE type IN ('table', 'index')
                ORDER BY type, name
            """)
            )

            print("Database objects:")
            for row in result:
                print(f"  {row.type}: {row.name}")

            # Example of a more complex query
            result = conn.execute(
                text("""
                SELECT
                    'users' as table_name,
                    COUNT(*) as row_count
                FROM users
                WHERE users.name IS NOT NULL
                UNION ALL
                SELECT
                    'products' as table_name,
                    COUNT(*) as row_count
                FROM products
                WHERE products.name IS NOT NULL
            """)
            )

            print("\nTable row counts:")
            for row in result:
                print(f"  {row.table_name}: {row.row_count} rows")

        except Exception as e:
            print(f"Error in raw SQL example: {e}")


def main():
    """Run all examples."""
    print("Cloudflare D1 SQLAlchemy Dialect Examples")
    print("=" * 50)

    # Check if credentials are set
    if ACCOUNT_ID == "your_account_id_here":
        print(
            "⚠️  Warning: Please set your Cloudflare credentials in environment variables:"
        )
        print("   export CF_ACCOUNT_ID=your_account_id")
        print("   export CF_API_TOKEN=your_api_token")
        print("   export CF_DATABASE_ID=your_database_id")
        print("\nOr modify the script with your actual values.")
        print("\nRunning examples with placeholder values (will fail)...")
        print()

    try:
        example_orm_usage()
        example_core_usage()
        example_raw_sql()

        print("\n✅ All examples completed!")

    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        print("\nMake sure you have:")
        print("1. Valid Cloudflare credentials")
        print("2. A D1 database created")
        print("3. Proper API token permissions (Account:D1:Edit)")


if __name__ == "__main__":
    main()
