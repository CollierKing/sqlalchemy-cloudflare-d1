# SQLAlchemy Cloudflare D1 Dialect

A SQLAlchemy dialect for [Cloudflare's D1 Serverless SQLite Database](https://developers.cloudflare.com/d1/) using the REST API.

## Features

- Full SQLAlchemy ORM and Core support
- Async and sync query execution via D1 REST API
- SQLite/D1 compatible SQL compilation
- Prepared statement support with parameter binding
- Connection pooling and management
- Type mapping for D1/SQLite data types

## Installation

```bash
pip install sqlalchemy-cloudflare-d1
```

Or install from source:

```bash
git clone https://github.com/collierking/sqlalchemy-cloudflare-d1.git
cd sqlalchemy-cloudflare-d1
pip install -e .
```

## Prerequisites

1. A Cloudflare account with D1 enabled
2. A D1 database created via the Cloudflare dashboard or CLI
3. A Cloudflare API token with D1 permissions

### Creating a D1 Database

Using the Cloudflare CLI:
```bash
wrangler d1 create my-database
```

Or via the [Cloudflare dashboard](https://dash.cloudflare.com/).

### Creating an API Token

1. Go to [Cloudflare API Tokens](https://dash.cloudflare.com/profile/api-tokens)
2. Click "Create Token"
3. Use the "Custom token" template
4. Add permissions: `Account:D1:Edit`
5. Add your account in "Account Resources"
6. Click "Continue to summary" and "Create Token"

## Usage

### Connection String Format

```python
from sqlalchemy import create_engine

# Format: cloudflare_d1://account_id:api_token@database_id
engine = create_engine(
    "cloudflare_d1://your_account_id:your_api_token@your_database_id"
)
```

### Basic Example

```python
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Create engine
engine = create_engine(
    "cloudflare_d1://account_id:api_token@database_id"
)

# Create base and define model
Base = declarative_base()

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    email = Column(String(100))

# Create tables
Base.metadata.create_all(engine)

# Create session and add data
Session = sessionmaker(bind=engine)
session = Session()

# Add a user
user = User(name="Alice", email="alice@example.com")
session.add(user)
session.commit()

# Query users
users = session.query(User).all()
for user in users:
    print(f"{user.name}: {user.email}")

session.close()
```

### Core API Example

```python
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, select

engine = create_engine("cloudflare_d1://account_id:api_token@database_id")

metadata = MetaData()
users = Table('users', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(50)),
    Column('email', String(100))
)

# Create table
metadata.create_all(engine)

with engine.connect() as conn:
    # Insert data
    conn.execute(users.insert().values(name="Bob", email="bob@example.com"))

    # Query data
    result = conn.execute(select(users))
    for row in result:
        print(row)
```

### Raw SQL Example

```python
from sqlalchemy import create_engine, text

engine = create_engine("cloudflare_d1://account_id:api_token@database_id")

with engine.connect() as conn:
    # Execute raw SQL
    result = conn.execute(text("SELECT * FROM sqlite_master WHERE type='table'"))

    for row in result:
        print(row)
```

## Configuration

### Connection Parameters

You can pass additional parameters via the connection string or engine creation:

```python
from sqlalchemy import create_engine

# Via connection string query parameters
engine = create_engine(
    "cloudflare_d1://account_id:api_token@database_id?timeout=60"
)

# Via connect_args
engine = create_engine(
    "cloudflare_d1://account_id:api_token@database_id",
    connect_args={
        "timeout": 60,
    }
)
```

### Environment Variables

You can also use environment variables:

```python
import os
from sqlalchemy import create_engine

engine = create_engine(
    f"cloudflare_d1://{os.getenv('CF_ACCOUNT_ID')}:"
    f"{os.getenv('CF_API_TOKEN')}@{os.getenv('CF_DATABASE_ID')}"
)
```

## Limitations

This dialect has some limitations due to D1's REST API nature:

1. **No transactions**: D1 REST API doesn't support explicit transactions. Each query is auto-committed.
2. **No isolation levels**: Connection isolation levels are not supported.
3. **Limited concurrency**: Connections are HTTP-based, not persistent database connections.
4. **No stored procedures**: D1 doesn't support stored procedures or custom functions.
5. **Rate limiting**: Subject to Cloudflare API rate limits.

## Type Mapping

| SQLAlchemy Type | D1/SQLite Type | Notes |
|----------------|----------------|-------|
| `Integer` | `INTEGER` | |
| `String(n)` | `VARCHAR(n)` | |
| `Text` | `TEXT` | |
| `Float` | `REAL` | |
| `Numeric` | `NUMERIC` | |
| `Boolean` | `INTEGER` | Stored as 0/1 |
| `DateTime` | `TEXT` | ISO format string |
| `Date` | `TEXT` | ISO format string |
| `Time` | `TEXT` | ISO format string |

## Error Handling

The dialect will raise appropriate SQLAlchemy exceptions:

```python
from sqlalchemy.exc import SQLAlchemyError, OperationalError

try:
    result = conn.execute("SELECT * FROM nonexistent_table")
except OperationalError as e:
    print(f"Database error: {e}")
except SQLAlchemyError as e:
    print(f"SQLAlchemy error: {e}")
```

## Development

For detailed development instructions, see [`.github/DEVELOPMENT.md`](.github/DEVELOPMENT.md).

### Quick Start

```bash
git clone https://github.com/collierking/sqlalchemy-cloudflare-d1.git
cd sqlalchemy-cloudflare-d1

# Install dependencies and setup pre-commit hooks
make install
make setup_hooks

# Run tests and linting
make check

# Build package
make build
```

### Development Tools

- **Ruff**: Fast Python linter and formatter
- **mypy**: Static type checking
- **codespell**: Spell checking
- **pre-commit**: Automated pre-commit checks
- **pytest**: Testing framework with socket control

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for your changes
5. Run the test suite
6. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Related Projects

- [SQLAlchemy](https://www.sqlalchemy.org/) - The Python SQL toolkit
- [Cloudflare D1](https://developers.cloudflare.com/d1/) - Serverless SQLite database
- [httpx](https://www.python-httpx.org/) - HTTP client library used for API communication

## Support

- [GitHub Issues](https://github.com/collierking/sqlalchemy-cloudflare-d1/issues)
- [Cloudflare D1 Documentation](https://developers.cloudflare.com/d1/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
