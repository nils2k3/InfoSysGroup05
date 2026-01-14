DB2 Database Setup
==================

Prerequisites
-------------
- DB2 client/CLP available (`db2` command in PATH).
- User has permission to create databases.

Quick setup (recommended)
-------------------------
Run the provided script from the repo root:

```
./setup_database.sh
```

What it does:
- Creates the database and schema using `create_db2_database.sql`.
- Populates data using `db2_inserts.sql`.
- Prints basic row counts.

Manual setup
------------
If you prefer to run the steps yourself:

```
db2 -tvf create_db2_database.sql
db2 +c -tvf db2_inserts.sql
```

Notes
-----
- The database name created by the schema script is `PLANTOOL`.
- After setup, you can connect with:

```
db2 connect to PLANTOOL
db2 "set current schema = PLANNING_TOOL"
```

