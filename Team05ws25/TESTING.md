DB2 Test Suite
==============

Prerequisites
-------------
- DB2 client/CLP available (`db2` command in PATH).
- Python 3 installed.
- Database already created and populated.

Install Python dependencies
---------------------------
From the repo root:

```
python3 -m pip install -r requirements.txt
```

Run the tests
-------------
Use pytest to execute `tests/test_db.py`:

```
pytest -q tests/test_db.py
```

Outputs
-------
- `tests/db_test_report.txt`
- `tests/db_test_report.pdf`

Environment variables (optional)
--------------------------------
Defaults used in `tests/test_db.py`:
- `DB2_DATABASE=plantool`
- `DB2_HOSTNAME=localhost`
- `DB2_PORT=25000`
- `DB2_UID=db2inst1`
- `DB2_PWD=Lab-Adm`
- `DB2_SCHEMA=PLANNING_TOOL`

You can override them before running pytest, for example:

```
DB2_UID=youruser DB2_PWD=yourpass pytest -q tests/test_db.py
```

