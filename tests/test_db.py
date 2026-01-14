import os

import ibm_db
import pytest

DB2_DATABASE = os.getenv("DB2_DATABASE", "planningtool")
DB2_HOSTNAME = os.getenv("DB2_HOSTNAME", "localhost")
DB2_PORT = os.getenv("DB2_PORT", "25000")
DB2_UID = os.getenv("DB2_UID", "db2inst1")
DB2_PWD = os.getenv("DB2_PWD", "Lab-Adm")
DB2_SCHEMA = os.getenv("DB2_SCHEMA", "PLANNING_TOOL")

def build_dsn():
    return (
        f"DATABASE={DB2_DATABASE};HOSTNAME={DB2_HOSTNAME};PORT={DB2_PORT};"
        f"PROTOCOL=TCPIP;UID={DB2_UID};PWD={DB2_PWD};CURRENTSCHEMA={DB2_SCHEMA};"
    )


@pytest.fixture(scope="module")
def conn():
    connection = ibm_db.connect(build_dsn(), "", "")
    yield connection
    ibm_db.close(connection)


@pytest.fixture(scope="session")
def report_items():
    items = []
    yield items
    report_path = os.path.join(os.path.dirname(__file__), "db_test_report.txt")
    with open(report_path, "w", encoding="ascii") as handle:
        for item in items:
            handle.write(f"Title: {item['title']}\n")
            handle.write(f"SQL: {item['sql']}\n")
            handle.write(f"Result: {item['result']}\n")
            if item.get("detail"):
                handle.write(f"Detail: {item['detail']}\n")
            if item.get("rows"):
                handle.write("Rows (first 10):\n")
                for row in item["rows"]:
                    handle.write(f"  {row}\n")
            handle.write("\n")
    write_pdf_report(items)


def write_pdf_report(items):
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
    except Exception:
        return
    report_path = os.path.join(os.path.dirname(__file__), "db_test_report.pdf")
    pdf = canvas.Canvas(report_path, pagesize=letter)
    width, height = letter
    y = height - 36
    pdf.setFont("Helvetica", 10)
    for item in items:
        lines = [
            f"Title: {item['title']}",
            f"SQL: {item['sql']}",
            f"Result: {item['result']}",
        ]
        if item.get("detail"):
            lines.append(f"Detail: {item['detail']}")
        if item.get("rows"):
            lines.append("Rows (first 10):")
            for row in item["rows"]:
                lines.append(f"  {row}")
        for line in lines:
            if y < 36:
                pdf.showPage()
                pdf.setFont("Helvetica", 10)
                y = height - 36
            pdf.drawString(36, y, line)
            y -= 14
        y -= 8
    pdf.save()


def record(report_items, title, sql, result, detail="", rows=None):
    item = {"title": title, "sql": sql, "result": result, "detail": detail}
    if rows:
        item["rows"] = rows
    report_items.append(item)


def exec_sql(conn, sql):
    return ibm_db.exec_immediate(conn, sql)


def run_statement(conn, report_items, title, sql, fetch_limit=10):
    try:
        stmt = exec_sql(conn, sql)
    except Exception as exc:
        record(report_items, title, sql, "error", str(exc))
        raise
    rows = fetch_rows(stmt, fetch_limit)
    affected = ibm_db.num_rows(stmt)
    detail = f"affected_rows={affected}" if affected >= 0 else ""
    record(report_items, title, sql, "ok", detail, rows)
    return affected, rows


def fetch_rows(stmt, limit=10):
    rows = []
    while len(rows) < limit:
        row = ibm_db.fetch_assoc(stmt)
        if not row:
            break
        rows.append(row)
    return rows


def test_db_connection(conn):
    assert conn is not None


def test_insert_subject_type_w(conn, report_items):
    title = "Insert subject with S_TYPE = W"
    
    sql = (
        "INSERT INTO SUBJECT "
        "(S_NR, S_STUDY_PROGRAM, S_NAME, S_SEMESTER, S_STUPO_HOURS, "
        "S_SCHEDULE_HOURS, S_COMMENT, S_TYPE) "
        "SELECT "
        "'T' || VARCHAR_FORMAT(CURRENT TIMESTAMP, 'YYMMDDHH24MI'), "
        "ST_NAME, "
        "'Test Subject W', "
        "1, "
        "1.00, "
        "1.00, "
        "'Test insert', "
        "'W' "
        "FROM STUDY_PROGRAM "
        "FETCH FIRST 1 ROW ONLY"
    )
    affected, _rows = run_statement(conn, report_items, title, sql)
    assert affected >= 1
