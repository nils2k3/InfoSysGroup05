import os
import time
import textwrap

import ibm_db
import pytest
import logging

DB2_DATABASE = os.getenv("DB2_DATABASE", "plantool")
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
        case_order = []
        case_items = {}
        for item in items:
            case = item.get("case", item["title"])
            if case not in case_items:
                case_items[case] = []
                case_order.append(case)
            case_items[case].append(item)
        for idx, case in enumerate(case_order, start=1):
            header = f"{'*' * 21} Test Case {idx} {'*' * 21}"
            footer = f"{'*' * 16} End of Test Case {idx} {'*' * 16}"
            handle.write(f"{header}\n")
            handle.write(f"Case: {case}\n")
            for item in case_items[case]:
                handle.write(f"Title: {item['title']}\n")
                handle.write(f"SQL: {item['sql']}\n")
                handle.write(f"Result: {item['result']}\n")
                if item.get("detail"):
                    handle.write(f"Detail: {item['detail']}\n")
                if item.get("rows"):
                    handle.write("Rows (first 10):\n")
                    for row in item["rows"]:
                        handle.write(f"  {row}\n")
            handle.write(f"{footer}\n\n")
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
    font_name = "Helvetica"
    font_size = 10
    pdf.setFont(font_name, font_size)
    max_width = width - 72
    max_chars = max(20, int(max_width / (font_size * 0.55)))

    def wrap_line(line):
        return textwrap.wrap(line, width=max_chars) or [""]

    case_order = []
    case_items = {}
    for item in items:
        case = item.get("case", item["title"])
        if case not in case_items:
            case_items[case] = []
            case_order.append(case)
        case_items[case].append(item)
    for idx, case in enumerate(case_order, start=1):
        header = f"{'*' * 21} Test Case {idx} {'*' * 21}"
        footer = f"{'*' * 16} End of Test Case {idx} {'*' * 16}"
        lines = [header, f"Case: {case}"]
        for item in case_items[case]:
            lines.extend([
                f"Title: {item['title']}",
                f"SQL: {item['sql']}",
                f"Result: {item['result']}",
            ])
            if item.get("detail"):
                lines.append(f"Detail: {item['detail']}")
            if item.get("rows"):
                lines.append("Rows (first 10):")
                for row in item["rows"]:
                    lines.append(f"  {row}")
        lines.append(footer)
        for line in lines:
            for chunk in wrap_line(line):
                if y < 36:
                    pdf.showPage()
                    pdf.setFont(font_name, font_size)
                    y = height - 36
                pdf.drawString(36, y, chunk)
                y -= 14
        y -= 8
    pdf.save()


def record(report_items, title, sql, result, detail="", rows=None, report=True, case=None):
    if not report:
        return
    case_name = case or title
    item = {"title": title, "sql": sql, "result": result, "detail": detail, "case": case_name}
    if rows:
        item["rows"] = rows
    report_items.append(item)


def exec_sql(conn, sql):
    return ibm_db.exec_immediate(conn, sql)


def run_statement(conn, report_items, title, sql, fetch_limit=10, report=True, case=None):
    try:
        stmt = exec_sql(conn, sql)
    except Exception as exc:
        record(report_items, title, sql, "error", str(exc), report=report, case=case)
        raise
    rows = fetch_rows(stmt, fetch_limit)
    affected = ibm_db.num_rows(stmt)
    detail = f"affected_rows={affected}" if affected >= 0 else ""
    record(report_items, title, sql, "ok", detail, rows, report=report, case=case)
    return affected, rows


def fetch_rows(stmt, limit=10):
    if ibm_db.num_fields(stmt) == 0:
        return []
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
    case = "Insert subject with S_TYPE = W"
    title = "Insert subject"
    test_id = int(time.time())
    
    sql = (
        "INSERT INTO SUBJECT "
        "(S_ID, FK_ST_NAME, S_NAME, S_SEMESTER, S_STUPO_HOURS, S_NOTES, S_TYPE) "
        "SELECT "
        f"{test_id}, "
        "ST_NAME, "
        "'Test Subject W', "
        "1, "
        "1.00, "
        "'Test insert', "
        "'W' "
        "FROM STUDY_PROGRAM "
        "FETCH FIRST 1 ROW ONLY"
    )
    try:
        affected, _rows = run_statement(conn, report_items, title, sql, case=case)
        assert affected >= 1
        select_title = f"Verify subject {test_id}"
        select_sql = f"SELECT * FROM SUBJECT WHERE S_ID = {test_id}"
        run_statement(conn, report_items, select_title, select_sql, case=case)
    finally:
        delete_title = f"Cleanup subject {test_id}"
        delete_sql = f"DELETE FROM SUBJECT WHERE S_ID = {test_id}"
        try:
            run_statement(conn, report_items, delete_title, delete_sql, fetch_limit=0, report=False, case=case)
        except Exception:
            pass


def test_insert_lecturer(conn, report_items):
    case = "Insert lecturer"
    test_id = int(time.time())

    teacher_sql = (
        "INSERT INTO TEACHER "
        "(T_ID, T_NAME, T_LASTNAME, FK_D_NAME, FK_ZIP, T_NOTES, T_IS_ACTIVE) "
        "VALUES "
        f"({test_id}, 'Test', 'Lecturer', NULL, NULL, 'Test insert', 1)"
    )
    lecturer_sql = (
        "INSERT INTO LECTURER "
        "(T_ID, L_STREET_ADDRESS) "
        "VALUES "
        f"({test_id}, 'Test Street 1')"
    )
    try:
        affected, _rows = run_statement(conn, report_items, "Insert teacher for lecturer", teacher_sql, case=case)
        assert affected >= 1
        affected, _rows = run_statement(conn, report_items, "Insert lecturer", lecturer_sql, case=case)
        assert affected >= 1
        select_title = f"Verify lecturer {test_id}"
        select_sql = f"SELECT * FROM LECTURER WHERE T_ID = {test_id}"
        run_statement(conn, report_items, select_title, select_sql, case=case)
    finally:
        delete_lecturer_title = f"Cleanup lecturer {test_id}"
        delete_lecturer_sql = f"DELETE FROM LECTURER WHERE T_ID = {test_id}"
        delete_teacher_title = f"Cleanup teacher {test_id}"
        delete_teacher_sql = f"DELETE FROM TEACHER WHERE T_ID = {test_id}"
        try:
            run_statement(conn, report_items, delete_lecturer_title, delete_lecturer_sql, fetch_limit=0, report=False, case=case)
        except Exception:
            pass
        try:
            run_statement(conn, report_items, delete_teacher_title, delete_teacher_sql, fetch_limit=0, report=False, case=case)
        except Exception:
            pass
