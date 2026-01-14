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
    with open(report_path, "w", encoding="utf-8", errors="replace") as handle:
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


def test_deactivate_lecturer(conn, report_items):
    case = "Deactivate lecturer"
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
        f"({test_id}, 'Test Street 2')"
    )
    update_sql = f"UPDATE TEACHER SET T_IS_ACTIVE = 0 WHERE T_ID = {test_id}"
    try:
        affected, _rows = run_statement(conn, report_items, "Insert teacher for lecturer", teacher_sql, case=case)
        assert affected >= 1
        affected, _rows = run_statement(conn, report_items, "Insert lecturer", lecturer_sql, case=case)
        assert affected >= 1
        affected, _rows = run_statement(conn, report_items, "Deactivate lecturer", update_sql, case=case)
        assert affected >= 1
        select_title = f"Verify lecturer inactive {test_id}"
        select_sql = f"SELECT T_ID, T_IS_ACTIVE FROM TEACHER WHERE T_ID = {test_id}"
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


def test_deactivate_professor(conn, report_items):
    case = "Deactivate professor"
    test_id = int(time.time())

    teacher_sql = (
        "INSERT INTO TEACHER "
        "(T_ID, T_NAME, T_LASTNAME, FK_D_NAME, FK_ZIP, T_NOTES, T_IS_ACTIVE) "
        "VALUES "
        f"({test_id}, 'Test', 'Professor', NULL, NULL, 'Test insert', 1)"
    )
    professor_sql = (
        "INSERT INTO PROFESSOR "
        "(T_ID, P_ROOM) "
        "VALUES "
        f"({test_id}, 'R-101')"
    )
    update_sql = f"UPDATE TEACHER SET T_IS_ACTIVE = 0 WHERE T_ID = {test_id}"
    try:
        affected, _rows = run_statement(conn, report_items, "Insert teacher for professor", teacher_sql, case=case)
        assert affected >= 1
        affected, _rows = run_statement(conn, report_items, "Insert professor", professor_sql, case=case)
        assert affected >= 1
        affected, _rows = run_statement(conn, report_items, "Deactivate professor", update_sql, case=case)
        assert affected >= 1
        select_title = f"Verify professor inactive {test_id}"
        select_sql = f"SELECT T_ID, T_IS_ACTIVE FROM TEACHER WHERE T_ID = {test_id}"
        run_statement(conn, report_items, select_title, select_sql, case=case)
    finally:
        delete_professor_title = f"Cleanup professor {test_id}"
        delete_professor_sql = f"DELETE FROM PROFESSOR WHERE T_ID = {test_id}"
        delete_teacher_title = f"Cleanup teacher {test_id}"
        delete_teacher_sql = f"DELETE FROM TEACHER WHERE T_ID = {test_id}"
        try:
            run_statement(conn, report_items, delete_professor_title, delete_professor_sql, fetch_limit=0, report=False, case=case)
        except Exception:
            pass
        try:
            run_statement(conn, report_items, delete_teacher_title, delete_teacher_sql, fetch_limit=0, report=False, case=case)
        except Exception:
            pass


def test_hire_professor(conn, report_items):
    case = "Hire professor"
    test_id = int(time.time())
    zip_code = f"Z{str(test_id)[-5:]}"

    postal_sql = (
        "INSERT INTO POSTAL_CODE "
        "(ZIP, CITY) "
        "VALUES "
        f"('{zip_code}', 'Test City')"
    )
    teacher_sql = (
        "INSERT INTO TEACHER "
        "(T_ID, T_NAME, T_LASTNAME, FK_D_NAME, FK_ZIP, T_NOTES, T_IS_ACTIVE) "
        "VALUES "
        f"({test_id}, 'Test', 'Professor', NULL, '{zip_code}', 'Test hire', 1)"
    )
    professor_sql = (
        "INSERT INTO PROFESSOR "
        "(T_ID, P_ROOM) "
        "VALUES "
        f"({test_id}, 'R-202')"
    )
    try:
        affected, _rows = run_statement(conn, report_items, "Insert postal code", postal_sql, case=case)
        assert affected >= 1
        affected, _rows = run_statement(conn, report_items, "Insert teacher for professor", teacher_sql, case=case)
        assert affected >= 1
        affected, _rows = run_statement(conn, report_items, "Insert professor", professor_sql, case=case)
        assert affected >= 1
        select_title = f"Verify hired professor {test_id}"
        select_sql = (
            "SELECT T.T_ID, T.T_IS_ACTIVE, T.FK_ZIP, P.P_ROOM "
            "FROM TEACHER T "
            "JOIN PROFESSOR P ON P.T_ID = T.T_ID "
            f"WHERE T.T_ID = {test_id}"
        )
        run_statement(conn, report_items, select_title, select_sql, case=case)
    finally:
        delete_professor_title = f"Cleanup professor {test_id}"
        delete_professor_sql = f"DELETE FROM PROFESSOR WHERE T_ID = {test_id}"
        delete_teacher_title = f"Cleanup teacher {test_id}"
        delete_teacher_sql = f"DELETE FROM TEACHER WHERE T_ID = {test_id}"
        delete_postal_title = f"Cleanup postal code {zip_code}"
        delete_postal_sql = f"DELETE FROM POSTAL_CODE WHERE ZIP = '{zip_code}'"
        try:
            run_statement(conn, report_items, delete_professor_title, delete_professor_sql, fetch_limit=0, report=False, case=case)
        except Exception:
            pass
        try:
            run_statement(conn, report_items, delete_teacher_title, delete_teacher_sql, fetch_limit=0, report=False, case=case)
        except Exception:
            pass
        try:
            run_statement(conn, report_items, delete_postal_title, delete_postal_sql, fetch_limit=0, report=False, case=case)
        except Exception:
            pass


def test_insert_semester_planning(conn, report_items):
    case = "Insert semester planning"
    test_id = int(time.time())

    insert_sql = (
        "INSERT INTO SEMESTER_PLANNING "
        "(SP_ID, SP_TERM, SP_VERSION_NR, SP_IS_FINAL) "
        "VALUES "
        f"({test_id}, 'WS99', 1, 0)"
    )
    try:
        affected, _rows = run_statement(conn, report_items, "Start a new semester planning session for a upcoming term", insert_sql, case=case)
        assert affected >= 1
        select_title = f"Verify semester planning {test_id}"
        select_sql = f"SELECT * FROM SEMESTER_PLANNING WHERE SP_ID = {test_id}"
        run_statement(conn, report_items, select_title, select_sql, case=case)
    finally:
        delete_title = f"Cleanup semester planning {test_id}"
        delete_sql = f"DELETE FROM SEMESTER_PLANNING WHERE SP_ID = {test_id}"
        try:
            run_statement(conn, report_items, delete_title, delete_sql, fetch_limit=0, report=False, case=case)
        except Exception:
            pass


def test_list_subjects_for_program_semester(conn, report_items):
    case = "List subjects for SWB semester 6"
    sql = (
        "SELECT S.S_ID, S.S_NAME, S.S_SEMESTER, ST.ST_NAME "
        "FROM SUBJECT S "
        "JOIN STUDY_PROGRAM ST ON S.FK_ST_NAME = ST.ST_NAME "
        "WHERE ST.ST_NAME = 'SWB' AND S.S_SEMESTER = 6"
    )
    inserted_o_ids = []
    try:
        _affected, rows = run_statement(
            conn,
            report_items,
            "Query subjects for SWB semester 6",
            sql,
            fetch_limit=1000,
            case=case,
        )
        _affected, semester_rows = run_statement(
            conn,
            report_items,
            "Get current semester",
            "SELECT MAX(SP_ID) AS SP_ID FROM SEMESTER_PLANNING",
            fetch_limit=1,
            case=case,
        )
        current_sp_id = semester_rows[0]["SP_ID"] if semester_rows else None
        assert current_sp_id is not None
        _affected, next_id_rows = run_statement(
            conn,
            report_items,
            "Get next offering id",
            "SELECT COALESCE(MAX(O_ID), 0) + 1 AS NEXT_ID FROM OFFERING",
            fetch_limit=1,
            case=case,
        )
        next_o_id = next_id_rows[0]["NEXT_ID"] if next_id_rows else 1
        for row in rows:
            s_id = row.get("S_ID")
            if s_id is None:
                continue
            insert_sql = (
                "INSERT INTO OFFERING "
                "(O_ID, FK_S_ID, FK_SP_ID, O_PLANNED_HOURS) "
                "VALUES "
                f"({next_o_id}, {s_id}, {current_sp_id}, 0)"
            )
            affected, _ = run_statement(
                conn,
                report_items,
                f"Insert offering for subject {s_id}",
                insert_sql,
                case=case,
            )
            if affected >= 1:
                inserted_o_ids.append(next_o_id)
                next_o_id += 1
        if inserted_o_ids:
            verify_sql = (
                "SELECT O_ID, FK_S_ID, FK_SP_ID "
                "FROM OFFERING "
                f"WHERE O_ID IN ({', '.join(str(x) for x in inserted_o_ids)})"
            )
            run_statement(conn, report_items, "Verify inserted offerings", verify_sql, fetch_limit=1000, case=case)
    finally:
        if inserted_o_ids:
            delete_sql = (
                "DELETE FROM OFFERING "
                f"WHERE O_ID IN ({', '.join(str(x) for x in inserted_o_ids)})"
            )
            try:
                run_statement(conn, report_items, "Cleanup offerings", delete_sql, fetch_limit=0, report=False, case=case)
            except Exception:
                pass


def test_missing_offering_assignments(conn, report_items):
    case = "Missing offering assignments"
    sql = (
        "SELECT O.O_ID, O.FK_S_ID, O.FK_SP_ID "
        "FROM OFFERING O "
        "LEFT JOIN OFFERING_ASSIGNMENT OA ON OA.FK_O_ID = O.O_ID "
        "WHERE OA.FK_O_ID IS NULL"
    )
    run_statement(conn, report_items, "Find offerings without assignments", sql, case=case)


def test_professor_workload_for_semester(conn, report_items):
    case = "Professor workload for semester"
    _affected, semester_rows = run_statement(
        conn,
        report_items,
        "Get current semester",
        "SELECT MAX(SP_ID) AS SP_ID FROM SEMESTER_PLANNING",
        fetch_limit=1,
        case=case,
    )
    current_sp_id = semester_rows[0]["SP_ID"] if semester_rows else None
    _affected, professor_rows = run_statement(
        conn,
        report_items,
        "Get a professor",
        "SELECT T_ID FROM PROFESSOR FETCH FIRST 1 ROW ONLY",
        fetch_limit=1,
        case=case,
    )
    professor_id = professor_rows[0]["T_ID"] if professor_rows else None
    assert current_sp_id is not None
    assert professor_id is not None
    sql = (
        "SELECT "
        "T_ID, "
        "T_NAME, "
        "T_LASTNAME, "
        "ASSIGNED_HOURS, "
        "REDUCTION_HOURS, "
        "TOTAL_WORKLOAD, "
        "SP_TERM "
        "FROM PROFESSOR_ESTIMATED_WORKLOAD "
        f"WHERE T_ID = {professor_id}"
    )
    run_statement(conn, report_items, "Compute professor workload", sql, case=case)




def test_report_offered_courses_for_semester(conn, report_items):
    case = "Report offered courses for semester"
    _affected, semester_rows = run_statement(
        conn,
        report_items,
        "Get current semester",
        "SELECT MAX(SP_ID) AS SP_ID FROM SEMESTER_PLANNING",
        fetch_limit=1,
        case=case,
    )
    current_sp_id = semester_rows[0]["SP_ID"] if semester_rows else None
    assert current_sp_id is not None
    sql = (
        "SELECT "
        "S_ID, "
        "S_NAME, "
        "S_SEMESTER, "
        "ST_NAME, "
        "FK_SP_ID, "
        "SP_TERM "
        "FROM STUDY_PROGRAM_OFFERED_COURSES "
        f"WHERE FK_SP_ID = {current_sp_id} "
        "ORDER BY S_NAME"
    )
    run_statement(conn, report_items, "Report offered courses for semester", sql, fetch_limit=10, case=case)


def test_update_offering_assignment_actual_hours(conn, report_items):
    case = "Update offering assignment actual hours"
    test_id = int(time.time())
    dept_name = f"D{test_id}"
    study_name = f"S{test_id}"

    insert_department_sql = (
        "INSERT INTO DEPARTMENT (D_NAME) "
        f"VALUES ('{dept_name}')"
    )
    insert_study_sql = (
        "INSERT INTO STUDY_PROGRAM (ST_NAME, FK_D_NAME) "
        f"VALUES ('{study_name}', '{dept_name}')"
    )
    insert_subject_sql = (
        "INSERT INTO SUBJECT "
        "(S_ID, FK_ST_NAME, S_NAME, S_SEMESTER, S_STUPO_HOURS, S_NOTES, S_TYPE) "
        f"VALUES ({test_id}, '{study_name}', 'Test Subject', 1, 1.00, 'Test', 'W')"
    )
    insert_semester_sql = (
        "INSERT INTO SEMESTER_PLANNING "
        "(SP_ID, SP_TERM, SP_VERSION_NR, SP_IS_FINAL) "
        f"VALUES ({test_id}, 'TS{test_id}', 1, 0)"
    )
    insert_offering_sql = (
        "INSERT INTO OFFERING "
        "(O_ID, FK_S_ID, FK_SP_ID, O_PLANNED_HOURS) "
        f"VALUES ({test_id}, {test_id}, {test_id}, 1)"
    )
    insert_teacher_sql = (
        "INSERT INTO TEACHER "
        "(T_ID, T_NAME, T_LASTNAME, FK_D_NAME, FK_ZIP, T_NOTES, T_IS_ACTIVE) "
        f"VALUES ({test_id}, 'Test', 'Teacher', '{dept_name}', NULL, 'Test', 1)"
    )
    insert_assignment_sql = (
        "INSERT INTO OFFERING_ASSIGNMENT "
        "(OA_ID, FK_O_ID, FK_T_ID, OA_ASSIGNED_HOURS, OA_ACTUAL_HOURS, OA_ROLE) "
        f"VALUES ({test_id}, {test_id}, {test_id}, 2.0, 0.0, NULL)"
    )
    update_sql = (
        "UPDATE OFFERING_ASSIGNMENT "
        "SET OA_ACTUAL_HOURS = 7.0 "
        f"WHERE OA_ID = {test_id}"
    )
    try:
        affected, _rows = run_statement(conn, report_items, "Insert department", insert_department_sql, case=case)
        assert affected >= 1
        affected, _rows = run_statement(conn, report_items, "Insert study program", insert_study_sql, case=case)
        assert affected >= 1
        affected, _rows = run_statement(conn, report_items, "Insert subject", insert_subject_sql, case=case)
        assert affected >= 1
        affected, _rows = run_statement(conn, report_items, "Insert semester planning", insert_semester_sql, case=case)
        assert affected >= 1
        affected, _rows = run_statement(conn, report_items, "Insert offering", insert_offering_sql, case=case)
        assert affected >= 1
        affected, _rows = run_statement(conn, report_items, "Insert teacher", insert_teacher_sql, case=case)
        assert affected >= 1
        affected, _rows = run_statement(conn, report_items, "Insert offering assignment", insert_assignment_sql, case=case)
        assert affected >= 1
        affected, _rows = run_statement(conn, report_items, "Update offering assignment actual hours", update_sql, case=case)
        assert affected >= 1
        select_sql = f"SELECT OA_ID, OA_ACTUAL_HOURS FROM OFFERING_ASSIGNMENT WHERE OA_ID = {test_id}"
        run_statement(conn, report_items, "Verify updated actual hours", select_sql, case=case)
    finally:
        cleanup_sqls = [
            ("Cleanup offering assignment", f"DELETE FROM OFFERING_ASSIGNMENT WHERE OA_ID = {test_id}"),
            ("Cleanup offering", f"DELETE FROM OFFERING WHERE O_ID = {test_id}"),
            ("Cleanup teacher", f"DELETE FROM TEACHER WHERE T_ID = {test_id}"),
            ("Cleanup subject", f"DELETE FROM SUBJECT WHERE S_ID = {test_id}"),
            ("Cleanup study program", f"DELETE FROM STUDY_PROGRAM WHERE ST_NAME = '{study_name}'"),
            ("Cleanup department", f"DELETE FROM DEPARTMENT WHERE D_NAME = '{dept_name}'"),
            ("Cleanup semester planning", f"DELETE FROM SEMESTER_PLANNING WHERE SP_ID = {test_id}"),
        ]
        for title, sql in cleanup_sqls:
            try:
                run_statement(conn, report_items, title, sql, fetch_limit=0, report=False, case=case)
            except Exception:
                pass


def test_teacher_actual_workload_for_semester(conn, report_items):
    case = "Teacher actual workload for WS1415"
    sql = (
        "SELECT "
        "T_ID, "
        "T_NAME, "
        "T_LASTNAME, "
        "ACTUAL_HOURS, "
        "SP_TERM "
        "FROM TEACHER_ACTUAL_WORKLOAD "
        "WHERE T_LASTNAME = 'Nonnast' AND SP_TERM = 'WS1415'"
    )
    run_statement(conn, report_items, "Compute teacher actual workload", sql, case=case)
