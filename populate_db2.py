#!/usr/bin/env python3
"""
DB2 Database Population Script

Lädt Daten aus CSV-Dateien über Extractors und importiert sie in DB2.
"""

import sys
import logging
import csv
from pathlib import Path
from typing import List, Dict, Any

# Add extractors to path before importing extractor modules
sys.path.insert(0, str(Path(__file__).parent / 'extractors'))

from extractors.position_semester import PositionSemesterExtractor
from extractors.position_assignment import PositionAssignmentExtractor
from extractors.professor import ProfessorExtractor
from extractors.lecturer import LecturerExtractor
from extractors.department import DepartmentExtractor
from extractors.position import PositionExtractor
from extractors.semester_planning import SemesterPlanningExtractor
from extractors.teacher import TeacherExtractor
from extractors.deputat_account import DeputatAccountExtractor
from extractors.service_request import ServiceRequestExtractor
from extractors.subject import SubjectExtractor
from extractors.study_program import StudyProgramExtractor
from extractors.offering import OfferingExtractor
from extractors.offering_assignment import OfferingAssignmentExtractor

import pandas as pd


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_csv_data():
    """Load CSV files"""
    logger.info("Loading CSV files...")
    
    try:
        OfferedCourses = pd.read_csv('data/offeredCourses.csv', sep=';', encoding='utf-8',
                                      on_bad_lines='skip', engine='python',
                                      quoting=csv.QUOTE_NONE)
    except:
        OfferedCourses = pd.read_csv('data/offeredCourses.csv', sep=';', encoding='latin-1',
                                      on_bad_lines='skip', engine='python',
                                      quoting=csv.QUOTE_NONE)
    
    try:
        WorkLoad = pd.read_csv('data/workload.csv', sep=';', encoding='utf-8',
                              on_bad_lines='skip', engine='python')
    except:
        WorkLoad = pd.read_csv('data/workload.csv', sep=';', encoding='latin-1',
                              on_bad_lines='skip', engine='python')
    
    logger.info(f"OfferedCourses: {len(OfferedCourses)} rows")
    logger.info(f"WorkLoad: {len(WorkLoad)} rows")
    
    return OfferedCourses, WorkLoad


def format_value_for_db2(value, column_name: str = None) -> str:
    """Format a value for DB2 SQL INSERT"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 'NULL'
    
    # Boolean values - DB2 doesn't have native BOOLEAN, uses SMALLINT or CHAR(1)
    if isinstance(value, bool):
        return '1' if value else '0'
    
    # String values
    if isinstance(value, str):
        # Escape single quotes
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    
    # Numeric values
    if isinstance(value, (int, float)):
        return str(value)
    
    return f"'{str(value)}'"


def generate_insert_sql(table_name: str, records: List[Dict[str, Any]]) -> List[str]:
    """Generate INSERT statements for DB2"""
    if not records:
        return []
    
    sql_statements = []
    
    for record in records:
        columns = list(record.keys())
        values = [format_value_for_db2(record[col], col) for col in columns]
        
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
        sql_statements.append(sql)
    
    return sql_statements


def extract_all_data(OfferedCourses, WorkLoad):
    """Extract all data using extractors in dependency order"""
    results = {}
    all_sql = []
    
    logger.info("\n" + "="*70)
    logger.info("PHASE 1: Base Tables (No Dependencies)")
    logger.info("="*70)
    
    # DEPARTMENT
    dept_ext = DepartmentExtractor()
    results['DEPARTMENT'] = dept_ext.extract(OfferedCourses, WorkLoad)
    logger.info(f"✓ DEPARTMENT: {len(results['DEPARTMENT'])} records")
    
    # POSITION
    pos_ext = PositionExtractor()
    results['POSITION'] = pos_ext.extract(WorkLoad)
    logger.info(f"✓ POSITION: {len(results['POSITION'])} records")
    
    # SEMESTER_PLANNING
    sp_ext = SemesterPlanningExtractor()
    results['SEMESTER_PLANNING'] = sp_ext.extract(OfferedCourses, WorkLoad)
    logger.info(f"✓ SEMESTER_PLANNING: {len(results['SEMESTER_PLANNING'])} records")
    
    # STUDY_PROGRAM
    study_ext = StudyProgramExtractor()
    results['STUDY_PROGRAM'] = study_ext.extract(OfferedCourses)
    logger.info(f"✓ STUDY_PROGRAM: {len(results['STUDY_PROGRAM'])} records")
    
    logger.info("\n" + "="*70)
    logger.info("PHASE 2: First Level Dependencies")
    logger.info("="*70)
    
    # TEACHER
    teacher_ext = TeacherExtractor()
    results['TEACHER'] = teacher_ext.extract(OfferedCourses, WorkLoad,
                                             department=results['DEPARTMENT'])
    logger.info(f"✓ TEACHER: {len(results['TEACHER'])} records")
    
    # SUBJECT
    subj_ext = SubjectExtractor()
    results['SUBJECT'] = subj_ext.extract(OfferedCourses,
                                         study_program=results['STUDY_PROGRAM'])
    logger.info(f"✓ SUBJECT: {len(results['SUBJECT'])} records")
    
    logger.info("\n" + "="*70)
    logger.info("PHASE 3: Second Level Dependencies")
    logger.info("="*70)
    
    # PROFESSOR
    prof_ext = ProfessorExtractor()
    results['PROFESSOR'] = prof_ext.extract(OfferedCourses,
                                           teacher=results['TEACHER'])
    logger.info(f"✓ PROFESSOR: {len(results['PROFESSOR'])} records")
    
    # LECTURER
    lec_ext = LecturerExtractor()
    results['LECTURER'] = lec_ext.extract(OfferedCourses,
                                         teacher=results['TEACHER'],
                                         professor=results['PROFESSOR'])
    logger.info(f"✓ LECTURER: {len(results['LECTURER'])} records")
    
    # OFFERING
    offer_ext = OfferingExtractor()
    results['OFFERING'] = offer_ext.extract(OfferedCourses,
                                           subject=results['SUBJECT'],
                                           semester_planning=results['SEMESTER_PLANNING'])
    logger.info(f"✓ OFFERING: {len(results['OFFERING'])} records")
    
    logger.info("\n" + "="*70)
    logger.info("PHASE 4: Third Level Dependencies")
    logger.info("="*70)
    
    # OFFERING_ASSIGNMENT
    oa_ext = OfferingAssignmentExtractor()
    results['OFFERING_ASSIGNMENT'] = oa_ext.extract(OfferedCourses,
                                                   offering=results['OFFERING'],
                                                   teacher=results['TEACHER'],
                                                   subject=results['SUBJECT'],
                                                   semester_planning=results['SEMESTER_PLANNING'])
    logger.info(f"✓ OFFERING_ASSIGNMENT: {len(results['OFFERING_ASSIGNMENT'])} records")
    
    # DEPUTAT_ACCOUNT
    da_ext = DeputatAccountExtractor()
    results['DEPUTAT_ACCOUNT'] = da_ext.extract(teacher=results['TEACHER'],
                                               semester_planning=results['SEMESTER_PLANNING'])
    logger.info(f"✓ DEPUTAT_ACCOUNT: {len(results['DEPUTAT_ACCOUNT'])} records")
    
    # SERVICE_REQUEST
    sr_ext = ServiceRequestExtractor()
    results['SERVICE_REQUEST'] = sr_ext.extract(OfferedCourses,
                                               subject=results['SUBJECT'],
                                               semester_planning=results['SEMESTER_PLANNING'],
                                               department=results['DEPARTMENT'])
    logger.info(f"✓ SERVICE_REQUEST: {len(results['SERVICE_REQUEST'])} records")

    # POSITION_SEMESTER
    ps_ext = PositionSemesterExtractor()
    results['POSITION_SEMESTER'] = ps_ext.extract(WorkLoad,
                                                 position=results['POSITION'],
                                                 semester_planning=results['SEMESTER_PLANNING'])
    logger.info(f"✓ POSITION_SEMESTER: {len(results['POSITION_SEMESTER'])} records")

    # POSITION_ASSIGNMENT
    pa_ext = PositionAssignmentExtractor()
    results['POSITION_ASSIGNMENT'] = pa_ext.extract(WorkLoad,
                                                   position_semester=results['POSITION_SEMESTER'],
                                                   position=results['POSITION'],
                                                   semester_planning=results['SEMESTER_PLANNING'],
                                                   professor=results['PROFESSOR'],
                                                   teacher=results['TEACHER'])
    logger.info(f"✓ POSITION_ASSIGNMENT: {len(results['POSITION_ASSIGNMENT'])} records")

    # Generate SQL directly from extracted values (string IDs preserved)
    for table in [
        'DEPARTMENT', 'POSITION', 'SEMESTER_PLANNING', 'STUDY_PROGRAM',
        'TEACHER', 'SUBJECT', 'PROFESSOR', 'LECTURER',
        'OFFERING', 'OFFERING_ASSIGNMENT', 'POSITION_SEMESTER', 'POSITION_ASSIGNMENT',
        'DEPUTAT_ACCOUNT', 'SERVICE_REQUEST'
    ]:
        if table in results:
            sql = generate_insert_sql(table, results[table])
            all_sql.extend(sql)
            logger.info(f"✓ {table}: {len(results[table])} records, {len(sql)} SQL statements")

    return results, all_sql


def main():
    """Main execution"""
    logger.info("="*70)
    logger.info("DB2 Database Population Script")
    logger.info("="*70)
    
    # Load CSV data
    OfferedCourses, WorkLoad = load_csv_data()
    
    # Extract all data
    results, all_sql = extract_all_data(OfferedCourses, WorkLoad)
    
    # Write SQL file
    output_file = 'db2_inserts.sql'
    logger.info(f"\nWriting SQL to {output_file}...")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("-- DB2 Database Population Script\n")
        f.write("-- Generated automatically from CSV data\n")
        f.write(f"-- Total statements: {len(all_sql)}\n\n")
        
        f.write("-- Disable autocommit for better performance\n")
        f.write("-- Run this in DB2: db2 +c -f db2_inserts.sql\n\n")
        
        for sql in all_sql:
            f.write(sql + '\n')
        
        f.write("\nCOMMIT;\n")
    
    logger.info(f"✓ Generated {len(all_sql)} SQL statements")
    logger.info(f"✓ File saved: {output_file}")
    
    # Summary
    logger.info("\n" + "="*70)
    logger.info("SUMMARY")
    logger.info("="*70)
    total_records = sum(len(records) for records in results.values())
    for table, records in results.items():
        logger.info(f"{table:30s}: {len(records):5d} records")
    logger.info(f"\nTotal records: {total_records}")
    logger.info(f"Total SQL statements: {len(all_sql)}")
    
    logger.info("\n" + "="*70)
    logger.info("NEXT STEPS - Import to DB2")
    logger.info("="*70)
    logger.info("1. Transfer db2_inserts.sql to your DB2 VM")
    logger.info("2. Connect to DB2: db2 connect to <database> user <username>")
    logger.info("3. Run the script: db2 +c -tvf db2_inserts.sql")
    logger.info("   (The +c flag continues on error, -t uses ; as terminator)")
    logger.info("="*70)


if __name__ == "__main__":
    main()
