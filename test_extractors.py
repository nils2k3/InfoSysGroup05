#!/usr/bin/env python3
"""
Test Script für alle Data Extractors

Testet alle Extractors und zeigt die extrahierten Datensätze an.
"""

import pandas as pd
import sys
import logging
import csv
from pathlib import Path

# Add extractors directory to path
sys.path.insert(0, str(Path(__file__).parent / 'extractors'))

# Import all extractors
from department import DepartmentExtractor
from position import PositionExtractor
from teacher import TeacherExtractor
from subject import SubjectExtractor
from study_program import StudyProgramExtractor
from professor import ProfessorExtractor
from lecturer import LecturerExtractor
from semester_planning import SemesterPlanningExtractor
from offering import OfferingExtractor
from offering_assignment import OfferingAssignmentExtractor
from deputat_account import DeputatAccountExtractor
from service_request import ServiceRequestExtractor
from position_semester import PositionSemesterExtractor
from position_assignment import PositionAssignmentExtractor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_csv_data():
    """Load CSV files"""
    logger.info("Loading CSV files...")
    
    # Read with more flexible parameters to handle malformed CSV
    try:
        OfferedCourses = pd.read_csv('data/offeredCourses.csv', sep=';', encoding='utf-8',
                                      on_bad_lines='skip', engine='python',
                                      quoting=csv.QUOTE_NONE)
    except Exception as e:
        logger.warning(f"First attempt failed, trying with latin-1 encoding: {e}")
        OfferedCourses = pd.read_csv('data/offeredCourses.csv', sep=';', encoding='latin-1',
                                      on_bad_lines='skip', engine='python',
                                      quoting=csv.QUOTE_NONE)
    
    try:
        WorkLoad = pd.read_csv('data/workload.csv', sep=';', encoding='utf-8',
                              on_bad_lines='skip', engine='python')
    except Exception as e:
        logger.warning(f"First attempt failed, trying with latin-1 encoding: {e}")
        WorkLoad = pd.read_csv('data/workload.csv', sep=';', encoding='latin-1',
                              on_bad_lines='skip', engine='python')
    
    logger.info(f"OfferedCourses: {len(OfferedCourses)} rows")
    logger.info(f"WorkLoad: {len(WorkLoad)} rows")
    
    return OfferedCourses, WorkLoad


def test_extractor(extractor, name, *args, **kwargs):
    """Test a single extractor and return results"""
    logger.info(f"\n{'='*60}")
    logger.info(f"Testing: {name}")
    logger.info(f"{'='*60}")
    
    try:
        records = extractor.extract(*args, **kwargs)
        logger.info(f"✓ {name} extracted {len(records)} records")
        if (name == 'TEACHER' or name == 'PROFESSOR') and len(records) > 0:
            # Show count of professors vs total teachers
            prof_count = sum(1 for r in records if r.get('T_ISPROFESSOR', False))
            logger.info(f"   - Professors: {prof_count} / {len(records)}")
        
        if records:
            logger.info(f"Sample record: {records[0]}")
        
        return records
    except Exception as e:
        logger.error(f"✗ {name} failed: {e}")
        import traceback
        traceback.print_exc()
        return []


def main():
    """Run all extractor tests"""
    logger.info("Starting Extractor Tests")
    logger.info("="*60)
    
    # Load data
    OfferedCourses, WorkLoad = load_csv_data()
    
    # Store results for dependencies
    results = {}
    
    # Test extractors in dependency order
    
    # 1. No dependencies
    logger.info("\n\n### PHASE 1: Base Tables (No Dependencies) ###\n")
    
    dept_ext = DepartmentExtractor()
    results['DEPARTMENT'] = test_extractor(dept_ext, "DEPARTMENT", OfferedCourses, WorkLoad)
    
    pos_ext = PositionExtractor()
    results['POSITION'] = test_extractor(pos_ext, "POSITION", WorkLoad)
    
    sp_ext = SemesterPlanningExtractor()
    results['SEMESTER_PLANNING'] = test_extractor(sp_ext, "SEMESTER_PLANNING", 
                                                   OfferedCourses, WorkLoad)
    
    study_ext = StudyProgramExtractor()
    results['STUDY_PROGRAM'] = test_extractor(study_ext, "STUDY_PROGRAM", OfferedCourses)
    
    # 2. First level dependencies
    logger.info("\n\n### PHASE 2: First Level Dependencies ###\n")
    
    teacher_ext = TeacherExtractor()
    results['TEACHER'] = test_extractor(teacher_ext, "TEACHER", OfferedCourses, WorkLoad,
                                       department=results['DEPARTMENT'])
    
    subj_ext = SubjectExtractor()
    results['SUBJECT'] = test_extractor(subj_ext, "SUBJECT", OfferedCourses,
                                       study_program=results['STUDY_PROGRAM'])
    
    # 3. Second level dependencies
    logger.info("\n\n### PHASE 3: Second Level Dependencies ###\n")
    
    prof_ext = ProfessorExtractor()
    results['PROFESSOR'] = test_extractor(prof_ext, "PROFESSOR", OfferedCourses,
                                         teacher=results['TEACHER'])
    
    lec_ext = LecturerExtractor()
    results['LECTURER'] = test_extractor(lec_ext, "LECTURER", OfferedCourses,
                                        teacher=results['TEACHER'],
                                        professor=results['PROFESSOR'])
    
    offer_ext = OfferingExtractor()
    results['OFFERING'] = test_extractor(offer_ext, "OFFERING", OfferedCourses,
                                        subject=results['SUBJECT'],
                                        semester_planning=results['SEMESTER_PLANNING'])
    
    # 4. Third level dependencies
    logger.info("\n\n### PHASE 4: Third Level Dependencies ###\n")
    
    oa_ext = OfferingAssignmentExtractor()
    results['OFFERING_ASSIGNMENT'] = test_extractor(oa_ext, "OFFERING_ASSIGNMENT",
                                                    OfferedCourses,
                                                    offering=results['OFFERING'],
                                                    teacher=results['TEACHER'],
                                                    subject=results['SUBJECT'],
                                                    semester_planning=results['SEMESTER_PLANNING'])
    
    da_ext = DeputatAccountExtractor()
    results['DEPUTAT_ACCOUNT'] = test_extractor(da_ext, "DEPUTAT_ACCOUNT",
                                                teacher=results['TEACHER'],
                                                semester_planning=results['SEMESTER_PLANNING'])
    
    sr_ext = ServiceRequestExtractor()
    results['SERVICE_REQUEST'] = test_extractor(sr_ext, "SERVICE_REQUEST",
                                                OfferedCourses,
                                                subject=results['SUBJECT'],
                                                semester_planning=results['SEMESTER_PLANNING'],
                                                department=results['DEPARTMENT'])
    
    ps_ext = PositionSemesterExtractor()
    results['POSITION_SEMESTER'] = test_extractor(ps_ext, "POSITION_SEMESTER",
                                                  WorkLoad,
                                                  position=results['POSITION'],
                                                  semester_planning=results['SEMESTER_PLANNING'])

    pa_ext = PositionAssignmentExtractor()
    results['POSITION_ASSIGNMENT'] = test_extractor(pa_ext, "POSITION_ASSIGNMENT",
                                                    WorkLoad,
                                                    position_semester=results['POSITION_SEMESTER'],
                                                    position=results['POSITION'],
                                                    semester_planning=results['SEMESTER_PLANNING'],
                                                    professor=results['PROFESSOR'],
                                                    teacher=results['TEACHER'])
    
    
    # Summary
    logger.info("\n\n" + "="*60)
    logger.info("SUMMARY")
    logger.info("="*60)
    
    total_records = 0
    for table, records in results.items():
        count = len(records) if records else 0
        total_records += count
        status = "✓" if count > 0 else "✗"
        logger.info(f"{status} {table:30s}: {count:5d} records")
    
    logger.info(f"\nTotal records extracted: {total_records}")
    logger.info("\nTest completed!")


if __name__ == "__main__":
    main()
