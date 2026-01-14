#!/bin/bash
# Complete DB2 Database Setup Script

echo "================================================"
echo "DB2 Planning Tool Database Setup"
echo "================================================"

echo "Step 1: Creating database and tables..."
db2 -tvf create_db2_database.sql

if [ $? -ne 0 ]; then
    echo "Error creating database! Check your DB2 installation and permissions."
    exit 1
fi

echo ""
echo "Step 2: Populating database with data..."
db2 +c -tvf db2_inserts.sql

if [ $? -ne 0 ]; then
    echo "Warning: Some data insertion statements may have failed."
    echo "Check the output above for specific errors."
fi

echo ""
echo "Step 3: Verifying database content..."
db2 "CONNECT TO PLANTOOL"
db2 "SET CURRENT SCHEMA = PLANNING_TOOL"

echo ""
echo "Table row counts:"
echo "=================="
db2 "SELECT 'DEPARTMENT' as TABLE_NAME, COUNT(*) as ROW_COUNT FROM DEPARTMENT
UNION ALL
SELECT 'SEMESTER_PLANNING', COUNT(*) FROM SEMESTER_PLANNING
UNION ALL
SELECT 'STUDY_PROGRAM', COUNT(*) FROM STUDY_PROGRAM
UNION ALL
SELECT 'TEACHER', COUNT(*) FROM TEACHER
UNION ALL
SELECT 'SUBJECT', COUNT(*) FROM SUBJECT
UNION ALL
SELECT 'PROFESSOR', COUNT(*) FROM PROFESSOR
UNION ALL
SELECT 'LECTURER', COUNT(*) FROM LECTURER
UNION ALL
SELECT 'OFFERING', COUNT(*) FROM OFFERING
UNION ALL
SELECT 'OFFERING_ASSIGNMENT', COUNT(*) FROM OFFERING_ASSIGNMENT
UNION ALL
SELECT 'DEPUTAT_ACCOUNT', COUNT(*) FROM DEPUTAT_ACCOUNT
UNION ALL
SELECT 'SERVICE_REQUEST', COUNT(*) FROM SERVICE_REQUEST
UNION ALL
SELECT 'POSITION', COUNT(*) FROM POSITION
UNION ALL
SELECT 'POSITION_ASSIGNMENT', COUNT(*) FROM POSITION_ASSIGNMENT
UNION ALL
SELECT 'POSITION_SEMESTER', COUNT(*) FROM POSITION_SEMESTER
UNION ALL
SELECT 'POSTAL_CODE', COUNT(*) FROM POSTAL_CODE
ORDER BY TABLE_NAME"

echo ""
echo "================================================"
echo "Database setup complete!"
echo "================================================"
echo ""
echo "To connect to your database:"
echo "  db2 connect to PLANNINGTOOL"
echo "  db2 \"set current schema = PLANNING_TOOL\""
echo ""
echo "================================================"
