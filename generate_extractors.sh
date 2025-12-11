#!/bin/bash
# Dieses Skript generiert alle 15 Extractor-Dateien basierend auf dem UV-Datenmodell.

echo "--- Starte Extractor-Generierung (1/15) ---"

# 1. Basisdaten (Lehrer, Semester, Abteilungen etc.)
python3 extractor_generator.py TEACHER --csv OfferedCourses,WorkLoadRedution
python3 extractor_generator.py SEMESTER_PLANNING --csv OfferedCourses,WorkLoadRedution
python3 extractor_generator.py DEPARTMENT --csv OfferedCourses,WorkLoadRedution
python3 extractor_generator.py SUBJECT --csv OfferedCourses
python3 extractor_generator.py STUDY_PROGRAM --csv OfferedCourses
python3 extractor_generator.py ASSIGNMENT_ROLE --csv WorkLoadRedution
python3 extractor_generator.py SEMESTER_TYPE

# 2. Core-Planung und Positionen
python3 extractor_generator.py COURSE --csv OfferedCourses --deps SUBJECT
python3 extractor_generator.py POSITION_PROFESSOR --csv WorkLoadRedution --deps TEACHER,ASSIGNMENT_ROLE
python3 extractor_generator.py OFFERING --csv OfferedCourses --deps COURSE,SEMESTER_PLANNING
python3 extractor_generator.py SERVICE_REQUEST --csv OfferedCourses --deps DEPARTMENT,SEMESTER_PLANNING

# 3. Zuweisungen und Deputatskonten
python3 extractor_generator.py OFFERING_ASSIGNMENT --csv OfferedCourses --deps OFFERING,TEACHER
python3 extractor_generator.py DEPUTAT_ACCOUNT --deps TEACHER,SEMESTER_PLANNING

# 4. Deputats-Einträge (Finale)
python3 extractor_generator.py DEPUTAT_ENTRY_COURSE --csv OfferedCourses --deps DEPUTAT_ACCOUNT,OFFERING_ASSIGNMENT
python3 extractor_generator.py DEPUTAT_ENTRY_REDUCTION --csv WorkLoadRedution --deps DEPUTAT_ACCOUNT,POSITION_PROFESSOR

echo "--- Generierung abgeschlossen. Alle 15 Extractor-Dateien wurden erstellt. ---"
