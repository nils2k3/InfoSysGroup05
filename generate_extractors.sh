#!/bin/bash
# Dieses Skript generiert alle Extractor-Dateien basierend auf dem ER-Datenmodell.

echo "--- Starte Extractor-Generierung ---"

# 1. Level 0/1 - Basisdaten (Unabhängig oder nur auf einfachen Lookups basierend)
python3 extractor_generator.py DEPARTMENT --csv OfferedCourses,WorkLoad
python3 extractor_generator.py STUDY_PROGRAM --csv OfferedCourses
python3 extractor_generator.py TEACHER --csv OfferedCourses,WorkLoad
python3 extractor_generator.py POSITION --csv WorkLoad
# NEU: PROFESSOR ist ein Subtyp von TEACHER
python3 extractor_generator.py PROFESSOR --csv OfferedCourses --deps TEACHER

# 2. Level 1/2 - Zeitlicher und struktureller Kontext
python3 extractor_generator.py SEMESTER_PLANNING --csv OfferedCourses,WorkLoad
python3 extractor_generator.py SUBJECT --csv OfferedCourses --deps STUDY_PROGRAM

# 3. Level 2/3 - Zuordnungen und komplexe Planungsdaten
python3 extractor_generator.py POSITION_PROFESSOR --csv WorkLoad --deps PROFESSOR,POSITION,SEMESTER_PLANNING
python3 extractor_generator.py DEPUTAT_ACCOUNT --deps TEACHER,SEMESTER_PLANNING
python3 extractor_generator.py OFFERING --csv OfferedCourses --deps SUBJECT,SEMESTER_PLANNING
python3 extractor_generator.py SERVICE_REQUEST --csv OfferedCourses --deps SUBJECT,SEMESTER_PLANNING,DEPARTMENT
python3 extractor_generator.py PROGRAMM_SUBJECT_REQUIREMENT --csv OfferedCourses --deps STUDY_PROGRAM,SUBJECT,SEMESTER_PLANNING

# 4. Level 3/4 - Detaillierte Zuweisungen und Ist-Daten
python3 extractor_generator.py OFFERING_ASSIGNMENT --csv OfferedCourses --deps OFFERING,TEACHER
python3 extractor_generator.py COURSE --csv OfferedCourses --deps OFFERING,TEACHER,SUBJECT

echo "--- Generierung abgeschlossen. Extractor-Dateien basierend auf dem ERD wurden erstellt. ---"
