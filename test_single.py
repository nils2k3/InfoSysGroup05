#!/usr/bin/env python3
"""
Quick test für einen einzelnen Extractor
"""

import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / 'extractors'))

# Beispiel: Teste SEMESTER_PLANNING
from semester_planning import SemesterPlanningExtractor

# Lade Daten
OfferedCourses = pd.read_csv('data/offeredCourses.csv', sep=';')
WorkLoad = pd.read_csv('data/workload.csv', sep=';')

# Teste Extractor
extractor = SemesterPlanningExtractor()
records = extractor.extract(OfferedCourses, WorkLoad)

print(f"Extracted {len(records)} records:")
for record in records[:5]:  # Zeige erste 5
    print(record)
