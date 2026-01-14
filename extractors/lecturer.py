import pandas as pd
import logging
from typing import Dict, List, Any
from base_extractor import DataExtractor

logger = logging.getLogger(__name__)

class LecturerExtractor(DataExtractor):
    """Extract lecturers (teachers where isprof = 'FALSCH') with supervisor lookup"""
    
    @property
    def table_name(self) -> str:
        return "LECTURER"
    
    @property
    def dependencies(self) -> List[str]:
        return ["TEACHER", "PROFESSOR"]  # Need teachers and professors data for supervisor lookup
    
    def extract(
        self,
        OfferedCourses: pd.DataFrame,
        teacher: List[Dict[str, Any]],
        professor: List[Dict[str, Any]],
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Extract lecturers and resolve supervisor foreign keys.
        Same logic as original getLecturers function with name-based supervisor lookup.
        """
        if not teacher or not professor:
            logger.warning("Missing dependencies")
            return []

        def normalize_name(value: Any) -> str:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return None
            text = str(value).strip().lower()
            if not text:
                return None
            text = text.replace('\ufffd', '')
            text = (
                text.replace('ß', 'ss')
                .replace('ä', 'ae')
                .replace('ö', 'oe')
                .replace('ü', 'ue')
            )
            return " ".join(text.split())

        professor_ids = set()
        for prof in professor:
            if prof.get('P_ID') is not None:
                professor_ids.add(prof['P_ID'])
            elif prof.get('T_ID') is not None:
                professor_ids.add(prof['T_ID'])

        professor_name_map = {}
        professor_records = []
        for teacher_record in teacher:
            teacher_id = teacher_record.get('T_ID')
            if teacher_id not in professor_ids:
                continue
            first_name = normalize_name(teacher_record.get('T_NAME'))
            last_name = normalize_name(teacher_record.get('T_LASTNAME'))
            professor_records.append({
                'id': teacher_id,
                'first': first_name,
                'last': last_name,
            })
            for key in {first_name, last_name}:
                if not key:
                    continue
                if key in professor_name_map and professor_name_map[key] != teacher_id:
                    logger.warning(
                        f"Ambiguous supervisor name '{key}' maps to multiple professors "
                        f"({professor_name_map[key]} vs {teacher_id})"
                    )
                    continue
                professor_name_map[key] = teacher_id

        # Filter for lecturers only (non-professors)
        lecturersDF = OfferedCourses[
            OfferedCourses['isprof'] == 'FALSCH'
        ][['lecNo', 'supervisor']].drop_duplicates()
        
        lecturers = []
        for index, row in lecturersDF.iterrows():
            if pd.isna(row['lecNo']) or row['lecNo'] == 0:
                continue
            
            # Find supervisor using name matching (same as original logic)
            supervisor = None
            if not pd.isna(row['supervisor']) and isinstance(row['supervisor'], str):
                supervisor_name = normalize_name(row['supervisor'])
                supervisor = professor_name_map.get(supervisor_name)
                if supervisor is None and supervisor_name:
                    matches = []
                    for record in professor_records:
                        if record['first'] and supervisor_name in record['first']:
                            matches.append(record['id'])
                        elif record['last'] and supervisor_name in record['last']:
                            matches.append(record['id'])
                    if matches:
                        supervisor = matches[0]
                        if len(matches) > 1:
                            logger.warning(
                                f"Supervisor '{row['supervisor']}' matches multiple professors {matches}; "
                                f"using {supervisor}"
                            )
                    else:
                        logger.warning(f"No professor match for supervisor '{row['supervisor']}'")
            
            lecturer = {
                'T_ID': int(row['lecNo']),  # References TEACHER.T_ID
                'L_STREET_ADDRESS': None,  # Default null as in original
                'L_SUPERVISOR': supervisor # Foreign key to PROFESSOR.T_ID
            }
            lecturers.append(lecturer)
        
        # Sort by ID (same as original)
        lecturers = sorted(lecturers, key=lambda x: x['T_ID'])
        
        return lecturers
