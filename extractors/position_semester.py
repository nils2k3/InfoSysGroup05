import pandas as pd
import logging
from typing import Dict, List, Any
from base_extractor import DataExtractor

logger = logging.getLogger(__name__)

class PositionSemesterExtractor(DataExtractor):

    @property
    def table_name(self) -> str:
        """Return the database table name this extractor targets"""
        return "POSITION_SEMESTER"
    
    @property
    def dependencies(self) -> List[str]:
        """Return list of table names this extractor depends on"""
        return ['POSITION', 'SEMESTER_PLANNING', 'PROFESSOR', 'TEACHER']
    
    def extract(
        self,
        WorkLoad: pd.DataFrame,
        position: List[Dict[str, Any]],
        semester_planning: List[Dict[str, Any]],
        professor: List[Dict[str, Any]],
        teacher: List[Dict[str, Any]],
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Extract data for POSITION_SEMESTER table.
        
        Args:
        CSV Data:
            WorkLoad: DataFrame loaded from workload.csv
        Dependencies:
            position: List of POSITION table records from dependency resolution
            semester_planning: List of SEMESTER_PLANNING table records from dependency resolution
            professor: List of PROFESSOR table records from dependency resolution
            teacher: List of TEACHER table records from dependency resolution
        Additional:
            **kwargs: Additional parameters passed by the extraction system
        Returns:
            List of dictionaries representing POSITION_SEMESTER table records
        """
        if WorkLoad is None or WorkLoad.empty or not position or not semester_planning or not professor or not teacher:
            logger.warning("Missing dependencies")
            return []
        
        def normalize(value: Any) -> str:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return ''
            return str(value).strip().lower()

        position_by_name = {
            normalize(p.get('PO_NAME')): p.get('PO_ID')
            for p in position
            if p.get('PO_NAME') and p.get('PO_ID') is not None
        }
        semester_lookup = {
            normalize(s.get('SP_TERM')): s.get('SP_ID')
            for s in semester_planning
            if s.get('SP_TERM') and s.get('SP_ID') is not None
        }

        professor_ids = set()
        for prof in professor:
            for key in ('P_ID', 'T_ID', 'PR_ID'):
                if key in prof and prof[key] is not None:
                    professor_ids.add(prof[key])
                    break

        teacher_by_lastname = {}
        for t in teacher:
            teacher_id = t.get('T_ID')
            if teacher_id not in professor_ids:
                continue
            last_name = normalize(t.get('T_LASTNAME'))
            if not last_name:
                continue
            if last_name in teacher_by_lastname and teacher_by_lastname[last_name] != teacher_id:
                logger.warning("Duplicate professor last name '%s'; keeping first match", last_name)
                continue
            teacher_by_lastname[last_name] = teacher_id

        records = []
        seen = set()
        id_counter = 1
        df = WorkLoad[['term', 'name', 'job title', 'reduction']].copy()
        for _, row in df.iterrows():
            pos_name = normalize(row.get('job title'))
            term = normalize(row.get('term'))
            prof_lastname = normalize(row.get('name'))

            pos_id = position_by_name.get(pos_name)
            sem_id = semester_lookup.get(term)

            if not pos_id or not sem_id:
                continue

            prof_id = None
            if prof_lastname:
                prof_id = teacher_by_lastname.get(prof_lastname)
                if prof_id is None:
                    logger.warning("No professor match for '%s' (%s, %s)", prof_lastname, pos_name, term)
                    continue

            if (pos_id, sem_id) in seen:
                logger.warning("Duplicate position for semester (%s, %s); skipping", pos_name, term)
                continue
            seen.add((pos_id, sem_id))

            reduction = pd.to_numeric(row.get('reduction'), errors='coerce')
            if pd.isna(reduction):
                reduction = 0.0

            record = {
                'PS_ID': id_counter,
                'FK_PO_ID': pos_id,
                'FK_SP_ID': sem_id,
                'FK_P_ID': prof_id,
                'PS_REDUCTION_HOURS': float(reduction)
            }
            records.append(record)
            id_counter += 1
        
        logger.info(f"{self.__class__.__name__} extracted {len(records)} records")
        return records
