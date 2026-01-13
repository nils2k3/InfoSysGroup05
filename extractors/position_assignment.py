import pandas as pd
import logging
import unicodedata
from typing import Dict, List, Any
from base_extractor import DataExtractor

logger = logging.getLogger(__name__)


class PositionAssignmentExtractor(DataExtractor):
    """Extract assignments of professors to positions per semester"""

    @property
    def table_name(self) -> str:
        return "POSITION_ASSIGNMENT"

    @property
    def dependencies(self) -> List[str]:
        return ["POSITION_SEMESTER", "POSITION", "SEMESTER_PLANNING", "PROFESSOR", "TEACHER"]

    def extract(
        self,
        WorkLoad: pd.DataFrame,
        position_semester: List[Dict[str, Any]],
        position: List[Dict[str, Any]],
        semester_planning: List[Dict[str, Any]],
        professor: List[Dict[str, Any]],
        teacher: List[Dict[str, Any]],
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Extract data for POSITION_ASSIGNMENT table.

        Args:
        CSV Data:
            WorkLoad: DataFrame loaded from workload.csv
        Dependencies:
            position_semester: List of POSITION_SEMESTER table records
            position: List of POSITION table records
            semester_planning: List of SEMESTER_PLANNING table records
            professor: List of PROFESSOR table records
            teacher: List of TEACHER table records
        """
        if WorkLoad is None or WorkLoad.empty or not position_semester or not position or not semester_planning:
            logger.warning("Missing dependencies")
            return []
        if not professor or not teacher:
            logger.warning("Professor or teacher data missing; assignments will be empty")
            return []

        def normalize(value: Any) -> str:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return ''
            text = str(value).strip().lower()
            text = text.replace('\ufffd', 'ss')
            text = text.replace('\u00df', 'ss')
            text = text.replace('\u00e4', 'ae').replace('\u00f6', 'oe').replace('\u00fc', 'ue')
            text = unicodedata.normalize('NFKD', text)
            return text.encode('ascii', 'ignore').decode('ascii')

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

        ps_lookup = {}
        for ps in position_semester:
            key = (ps.get('FK_PO_ID'), ps.get('FK_SP_ID'))
            if key[0] is None or key[1] is None:
                continue
            ps_lookup[key] = ps.get('PS_ID')

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

            ps_id = ps_lookup.get((pos_id, sem_id))
            if ps_id is None:
                continue

            prof_id = None
            if prof_lastname:
                prof_id = teacher_by_lastname.get(prof_lastname)
                if prof_id is None:
                    logger.warning("No professor match for '%s' (%s, %s); skipping assignment",
                                   prof_lastname, pos_name, term)
                    continue

            key = (ps_id, prof_id)
            if key in seen:
                logger.warning("Duplicate assignment for position semester (%s, %s); skipping", pos_name, term)
                continue
            seen.add(key)

            reduction = pd.to_numeric(row.get('reduction'), errors='coerce')
            if pd.isna(reduction):
                reduction = 0.0

            record = {
                'PA_ID': id_counter,
                'FK_PS_ID': ps_id,
                'FK_P_ID': prof_id,
                'PA_REDUCTION_HOURS': float(reduction)
            }
            records.append(record)
            id_counter += 1

        logger.info(f"{self.__class__.__name__} extracted {len(records)} records")
        return records
