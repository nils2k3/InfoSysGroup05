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
        return ['POSITION', 'SEMESTER_PLANNING']
    
    def extract(
        self,
        WorkLoad: pd.DataFrame,
        position: List[Dict[str, Any]],
        semester_planning: List[Dict[str, Any]],
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
        Additional:
            **kwargs: Additional parameters passed by the extraction system
        Returns:
            List of dictionaries representing POSITION_SEMESTER table records
        """
        if WorkLoad is None or WorkLoad.empty or not position or not semester_planning:
            logger.warning("Missing dependencies")
            return []
        
        def normalize(value: Any) -> str:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return ''
            text = str(value).strip().lower()
            text = text.replace('\ufffd', 'ss')
            text = text.replace('\u00df', 'ss')
            text = text.replace('\u00e4', 'ae').replace('\u00f6', 'oe').replace('\u00fc', 'ue')
            import unicodedata
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

        records_by_key = {}
        df = WorkLoad[['term', 'job title']].copy()
        for _, row in df.iterrows():
            pos_name = normalize(row.get('job title'))
            term = normalize(row.get('term'))

            pos_id = position_by_name.get(pos_name)
            sem_id = semester_lookup.get(term)
            if not pos_id or not sem_id:
                continue

            key = (pos_id, sem_id)
            if key not in records_by_key:
                records_by_key[key] = {
                    'FK_PO_ID': pos_id,
                    'FK_SP_ID': sem_id
                }

        records = []
        id_counter = 1
        for record in records_by_key.values():
            record['PS_ID'] = id_counter
            records.append(record)
            id_counter += 1
        
        logger.info(f"{self.__class__.__name__} extracted {len(records)} records")
        return records
