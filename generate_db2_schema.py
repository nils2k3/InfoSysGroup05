#!/usr/bin/env python3
"""
Generate DB2 DDL (CREATE SCHEMA/TABLES/FKs) from dbschema/schema.dbs (XML).
Outputs db2_schema.sql in the workspace root.
"""
import xml.etree.ElementTree as ET
from pathlib import Path

SCHEMA_XML = Path('dbschema/normalizedschema.dbs')
OUTPUT_SQL = Path('db2_schema.sql')


def map_type(col):
    t = col.get('type', 'VARCHAR')
    if t == 'INT':
        return 'INT'
    if t == 'VARCHAR':
        length = col.get('length')
        return f"VARCHAR({length or '100'})"
    if t == 'DECIMAL':
        length = col.get('length') or '10'
        scale = col.get('decimal') or '2'
        return f"DECIMAL({length},{scale})"
    if t == 'BOOLEAN':
        # DB2: use SMALLINT for booleans
        return 'SMALLINT'
    if t == 'DATE':
        return 'DATE'
    if t == 'ENUM':
        # Map ENUM to VARCHAR with reasonable length
        return 'VARCHAR(20)'
    if t == 'SUBENTITY':
        # Subentity markers do not map to a data column
        return None
    # Fallback
    return 'VARCHAR(100)'


def build_tables_and_constraints(root_schema):
    tables = []
    fks = []
    schema_name = root_schema.get('name', 'PLANNING_TOOL')

    for table in root_schema.findall('table'): 
        t_name = table.get('name')
        columns_sql = []
        pk_cols = []

        # Collect columns
        for col in table.findall('column'):
            col_name = col.get('name')
            col_type = map_type(col)
            if col_type is None:
                # Skip SUBENTITY pseudo-columns
                continue
            mandatory = col.get('mandatory') == 'y'
            col_def = f"{col_name} {col_type}{' NOT NULL' if mandatory else ''}"
            columns_sql.append(col_def)

        # Find PK index
        for idx in table.findall('index'):
            if idx.get('unique') == 'PRIMARY_KEY':
                pk_cols = [c.get('name') for c in idx.findall('column')]
                break

        # Record table
        tables.append({
            'schema': schema_name,
            'name': t_name,
            'columns_sql': columns_sql,
            'pk_cols': pk_cols,
        })

        # Collect FKs for later ALTER TABLE
        for fk in table.findall('fk'):
            fk_name = fk.get('name') or f"fk_{t_name.lower()}"
            ref_schema = fk.get('to_schema') or schema_name
            ref_table = fk.get('to_table')
            local_cols = []
            ref_cols = []
            for fkc in fk.findall('fk_column'):
                local_cols.append(fkc.get('name'))
                ref_cols.append(fkc.get('pk'))
            fks.append({
                'schema': schema_name,
                'table': t_name,
                'name': fk_name,
                'local_cols': local_cols,
                'ref_schema': ref_schema,
                'ref_table': ref_table,
                'ref_cols': ref_cols,
            })

    return tables, fks, schema_name


def generate_sql():
    if not SCHEMA_XML.exists():
        raise FileNotFoundError(f"Schema XML not found: {SCHEMA_XML}")

    tree = ET.parse(SCHEMA_XML)
    root = tree.getroot()
    schema = root.find('schema')
    if schema is None:
        raise ValueError('No <schema> element found')

    tables, fks, schema_name = build_tables_and_constraints(schema)

    lines = []
    lines.append(f"-- Generated DB2 DDL from {SCHEMA_XML}")
    lines.append(f"-- Schema: {schema_name}\n")
    lines.append(f"CREATE SCHEMA {schema_name};\n")

    # Create tables first
    for t in tables:
        lines.append(f"-- Table {t['name']}")
        lines.append(f"CREATE TABLE {schema_name}.{t['name']} (")
        # Columns
        for i, csql in enumerate(t['columns_sql']):
            sep = ',' if i < len(t['columns_sql']) - 1 else ''
            lines.append(f"  {csql}{sep}")
        # Primary key (add comma if there are any columns already)
        if t['pk_cols']:
            if t['columns_sql']:
                lines.append(f"  , PRIMARY KEY ({', '.join(t['pk_cols'])})")
            else:
                lines.append(f"  PRIMARY KEY ({', '.join(t['pk_cols'])})")
        lines.append(")")
        lines.append(";")
        lines.append("")

    # Add foreign keys via ALTER TABLE
    for fk in fks:
        lc = ', '.join(fk['local_cols'])
        rc = ', '.join(fk['ref_cols'])
        # Sanitize constraint names (DB2: no hyphens)
        cname = ''.join(ch if ch.isalnum() or ch == '_' else '_' for ch in fk['name'])
        lines.append(f"ALTER TABLE {schema_name}.{fk['table']} ADD CONSTRAINT {cname} FOREIGN KEY ({lc}) REFERENCES {fk['ref_schema']}.{fk['ref_table']} ({rc});")

    OUTPUT_SQL.write_text('\n'.join(lines), encoding='utf-8')
    return OUTPUT_SQL


if __name__ == '__main__':
    out = generate_sql()
    print(f"✓ Wrote {out}")
