import pandas as pd

# Load data
df = pd.read_csv('data/offeredCourses.csv', sep=';', encoding='latin-1', 
                 on_bad_lines='skip', engine='python')

print(f'Gesamte Zeilen in CSV: {len(df)}')
print(f'Zeilen mit lecNo != 0: {len(df[df["lecNo"] != 0])}')
print(f'Zeilen mit lecNo == 0 (Service G): {len(df[df["lecNo"] == 0])}')

print(f'\n--- Verteilung lecNo ---')
print(df['lecNo'].value_counts().head(10))

# Simuliere die Course Extractor Logik
filtered = df[df['lecNo'] != 0].copy()
print(f'\nNach Filter (lecNo != 0): {len(filtered)} Zeilen')

# Gruppiere nach relevanten Feldern
unique_courses = filtered[['sbjNo', 'lecNo', 'term']].drop_duplicates()
print(f'Einzigartige Kombinationen (sbjNo, lecNo, term): {len(unique_courses)}')

print(f'\n--- Beispiele ---')
print(filtered[['sbjNo', 'sbjName', 'lecNo', 'lecName', 'term', 'cntLec']].head(10))
