import pandas as pd

# Load data
df = pd.read_csv('data/offeredCourses.csv', sep=';', encoding='latin-1', 
                 on_bad_lines='skip', engine='python')

print(f'Gesamte Zeilen in CSV: {len(df)}')
print(f'\nVerschiedene Kurse (sbjNo): {df["sbjNo"].nunique()}')
print(f'Verschiedene Kurs-Namen (sbjName): {df["sbjName"].nunique()}')
print(f'Verschiedene Studiengänge (studyPrg): {df["studyPrg"].nunique()}')
print(f'Verschiedene Kombinationen (sbjNo + studyPrg): {df.groupby(["sbjNo", "studyPrg"]).ngroups}')

print(f'\n--- Top 10 häufigste Kurse ---')
print(df['sbjNo'].value_counts().head(10))

print(f'\n--- Beispiel Kurse ---')
print(df[['sbjNo', 'sbjName', 'studyPrg', 'term']].drop_duplicates('sbjNo').head(10))
