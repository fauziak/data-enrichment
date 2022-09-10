import pandas as pd 
import dask.dataframe as dd

# Step 1 load first dataset into python 
df = pd.read_csv('data/NY_2015_ADI_9 Digit Zip Code_v3.1.csv')
print(df)
# Step 2 load secodn dat set into python 
df2 = pd.read_csv('data/Hospital_Inpatient_Discharges__SPARCS_De-Identified___2015.csv')
print(df2)
# Step 3 : Getting a DtypeWarning: Columns (10) have mixed types. Specify dtype option on import or set low_memory=False., looking into why
print(df2.columns[10]) 
#Step 4 converting column 10 into string because one row had '120 +' as a value which is not a int
df2 = pd.read_csv('data/Hospital_Inpatient_Discharges__SPARCS_De-Identified___2015.csv',dtype= {'Length of Stay':'string'})
print(df2)
# Step 5 printing out colum names from df to identify what to merge on 
print(df.columns)
# Step 6 printing out column names from df2 to identify what to merge on 
print(df2.columns)
# Step 7 stripping first 7 characters on column 'ZIPID' in df to match df2 'Zip Code - 3 digits'
df['ZIPID'] = df['ZIPID'].str[7:]
print(df)
# Step 8 merging df and df 'ZIPID' and 'Zip Code - 3 digits' (left) [FAILED BECAUSE OF MEMORY]

# df2 = df2[['Zip Code - 3 digits','Gender','Race','Ethnicity']]
# merged_df = pd.merge(df, df2, how="left", left_on=["ZIPID"], right_on=["Zip Code - 3 digits"])
# print(merged_df)]

# Step 9 trying dask instead of pandas
df = dd.read_csv('data/NY_2015_ADI_9 Digit Zip Code_v3.1.csv',dtype= {'ADI_NATRANK':'string', 'ADI_STATERNK':'string', 'FIPS.x': 'float', 'FIPS.y': 'float'})
df['ZIPID'] = df['ZIPID'].str[7:].astype('string')
print(df.head())

df2 = dd.read_csv('data/Hospital_Inpatient_Discharges__SPARCS_De-Identified___2015.csv',dtype= {'Length of Stay':'string', 'Zip Code - 3 digits': 'string'})
# Ignoring rows where zipcode is OOS
df2 = df2[df2['Zip Code - 3 digits'] != 'OOS']
df2 = df2[['Zip Code - 3 digits','Gender','Race','Ethnicity']]
print(df2.head())

# Step 10 merging df and df 'ZIPID' and 'Zip Code - 3 digits' (left) using dask
merged_df_left = dd.merge(df, df2, how='left', left_on=["ZIPID"], right_on=["Zip Code - 3 digits"])
print(merged_df_left.head())

# Step 11 merging df and df 'ZIPID' and 'Zip Code - 3 digits' (left) using dask
merged_df_right = dd.merge(df, df2, how='right', left_on=["ZIPID"], right_on=["Zip Code - 3 digits"])
print(merged_df_right.head())