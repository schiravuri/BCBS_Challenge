import pandas as pd
import numpy as np
import DataPreprocessing as DPre
import sqlite3
import time
import sys

nan = np.nan

# init data preprocessing class
covid_data_processing = DPre.CovidDataProcessing()

# data source paths
covid_link = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
population_link = 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/co-est2019-alldata.csv'
population_encoding = 'ISO-8859-1'
# create DFs of interest

print('---Beginning Preprocessing and Data Validation---')
covid_df = covid_data_processing.open_csv('url', covid_link, {'fips': 'str', 'cases': 'int', 'deaths': 'int'}, None)
covid_df['date'] = pd.to_datetime(covid_df['date'])
covid_df.query('fips != @nan', inplace=True)
print(len(covid_df))

population_df = covid_data_processing.open_csv('url', population_link, {'STATE': 'str', 'COUNTY': 'str', 'POPESTIMATE2019': 'int'}, population_encoding)

print('Read covid data')
print('Read population data')

# Create fips column from STATE and COUNTY columns in population df
population_df.loc[:,'fips'] = population_df['STATE'] + population_df['COUNTY']

# select columns of interest and create final df
population_cols_interest = ['STATE','CTYNAME','COUNTY','fips','POPESTIMATE2019']
final_population_df = population_df[population_cols_interest]
final_population_df.rename(columns={'POPESTIMATE2019':'population'}, inplace=True)

pop_fips_unique = covid_df.fips.unique()

pd.DataFrame(pop_fips_unique).to_csv('pop_fips_unique.csv')

# check overlap of county 

left_nan_df = covid_data_processing.data_overlap(covid_df, final_population_df, 'left','fips', 'STATE == @nan')

right_nan_df = covid_data_processing.data_overlap(final_population_df, covid_df, 'left','fips', 'STATE == @nan')

# final_nan_df = pd.concat([left_nan_df, right_nan_df])


if left_nan_df.empty and right_nan_df.empty:
    print('All fips codes are mutually inclusive')
else:
    if not left_nan_df.empty:
        print('Some FIPS codes in covid data vs population data')
        sys.exit(1)
    elif not right_nan_df.empty:
        print('Some FIPS codes in population data vs covid data')
        sys.exit(1)        

# check for invalid number for covid case data

covid_data_processing.data_quality_check(covid_df, 'cases', '<0')
covid_data_processing.data_quality_check(covid_df, 'deaths', '<0')
print('No unexpected values found in covid data')

# check for invalid number for population
covid_data_processing.data_quality_check(population_df, 'POPESTIMATE2019', '<0')
print('---Completed Preprocessing and Data Validation---')
print('---Beginning Processing Output Generation---')


print('Starting cumulative cases and cumulative deaths calculation')
print('This section takes about 3 mins')

conn = sqlite3.connect(':memory:')

covid_df.to_sql('covid_data', conn, index=False)

query = """
    SELECT
        A_frame.date,
        A_frame.fips, 
        A_frame.county,
        A_frame.state,
        A_frame.cases,
        A_frame.deaths,
        SUM(B_frame.cases) as cumulative_cases,
        SUM(B_frame.deaths) as cumulative_deaths
    FROM covid_data AS A_frame
    LEFT join covid_data as B_frame
        ON A_frame.date >= B_frame.date and A_frame.fips = B_frame.fips
    WHERE A_frame.fips != ''
    GROUP BY 1, 2, 3, 4, 5, 6
    ORDER BY 1, 2
    """
covid_df = pd.read_sql_query(query, conn)

# join dataframes together
joined_df = pd.merge(covid_df, final_population_df, on='fips', how='inner')

# select columns of interest and create final joined df
final_cols_interest = ['date','county', 'state', 'fips', 'cases', 'deaths','cumulative_cases','cumulative_deaths', 'population']
final_joined_df = joined_df[final_cols_interest]

print('Finished cumulative cases and cumulative deaths calculation')

final_joined_df.to_csv('final_output.csv')
print('---Completed Processing Output Generation---')