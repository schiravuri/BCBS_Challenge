import pandas as pd
import numpy as np
from os import path
import requests
import errno
import os

nan = np.nan
population_encoding = "ISO-8859-1"
class CovidDataProcessing():
    '''
        This class contains methods for processing Covid data and Population data.
    '''
    def open_csv(self, src_type, file_path, dtype_dict, encoding_input):
        '''
            This method is used to open a csv file and load as a dataframe
            
            Input:
                src_type: source of the file ('url' or 'file_dir')
                file_path: string for the file path of interest
                dtype_dict: dictionary of datatype for dataframe columns
                encoding_input: encoding type for the given dataframe
            
            Output:
                output_df: dataframe object for the csv file that was read

        '''

        # confirm url file path exists
        if src_type == 'url':   
            response = requests.head(file_path)
            if response.status_code != 200:
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), file_path)
        elif src_type == 'file_dir':
            if not os.path.exists(file_path):
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), file_path)
        else:
            raise NotImplementedError('Unknown src_type: {0}. Only valid source types are: url and file_dir'.format(src_type))
            
        # confirm file is csv
        rev_file_path = file_path[::-1]

        if rev_file_path.split(".")[0][::-1] != 'csv':
            raise TypeError('Input given is not csv')        
        
        
        
        # check to make sure columns in dtype_dict exist in csv
        self.cols_in_df('path', file_path, encoding_input, dtype_dict)                
        
        output_df = pd.read_csv(file_path, dtype=dtype_dict, encoding=encoding_input)
        
        return output_df
        
    def data_overlap(self, covid_df, population_df, join_type, join_col, query):
        '''
            This method is used to compare the covid_df to population_df to confirm 
            that there are no fips code found in covid_df not in population_df and 
            vice versa
            
            Input:
                covid_df: dataframe object with the covid data
                population_df: dataframe object with the population data
                join_type: type of desired join: inner, outer, left, or right
                join_col: column to be used to join the two dataframes
                query: string to determine if there is a data issue
            
            Output:
                result: dataframe after join
               
        '''
    
        # check to make sure join type passed is a valid join type
        if join_type.upper() not in ['LEFT', 'RIGHT', 'INNER', 'OUTER']:
            raise NotImplementedError('Expected join types are: left, right, inner, outer. Received {0}'.format(join_type))
            
        self.cols_in_df('df', covid_df, None, {join_col: ''})
        self.cols_in_df('df', population_df, population_encoding, {join_col: ''})
        return pd.merge(covid_df, population_df, on='fips', how=join_type).query(query)
                
    
    def data_quality_check(self, df, column_to_check, condition):
        '''
            This method is used to do a data quality check on the data based on a 
            condition passed.
            
            Input:
                df: dataframe object of interest to perform the data quality check on
                column_to_check: column of interest to check for the data quality check
                condition: string form of the condition that will be checked: For example,
                'age > 18' can be passed
                
            Output:
                None
        '''
        self.cols_in_df('df', df, None, {column_to_check: True})
        query_string = ''.join([column_to_check, condition])
        invalid_value_df = df.query(query_string)
        
        if len(invalid_value_df)>0:
            raise ValueError('Invalid value found in {0} dataframe'.format(df))
            
    
    def cols_in_df(self, input_type, input, encoding_input, dtype_dict):
        '''
            This method is used to check if columns of interest are present in the 
            dataframe passed.
            
            Input:
                input_type: type of input given (dataframe or file_path)
                input: dataframe or file_path input
                encoding_input: encoding_input to encode the contents of the df properly
                dtype_dict: dictionary of the datatypes of the dataframe
           
            Output:
                None
        '''
        if input_type == 'path':
            df = pd.read_csv(input, encoding=encoding_input)
        
            cols = df.columns
            
        elif input_type == 'df':
            cols = input.columns
            
        else:
            raise NotImplementedError('Expected input_types are df or path. Got {0}'.format(join_type))
             
            
        for key in dtype_dict:
            if key not in cols:
                raise KeyError('{0} in dtype_dict is not found in csv columns'.format(key))