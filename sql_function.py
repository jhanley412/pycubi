'''******************************************************************************

    sql Function Module
    Date Last Updated: 02/25/2015
    Created by: Justin Hanley
    Notes: Beginning of dynamic SQL Builder.
    Change Log: Please see end of file.

*****************************************************************************'''

import re
import os
import datetime
import sys
from cubi import cubi_sql

class Function_Builder(object):

    '''***********************************************************

        Function Builder Class
        Description:  Manipulate SQL table valued function
        Date Last Updated:  02/25/2016
        Notes:

    ***********************************************************'''

    def __init__(self, sql_file_path, sql_file_name):
        self.sql_file_path = sql_file_path
        self.sql_file_name = sql_file_name
        self.file = os.path.join(str(self.sql_file_path), str(self.sql_file_name))
        self.parameters_dictionary = {'File Path':self.sql_file_path, 'File Name':self.sql_file_name, 'File': self.file }

        '''
        #Logging initialization
        easylogging.setupLogging(logoutput_path = logdirectory)
        self.versatilelogger = logging.getLogger("versatile")
        self.errorlogger = logging.getLogger("error")
        self.consolelogger = logging.getLogger("consoleinfo")
        
        
        try:
            #Check file existence
            file = None
            file.open
            file.close()
            
        #Catch and log any errors that cause program to fail
        except Exception as ex:
            self.errorlogger.exception(ex)
            self.versatilelogger.error(ex)
            self.consolelogger.error('Failed connection to server {0}'.format(self.server))
            raise

        '''
        
    #Working Idea. This may neeed to be placed in new module for item manipulations
    def replace_chars(self, item
                      ,replace_chars_dictionary):
        '''Replace string characters within a string objects, Takes dictionary {current character: new charater}'''
        
        alter_item = item
        for strip_chars in replace_chars_dictionary:
            if strip_chars in alter_item:
                alter_item = alter_item.replace(strip_chars, replace_chars_dictionary[strip_chars])

        return alter_item


    def query_variable_search(self
                              ,cubi_formatted ='N'
                              ,lens_search = 'N'):
        '''Search through CUBI formatted file for all user defined variables. Additional feature to find lenses of standardized table value function'''

        try:
            read_file_string = open(self.file).read()

            if cubi_formatted.upper() == 'Y':
                sql_output_head = re.split(r'--:BEGINHEAD--|--:ENDHEAD--', read_file_string)[1]
                sql_output_main_query = re.split(r'--:BEGINMAINQUERY--|--:ENDMAINQUERY--', read_file_string)[1]
                sql_left_join_list = sql_output_main_query.split('LEFT OUTER JOIN')     #Extracts the left join sets in main query of sql


                
            strip_items_dictionary = {'(':'', ')':'', ',':''}

            ##Capture all variables that need to be declared in sql function
            sql_output_head_list = sql_output_head.split('\n')
            variables_dictionary = {}
            for line in sql_output_head_list:
                word_list = line.split()
                for list_index, word in enumerate(word_list):
                    if '@' in word:
                        word_alter = word
                        for strip_item in strip_items_dictionary:
                            if strip_item in word_alter:
                                word_alter = word_alter.replace(strip_item, strip_items_dictionary[strip_item])
                            else:
                                continue
                        if cubi_formatted.upper() == 'Y':
                            variables_dictionary[word_alter] = word_list[list_index +1]
                        else:
                            variables_dictionary[word_alter] = 'Needs Cubi Formatting'
            '''
            variable_match_count = 0
            for line in read_file_list:
                word_list = line.split()
                while variable_match_count < len(variables_dictionary):
                    for list_index, word in enumerate(word_list):
                        if '@' in word:
                            word_alter = word
                            for strip_item in strip_items_dictionary:
                                if strip_item in word_alter:
                                    word_alter = word_alter.replace(strip_item, strip_items_dictionary[strip_item])
                                else:
                                    continue
                                
                            variable_match_count += 1
                            variables_dictionary[word_alter] = word_list[list_index +1]
            '''                
                

            if cubi_formatted.upper() == 'Y' and lens_search == 'Y':
                # Build list of available SQL Lens parameters 
                lens_parameters_list = []       ##Use this to display available parameters
                for left_join_group in sql_left_join_list:
                    words_list = re.split(r' |\n*', left_join_group)
                    for word in words_list:
                        if "'%:" in word:
                            clean_word = re.sub(r"\)|\%|\'|\(", '', word)
                            lens_parameters_list.append(clean_word.title())
                variables_dictionary['Lenses'] = lens_parameters_list
                
            return variables_dictionary

        except Exception as ex:
            raise

        finally:
            pass


    

    def query_function(self
                       ,server = 'ACUREPORTS\ACUREPORTS'
                       ,database = 'EfficiencyDev'
                       ,lens = 'None'
                       ,output = 'NULL'
                       ,output_format = 'Dictionary'
                       ,is_temp = True
                       ,output_sql_file_path = None):
        '''Doing some fancy string manipulations to Return standardized data for or from SQL.'''

        try:
            #Read SQL file and output as string for use
            read_file_string = open(self.file).read()


            ##Break out sql file into appropriate parts for synthesis
            sql_output_head = re.split(r'--:BEGINHEAD--|--:ENDHEAD--', read_file_string)[1]
            sql_output_body = re.split(r'--:BEGINBODY--|--:ENDBODY--', read_file_string)[1]
            sql_output_main_query = re.split(r'--:BEGINMAINQUERY--|--:ENDMAINQUERY--', read_file_string)[1]
            sql_output_test = re.split(r'--:BEGINTESTS--|--:ENDTESTS--', read_file_string)[1]
            sql_output_footer = re.split(r'--:BEGINFOOTER--|--:ENDFOOTER--', read_file_string)[1]
            sql_left_join_list = sql_output_main_query.split('LEFT OUTER JOIN')     #Extracts the left join sets in main query of sql


            variables_dictionary = self.query_variable_search(cubi_formatted ='Y')
            lens_parameters_list = self.query_variable_search(cubi_formatted ='Y', lens_search = 'Y')['Lenses'] #Updated query_variable_function to inlcude lens list, this would allow us to consolidate the variables dict in future
                            
            '''
            ##Capture all variables that need to be declared in sql function
            read_file_list = read_file_string.split('\n')
            variables_dictionary = {}
            for line in read_file_list:
                word_list = line.split()
                for word in word_list:
                    if '@' in word:
                        word_alter = word
                        for strip_item in strip_items_dictionary:
                            if strip_item in word_alter:
                                word_alter = word_alter.replace(strip_item, strip_items_dictionary[strip_item])
                            else:
                                continue
                        variables_dictionary[word_alter] = None
            '''





            
            '''
            Build output sql query for relevant lens paramters to optimize final query
            execution. Read through eac set of LEFT OUTER JOINS and build requested
            select statement accoring to string format parameter
            '''

            ''' Incorporated this logic into the query_variable_search method above. Delete this commented section when battle tested
            # Build list of available SQL Lens parameters 
            lens_parameters_list = []       ##Use this to display available parameters
            for left_join_group in sql_left_join_list:
                words_list = re.split(r' |\n*', left_join_group)
                for word in words_list:
                    if "'%:" in word:
                        clean_word = re.sub(r"\)|\%|\'|\(", '', word)
                        lens_parameters_list.append(clean_word.title())
            print(lens_parameters_list)
            '''
            #Lens check. Fail if input lens is not in query
            lens_check_list = lens.split(':')
            for lens_item in lens_check_list:
                if len(lens_item) > 0 and ':'+lens_item.title() not in lens_parameters_list:
                    sys.exit("Invalid lens '{0}' . Exiting program".format(':'+lens_item.title()))
            
            lens_list = [lens_set.split('/') for lens_set in lens.split(':')]     #Split input lens parameter and compare appropriate combos to sql parameter
            optimized_sql_output_main_query = sql_left_join_list[0]     #Variable to build the main query outout that occurs after CTE declarations. Variable initialized with the SELECT statement and base table
            for parameter_item in lens_list:
                lens_string_build = ''          #Variable to build all available lens calls from pythons input lens parameter. Acts similar to SQL 'LIKE' statement 
                comment_sql_join_temp_list = []
                for word_item in parameter_item:
                    lens_string_build += word_item + '/'
                    lens_item_comparision = '%:{0}%'.format(lens_string_build) 
                    ##Begin to build main query output by commenting unwanted LEFT JOINS according to python lens parameter
                    for left_join_item in sql_left_join_list[1:]:
                        add_sql_join_string = 'LEFT OUTER JOIN' + left_join_item
                        if lens_item_comparision.title() in left_join_item.title():
                            optimized_sql_output_main_query += add_sql_join_string
                        ''' Consider how to add a commented section to output
                        else:
                            comment_sql_join_temp_list.append(add_sql_join_string)
                for comment_item in comment_sql_join_temp_list:
                    if comment_item not in optimized_sql_output_main_query:
                        optimized_sql_output_main_query += '/* \n{0}\n*/\n'.format(comment_item)
                '''
                        



            '''
            Setup query header
            '''
            # Query name. Build unique query name to identify item being used
            dttime = datetime.datetime.now().timetuple()
            time_name = ''
            for n, t in enumerate(dttime[0:7]):
                time_name += str(dttime[n])     ##Build datetime from tuple YYYYMMDDHMMSS
            query_name = re.sub(r'\.sql|\.txt', '', self.sql_file_name) + '_' + self.replace_chars(lens, {'/':'_', ':':'_'}) + '_' + time_name
            # Insert TEMP prefix that will enable a deletion process for all temp functions
            if is_temp == True:
                query_name = 'TEMP_'+query_name
            if output.upper() == 'GETFUNCTION':
                query_name = 'ufn_'+ query_name


            # Set up full query shell that will run through specified output workflow
            query_header = ''
            query_footer = ''

            # Create specified query outputs by dynamically building query header, and footer
            if output.upper() == 'GETRESULTSET':
                for parameter in variables_dictionary:
                    parameter_value = 'NULL'
                    if parameter == '@Lens':
                        parameter_value = "'{0}'".format(lens)
                    query_header += '\tDECLARE {0} {1}\n\tSET {0} = {2}\n'.format(parameter
                                                                              ,variables_dictionary[parameter]
                                                                              ,parameter_value)
                query_header += ';'
                
            if output.upper() == 'GETFUNCTION':
                function_variables_string = ''
                for function_number, parameter in enumerate(variables_dictionary):
                    function_comma = ','
                    parameter_value = 'NULL'
                    if function_number == 0:
                        function_comma =''
                    if parameter == '@Lens':
                        parameter_value = "'{0}'".format(lens)
                    function_variables_string += '\t{0} {1} {2} = {3}\n'.format(function_comma
                                                                         ,parameter
                                                                         ,variables_dictionary[parameter]
                                                                         ,parameter_value)
                query_header = 'CREATE FUNCTION dbo.{0} ( \n\n{1} \n) \n\nRETURNS TABLE \nAS \nRETURN (\n'.format(query_name, function_variables_string)

                query_footer = ')'
                



            # Compose output query for user use
            full_query =  '''
                    \n--:BEGINHEAD--\n{0}\n--:ENDHEAD--\n
                    \n--:BEGINBODY--\n{1}\n--:ENDBODY--\n
                    \n--:BEGINMAINQUERY--\n{2}\n--:ENDMAINQUERY--\n
                    \n--:BEGINFOOTER--\n{3}\n--:ENDFOOTER--\n
            '''.format( query_header
                        ,sql_output_body
                        ,optimized_sql_output_main_query
                        ,query_footer)



            '''
            sql-compiler
            Execute full_query according to output parameter and build output options
            '''
            #Create output sql file
            if output_sql_file_path != None:
                file_object = open(output_sql_file_path + query_name +'.sql', 'w')
                file_object.write(full_query)
                file_object.close() 

            sql_connect = cubi_sql.SQL(server, database)
            if output.upper() == 'GETRESULTSET':
                # GetContents of query
                if output_format.upper() == 'DICTIONARY':
                    return(sql_connect.queryToDictionaryAdmin(full_query, 'GetContents'))
                elif output_format.upper() == 'DATAFRAME':
                    return(sql_connect.queryToDataframe(full_query))                
                else:
                    print("Don't recognize 'output_format'. Please enter 'Dictionary' or 'Dataframe'")
                
            elif output.upper() == 'GETFUNCTION':
                # CommitQuery
                sql_connect.queryToDictionaryAdmin(full_query, 'CommitQuery')
                
                # Build output parameter list to be used in function call for ease of use. Currently mapping just the lens portion, all others are default
                function_select_parameters_list = []
                for function_number, parameter in enumerate(variables_dictionary):
                    if parameter == '@Lens':
                        function_select_parameters_list.append(lens)
                    else:
                        function_select_parameters_list.append('Default')

                #Build Select statement to call Table valued Function in SQL. Extra logic to properly format output to avoid SQL errors
                function_select_parameters_item = ''
                if len(function_select_parameters_list) == 1:
                    function_select_parameters_item = "('{0}')".format(function_select_parameters_list[0])
                else:
                    function_select_parameters_tuple = tuple(function_select_parameters_list)
                    
                select_function_query = '''
                        \nSELECT \n\t* \nFROM \n\t{0}.dbo.{1}{2}
                '''.format(database, query_name, function_select_parameters_item)
                return(select_function_query)

            else:
                print("Don't recognize 'output'. Please enter 'GetResultSet' or 'GetFunction'")

               


            

        
        except Exception as ex:
            raise

        finally:
            pass



    if __name__ == '__main__':
        #print(query_variable_search(r'C:\Python34\VirtualEnvs\CUBITest\TheFunction','ACUFunction.sql', 'Y'))
        #query_function(input_sql_file_path = r'C:\Python34\VEnvs\MSSQL\pymssql\Projects\TheFunction'
        #               ,input_sql_file_name = 'ACUFunction.sql'
        #               ,lens = ':Members/Names/'
        #               ,output = 'GetResultset'
        #               ,is_temp = True
        #               ,output_sql_file_path = r'C:\Python34\VEnvs\MSSQL\pymssql\Projects\TheFunction\\')
        pass



'''******************************************************************************
    Change log:

    02/25/2015 - Initial File created by Justin Hanley
    
*****************************************************************************'''

