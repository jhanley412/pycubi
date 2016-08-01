'''******************************************************************************

    sql Module
    Date Last Updated: 08/25/2015
    Created by: Justin Hanley
    Notes:
    Change Log: Please see end of file.

*****************************************************************************'''

import easylogging

from pycubi.settingsconfig import *

import pymssql
import datetime
import logging
import sys
from pandas.io import sql
import numpy
import matplotlib




class SQL(object):
    '''***********************************************************

        SQL Class
        Description:  Extract and write relevant information from SQL schema
        Date Last Updated:  08/25/2015
        Notes:

    ***********************************************************'''

    def __init__(self, server, database = None, table = None):
        self.server = server
        self.database = database
        self.table = table
        self.parameters_dictionary = {'Server':self.server, 'Database':self.database, 'TableName':self.table}
        
        #Logging initialization
        easylogging.setupLogging(logoutput_path = logdirectory)
        self.versatilelogger = logging.getLogger("versatile")
        self.errorlogger = logging.getLogger("error")
        self.consolelogger = logging.getLogger("consoleinfo")
        
        try:
            #Create connection to database
            connection = pymssql.connect(server= self.server)
            connection.close()
            
        #Catch and log any errors that cause program to fail
        except Exception as ex:
            self.errorlogger.exception(ex)
            self.versatilelogger.error(ex)
            self.consolelogger.error('Failed connection to server {0}'.format(self.server))
            raise


    def __str__(self):
        return 'You can access Server: {0}, Table: {1}.dbo.{2}'.format(self.server,self.database,self.table)


    def queryToDataframe(self, query):
        #Set up logging to output and track all uncaught errors
        vlogger = self.versatilelogger
        elogger = self.errorlogger
        clogger = self.consolelogger

        #Begin main logic of function
        try:
            #Logging
            parameters_dictionary = {'Server':self.server, 'Database':self.database, 'TableName':self.table, 'Query':query}
            #Create connection to database
            connection = pymssql.connect(server=self.server)
            QueryResults = sql.read_sql(query, con = connection)
            #print('Table Size\nRows: {0}, Columns: {1}'.format(QueryResults.shape[0],QueryResults.shape[1]))
            return QueryResults
                          
        #Catch and log any errors that cause program to fail
        except Exception as ex:
            #elogger.exception(ex)
            #vlogger.error(ex)
            #clogger.error('Failed connection to {0}'.format(self.parameters_dictionary)
            raise
            

    def tableLookup(self, output):

       #Set up logging to output and track all uncaught errors
        vlogger = self.versatilelogger
        elogger = self.errorlogger
        clogger = self.consolelogger

        #Begin main logic of function
        try:
            #Logging
            parameters_dictionary = {'Server':self.server, 'Database':self.database, 'TableName':self.table, 'Output':output}
            vlogger.info('Method Start with parameters {0}'.format(parameters_dictionary))
            
            '''Extract schema information from a table. Define output (GETSCHEMA,GETKEYS',GETSQLHEADERS,GETSQLDATATYPES)'''

            #outputs list used to map functions available outputs. To map new items, enter item in list and add logic in the if...else... statemment below
            table_list = ['GETSQLHEADER', 'GETSQLDATATYPES', 'GETINSERTHEADER']
            admin_list = ['GETSCHEMA', 'GETFOREIGNKEYS']
            output_list = table_list+admin_list
            #Connect to server
            connection = pymssql.connect(server = self.server, database = self.database)
            cursor = connection.cursor()
         
            ##Define table information and query to execute. Lookup query extracts table and column charactersitics to reference to on input
            Query = "SELECT * FROM information_schema.columns isc JOIN sys.columns sc ON sc.object_id = OBJECT_ID('{0}') AND sc.name = isc.COLUMN_NAME WHERE isc.table_name = '{0}' ORDER BY isc.ORDINAL_POSITION".format(self.table)
            if output.upper() == 'GETFOREIGNKEYS':
                Query = '''
                    SELECT  
                        KCU1.CONSTRAINT_CATALOG AS 'FK_CONSTRAINT_DATABASE'
                        ,KCU1.CONSTRAINT_Schema AS 'FK_CONSTARINT_SCHEMA'
                        ,KCU1.TABLE_NAME AS 'FK_CONSTRAINT_TABLE_NAME' 
                        ,KCU1.CONSTRAINT_NAME AS 'FK_CONSTRAINT_NAME' 
                        ,KCU1.COLUMN_NAME AS 'FK_CONSTRAINT_COLUMN_NAME' 
                        ,KCU1.ORDINAL_POSITION AS 'FK_CONSTRAINT_ORDINAL_POSITION'
                        ,KCU1.CONSTRAINT_CATALOG AS 'REFERENCED_CONSTRAINT_DATABASE'
                        ,KCU1.CONSTRAINT_Schema AS 'REFERENCED_CONSTARINT_SCHEMA'
                        ,KCU2.TABLE_NAME AS 'REFERENCED_TABLE_NAME'  
                        ,KCU2.CONSTRAINT_NAME AS 'REFERENCED_CONSTRAINT_NAME' 
                        ,KCU2.COLUMN_NAME AS 'REFERENCED_COLUMN_NAME'
                        ,KCU2.ORDINAL_POSITION AS 'REFERENCED_ORDINAL_POSITION'

                    FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC 

                    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU1 
                        ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG  
                        AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA 
                        AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME 

                    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU2 
                        ON KCU2.CONSTRAINT_CATALOG = RC.UNIQUE_CONSTRAINT_CATALOG  
                        AND KCU2.CONSTRAINT_SCHEMA = RC.UNIQUE_CONSTRAINT_SCHEMA 
                        AND KCU2.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME 
                        AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION
                    WHERE
                            KCU1.TABLE_NAME = '{0}'
                '''.format(self.table)

            cursor.execute(Query)
            
            ##Initialize colum header list for dictionary key relation. Exporting info from SQL as a list to maintain index relation
            description = cursor.description
            header = []
            read_ColHead_list = []
         
            #Export column header info into header list for output readability
            for listing in description:
                    header.append(listing[0])
                
            #Open cursor object and append values into nested dictionary. Index value on row number. Nested Dictionary format { Row# : {ColumnHeader : Value}}
            results_dict = {}
            n=0
            for row in cursor:
                    row_dict = {}
                    results_dict[n] = row_dict
                    for col_number in range(len(header)):
                            row_dict[header[col_number]]= row[col_number]
                    n+=1
         
            

            if output.upper() in admin_list:
                return results_dict
                
            elif output.upper() in table_list:
                #Lookup to SQL data source for table column names and datatypes. This will add aditional outputs to function
                SQLHeader_List = []
                InsertHeader_List = []
                SQLHeaderDataType_Dictionary = {}
                for header in results_dict:
                    SQLHeader_List.append(results_dict[header]['COLUMN_NAME'].title())
                    SQLHeaderDataType_Dictionary[results_dict[header]['COLUMN_NAME'].title()] = results_dict[header]['DATA_TYPE']
                    if results_dict[header]['is_identity'] == 0:
                        InsertHeader_List.append(results_dict[header]['COLUMN_NAME'].title())

                if output.upper() == 'GETSQLHEADER':
                    return SQLHeader_List
                elif output.upper() == 'GETINSERTHEADER':
                    return InsertHeader_List
                elif output.upper() == 'GETSQLDATATYPES':
                    return SQLHeaderDataType_Dictionary
                

            else:
                vlogger.info("Exiting program. Didn't recognize 'output' parameter {0}. Need to enter {1}".format(output,output_list))
                sys.exit()

            
                
        #Catch and log any errors that cause program to fail
        except Exception as ex:
            elogger.exception(ex)
            vlogger.error(ex)
            raise

        finally:
            connection.close()
            vlogger.info('Method Complete')





    def queryToDictionary(self, output):
        ''' Extract the header or ALL contents of a SQL table. Output Parameter(GetContents, GetHeader, GetInsertHeader)'''

        #Set up logging to output and track all uncaught errors
        vlogger = self.versatilelogger
        elogger = self.errorlogger
        clogger = self.consolelogger

        #Begin main logic of function
        try:
            #Logging
            parameters_dictionary = {'Server':self.server, 'Database':self.database, 'TableName':self.table, 'Output':output}
            vlogger.info('Method Start with parameters {0}'.format(parameters_dictionary))

            output_list = ['GetContents', 'GetHeader', 'GetInsertHeader']
            #Initialize variables for use
            ReturnObject = output
            connection = pymssql.connect(server = self.server, database = self.database)

            ##Run SELECT * query to extract a table's contents and header
            if ReturnObject.upper() == 'GetContents'.upper() or ReturnObject.upper() == 'GetHeader'.upper():
                cursor = connection.cursor()
                Query = 'SELECT * FROM {0}'.format(self.table)
                cursor.execute(Query)
                description= cursor.description    #.description -> (columnname, type_code, display_size, internal_size, precision, scale, null_ok)

                List_Header = [column[0] for column in description]
                Dict_Contents = {}

                n=0
                for row in cursor:
                    Dict_Row = {}
                    Dict_Contents[n] = Dict_Row
                    for ColNum in range(len(List_Header)):
                        Dict_Row[List_Header[ColNum]] = row[ColNum]
                    n+=1

                if ReturnObject.upper() == 'GetContents'.upper():
                    return(Dict_Contents)
                elif ReturnObject.upper() == 'GetHeader'.upper():
                    return(List_Header)

            ##Run Schema query to extract column headers that can be used in insert query.
            elif ReturnObject.upper() == 'GetInsertHeader'.upper():
                cursor = connection.cursor(as_dict = True)
                TableLookup_Query = "SELECT * FROM information_schema.columns isc JOIN sys.columns sc ON sc.object_id = OBJECT_ID('{0}') AND sc.name = isc.COLUMN_NAME WHERE isc.table_name = '{0}' ORDER BY isc.ORDINAL_POSITION".format(table)
                cursor.execute(TableLookup_Query)

                List_InsertHeader = []
                
                for row in cursor:
                    if row['is_identity'] == 0:
                        List_InsertHeader.append(row['COLUMN_NAME'])
                return(List_InsertHeader)

            ##Function error handeling    
            else:
                vlogger.info("Exiting program. Didn't recognize 'output' parameter {0}. Need to enter {1}".format(output, output_list))
                connection.close()
                sys.exit()

            

        #Catch and log any errors that cause program to fail
        except Exception as ex:
            elogger.exception(ex)
            vlogger.error(ex)
            raise

        finally:
            connection.close()
            vlogger.info('Method Complete')
        

    def queryToDictionaryAdmin(self, query, output):
        '''Extract header or select contents of a table. ADMIN may pass through SQL query for specific selections.'''

        #Set up logging to output and track all uncaught errors
        vlogger = self.versatilelogger
        elogger = self.errorlogger
        clogger = self.consolelogger

        #Begin main logic of function
        try:
            #Logging
            parameters_dictionary = {'Server':self.server, 'Database':self.database, 'TableName':self.table, 'Output':output, 'Query':repr(query)}
            vlogger.info('Method Start with parameters {0}'.format(parameters_dictionary))

            #Initialize variables for use
            output_list = ['GetContents' 'GetHeader', 'GetInsertHeader', 'CommitQuery']
            ReturnObject = output
            Query = query
            connection = pymssql.connect(server = self.server, database = self.database)

            
            ##Run SELECT * query to extract a table's contents and header
            if ReturnObject.upper() == 'GetContents'.upper() or ReturnObject.upper() == 'GetHeader'.upper():
                cursor = connection.cursor()
                cursor.execute(Query)
                description= cursor.description    #.description -> (columnname, type_code, display_size, internal_size, precision, scale, null_ok)

                List_Header = [column[0] for column in description]
                Dict_Contents = {}

                n=0
                for row in cursor:
                    Dict_Row = {}
                    Dict_Contents[n] = Dict_Row
                    for ColNum in range(len(List_Header)):
                        Dict_Row[List_Header[ColNum]] = row[ColNum]
                    n+=1

                if ReturnObject.upper() == 'GetContents'.upper():
                    connection.close()
                    return Dict_Contents
                elif ReturnObject.upper() == 'GetHeader'.upper():
                    connection.close()
                    return(List_Header)

            ##Run Schema query to extract column headers that can be used in insert query. Removes identity columns.
            elif ReturnObject.upper() == 'GetInsertHeader'.upper():
                cursor = connection.cursor(as_dict = True)
                TableLookup_Query = "SELECT * FROM information_schema.columns isc JOIN sys.columns sc ON sc.object_id = OBJECT_ID('{0}') AND sc.name = isc.COLUMN_NAME WHERE isc.table_name = '{0}' ORDER BY isc.ORDINAL_POSITION".format(table)
                cursor.execute(TableLookup_Query)

                List_InsertHeader = []
                
                for row in cursor:
                    if row['is_identity'] == 0:
                        List_InsertHeader.append(row['COLUMN_NAME'])

                return(List_InsertHeader)

            ##Run and commit query
            elif  ReturnObject.upper() == 'CommitQuery'.upper():
                cursor = connection.cursor()
                cursor.execute(Query)
                connection.commit()
                vlogger.info('Committed query to SQL')

            ##Function error handeling    
            else:
                vlogger.info("Exiting program. Didn't recognize 'output' parameter {0}. Need to enter {1}".format(output, output_list))
                sys.exit()

                

        #Catch and log any errors that cause program to fail
        except Exception as ex:
            elogger.exception(ex)
            vlogger.error(ex)
            raise    

        finally:
            connection.close()
            vlogger.info('Method Complete')





    def createTableAdmin(self, header_list = [], sqldatatype_dictionary = {}):
        '''Write SQL table when provided header list and a corresponding data type dictionary '''

        #Set up logging to output and track all uncaught errors
        vlogger = self.versatilelogger
        elogger = self.errorlogger
        clogger = self.consolelogger

        #Begin main logic of function
        try:
            #Logging - This may be too long to log with list and dictionary
            parameters_dictionary = {'Server':self.server, 'Database':self.database, 'TableName':self.table, 'Headers':header_list, 'DataTypes':sqldatatype_dictionary}
            vlogger.info('Method Start with parameters {0}'.format(parameters_dictionary))


            #Initialize parameters for use
            Server = self.server
            Database = self.database
            TableName = self.table
            SQLDataType_Dictionary = sqldatatype_dictionary
            InputHeader_List = header_list
            

            #Create list of column names and datatypes to be entered into SQL create table.
            InputData_List = []
            for header in InputHeader_List:
                if header in SQLDataType_Dictionary:
                    InputData_List.append(' '.join([header,SQLDataType_Dictionary[header]]))
                else:
                    vlogger.info('{0} was not found in DataType list and will not be added to table'.format(header))
                    continue
            ##Define SQL connection
            connection = pymssql.connect(server = Server, database = Database)
            cursor = connection.cursor()

            #SQL Create table. Will utilize Database name, table name, and input list
            Query ='''CREATE TABLE {0}.dbo.{1} ({2})'''.format(Database, TableName, ', '.join(InputData_List))

            

            #Confirmation workflow. Table will not be created unless specified in prompt
            Confirmation = False
            while Confirmation == False:
                vlogger.info('Table creation query - {0}'.format(repr(Query)))
                confirm_prompt = (input('Please Verify SQL: Creating {0}.dbo.{1} table. Would you like to continue? (Y/N) '.format(Database, TableName))).upper()
                if confirm_prompt == 'Y':
                        cursor.execute(Query)
                        connection.commit() #Commits current transaction to DB. Must call this method to persist data. Must use this after every cursor.execute()
                        vlogger.info('Commited table creation')
                        Confirmation = True
                elif confirm_prompt == 'N':
                        vlogger.info('Cancelled table creation')
                        Confirmation = True
                ##Secret Option for ease of use. This will need to be patched when moved to production (SQL Injection)
                elif confirm_prompt == 'EDIT':
                    vlogger.info('Entered Creation Table edit mode')
                    Query = input("Enter 'Create Table' query")
                    cursor.execute(Query)
                    connection.commit()
                    vlogger.info('Altered Table creation query - {0}\nCommited table creation'.format(repr(Query)))
                    Confirmation = True
                    
                    
                else:
                    print('Hmmm...not sure what you mean. Please enter Y or N')
          

        #Catch and log any errors that cause program to fail
        except Exception as ex:
            elogger.exception(ex)
            vlogger.error(ex)
            raise

        finally:
            connection.close()
            vlogger.info('Method Complete')


    def alter_table(self, action):
        
        #Set up logging to output and track all uncaught errors
        vlogger = self.versatilelogger
        elogger = self.errorlogger
        clogger = self.consolelogger

        #Begin main logic of function
        try:
            #Logging
            parameters_dictionary = {'Server':self.server, 'Database':self.database, 'TableName':self.table, 'Action': action}
            vlogger.info('Method Start with parameters {0}'.format(parameters_dictionary))

            if action.upper() == 'DELETE':
                query = 'DELETE FROM {0}.dbo.{1}'.format(self.database, self.table)
                
            elif action.upper() == 'DROP':
                pass
            
            else:
                vlogger.info('Unknown table action')

            connection = pymssql.connect(server=self.server, database = self.database)
            cursor = connection.cursor()
            cursor.execute(query)
            connection.commit()
            
            vlogger.info('Execute {0}'.format(query))

        #Catch and log any errors that cause program to fail
        except Exception as ex:
            elogger.exception(ex)
            vlogger.error(ex)
            raise

        finally:
            connection.close()
            vlogger.info('Method Complete')

            
    def alter_column(self, altertype, columnname, datatype):
        ''' Add columns to existing SQL table'''

        #Set up logging to output and track all uncaught errors
        vlogger = self.versatilelogger
        elogger = self.errorlogger
        clogger = self.consolelogger

        #Begin main logic of function
        try:
            #Logging
            parameters_dictionary = {'Server':self.server, 'Database':self.database, 'TableName':self.table, 'ColumnName':columnname, 'DataType':datatype, 'AlterType': altertype}
            vlogger.info('Method Start with parameters {0}'.format(parameters_dictionary))

            #Initialize paramters for use    
            Server = self.server
            Database = self.database
            Table = self.table
            AlterType = altertype
            ColumnName = columnname
            DataType = datatype
            connection = pymssql.connect(server=Server, database = Database)
            cursor = connection.cursor()

             
            ##Define table information and query to execute. Lookup query extracts table and column charactersitics to reference to on input
            Query = None
            if AlterType.upper() == 'ADDCOLUMN':
                Query = 'ALTER TABLE {0} ADD {1} {2}'.format(Table,ColumnName.strip().title(),DataType)
            if AlterType.upper() == 'DELETECOLUMN':
                vlogger.info('This only to show logic. Not currently mapped to query')
            else:
                vlogger.info('Please define appropriate AlterType. (ColumnAdd)')

            cursor.execute(Query)
            connection.commit()
            vlogger.info('Commited table alteration with query - {0}'.format(repr(Query)))

        #Catch and log any errors that cause program to fail
        except Exception as ex:
            elogger.exception(ex)
            vlogger.error(ex)
            raise

        finally:
            connection.close()
            vlogger.info('Method Complete')

   
    def tableTransfer(self, transferdatabase, transfertable, transferheaders_list):
        '''Transfer table contents from transfer table to the instantiated table using INSERT INTO query. This will transfer ALL information from transfer table. '''

        #Set up logging to output and track all uncaught errors
        vlogger = self.versatilelogger
        elogger = self.errorlogger
        clogger = self.consolelogger

        #Begin main logic of function
        try:
            #Logging
            parameters_dictionary = {'Server':self.server, 'Database':self.database, 'TableName':self.table, 'TransferDatabase':transferdatabase, 'TransferTable': transfertable, 'TransferHeaders':transferheaders_list}
            vlogger.info('Method Start with parameters {0}'.format(parameters_dictionary))
            
            #Initialize parameters for use
            Server = self.server
            InsertDatabase = self.database
            InsertTable = self.table
            CopyDatabase = transferdatabase 
            CopyTable = transfertable
            Header_List = transferheaders_list

            ##Define SQL connection
            connection = pymssql.connect(server = Server, database = InsertDatabase)
            cursor = connection.cursor()
            
            InsertIntoQuery = 'INSERT INTO {0} ({1}) SELECT {1} FROM {2}.dbo.{3}'.format(InsertTable,', '.join(Header_List),CopyDatabase,CopyTable)
            vlogger.info(repr(InsertIntoQuery))
            Verified = False
            while Verified == False:
                Verification = input('Would you like to execute this insert statement? (y/n)').upper()
                if Verification == 'Y':
                    cursor.execute(InsertIntoQuery)
                    connection.commit()
                    vlogger.info('Committed table transfer query {0}. {1} row(s) inserted'.format(repr(InsertIntoQuery), cursor.rowcount))
                    Verified = True

                    VerifiedDelete = False
                    while VerifiedDelete == False:
                        Delete = input('Would you like to delete contents of copied table ({0}.dbo.{1})? (y/n)'.format(CopyDatabase,CopyTable)).upper()
                        if Delete == 'Y':
                            DeleteQuery = 'DELETE FROM {0}.dbo.{1}'.format(CopyDatabase,CopyTable)
                            cursor.execute(DeleteQuery)
                            connection.commit()
                            vlogger.info('Deleted {0}.dbo.{1} contents after transfer.'.format(CopyDatabase,CopyTable))
                            VerifiedDelete = True
                        elif Delete == 'N':
                            vlogger.info('Retained {0}.dbo.{1} after transfer'.format(CopyDatabase,CopyTable))
                            VerifiedDelete = True
                        else:
                            clogger.info('Invalid response. Please enter Y or N')
                                  
                elif Verification == 'N':
                    vlogger.info('Cancelled Table transfer with query {0}'.format(repr(InsertIntoQuery)))
                    Verified = True
                else:
                    clogger.info('Invalid response. Please enter Y or N')
                

        #Catch and log any errors that cause program to fail
        except Exception as ex:
            elogger.exception(ex)
            vlogger.error(ex)
            raise    

        finally:
            connection.close()
            vlogger.info('Method Complete')


'''******************************************************************************
    Change Log
    
        06/10/2015 - Initial file created by Justin Hanley
        06/22/2015 - Jhanley - tableLookup
                       - Add Foreign Key output
                       - Add GetInsertHeader output
        08/01/2015 - JHanley - Add logging logic using easylogging module
        08/25/2015 - JHanley - Add pandas query tool for easy SQL panda use
                       - Add connection test when initializing SQL class

*****************************************************************************'''
