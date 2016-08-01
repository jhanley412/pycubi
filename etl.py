'''******************************************************************************

    ETL Module
    Date Last Updated: 08/04/2015
    Created by: Justin Hanley
    Notes: Dependencies on CUBI package
               cubi.filer
               cubi.sql
               cubi.datananlysis
               easeylogging.setupLogger
    Change Log: Please see end of file.

*****************************************************************************'''

import easylogging

from pycubi.settingsconfig import *
from pycubi.filer import Filer
from pycubi.cubi_sql import SQL
from pycubi.dataanalysis import DataAnalysis

import datetime
import pymssql
import fnmatch
import logging
import os
import traceback


class ETL(object):
    '''***********************************************************

        ETL Class
        Description:  Extract and import data from source into SQL.
        Date Last Updated:  09/10/2015
        Notes:

    ***********************************************************'''

    
    def __init__(self, wtserver, wtdatabase, wttable, filepath = None, filename = None, file_delimiter = '\t', data_header_list = [], data_contents_dictionary = {}):
        self.wtserver = wtserver
        self.wtdatabase = wtdatabase
        self.wttable = wttable
        self.filepath = filepath
        self.filename = filename
        self.file_delimiter = file_delimiter
        self.data_header_list = data_header_list
        self.data_contents_dictionary = data_contents_dictionary

        easylogging.setupLogging(logoutput_path = logdirectory) #__logs file will be located in Q:\Efficiency 
        self.versatilelogger = logging.getLogger("versatile")
        self.errorlogger = logging.getLogger("error")
        self.consolelogger = logging.getLogger("consoleinfo")

        # Standard output for multiple data input options. Create etl header and contents for sql upload from either a file or data dictionary {R:{C:V}}
        self.header_list = []
        self.contents_dictionary = {}
        self.original_contents_dictionary = {}# Used in excpetion report output. Unaltered, excepted items will write to csv file

        try:

            if len(self.data_header_list) != 0 and len(self.data_contents_dictionary) != 0:
                #***Consider update. Data dictionary input is not refined. Assuming user input will enter 'Refined' data according to SQL standards.
                self.header_list = self.data_header_list  #[header.strip().title() for header in self.data_header_list]   
                self.contents_dictionary = self.data_contents_dictionary #{str(header).strip().title():self.data_contents_dictionary[header] for header in self.data_contents_dictionary}
                self.original_contents_dictionary = self.data_contents_dictionary #{str(header).strip().title():self.data_contents_dictionary[header] for header in self.data_contents_dictionary}

            elif self.filepath != None and self.filename != None:
                file = Filer(self.filepath, self.filename)
                self.header_list = file.fileExtract(output = 'GetRefinedHeader', delimiter = file_delimiter)
                self.contents_dictionary = file.fileExtract(output = 'GetRefinedContents',delimiter = file_delimiter)
                self.original_contents_dictionary = file.fileExtract(output = 'GetRefinedContents',delimiter = file_delimiter)

            else:
                self.versatilelogger.info('No input data defined')
            
        except Exception as ex:
            self.errorlogger.exception(ex)
            self.versatilelogger.error(ex)
            raise


        
    def insertTable(self, tableexist, action = 'Insert', email = 'jhanley@arrowheadcu.org', additems_dictionary={}):
        '''Map and load data from external source into SQL table.
           Dependent on cubi.file, cubi.sql, cubi.sqlwriter, cubi.dataanalysis'''

        #Set up logging to output and track all uncaught errors
        vlogger = self.versatilelogger
        elogger = self.errorlogger
        clogger = self.consolelogger
        
        #Begin main logic of function
        try:
            
            script_start = datetime.datetime.now()
            
            server = self.wtserver
            database = self.wtdatabase
            table = self.wttable

            parameter_dictionary = {'Server':server, 'DataBase':database, 'Table':table, 'Filepath':self.filepath, 'Filename': self.filename, 'Delimiter': self.file_delimiter, 'data_header_list':self.data_header_list , 'data_contents_dictionary':self.data_contents_dictionary, 'TableExist': tableexist, 'Action':action, 'Delimiter': self.file_delimiter, 'Email': email, 'AddItems': additems_dictionary} 
            vlogger.info('Method Start with parameters {0}'.format(parameter_dictionary))

            ####
            # Dependencies
            ####

            Header = self.header_list
            Contents = self.contents_dictionary
            OriginalContents = self.original_contents_dictionary    
            refinedadditems_dictionary = {header.strip().title():additems_dictionary[header] for header in additems_dictionary}
            
            # Add headers from additems_dictionary to header
            for newheader in refinedadditems_dictionary:
                Header.append(newheader)
    
            # Add key:value pairs from additems_dictionary to contents
            for updaterow in Contents:
                Contents[updaterow].update(refinedadditems_dictionary)
            
            
            #DataAnalysis Class
            Data = DataAnalysis(Header, Contents)
            InputHeaderDataTypes_Dictionary = Data.dataType(output='GETSQLDATATYPES')

            #SQL action. Process table actions prior to insert.
            sql = SQL(server, database,table)
            
            #Prompt to ask if SQL table already exists before adding conents. Will create table if prompted to.
            ##This will need to change to look for table automatically
            ''' Changed this logic for automation
            TableExistPrompt = False
            while TableExistPrompt == False:
                TableExist = input('Does the table already exist? (Y/N) ').upper()
                if TableExist == 'Y':
                    TableExistPrompt = True
                    continue
                elif TableExist == 'N':
                    sql.createTableAdmin(Header, InputHeaderDataTypes_Dictionary)
                    TableExistPrompt = True
                else:
                    print("I don't understand...Please enter 'Y' or 'N'")
            '''
            
            if tableexist.upper() == 'N':
                sql.createTableAdmin(Header, InputHeaderDataTypes_Dictionary)
            elif tableexist.upper() == 'Y' and action.upper() == 'REPLACE':
                sql.alter_table('DELETE')
            elif tableexist.upper() == 'Y':
                pass
            else:
                vlogger.info('Did not properly define Table Exist parameter {0}'.format({'TableExist':tableexist}))
                #sys.exit() #Do we want to exit program or try to find match if table is defined?

                

            
            # SQL Class - This pulls information after table creation (if necessary) so that we can have most updated table info
            sql1_object = SQL(server, database, table)
            SQLHeader_List = sql1_object.tableLookup('GetInsertHeader')
            SQLHeaderDataType_dictionary = sql1_object.tableLookup('GetSQLDataTypes')




            #Initialize transformation list for user input mapping definitions. Define Update, Add, and Delete columns
            #Need to lookup datatype to ensure if will match sql
            Transform_List = []
            for HeaderItem in Header:

                ProperHeaderItem = HeaderItem.title()
                Temp_Transform_Dict = {}
             
                if ProperHeaderItem.strip() in SQLHeader_List:
                    Temp_Transform_Dict['Input'] = HeaderItem
                    vlogger.info('{0} column mapped. Continuing to next column'.format( ProperHeaderItem.strip() ) )

                # This logic will need to change for automation
                else:
                    #Typo error catch. Restart AddColumn prompt if input is not y or n. 
                    AddColumnAnswer = False
                    while AddColumnAnswer == False:
                        #print(CSVheader+': No match found')

                        ##Question 1: Input Column
                        AddColumn = input('{0} is not defined in {1}. Would you like to input this column? (Y/N) '.format(ProperHeaderItem, table)).upper()

                        #Map CSV column header names. Loop through mapping questions until questions are answered correctly
                   

                        if AddColumn == 'Y':
                            #Typo error catch. Restart HeaderExists prompt if input not y or n
                            ColumnExistAnswer = False
                            while ColumnExistAnswer == False:
                                    
                                ##Question 2: Header already in table
                                print(table + ' columns:',SQLHeader_List)
                                HeaderExists = input('Does {0} already exist in the SQL table? (Y/N) '.format(ProperHeaderItem)).upper()
                                
                                #UPDATE  Dictionary {'Update':{ColumnBefore:ColumnAfter}}
                                if HeaderExists == 'Y':
                                    HeaderMatchAnswer = False
                                    while HeaderMatchAnswer == False:
                                        MapHeader = input('Please enter SQL column name as you see it in defined SQL column list: ').title().strip()
                                        #Include if else to error if header is not in table
                                        MapDict = {}
                                        if MapHeader in SQLHeader_List:
                                            MapDict[HeaderItem] = MapHeader
                                            Temp_Transform_Dict['Update'] = MapDict
                                            print('Mapped {0} to {1}. Continuing to next column.'.format(ProperHeaderItem,MapHeader))
                                            HeaderMatchAnswer = True
                                        else:
                                            print(MapHeader.upper()+' does not match a SQL column name. Please reenter.')
                                    ColumnExistAnswer = True
                                #ADD   Dictionary {'Add':{ColumnAdd:DataType}}
                                elif HeaderExists == 'N':
                                    AlterTableAnswer = False
                                    while AlterTableAnswer == False:
                                        #print('Datatype listing:',DataTypeList)
                                        #print('Suggested DataType: {0} {1}'.format(HeaderItem, InputHeaderDataTypes_Dictionary[HeaderItem]))
                                        AddColumnType = 'VARCHAR(MAX)'
                                        if HeaderItem in InputHeaderDataTypes_Dictionary:
                                            AddColumnType = InputHeaderDataTypes_Dictionary[HeaderItem]
                                        else:  
                                            AddColumnType = input("Couldn't find a good data type for column. What data type would you like for new column {0}? ".format(ProperHeaderItem))
                                        VerifyAdd = input('Please verify. Adding {0} column with the datatype {1} into the {2} table. Would you like to continue table alter? (Y/N) '.format(ProperHeaderItem.upper(), AddColumnType.upper(),table.upper())).upper()
                                        if VerifyAdd == 'Y':
                                            HeaderType_dict = {}
                                            HeaderType_dict[ProperHeaderItem.strip()] = AddColumnType
                                            Temp_Transform_Dict['Add'] = HeaderType_dict
                                            sql.alter_column('AddColumn',ProperHeaderItem,AddColumnType)
                                            print('Adding',ProperHeaderItem,'. Continuing to next column')
                                            AlterTableAnswer = True
                                        elif VerifyAdd == 'N':
                                            VerifyCancel = input('Cancelling table alter and {0} contents add. Continue with cancellation? (Y/N) '.format(ProperHeaderItem)).upper()
                                            if VerifyCancel == 'Y':
                                                print('Cancelled table alter. Column and contents will not be added to table.')
                                                Temp_Transform_Dict['Delete'] = HeaderItem
                                                AlterTableAnswer = True
                                            else:
                                                AlterTableAnswer = False
                                        else:
                                            print('Invalid response. Please enter Y or N')
                                            AlterTableAnswer = False
                                    ColumnExistAnswer = True

                                else:
                                    print('Invalid response. Please enter Y or N')
                                    ColumnExistAnswer = False
                                    
                                AddColumnAnswer = True
                                
                        elif AddColumn == 'N':
                            print(ProperHeaderItem+' will not be mapped. Continuing to next column. ')
                            Temp_Transform_Dict['Delete'] = HeaderItem
                            AddColumnAnswer = True
                            

                        else:
                            print('Please enter Y or N')
                        

                Transform_List.append(Temp_Transform_Dict)
               

            '''Input Load Contents'''

            vlogger.info('Transform List: {0}'.format(Transform_List))




            #Using transform_List. Complete actions on input dictionary. Logic is scrubbing contents columnwise
            for action in Transform_List:
                
                for item in Contents:

                    if 'Input' in action:
                        ActionItem = action['Input'].title()
                        for head in Contents[item]:
                            if ActionItem == head.title():
                                Contents[item][ActionItem.strip()] = Contents[item].pop(head)
                                #buildTranContents_Dictionary[ActionItem.strip()] = Contents[item][head]    #Add file contents to new dictionary

                    elif 'Delete' in action and action['Delete'] in Contents[item]:
                        ActionItem = action['Delete']
                        del Contents[item][ActionItem]
                        #pass

                    elif 'Update' in action:
                        for k, v in action['Update'].items():
                            RemoveKeyName = k
                            UpdateKeyName = v
                        Contents[item][UpdateKeyName.strip()] = Contents[item].pop(RemoveKeyName)
                        #buildTranContents_Dictionary[UpdateKeyName] = Contents[item][RemoveKeyName]    #Add                        
                    
                    elif 'Add' in action:   #Add headers that don't exist in SQL table
                        ActionItem = None
                        for key in action['Add']:   #Need to loop through action nested dictionary {Add: {Header: DataType}}
                            ActionItem = key
                        for head in Contents[item]:
                            if ActionItem == head.title():
                                Contents[item][ActionItem.strip()] = Contents[item].pop(head)
                                #buildTranContents_Dictionary[ActionItem.strip()] = Contents[item][head]
                                

            
            #This looks back to SQL table for any potential columns that may have been added
            sql2_object = SQL(server, database, table)
            UpdatedSQLHeaderDict = sql2_object.tableLookup('GetSQLDataTypes')

            vlogger.info('UpdatedSQLHeaderDict: {0}'.format(UpdatedSQLHeaderDict))
            vlogger.info('Beginning SQL Upload')


            #Section will upload Input to SQL row by row. Use this portion to filter any unwanted items
            query_start= datetime.datetime.now()
            connection = pymssql.connect(server=server, database = database)
            cursor = connection.cursor()
            n=0
            exceptionrows_dictionary = {}
            for row in Contents:
                insert_list = []
                insert_header = []
                exception_WriterRow = {}
                error_column = None
                error_value = None
                #exception_header = []

                exceptionitem_dictionary = {}
                try:
                # need to error out if add statement doesnt work!!
                    for colname in UpdatedSQLHeaderDict:
                        if colname in Contents[row]:
                            insertColumn = colname.strip()
                            insertValue = Contents[row][colname]
                            error_value = Contents[row][colname]
                            error_column = colname
                            if len(str(insertValue)) > 0 and type(insertValue).__name__ != 'NoneType':
                                if UpdatedSQLHeaderDict[colname] == 'int' or UpdatedSQLHeaderDict[colname] == 'smallint' or UpdatedSQLHeaderDict[colname] == 'bigint' or UpdatedSQLHeaderDict[colname] == 'tinyint' :
                                    insert_header.append(insertColumn)
                                    #Clean and insert int values to SQL
                                    ScrubbedInsertValue = None
                                    if insertValue == '-': 
                                        ScrubbedInsertValue = 0
                                    else:
                                        ScrubbedInsertValue = int(insertValue)

                                    insert_list.append(ScrubbedInsertValue)
                                    #exception_header.append(colname)
                                    #exception_list.append(int(InputFile_Dict[row][colname]))
                                    
                                elif UpdatedSQLHeaderDict[colname] == 'decimal' or UpdatedSQLHeaderDict[colname] == 'money':
                                    insert_header.append(insertColumn)

                                    ScrubbedInsertValue = None
                                    if insertValue == '-': 
                                        ScrubbedInsertValue = 0
                                    else:
                                        ScrubbedInsertValue = float(insertValue)

                                    insert_list.append(ScrubbedInsertValue)
                                    #exception_header.append(colname)
                                    #exception_list.append(float(InputFile_Dict[row][colname]))
                                else:
                                    insert_header.append(insertColumn)
                                    if fnmatch.fnmatch(Contents[row][colname],"*'*"):
                                        insert_list.append((insertValue.replace("'"," ").strip()))
                                    else:
                                        insert_list.append(insertValue)
                            #else:
                                #print('ColName', colname, 'Value', InputFile_Dict[row][colname])
                                #insert_header.append(colname)
                                #insert_list.append(InputFile_Dict[row][colname])
                        #else:
                            #print('In row {0}, {1} was not found. Contents will fill as NULL if possible'.format(row,colname))

                    insert = "INSERT INTO {0} ({1}) VALUES ({2})".format(table,', '.join(insert_header),repr(insert_list).strip('[]'))

                    ###Uncomment these when comfortable with upload
                    cursor.execute(insert)
                    connection.commit()

                    n+=1
                    
                except Exception as inst:
                    
                    '''Grab row number, and contents, plug into exception rows dictionary to write to csv'''
                    exceptionrows_dictionary[row] = OriginalContents[row]
                    

                    '''Is all this necessary if we are creating an exception file?'''
                    Error_Dict = {}
                    clogger.error('Error occurred on row: {0} Error {1}'.format(row,inst))
                    #print( "Error Column:",error_column, "Error Value:",error_value,"Error Value Length:",len(error_value),"Error value type:",type(error_value))
                    for index in range(len(insert_list)):
                        cell_value = insert_list[index]
                        head_value = insert_header[index]
                        column_type = UpdatedSQLHeaderDict[head_value]
                        Error_Dict[head_value] = {}
                        #print('cell_value',cell_value,'head_value',head_value,'column_type',column_type)
                        if type(cell_value) == str:
                            Error_Dict[head_value][cell_value] = (column_type,len(cell_value))
                        else:
                            Error_Dict[head_value][cell_value] = (column_type,type(cell_value))                                     
                    clogger.error('Values: {0}'.format(insert_list))
                    elogger.exception(inst)
                    vlogger.error(inst)
                    #clogger.error(Error_Dict)
                    #raise



            if len(exceptionrows_dictionary) > 0:
                # Create exception file with rows that were not transferred to table
                exceptionfilename = self.filename.split('.')[0]
                exception_file = Filer(self.filepath, '{0}_Exceptions'.format(exceptionfilename))
                exception_file.csvCreator(Header, exceptionrows_dictionary)
                    
            query_finish = datetime.datetime.now()
            query_runtime = query_finish-query_start
            connection.close()

            script_finish = datetime.datetime.now()
            python_runtime = (script_finish-script_start)-(query_finish-query_start)
            script_runtime = script_finish-script_start
            vlogger.info("Upload to table {0} completed. Connection Closed.".format(table))
            vlogger.info("Input file size: {0} row(s). {1} row(s) imported successfully".format(len(Contents),n))
            clogger.info('Program Runtime: {0} Query Runtime: {1} Python Runtime: {2}'.format(script_runtime, query_runtime,python_runtime))


        #Catch and log any errors that cause program to fail
        except Exception as ex:
            elogger.exception(ex)
            vlogger.error(ex)

            #Send email if error occurs
            frm = 'cubi@arrowheadcu.org'
            to = email
            subject = 'Error occurred in {0}'.format(__name__)
            message = 'Exception error occurred within {1}.\n\nCheck logs for more info.\nTime: {0}\n\n{2}'.format(str(datetime.datetime.now()),__file__, traceback.format_exc())
            easylogging.sendMail(frm, to, subject, message)

            raise

        finally:
            vlogger.info('Method Complete')



'''******************************************************************************
    Change log:

    06/10/2015 - Initial File created by Justin Hanley
    07/29/2015 - Added logging logic
    08/04/2015 - Added tableexist and additems parameters to insertTable for better automation,
                 Resolved logic errors created by module resturcturing and logging
    09/10/2015 - Add delimiter parameter to InsertTable for better file control
    09/11/2015 - Add email parameter to InsertTable for better email control
    11/23/2015 - Add loggers for INSERT INTO error    
    
*****************************************************************************'''

