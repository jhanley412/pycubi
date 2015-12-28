'''******************************************************************************

    DataAnalysis Module
    Date Last Updated: 08/01/2015
    Created by: Justin Hanley
    Notes
    Change Log: Please see end of file.

*****************************************************************************'''

import easylogging

from cubi.settingsconfig import *

import collections
import math
import datetime
import logging
import sys

class DataAnalysis(object):
    '''***********************************************************

        DataAnalysis Class
        Description:  Extract analytical information on data dictionaries (RCV) and lists
        Date Last Updated:  06/10/2015
        Notes:

    ***********************************************************'''
    
    def __init__(self, dataheader = [], datacontents = {}):
        self.datacontents = datacontents
        self.dataheader = dataheader
        
        easylogging.setupLogging(logoutput_path = logdirectory)
        self.versatilelogger = logging.getLogger("versatile")
        self.errorlogger = logging.getLogger("error")
        self.consolelogger = logging.getLogger("consoleinfo")

    
    def homogeneous_type(seq = []):
        '''Find list item type if all types are the same'''
        #Set up logging to output and track all uncaught errors
        vlogger = self.versatilelogger
        elogger = self.errorlogger
        clogger = self.consolelogger

        #Begin main logic of function
        try:
            #Logging
            vlogger.info('Method Start')
            
            self.seq = seq
            iseq = iter(self.seq)
            first_type = type(next(iseq))
            return first_type if all( (type(x) is first_type) for x in iseq ) else False
            
        #Catch and log any errors that cause program to fail
        except Exception as ex:
            elogger.exception(ex)
            vlogger.error(ex)
            raise

        finally:
            vlogger.info('Method Complete')



    def dataType(self, output):
        ''' Extracts max data types and values on a data dictionary. Requires Contents_dictionary in {R:{C:V}} format, header list, and Output item (GETDATAVALUES, GETDATATYPES, GETMAXDATATYPES, GETSQLDATATYPES)'''

        #Set up logging to output and track all uncaught errors
        vlogger = self.versatilelogger
        elogger = self.errorlogger
        clogger = self.consolelogger

        #Begin main logic of function
        try:
            #Logging
            vlogger.info('Method Start')
            
            #Initialize parameters for use
            Output = output
            Contents_Dictionary = self.datacontents 
            Header_List = self.dataheader
            ContentsHeader_List = [i for i in Contents_Dictionary[0]]   #Note this is only looking on first row of contents for header. Not intelligent
            MismatchContents_List = [j for j in ContentsHeader_List if j not in Header_List]
            MismatchHeader_List = [k for k in Header_List if k not in ContentsHeader_List]
            output_list = ['GetDataValues', 'GetDataTypes', 'GetMaxDataTypes', 'GetSQLDataTypes']

            if len(MismatchContents_List)> 0 or len(MismatchHeader_List) > 0:
                vlogger.info("Exiting program. The header list doesn't match the contents. Please verify inputs. - Mismatched header items {0} - Mismatched contents header items {1}".format(MismatchHeader_List, MismatchContents_List))
                sys.exit()
            else:
                ColumnValue_Dictionary = {}
                ColumnType_Dictionary = {}
                ColumnMaxDataType_Dictionary = {}
                ColumnSQLDataType_Dictionary = {}


                #SQL Data type mapping. Use this to tie SQL Data types for table creation and type optimization. Format {DataTypeName:{min:max}}
                #SQLDataType_Dictionary = {'float':(('decimal',4,15)),'int':(('bit',0,1),('tinyint',0,255),('smallint',-32768,32767), ('int',-2147483648,2147483647),('bigint',math.pow(-2,63),(math.pow(2,63)-1))),'datetime':(('smalldatetime',datetime.datetime(1900,1,1,0,0),datetime.datetime(2079,6,6,0,0)),('datetime',datetime.datetime(1753,1,1,0,0),datetime.datetime(9999,12,31,0,0))),'str':(('varchar',0,8000)), 'NoneType':(('varchar',0,8000))}
                SQLDataType_Dictionary = {'float':[('decimal',5,15)],'int':[('bit',0,1),('tinyint',0,255),('smallint',-32768,32767), ('int',-2147483648,2147483647),('bigint',math.pow(-2,63),(math.pow(2,63)-1))],'datetime':[('smalldatetime',datetime.datetime(1900,1,1,0,0),datetime.datetime(2079,6,6,0,0)),('datetime',datetime.datetime(1753,1,1,0,0),datetime.datetime(9999,12,31,0,0))],'str':[('varchar',0,8000)], 'NoneType':[('varchar',0,8000)]}

                #Initialize dictionaries with empty header lists. This will be used to append data values and types to above dictionaries.
                for Header in Header_List:
                    ColumnValue_Dictionary[Header]=[]
                    ColumnType_Dictionary[Header] = []


                #####
                # Create ColumnValue_Dictionary and ColumnType_Dictionary dictionaries
                #       Looks at each value of input and attempts to coerce data into best fit type.
                #       Data type coersion order: int,float, datetime(MM/DD/YYYY), datetime(MM-DD-YYYY), datetime(YYYY-MM-DD), string
                #####
                
                #Build data dictionaries (value and type)
                for row in Contents_Dictionary:
                    for Header in Header_List:

                        

                        #Change all empty strings to NoneType or 0
                        if Contents_Dictionary[row][Header] == '':
                            No_Value = None
                            ColumnValue_Dictionary[Header].append(No_Value)
                            ColumnType_Dictionary[Header].append(type(No_Value).__name__)
                        elif Contents_Dictionary[row][Header] == '-':
                            NoInt_Value = 0
                            ColumnValue_Dictionary[Header].append(NoInt_Value)
                            ColumnType_Dictionary[Header].append(type(NoInt_Value).__name__)

                        #Begin item values conversion
                        else:
                            #To properly raise a python ValueError, item needs convert from a string
                            data_str_item = str(Contents_Dictionary[row][Header])

                            #Convert value to an integer
                            try:
                                int_value = int(data_str_item)
                                ColumnValue_Dictionary[Header].append(int_value)
                                ColumnType_Dictionary[Header].append(type(int_value).__name__)
                                #print(item, '\t', Contents_Dictionary[0][item], '\t', int(Contents_Dictionary[0][item]))

                            except ValueError:

                                #Convert value to float
                                try:
                                    float_value = float(data_str_item)
                                    ColumnValue_Dictionary[Header].append(float_value)
                                    ColumnType_Dictionary[Header].append(type(float_value).__name__)
                                    #print(item, '\t', Contents_Dictionary[0][item], '\t', float(Contents_Dictionary[0][item]))

                                except ValueError:

                                    #Convert value to datetime with input MM/DD/YYYY
                                    try:
                                        date_value = datetime.datetime.strptime(data_str_item, '%m/%d/%Y')
                                        ColumnValue_Dictionary[Header].append(date_value)
                                        ColumnType_Dictionary[Header].append(type(date_value).__name__)
                                        
                                    except ValueError:

                                        #Convert value to datetime with input MM/DD/YY
                                        try:
                                            date_value = datetime.datetime.strptime(data_str_item, '%m/%d/%y')
                                            ColumnValue_Dictionary[Header].append(date_value)
                                            ColumnType_Dictionary[Header].append(type(date_value).__name__)
                                            
                                        except ValueError:

                                            #Convert value to datetime with input MM-DD-YYYY
                                            try:
                                                date_value = datetime.datetime.strptime(data_str_item, '%m-%d-%Y')
                                                ColumnValue_Dictionary[Header].append(date_value)
                                                ColumnType_Dictionary[Header].append(type(date_value).__name__)

                                            except ValueError:

                                                #Convert value to datetime with input MM-DD-YY
                                                try:
                                                    date_value = datetime.datetime.strptime(data_str_item, '%m-%d-%y')
                                                    ColumnValue_Dictionary[Header].append(date_value)
                                                    ColumnType_Dictionary[Header].append(type(date_value).__name__)

                                                except ValueError:


                                                    #Convert value to datetime with input YYYY-MM-DD
                                                    try:
                                                        date_value = datetime.datetime.strptime(data_str_item, '%Y-%m-%d')
                                                        ColumnValue_Dictionary[Header].append(date_value)
                                                        ColumnType_Dictionary[Header].append(type(date_value).__name__)

                                                    except ValueError:
                                                        
                                                        #Convert value to string
                                                        try:
                                                            string_value = str(data_str_item)
                                                            ColumnValue_Dictionary[Header].append(string_value)
                                                            ColumnType_Dictionary[Header].append(type(string_value).__name__)
                                                            #print(item, '\t', Contents_Dictionary[0][item], '\t', str(Contents_Dictionary[0][item]))

                                                        except ValueError as ve:
                                                            vlogger.info('Value conversion error - {0} {1}'.format(Header, ve.args))

                #####
                # Create ColumnMaxDataType_Dictionary and ColumnSQLDataType dictionary
                #       Runs through each column and looks at best option for data type, finds max of that data type and builds SQL version of dataname
                #####
                print(ColumnType_Dictionary)
                print(ColumnValue_Dictionary)

                #List data types to categorize max values (in order of precedence)
                DataTypes_List = ('str','float', 'int', 'datetime', 'NoneType')
                #Search through data dictionary to find max values per column header. If column has multiple data types, logic will catgorize data types according to previous Data_Type List
                for n, header in enumerate(ColumnType_Dictionary):
                    MaxType_Dictionary = {}
                    #SQLType_Dictionary = {}
                    
                    
                    #ColumnSQLDataType_Dictionary[n] = SQLType_Dictionary
                    ErrorItems_List = []
                    try:
                        #Look through previously defined DataTypes list in specified precedence. Once match between data type list and column data type is found, break loop
                        for DataTypeItem in DataTypes_List:
                            
                            #Match data type to all data types in column. Collections.Counter will provide dictionary with count of data types in column
                            if DataTypeItem in collections.Counter(ColumnType_Dictionary[header]):
                            
                                #Find max values in columns. Filtering out all 'NoneTypes' (and 'int' for string len) during calculations
                                MaxItemValue = None
                                ErrorItems_List = [n, header, DataTypeItem, ColumnValue_Dictionary[header]]
                                
                                #Find max length of string
                                if DataTypeItem == 'str':
                                    Filtered_List = [i for i in ColumnValue_Dictionary[header] if type(i).__name__ != 'NoneType' and type(i).__name__ != 'int' and type(i).__name__ != 'float' and type(i).__name__ != 'datetime'] #removes all nonetype and integer items for max items in string lists
                                    MaxItemValue = len(max(Filtered_List, key=len))
                                    #MaxType_Dictionary[item] = len(max(ColumnValue_Dictionary[header], key=len))

                                elif DataTypeItem == 'NoneType':
                                    MaxItemValue = 0

                                #Find max of non string and none types
                                else:
                                    #Use list comprehenshion to remove strings in non string columns
                                    Filtered_List = [i for i in ColumnValue_Dictionary[header] if type(i).__name__ != 'NoneType' ]  #removes Nonetype items in non string item lists
                                    MaxItemValue = max(Filtered_List)

                                    #Dynamically build the precision and scale of all float items for better accuracy
                                    if DataTypeItem == 'float':
                                        float_precision_size_list = []
                                        float_scale_size_list = []
                                        for float_item in Filtered_List:
                                            split_float_list = str(float(float_item)).split('.')
                                            len_left_decimal = len(split_float_list[0])
                                            len_right_decimal = len(split_float_list[1])
                                            float_precision_size_list.append(len_left_decimal + len_right_decimal)
                                            float_scale_size_list.append(len_right_decimal)

                                        max_float_precision = max(float_precision_size_list)
                                        max_float_scale = max(float_scale_size_list)
                                            
                                        
                                #print(n, header, item, MaxItemValue)

                                
                                OutputSQLDatatype = None
                                
                                #Create SQL datatypes according to MaxItemValues
                                for SQLDataType in SQLDataType_Dictionary[DataTypeItem]:
                                    #print(n, header, item, MaxItemValue,SQLDataType)
                                    if DataTypeItem == 'str' and SQLDataType[1] <= MaxItemValue +10 <= SQLDataType[2]:
                                        OutputSQLDatatype = ''.join([SQLDataType[0],'(', str(MaxItemValue+10), ')'])
                                        #print(n, header, OutputSQLDatatype)
                                        break                        
                                    elif DataTypeItem == 'int' and SQLDataType[1] <= MaxItemValue <= SQLDataType[2]:
                                        OutputSQLDatatype = SQLDataType[0]
                                        #print(n, header, OutputSQLDatatype)
                                        break
                                    elif DataTypeItem == 'float':
                                        #OutputSQLDatatype = ''.join([SQLDataType[0],'(',str(SQLDataType[2]),',',str(SQLDataType[1]),')'])
                                        OutputSQLDatatype = ''.join([SQLDataType[0],'(',str(max_float_precision),',',str(max_float_scale),')'])
                                        #print(n, header, OutputSQLDatatype)
                                        break
                                    elif DataTypeItem == 'datetime' and SQLDataType[1] <= MaxItemValue <= SQLDataType[2]:
                                        OutputSQLDatatype = SQLDataType[0]
                                        #print(n, header, OutputSQLDatatype)
                                        break
                                    elif DataTypeItem == 'NoneType':
                                        OutputSQLDatatype = ''.join([SQLDataType[0],'(', str(50),')'])
                                        break
                                    
                                #SQLType_Dictionary[header] = OutputSQLDatatype
                                ColumnSQLDataType_Dictionary[header] = OutputSQLDatatype
                                break
                            
                    except TypeError as te:
                        elogger.exception(te)
                        vlogger.error('{0} - Error Item(s): {1}'.format(te, ErrorItems_List))
                        
                        

                    ColumnMaxDataType_Dictionary[header] = MaxType_Dictionary
                            
               
                '''
                for SQLDataType in SQLDataType_Dictionary:
                    print('SQLDataType:',SQLDataType)
                    if item == 'str':
                        print(n,header, ColumnValue_Dictionary[header])
                        print(n,header, item, max(ColumnValue_Dictionary[header], key=len))
                        MaxType_Dictionary[item] = len(max(ColumnValue_Dictionary[header], key=len))
                        break
                    else:
                        print(n,header, ColumnValue_Dictionary[header])
                        NoString_List = [i for i in ColumnValue_Dictionary[header] if type(i).__name__ != 'str']
                        print(n,header, item, max(NoString_List))
                        MaxType_Dictionary[item] = max(NoString_List)
                        break
                '''

                    
                if Output.upper() == 'GETDATAVALUES':
                    if len(ColumnValue_Dictionary) == len(Header_List):
                        vlogger.info('{0} of {1} Data Values mapped. Good to go.'.format(len(ColumnValue_Dictionary),len(Header_List)))
                    else:
                        vlogger.info('Uh oh...{0} of {1} Data Values mapped. Columns may have been missed. Check logic'.format(len(ColumnValue_Dictionary),len(Header_List)))
                        MissingColumns_List = []
                        for item in Header_List:
                            if item not in ColumnValue_Dictionary:
                                MissingColumns_List.append(item)
                        vlogger.info('Missing Column(s):{0}'.format(MissingColumns_List))
                    return(ColumnValue_Dictionary)    

                elif Output.upper() == 'GETDATATYPES':
                    if len(ColumnType_Dictionary) == len(Header_List):
                        vlogger.info('{0} of {1} Data Types mapped. Good to go.'.format(len(ColumnType_Dictionary),len(Header_List)))
                    else:
                        vlogger.info('Uh oh...{0} of {1} Data Types mapped. Columns may have been missed. Check logic'.format(len(ColumnType_Dictionary),len(Header_List)))
                        MissingColumns_List = []
                        for item in Header_List:
                            if item not in ColumnType_Dictionary:
                                MissingColumns_List.append(item)
                        vlogger.info('Missing Column(s): {0}'.format(MissingColumns_List))
                    return(ColumnType_Dictionary)
                          
                elif Output.upper() == 'GETMAXDATATYPES':
                    if len(ColumnMaxDataType_Dictionary) == len(Header_List):
                        vlogger.info('{0} of {1} Max Data Types mapped. Good to go.'.format(len(ColumnMaxDataType_Dictionary),len(Header_List)))
                    else:
                        vlogger.info('Uh oh...{0} of {1} Max Data Types mapped. Columns may have been missed. Check logic'.format(len(ColumnMaxDataType_Dictionary),len(Header_List)))
                        MissingColumns_List = []
                        for item in Header_List:
                            if item not in ColumnMaxDataType_Dictionary:
                                MissingColumns_List.append(item)
                        vlogger.info('Missing Column(s): {0}'.format(MissingColumns_List))
                    return(ColumnMaxDataType_Dictionary)

                elif Output.upper() == 'GETSQLDATATYPES':
                    if len(ColumnSQLDataType_Dictionary) == len(Header_List):
                        vlogger.info('{0} of {1} SQL Data Types mapped. Good to go.'.format(len(ColumnSQLDataType_Dictionary),len(Header_List)))
                    else:
                        vlogger.info('Uh oh...{0} of {1} SQL Data Types mapped. Columns may have been missed. Check logic'.format(len(ColumnSQLDataType_Dictionary),len(Header_List)))
                        MissingColumns_List = []
                        for item in Header_List:
                            if item not in ColumnSQLDataType_Dictionary:
                                MissingColumns_List.append(item)
                        vlogger.info('Missing Column(s): {0}'.format(MissingColumns_List))
                    return(ColumnSQLDataType_Dictionary)

                else:
                    vlogger.info("Exiting program. Don't recognize input {0}. Need to choose an output: .".format(output, output_list))
                    sys.exit()

                


        #Catch and log any errors that cause program to fail
        except Exception as ex:
            elogger.exception(ex)
            vlogger.error(ex)
            raise

        finally:
            vlogger.info('Method Complete')

'''******************************************************************************
    Change log

        6/10/2015 - Initial file created by Justin Hanley
        7/29/2015 - Added logging logic

*****************************************************************************'''
