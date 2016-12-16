'''******************************************************************************

    filer Module
    Date Last Updated: 08/13/2015
    Created by: Justin Hanley
    Notes:
    Change Log: Please see end of file.

*****************************************************************************'''


import easylogging

from pycubi.settingsconfig import *

import sys
import csv
import collections
import datetime
import os
import logging

class Filer(object):
    '''***********************************************************

        File Class
        Description:  Extract relevant information from a file object
        Date Last Updated:  08/05/2015
        Notes:
                UPDATE NEEDED - Need to remove rows that dont have a value. Should this be done in fileExtract('GetRefinedContents') or ETL?

    ***********************************************************'''

    def __init__(self, filepath, filename):
        self.filepath = filepath
        self.filename = filename
        self.file = os.path.join(str(self.filepath), str(self.filename))

        easylogging.setupLogging(logoutput_path = logdirectory)
        self.versatilelogger = logging.getLogger("versatile")
        self.errorlogger = logging.getLogger("error")
        self.consolelogger = logging.getLogger("consoleinfo")



    def fileExtract(self, output, delimiter = '\t'):
        '''Extract header and contents of a file and transform it into an {R:{C:V}} dictionary or [] List.
           Function determinesbest extraction method based on filename extension'''

        #Set up logging to output and track all uncaught errors
        vlogger = self.versatilelogger
        elogger = self.errorlogger
        clogger = self.consolelogger
        

        #Begin main logic of function
        try:

            parameters_dictionary = {'FilePath':self.filepath, 'FileName':self.filename, 'Output':output, 'Delimiter':delimiter}
            vlogger.info('Method Start with parameters {0}'.format(parameters_dictionary))


            #Defining input parameters for use
            filename = self.filename
            filenameext = filename.split('.')[-1]
            input_file = open(self.file)

            #Raw input items from input file
            RawHeader_List = []
            RawRowContents_List = []
            Duplicate_List = []
            RawData_Dictionary = {}

            #Altered input items based on altercation dictionaries
            AlteredHeader_List = []
            AlteredDuplicate_List = []
            AlteredData_Dictionary = {}

            #Initialize character items that we want to scrub from inputs
            ColumnNameAlter_Dictionary = {' ':'' , "'":'', '-':'_' , '+':'PLUS',  '#':'Number',  '/':'_' , '(':'_' , ')':'', "'":'', '.':'', '3rd':'Third', '\n':'', '\\':'', '?':'', ':':'_'}
            ContentValueAlter_Dictionary = {',':'','$':'','%':'','"':'', '\n':'', '(':'-', ')': '', '[':'', ']':''}

            ##################################
            #
            #       Begin File Extraction
            #           Section will seach for best option to extract raw data based on FileName extension
            #           This should be interchangeable and easily upgradeable to work with Altercation step
            #
            ##################################

            #Txt file parser
            if filenameext == 'txt':

                FileContents_List = input_file.readlines()
                for n, line in enumerate(FileContents_List):
                    if n == 0:
                        RawHeader_List = FileContents_List[n].split(delimiter)
                        ItemCount_Dictionary = collections.Counter([j.upper() for j in RawHeader_List])
                        Duplicate_List = [j for j in ItemCount_Dictionary if ItemCount_Dictionary[j] > 1]
                    else:
                        InputRowContents = FileContents_List[n].split(delimiter)
                        RawRowContents_List.append(InputRowContents)

                        #Create Raw data dictionary from header and row items, row by row
                        Row_Dictionary = {}   
                        for ColNum, header in enumerate(RawHeader_List):
                            Row_Dictionary[header] = InputRowContents[ColNum]
                        RawData_Dictionary[n-1] = Row_Dictionary 

            #CSV file parser
            elif filenameext == 'csv':
                FileContents_List = [i for i in csv.reader(input_file)]
                for n, line in enumerate(FileContents_List):
                    if n == 0:
                        RawHeader_List = FileContents_List[n]
                    else:
                        InputRowContents = FileContents_List[n]
                        RawRowContents_List.append(InputRowContents)

                        #Create Raw data dictionary from header and row items, row by row
                        Row_Dictionary = {}
                        for ColNum, header in enumerate(RawHeader_List):
                            Row_Dictionary[header] = InputRowContents[ColNum]
                        RawData_Dictionary[n-1] = Row_Dictionary

            else:
                vlogger.info("Exiting Method. Didn't recognize filetype {0}. Extensions currently mapped to .txt or .csv files.".format(filenameext))
                sys.exit()

            #Find duplicate headers in Raw Data
            ItemCount_Dictionary = collections.Counter([j.upper() for j in RawHeader_List])
            Duplicate_List = [j for j in ItemCount_Dictionary if ItemCount_Dictionary[j] > 1]


            ##############################
            #
            #       Alteration Logic
            #           Section will use raw extracts header and contents list and look to alteration dictionaries to scrub for any unwanted characters
            #
            ##############################

            if output.upper() == 'GETREFINEDHEADER' or output.upper() == 'GETREFINEDCONTENTS':
                #Logic to use ColumnName Alter mappings to remove unwanted characters in output
                for item in RawHeader_List:
                    n=2
                    header_alter = item
                    for alteration in ColumnNameAlter_Dictionary:
                        if alteration in header_alter:
                            header_alter = header_alter.replace(alteration, ColumnNameAlter_Dictionary[alteration])
                        else:
                            continue
                    if header_alter in AlteredHeader_List:
                        print(header_alter, 'in list')
                        header_alter = header_alter + '_{0}'.format(n)
                        print(header_alter)
                        n+=1

                    AlteredHeader_List.append(header_alter)

                #Find duplicate item in alter header
                ItemCount_Dictionary = collections.Counter([j.upper() for j in AlteredHeader_List])
                AlteredDuplicate_List = [j for j in ItemCount_Dictionary if ItemCount_Dictionary[j] > 1]   
                
                #Build scrubbed value list (row by row)
                for i, rows in enumerate(RawRowContents_List):
                    #Build Altered Row Contents List
                    AlteredRowContents_List = []
                    for n, value in enumerate(rows):
                        value_alter = value
                        for alteration in ContentValueAlter_Dictionary:
                            if alteration in value_alter:
                                value_alter = value_alter.replace(alteration, ContentValueAlter_Dictionary[alteration])
                            else:
                                continue
                        AlteredRowContents_List.append(value_alter.strip())

                    #Combine header list to row values into row dictionary that will build data dictionary, row by row
                    Row_Dictionary = {}
                    for ColNum, header in enumerate(AlteredHeader_List):
                        Row_Dictionary[header] = AlteredRowContents_List[ColNum]
                    AlteredData_Dictionary[i] = Row_Dictionary


            ##############################
            #
            #   Output Workflows
            #       Return data_dictionaries based on 'Output'
            #
            ##############################

                if len(AlteredDuplicate_List) > 0:
                    vlogger.info('Duplicate column names were found in file and will be removed from output. Duplicate items: {0}'.format(AlteredDuplicate_List))

                if output.upper() == 'GETREFINEDCONTENTS':
                    #Logic to remove duplicate column headers and contents in input file
                    OutputContents_Dictionary = {}
                    for RowNumber in AlteredData_Dictionary:
                        RowContents_Dictionary = {}
                        for Header in AlteredData_Dictionary[RowNumber]:
                            if Header.upper() in AlteredDuplicate_List:
                                continue
                            else:
                                RowContents_Dictionary[ Header ] = AlteredData_Dictionary[RowNumber][Header]
                        OutputContents_Dictionary[RowNumber] = RowContents_Dictionary
                    return(OutputContents_Dictionary)
                
                elif output.upper() == 'GETREFINEDHEADER':
                    #Logic to remove duplicate column headers in input file
                    OutputHeader_List = []
                    for Header in AlteredHeader_List:
                        if Header.upper() in AlteredDuplicate_List:
                            continue
                        else:
                            OutputHeader_List.append(Header)
                    return(OutputHeader_List)
                #Error handeling for function 'output' definition error
                else:
                    vlogger.info('Did not use appropriate "output". (GetRawHeaders, GetRawContents, GetRefinedContents, or GetRefinedHeaders)')




            elif output.upper() == 'GETRAWCONTENTS' or Output.upper() == 'GETRAWHEADER':
                #Output definition workflow
                if len(Duplicate_List) > 0:
                    vlogger.info('Duplicate column names were found in file and will be removed from output. Duplicate items: {0}'.format(Duplicate_List))


                if output.upper() == 'GETRAWCONTENTS':
                    #Logic to remove duplicate column headers and contents in input file
                    OutputContents_Dictionary = {}
                    for RowNumber in RawData_Dictionary:
                        RowContents_Dictionary = {}
                        for Header in RawData_Dictionary[RowNumber]:
                            if Header.upper() in Duplicate_List:
                                continue
                            else:
                                RowContents_Dictionary[ Header ] = RawData_Dictionary[RowNumber][Header]
                        OutputContents_Dictionary[RowNumber] = RowContents_Dictionary
                    return(OutputContents_Dictionary)
                
                elif output.upper() == 'GETRAWHEADER':
                    #Logic to remove duplicate column headers in input file
                    OutputHeader_List = []
                    for Header in RawHeader_List:
                        if Header.upper() in Duplicate_List:
                            continue
                        else:
                            OutputHeader_List.append(Header)
                    return(OutputHeader_List)
            else:
                vlogger.info("Exiting Program. Didn't use recoginze 'output'. (GetRawHeader, GetRawContents, GetRefinedContents, or GetRefinedHeader)")
                sys.exit()



        #Catch and log any errors that cause program to fail
        except Exception as ex:
            elogger.exception(ex)
            vlogger.error(ex)
            raise

        finally:
            input_file.close()
            vlogger.info('Method Complete')



    def csvCreator(self, header_list = [], contents_dictionary = {}):

        #Set up logging to output and track all uncaught errors
        vlogger = self.versatilelogger
        elogger = self.errorlogger
        clogger = self.consolelogger


        #Begin main logic
        try:
            vlogger.info('Method Start')
            
            FilePath = self.filepath
            FileName = self.filename
            Header = [str(i).strip() for i in header_list]
            Contents = contents_dictionary
            
            start= datetime.datetime.now()
            Name = FileName.split('.')[0]+'.csv'
            

            #CSV Writer. This will read contents using the standardized format {Row#:{Header:Value}}
            
            Output_file = open(os.path.join(FilePath, Name), 'w')
            csv_writer = csv.writer(Output_file, lineterminator='\n')
            csv_writer.writerow(Header)

            for RowNumber in Contents:
                WriteRow_List = []
                for value in Header:
                    #item = value).strip()
                    if value == 'First':
                        WriteRow_List.append(Contents[RowNumber].get(value).title().strip())     #Proper case first name
                    elif value == 'Last':
                        WriteRow_List.append(Contents[RowNumber].get(value).title().strip())     #Proper case Last name
                    elif value == 'Street':
                        WriteRow_List.append(Contents[RowNumber].get(value).title().strip())     #Proper case Street
                    elif value == 'City':
                        WriteRow_List.append(Contents[RowNumber].get(value).title().strip())     #Proper case city
                    else:
                        WriteRow_List.append(Contents[RowNumber].get(value)) 
                csv_writer.writerow(WriteRow_List)


            finish = datetime.datetime.now()
            vlogger.info('Created csv File: {0}'.format(os.path.join(FilePath, Name)))
            clogger.info('Finishing CSVCreator({0}). Closing connection'.format(Name), 'RunTime:',finish-start)
        

        #Catch and log any errors that cause program to fail
        except Exception as ex:
            elogger.exception(ex)
            vlogger.error(ex)
            raise

        finally:
            Output_file.close()
            vlogger.info('Method Complete')



'''******************************************************************************
    Change Log

        06/10/2015 - Initial file created by Justin Hanley
        07/29/2015 - Added logging logic and combined file creator logic for ease of use
        08/01/2015 - Expanded logging logic and altered error handling. fileExtractor will exit program if
                     filetype is not recognized
        08/13/2015 - File and filename parameters string conversion for use with os.path.join

*****************************************************************************'''
