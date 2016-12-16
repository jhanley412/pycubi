'''
' Basic module for file handeling
'     Notes: add logging feature
'''
import os
import shutil
import datetime

def file_collector(root_path, root_filext):
    '''
    Loops through root_path and gathers all /files/ with desired ext into dictionary.
    '''
    item_dict = {}
    n = 1
    os.chdir(root_path)
    for item in os.listdir():
        if os.path.isdir(item) == False and os.path.splitext(item)[1] == root_filext:
            item_dict[n] = {}
            item_dict[n]['filepath'] = root_path
            item_dict[n]['filename'] = item
            n += 1
    return item_dict

def file_mover(src_filepath, src_filename, dest_dir):
    '''
    Moves source file to destination
    '''
    shutil.move(os.path.join(src_filepath, src_filename), dest_dir)

def print_subitems(root_path = os.getcwd()):
    '''
    Prints all sub contents (directories and respective files) in
    root_path.

    Defaults to os.getcwd()
    '''
    for dirname, dirnames, filenames in os.walk('.'):
        for subdirname in dirnames:
            print(os.path.join(dirname, subdirname))

        for filename in filenames:
            print(os.path.join(dirname, filename))
    
'''
# Logging
log_file = open(os.path.join(root_path, 'log.txt'), 'a')
log_note = (str(datetime.datetime.now()) + '\t' +
            str(os.getlogin()) + '\t' +
            file_dict[1]['filepath'] + '\t' +
            file_dict[1]['filename'] + '\n')
log_file.write(log_note)
log_file.close()
'''
