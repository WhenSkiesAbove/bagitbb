'''
 _____________________________________________
/     ____                 __    ____   ____  \
|    / _  \ ____  ________/ /__ / _  \ / _  \ |
|   /     //__  \/ _  /  /  __//     //     / |
|  /  _  \/  _  /__  /  /  /_ /  _  \/  _  \  |
| /______/\____/____/__/_____/______/______/  |
|                                             |
|              Bagit, but better              |     
|            ©2024 Jarad Buckwold             |
\_____________________________________________/

This program expands on the Library of Congress' Bagit Python
module by bagging files into target directories rather than just
in place, and similarily unbagging bags into target directories.

Meant to be more light-weight than LoC's Bagger and AVP's Exactly
GUI apps.

bagit.py is obviously required (made with v.1.8.1 in mind)
    https://github.com/LibraryOfCongress/bagit-python
Much of the code directly interacting with bagit came from the documentation on
this page.


Methodology for...

Bagging to target:
1) generate checksums for original files as a list of (path, checksum) tuples
2) copy original files to target folder using shutil
3) bag copies using bagit.py
4) compare bag manifest to that generated in step 1

Unbagging to target:
1) validate bag using bagit.py
2) convert bag manifest to list of (path, checksum) tuples
3) copy files from data folder to target folder using shutil
4) generate new checksum list of copied files and compare to manifest from step 3
5) copy bag metadata (info files, manfiests) to target folder (in created subfolder)

Unbagging can be done using Archivematica (version in constants section) folder structure,
in which payload is unbagged to an 'objects' folder and the bag metadata is unbagged to
a metadata folder along with a checksum manifest (see URL in constants section for details).

It can also do the standard bagit.py validating, bagging in place, and updating bag metadata.

Supports sha256 (default) and md5 checksums (though it wouldn't be hard to add other options).

Supports custom bag-info.txt metadata fields: just add or subtract from metadata dict
in main() and update corresponding optparse options in setup_opts(). Also
supports using json file for metadata, the fields for which can be altered without
changing any code.

Feel free to use, alter, and import in your own code as you see fit. Credit would
be nice, but not required. You can probably write a better program yourself, can't you?

'''

'''*** Imports and Constants ***'''

import bagit
import hashlib
from optparse import OptionParser, OptionGroup
from pathlib import Path
import os
import shutil
import json
import time
import platform
import multiprocessing
import sys
sys.setrecursionlimit(10**6) #shutil was crashing due to the default recursion cap

VERSION = '1.0.0'
ARCHIVEMATICA_URL = 'https://www.archivematica.org/en/docs/'
ARCHIVEMATICA_VERSION = '1.14.1'

#time formats
TIME_FMT_UNBAG = '%B' + ' ' + '%d' + ', ' + '%Y' + ' ' + '%H' + ':' + '%M' + ':' + '%S' + ' ' + '%z' #for unbag.txt
TIME_FMT_METADIR = '_' + '%H' + 'h' + '%M' + 'm' + '%S' + 's' #for bag metadata folder (when unbagging)

#auto-generated bag-info.txt metadata
BAG_INFO_VERSION = 'Bag-Software-Agent' #from bagit.py
BAG_INFO_DATE = 'Bagging-Date' #from bagit.py
BAG_INFO_SIZE = 'Payload-Oxum' #from bagit.py
BAG_INFO_VAL_TYPE = 'Fixity check type' #method of fixity validation
BAG_INFO_CHECKSUM = 'checksum validation' #using checksum for fixity
BAG_INFO_FAST = 'file number and size validation' #use size/quantity for fixity ("fast mode")
BAG_INFO_UPDATE = 'Bag info last updated' #last time bag metadata was updated
BAG_INFO REGEN = 'Bag manifests last updated' #last time bag manifests were updated
SUB_DOC_KEY = 'submission documentation' #JSON key for submission docs

STATUS_MSG = { #general progress messages
    'analyze': 'analyzing files',
    'copy': 'copying files',
    'val_bag': 'validating bag',
    'val_copies': 'validating copies',
    'bag': 'bagging folder',
    'open': 'opening bag',
    'invalid': 'INVALID',
    'done': 'done!',
    'update': 'updating bag',
    'error': 'ERROR'
}

#FIXME is this technically a constant??
TEXT_TYPE = { #text colours for logo display
    #I don't know Windows or other OS colour equivolents, so everyone else gets bland default
    'col1': '\033[92m' if os.name == 'posix' else '', #green
    'col2': '\033[95m' if os.name == 'posix' else '', #purple
    'none': '\033[0m' if os.name == 'posix' else '' #back to basics
}

ERASE = '\033[K'



'''*** Classes ***'''

#class for manipulating bags (better bags, in fact!)
class BetterBag():

    '''Conceptually similar to bagit's Bag class (though more poorly coded).
    The name isn't a dig to the bagit standard and does not mean this is a
    different, somehow better standard than bagit; Just a name I found amusing
    for this program. No shade.'''

    def __init__(self, bag_path, processes=1, quiet=True, bagit_output=False):
        
        self.bag_path = bag_path
        self.quiet = quiet
        self.processes = processes
        self.data_path = os.path.join(bag_path, 'data')
        self.bag_name = os.path.basename(bag_path)
        self.bagit_output = bagit_output
        self.bag = self.open_bag() #create bagit.py Bag object
        self.alg = self.bag.algorithms[0] #this prog only uses one algorithm unlike bagit.py
        
        #turn on or off bagit output
        if bagit_output and not quiet:
            bagit.logging.basicConfig(level=bagit.logging.INFO)
        else:
            bagit.logging.basicConfig(level=bagit.logging.ERROR)
            
    #make sure bag can be opened w/o error
    def open_bag(self):
        
        if not os.path.isdir(self.bag_path) and not self.bagit_output:
            get_status(STATUS_MSG['val_bag'], self.quiet, fin_text=STATUS_MSG['error'])
            throw_error('bag does not exist: ' + self.bag_path, FileNotFoundError, quiet=self.quiet)
        
        get_status(STATUS_MSG['open'], self.quiet)
        
        try:
            bag = bagit.Bag(self.bag_path)
                   
        except (bagit.BagValidationError, bagit.BagError) as e:
            get_status(STATUS_MSG['open'], self.quiet, fin_text=STATUS_MSG['error'])
            throw_error(e, FileNotFoundError, quiet=self.quiet)
                
        except Exception as e:
            throw_error(e, Exception, quiet=self.quiet)
        
        get_status(STATUS_MSG['open'], self.quiet, STATUS_MSG['done'])
        
        return bag
    
    #validate bag for completeness / checksums
    def validate_bag(self, fast=False):
                
        if not self.bagit_output: get_status(STATUS_MSG['val_bag'], self.quiet)    
        
        #validate bag
        try:
            self.bag.validate(processes=self.processes, fast=fast)
        except (bagit.BagValidationError, bagit.BagError) as e:
            get_status(STATUS_MSG['val_bag'], self.quiet, fin_text=STATUS_MSG['invalid'])
            throw_error(e, ValidationError, quiet=self.quiet)
        except Exception as e:
            throw_error(e, Exception, quiet=self.quiet)
        
        get_status(STATUS_MSG['val_bag'], self.quiet, fin_text=STATUS_MSG['done'])
        
    #unbag content to target folder        
    def unbag(self, outdir, make_unbag_file=False, archivematica=False, copy_bag_files=False, fast=False, inplace=False, archivematica_manifest=True):
    
        times = {}    
        times['began'] = time.localtime()
        
        #sorting out paths
        paths = self.set_unbag_paths(outdir, archivematica, inplace)
        
        #validate bag first
        self.validate_bag(fast=fast)
        times['bag_validated'] = time.localtime()
                
        #making dirs
        self.make_unbag_dirs(archivematica, paths, inplace)
        
        #parsing checksums in bag manifest
        old_manifest = Manifest(self.alg, processes=self.processes, quiet=self.quiet)
        old_manifest.manifest = self.read_bag_manifest()
            
        #for bag metadata during inplace unbagging
        tmp_dir = self.setup_tmp_dir(inplace)
        
        #copying files from bag data folder to target        
        get_status(STATUS_MSG['copy'], self.quiet)
        copy_files(self.data_path, paths['unbag'], 'recursive', quiet=self.quiet)
        times['copied'] = time.localtime()
        get_status(STATUS_MSG['copy'], self.quiet, fin_text=STATUS_MSG['done'])
        
        #validating checksums of copies vs those in bag manifest
        get_status(STATUS_MSG['val_copies'], self.quiet)
        exclude = [tmp_dir, self.data_path] if inplace else []

        if fast:
            try:
                fast_compare(self.data_path, paths['unbag'], exclude=exclude, quiet=self.quiet)
            except ManifestError as e:
                throw_error(e, ManifestError, quiet=self.quiet)
            except Exception as e:
                throw_error(e, Exception, quiet=self.quiet)                
        else:
            new_manifest = Manifest(self.alg, processes=self.processes, quiet=self.quiet)
            new_manifest.create(paths['unbag'], exclude=exclude)                    
            new_manifest.compare(old_manifest.manifest)
        times['copies_validated'] = time.localtime()
        
        #create new checksum manifest compliant with Archivematica
        if archivematica and archivematica_manifest:
            old_manifest.save_for_archivematica(paths['archivematica_metadata'])

        #Copybag metadata
        if make_unbag_file or copy_bag_files:
            bag_metadata_folder = self.make_bag_metadata_folder(paths['bag_metadata'])
        if copy_bag_files:
            self.copy_bag_files(bag_metadata_folder, tmp_dir, inplace=inplace)
        if make_unbag_file:
            self.make_unbag_file(bag_metadata_folder, paths['unbag'], 'unbag.txt', inplace, times, fast)        
        
        get_status(STATUS_MSG['val_copies'], self.quiet, fin_text=STATUS_MSG['done'])    
    
    #make all necessary directories for unbagging
    def make_unbag_dirs(self, archivematica, paths, inplace):
        
        try:
            if archivematica:                
                if not inplace: os.mkdir(paths['parent'])
                os.mkdir(paths['unbag'])
                os.mkdir(paths['archivematica_metadata'])
                os.mkdir(paths['submission_docs'])
            else:    
                if not inplace: os.mkdir(paths['unbag'])
        except FileNotFoundError as e:
            throw_error(e, FileNotFoundError, quiet=self.quiet)
        except FileExistsError as e:
            throw_error(e, FileExistsError, quiet=self.quiet)
        except Exception as e:
            throw_error(e, Exception)
            
    #used to exclude checksum validation for bag metadata when unbagging inplace
    def setup_tmp_dir(self, inplace):
    
        if inplace:
            tmp_dir = os.path.join(self.bag_path, 'tmp' + time.strftime(TIME_FMT_METADIR, time.localtime()))
            try:
                os.mkdir(tmp_dir)
            except FileNotFoundError as e:
                throw_error(e, FileNotFoundError, quiet=self.quiet)
            except FileExistsError as e:
                throw_error(e, FileExistsError, quiet=self.quiet)
            except Exception as e:
                throw_error(e, Exception, quiet=self.quiet)
            copy_files(self.bag_path, tmp_dir, 'move', quiet=self.quiet)
        else:
            tmp_dir = ''
        
        return tmp_dir
        
    #copy bag metadata files when unbagging
    def copy_bag_files(self, bag_metadata_path, tmp_dir, inplace=False):
        
        #copy over bag metadata and delete temp dir
        if inplace:
            copy_files(tmp_dir, bag_metadata_path, 'flat', quiet=self.quiet)
            shutil.rmtree(tmp_dir)
            shutil.rmtree(self.data_path)
        else:
            copy_files(self.bag_path, bag_metadata_path, 'flat', quiet=self.quiet)
    
    #make folder for bag metadata
    def make_bag_metadata_folder(self, bag_metadata_path):
        
        bag_metadata_path += time.strftime(TIME_FMT_METADIR, time.localtime())
        try:
            os.mkdir(bag_metadata_path)            
        except FileNotFoundError as e:
            throw_error(e, FileNotFoundError, quiet=self.quiet)
        except FileExistsError as e:
            throw_error(e, FileExistsError, quiet=self.quiet)
        except Exception as e:
            throw_error(e, quiet=self.quiet)
            
        return bag_metadata_path
    
    #setup paths for unbag method
    def set_unbag_paths(self, outdir, archivematica, inplace):
    
        paths = {}
    
        unbag_path_init = self.bag_path if inplace else os.path.join(outdir, self.bag_name)
        
        '''
        path descriptions:
        
        *parent: top level folder of unbagged content (archivematica mode only) 
        *unbag: place where original bagged data will be copied
            [parent]/objects for archivematica, same as parent for standard unbag
        *archivematica metadata: [parent]/metadata
        *submissionDocumentation: [parent]/metadata/submissionDocumentation
        *bag_metadata: in submissionDocumentation if archivematica, otherwise in unbag        
        '''
        
        if archivematica:            
            paths['unbag'] = os.path.join(unbag_path_init, 'objects')
            paths['parent'] = unbag_path_init
            paths['archivematica_metadata'] = os.path.join(unbag_path_init, 'metadata')
            paths['submission_docs'] = os.path.join(paths['archivematica_metadata'], 'submissionDocumentation')
            paths['bag_metadata'] = os.path.join(paths['submission_docs'], self.bag_name + '_bagfiles')        
        else:    
            paths['unbag'] = unbag_path_init
            paths['bag_metadata'] = os.path.join(paths['unbag'], self.bag_name + '_bagfiles')
            paths['parent'] = ''
            paths['submission_docs'] = ''
            paths['archivematica_metadata'] = ''
            
        return paths
    
    #uses bag manifest instead of generating brand new checksums
    def read_bag_manifest(self):

        manifest = []

        for path, fixity in self.bag.entries.items():
            if path.startswith('data' + os.sep):
                pair = (path.replace('data' + os.sep, '', 1), fixity[self.alg])
                manifest.append(pair)
                
        return sorted(manifest)
        
    #create unbag.txt in bag metadata folder
    def make_unbag_file(self, file_path, unbag_path, filename, inplace, times, fast=False):
        
        fields = {
            'bagitBB version': VERSION,
            'OS': platform.platform(),
            'bag name': os.path.basename(self.bag_path),
            'origin': os.path.dirname(self.bag_path),
            'target': 'in place' if inplace else os.path.dirname(unbag_path),
            'unbagging began': time.strftime(TIME_FMT_UNBAG, times['began']),
            'bag validated': time.strftime(TIME_FMT_UNBAG, times['bag_validated']),
            'copied to target': time.strftime(TIME_FMT_UNBAG, times['copied']),
            'copies validated': time.strftime(TIME_FMT_UNBAG, times['copies_validated'])
        }
        if fast:
            fields[BAG_INFO_VAL_TYPE] = BAG_INFO_FAST
        else:
            fields[BAG_INFO_VAL_TYPE] = BAG_INFO_CHECKSUM
        
        with open(os.path.join(file_path, filename), 'w') as unbag_file: 
            for key, value in fields.items():
                unbag_file.write(key + ': ' + value + '\n')
        
    #update bag metadata in baginfo.txt
    def update_metadata(self, metadata, manifests=False, fast=False):
        
        '''appends bag-info.txt; new fields will overwrite old fields with same name
        (note that empty JSON fields are not counted and will therefore not overwrite
        anything)'''
        
        #update fixity check type if regenerating
        if manifests:
            if fast:
                metadata[BAG_INFO_VAL_TYPE] = BAG_INFO_FAST
            else:
                metadata[BAG_INFO_VAL_TYPE] = BAG_INFO_CHECKSUM
                
        #work out new metadata
        if not self.bagit_output: get_status(STATUS_MSG['update'], self.quiet)
        for key in metadata:
            self.bag.info[key] = metadata[key]
        
        #update and save
        try:
            self.bag.save(processes=self.processes, manifests=manifests)
        except (bagit.BagError, bagit.BagValidationError) as e:
            throw_error(e, ValidationError, quiet=self.quiet)
        except Exception as e:
            throw_error(e, Exception, quiet=self.quiet)
        get_status(STATUS_MSG['update'], self.quiet, fin_text=STATUS_MSG['done'])
         

#for all them checksum manifests (NOT fast comparisons)
class Manifest():

    def __init__(self, alg, processes=1, quiet=True):
    
        self.alg = alg
        self.manifest = []
        self.processes = processes
        self.quiet = quiet
    
    #Creates a checksum manifest as a list of corresponding tuples (path, checksum)
    def create(self, target, exclude=[]):
        
        file_list = self.get_file_list(target, exclude=exclude)       
        pruned_file_list = self.prune_filenames(file_list, target)
        hash_list = self.get_hash_list(file_list)
        manifest = self.get_manifest(pruned_file_list, hash_list)
        
        self.manifest = manifest
    
    #list of files in manifest
    def get_file_list(self, target, exclude=[]):
        
        file_list = []      
        path = Path(target)
        
        if os.path.isdir(target):    
            for f in path.rglob('*'):                
                #ignore excluded folders; thank you, anonymous Stack Overflow person.
                if os.path.isfile(f) and not any(p in str(f) for p in exclude):
                    file_list.append(str(f))            
        else:
            file_list.append(str(target))
        
        return file_list

    #list of hash values to go with corresponding file list in manifest
    def get_hash_list(self, file_list):

        try:
            #generate checksums with multiprocessing (if selected) - really hope I'm using this right.
            with multiprocessing.Pool(processes=self.processes) as pool:
                hash_list = pool.map(self.get_hash, file_list)                        
        except (multiprocessing.ProcessError, multiprocessing.BufferTooShort, multiprocessing.AutheticationError, multiprocessing.TimeoutError) as e:
            throw_error(e, MultiprocessingError, quiet=self.quiet)
        except Exception as e:
            throw_error(e, Exception, quiet=self.quiet)
        
        return hash_list
    
    #creates list of corresponding file/hash tuples
    def get_manifest(self, file_list, hash_list):
        
        #joining file list and checksum list into list of tuples
        manifest = []                
        for f in zip(file_list, hash_list):
            manifest.append(f)

        return sorted(manifest)
    
    #compare manifest to find checksum mismatches
    def compare(self, target_manifest):
        
        try:
            self.get_exception(target_manifest)
        except ManifestError as e:
            get_status(STATUS_MSG['val_copies'], self.quiet, fin_text=STATUS_MSG['error'])
            throw_error(e, ManifestError, quiet=self.quiet)
        except Exception as e:
            throw_error(e, Exception, quiet=self.quiet)
    
    #raise exception for manifest comparisons
    def get_exception(self, target_manifest):
    
        #if the lengths don't match, it's obviouly a fail
        if len(target_manifest) != len(self.manifest):
            raise ManifestError('Number of copied files do not match source')
                
        #check for mismatches
        for x in range(len(target_manifest)):
            if self.manifest[x][1] != target_manifest[x][1]:
                raise ManifestError('Checksum mismatch')
                    
    #generate checksum hash for a file
    def get_hash(self, filename):

        #another snippet from Stack Overflow. I owe you, random person. Be well.

        if self.alg == 'md5':
            checksum = hashlib.md5()
        elif self.alg == 'sha256':
            checksum = hashlib.sha256()
        with open (filename, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                checksum.update(chunk)
        return checksum.hexdigest()
    
    #remove leading part of path name relative to target dir
    #for the sake of sorting and comparing with other manifests
    def prune_filenames(self, file_list, target):
    
        pruned_file_list = []
        for f in file_list:
            f = f.replace(os.path.dirname(target) + os.sep, '')
            pruned_file_list.append(f)
        return pruned_file_list
        
    #convert manifest to format compatible with Archivematica and save
    def save_for_archivematica(self, save_target):
    
        '''Formatted for Archivematica version 1.14.1. See documentation for
        Archivematica, created by Artefactual Systems, for more information
        on the formatting in this file: https://www.archivematica.org/en/docs/'''
        
        with open(save_target + os.sep + 'checksum.' + self.alg, 'w') as new_manifest:    
            for x in self.manifest:
                x = list(x)
                x[0] = '../objects/' + x[0]
                line = x[1] + ' ' + x[0] + '\n'
                new_manifest.write(line)


#creation of metadata and submission docs for bags
class BagMetadata():

    def __init__(self, manual_fields={}, json=None, manifest_update=False, quiet=True):
    
        self.manual_fields = manual_fields
        self.json = json
        self.manifest_update = manifest_update
        self.quiet = quiet
        self.metadata = {}
        self.doc_list = []
        
    #setup metadata content for bag-info.txt
    def set_bag_metadata(self):

        if self.json != None:
            self.read_json()
            if SUB_DOC_KEY in self.metadata:
                self.get_sub_docs()
                self.fmt_sub_doc_text()
            self.parse_bag_metadata()
        
        elif len(self.manual_fields) != 0:
            self.metadata = self.manual_fields
            self.parse_bag_metadata()

    #list of submission documents to copy with bag
    def get_sub_docs(self):

        for key in self.metadata[SUB_DOC_KEY]:
            self.doc_list.append(os.path.abspath(self.metadata[SUB_DOC_KEY][key]))
    
    #reformat submission docs listed in json file for display in bag-info.txt
    def fmt_sub_doc_text(self):  

        text = ''

        for x in self.metadata[SUB_DOC_KEY]:
            filename = str(os.path.basename(self.metadata[SUB_DOC_KEY][x]))
            text += x + ' (' + filename + ')'
            text += ', '
        text = text[:-2]

        self.metadata[SUB_DOC_KEY] = text

    #parse JSON file for bag-info.txt metadata
    def read_json(self):

        try:
            with open(self.json, 'r') as json_file:
                self.metadata = json.loads(json_file.read())
        except FileNotFoundError as e:
            throw_error(e, FileNotFoundError, quiet=self.quiet)
        except json.JSONDecodeError as e:
            throw_error('JSON error: ' + str(e), JSONError, quiet=self.quiet)
        except Exception as e:
            throw_error(e, Exception, quiet=self.quiet)
            
    #remove metadata fields for bag-info.txt that user did not enter information for
    def parse_bag_metadata(self):

        metadata_list = list(self.metadata)
        for x in range(len(self.metadata)):
            if self.metadata[metadata_list[x]] == None or self.metadata[metadata_list[x]] == '':
                del self.metadata[metadata_list[x]]

        #to document when the info in baginfo.txt and/or manifest was last updated
        if len(self.metadata) != 0:
            self.metadata['Bag info last updated'] = time.strftime(TIME_FMT_UNBAG, time.localtime())
        if self.manifest_update:
            self.metadata['Bag manifests last updated'] = time.strftime(TIME_FMT_UNBAG, time.localtime())


#Custom exceptions - probably don't need all of these, but I'm still learning                

class ManifestError(Exception):
    pass  

class OptError(Exception):
    pass

class ValidationError(Exception):
    pass
    
class CopyError(Exception):
    pass
    
class MultiprocessingError(Exception):
    pass

class JSONError(Exception):
    pass



'''*** operational functions ***'''

#create bag
def bag_files(indirs, outdir, alg, inplace=False, metadata={}, processes=1, quiet=True, fast=False, compression=0, bagit_output=False, sub_docs=[]):
    
    #confirm submission docs exist
    if len(sub_docs) != 0:
        for x in sub_docs:
            if not os.path.isfile(x):
                throw_error('Submission document not found: ' + str(x), FileNotFoundError, quiet=quiet)
    
    #mandatory custom metadata for bag-info.txt
    metadata[BAG_INFO_VERSION] = 'bagit.py ' + bagit.VERSION + ' ' + bagit.PROJECT_URL + ' (via bagitbb.py v.' + VERSION + ')'
    metadata[BAG_INFO_VAL_TYPE] = BAG_INFO_FAST if fast else BAG_INFO_CHECKSUM
    
    #turn on bagit output
    if bagit_output and not quiet: bagit.logging.basicConfig(level=bagit.logging.INFO)
        
    #if bagging in place
    if inplace:
        get_status(STATUS_MSG['bag'], quiet)
        bag = bagit.make_bag(indirs[0], checksums=[alg], bag_info=metadata, processes=processes)
        get_status(STATUS_MSG['bag'], quiet, fin_text=STATUS_MSG['done'])
        return

    #make target dir        
    os.mkdir(outdir)
    
    #initialize manifest var
    old_manifest = Manifest(alg, processes=processes, quiet=quiet)
    
    #process loop for every dir or file in indirs
    for x in range(len(indirs)):
        if not quiet:
            if x != 0: print()
            print('[directory ' + str(x+1) + '/' + str(len(indirs)) + ']')
        
        #add checksums of dir/file to master manifest
        if not fast:
            get_status(STATUS_MSG['analyze'], quiet)
            current_manifest = Manifest(alg, processes=processes, quiet=quiet)
            current_manifest.create(indirs[x])
            old_manifest.manifest += current_manifest.manifest
            get_status(STATUS_MSG['analyze'], quiet, fin_text=STATUS_MSG['done'])
        
        #copy source files to destination dir
        get_status(STATUS_MSG['copy'], quiet)
        if os.path.isdir(indirs[x]):
            target_dir = os.path.join(outdir, os.path.basename(indirs[x]))
            compare_path = target_dir #used for fast compare
        else:
            target_dir = outdir
            compare_path = os.path.join(target_dir, os.path.basename(indirs[x])) #used for fast compare
        if os.path.isdir(indirs[x]):
            copy_files(indirs[x], target_dir, 'recursive', quiet=quiet)
        else:
            copy_files(indirs[x], target_dir, 'file', quiet=quiet)
        get_status(STATUS_MSG['copy'], quiet, fin_text=STATUS_MSG['done'])

        #fast compare
        if fast:
            get_status(STATUS_MSG['val_copies'], quiet)
            try:
                fast_compare(indirs[x], compare_path, quiet=quiet)
            except ManifestError as e:
                throw_error(e, ManifestError, quiet=quiet)
            except Exception as e:
                throw_error(e, Exception)
            get_status(STATUS_MSG['val_copies'], quiet, fin_text=STATUS_MSG['done'])

    if not quiet: print()
    
    #bag copied files
    if not bagit_output: get_status(STATUS_MSG['bag'], quiet)
    bag = bagit.make_bag(outdir, checksums=[alg], bag_info=metadata, processes=processes)
    get_status(STATUS_MSG['bag'], quiet, fin_text=STATUS_MSG['done'])
    
    #setup BetterBag object
    better_bag = BetterBag(bag.path)
    
    #checksum validating bag copies
    if not fast:
        get_status(STATUS_MSG['val_copies'], quiet)        
        old_manifest.manifest.sort()
        #get checksums of copies from bag manifest        
        new_manifest = Manifest(better_bag.alg, processes=processes, quiet=quiet)
        new_manifest.manifest = better_bag.read_bag_manifest()
        new_manifest.compare(old_manifest.manifest)
        get_status(STATUS_MSG['val_copies'], quiet, fin_text=STATUS_MSG['done'])

    #copy submission docs
    if len(sub_docs) > 0:
        for doc in sub_docs:
            copy_files(doc, bag.path, 'file')

    return better_bag


#validate bags and copies using only file size and quantity (as per bagit.py option)
def fast_compare(indir, outdir, exclude=[], quiet=True):
    
    indir_size, indir_count = get_file_details(indir)
    outdir_size, outdir_count = get_file_details(outdir, exclude=exclude)        
        
    if indir_size != outdir_size or indir_count != outdir_count:
        get_status(STATUS_MSG['val_copies'], quiet, fin_text=STATUS_MSG['error'])
        err_msg = 'Expected ' + str(indir_count) + ' files and ' + str(indir_size) + ' bytes, but found ' + str(outdir_count) + ' files and ' + str(outdir_size) + ' bytes.'
    
        raise ManifestError(err_msg)


#get file size and quantity for fast validation option                
def get_file_details(target, exclude=[]):

    count = 0
    size = 0
    
    if os.path.isdir(target):
        path = Path(target)
        for filename in path.rglob('*'):
            if os.path.isfile(filename):
                if len(exclude) == 0: #no excluded dirs/files to worry about
                    size += os.stat(filename).st_size
                    count += 1
                else:
                    if not any(p in str(filename) for p in exclude): #for excluding
                        size += os.stat(filename).st_size
                        count += 1
    elif os.path.isfile(target):
        count += 1
        size += os.stat(target).st_size        
    
    return size, count

        
#file copying
def copy_files(src, dest, copy_type, quiet=True):

    try:
        if copy_type == 'recursive': #copy dir and all subdirs        
            shutil.copytree(src, dest, dirs_exist_ok=True)
        elif copy_type == 'file': #copy single file only
                shutil.copy2(src, dest)
        elif copy_type == 'flat': #copy dir, but no subfolders
            path = Path(src)
            for filename in sorted(path.glob('*')):
                if os.path.isfile(filename):
                    shutil.copy2(filename, dest)
        elif copy_type == 'move': #move flat
            path = Path(src)
            for filename in sorted(path.glob('*')):
                if os.path.isfile(filename):
                    shutil.move(filename, dest)
    
    except shutil.Error as e:
        throw_error(e, CopyError, quiet=quiet)
    
    except Exception as e:
        throw_error(e, Exception, quiet=quiet)


def confirm_task(question_text, cancel_text):
    
    '''NOTE: this prompt will not trigger in quiet mode.
    You've been warned.'''
    
    while True:
        choice = input(question_text)
        if choice.lower() == 'y' or choice.lower() == 'yes':
            return
        elif choice.lower() == 'n' or choice.lower() == 'no':
            print(cancel_text)
            exit()
        else:
            print('invalid selection')
            


'''*** arg and opt functions ***'''

#create options and args via optparse module
def setup_opts():

    logo = print_logo()

    usage = logo + '\n\n'
    usage += 'Copyright 2023 Jarad Buckwold - free to use or alter as needed; credit is appreciated, but not required.\n\n'
    usage += 'v' + VERSION + ' (bagit.py v' + bagit.VERSION + ')\n\n'
    usage += '%prog [options] [mode] [input dir1] [input dir2] [...] [output dir]\n\nBags files using Library of Congress\' Bagit python module, but can bag files to a target directory\ninstead of just bagging them in place. Can similarly unbag to target directory. In both cases,\nchecksums are generated prior to copying files and then compared to those generated from the copied\nfiles in the target folder, ensuring file integrity.\n\n\n'
    usage += 'MODES:\n\n'
    usage += 'bag\n\n    Bags one or more folders or files to target folder.\n    ex: %prog --accession-number A2020-335 bag /home/folder1 /home/file1.file /home/bags/bag1\n\n'
    usage += 'unbag\n\n    Unbags preexisting bag and bag metadata to target folder.\n    ex: %prog unbag /home/bags/bag1 /home/unbagged_files\n\n'
    usage += 'validate\n\n    Validates integrity of existing bag.\n    ex: %prog validate /home/bags/bag1\n\n'
    usage += 'update\n\n    Updates metadata in bag-info.txt (fields with same names will be overwriten). Regenerates manifests if regen option is used.\n    ex: %prog -j /path/to/json.json update /path/to/bag1\n\n'
    usage += 'Note: for bash shell (not sure about others), wildcard * character can be used for bagging to target (NOT for anything else) only if there are NO loose files in the base directory.\n\n\n'
    usage += 'SUBMISSION DOCUMENTS\n\n'
    usage += 'Use ' + SUB_DOC_KEY + ' heading in json metadata to identify documents to copy alongside bagit.txt and bag-info.txt (ie outside the /data folder).\n'
    usage += 'ex:\n'
    usage += '"' + SUB_DOC_KEY + '": {\n    "accession form": "/path/to/accession.pdf",\n    "format report": "/path/to/report.txt"\n}'
    
    
    parser = OptionParser(usage=usage)

    #operational options
    parser.add_option(
        '-a',
        '--algorithm',
        action = 'store',
        type = 'string',
        dest = 'alg',
        default = 'sha256',
        help = 'Algorithm used to generate checksums both for copied files and for bag. Choose either sha256 (default) or md5. ex: -c md5'
    )
    parser.add_option(
        '-i',
        '--inplace',
        action = 'store_true',
        dest = 'inplace',
        help = 'Bags or unbags files in place (ie does not copy files to target). Bag in place is default bagit.py functionality.'
    )
    parser.add_option(
        '-A',
        '--archivematica',
        action = 'store_true',
        dest = 'archivematica',
        help = 'Unbags in target directory structured for use with Artefactual Systems\' Archivematica software. Made with Archivematica v' + ARCHIVEMATICA_VERSION + ' in mind. See ' + ARCHIVEMATICA_URL + ' for details.'
    )
    parser.add_option(
        '-j',
        '--json',
        action = 'store',
        type = 'string',
        dest = 'json',
        help = 'Import bag metadata for bag-info.txt from json file instead of using options. Metadata from options will be ignored. Can also be used to identify submission documents to bag algonside bagit.txt and bag-info.txt (see usage instructions above). ex: -j /path/to/metadata.json'
    )
    parser.add_option(
        '-q',
        '--quiet',
        action = 'store_true',
        default = False,
        dest = 'quiet',
        help = 'Hide progress updates. Errors will raise exceptions instead of messages. NOTE: you will not be prompted to confirm when unbagging in place or updating a bag manifest.'
    )
    parser.add_option(
        '-v',
        '--version',
        action = 'store_true',
        default = False,
        dest = 'version',
        help = 'Show version (ignores other options/args).'
    )
    parser.add_option(
        '-p',
        '--processes',
        action = 'store',
        type = 'int',
        default = 1,
        help = 'Number of parallel processes used to create, validate, or update bag. Original bagit.py option. Ex. -p 8'
    )
    parser.add_option(
        '-f',
        '--fast',
        action = 'store_true',
        default = False,
        help = 'Only compare total size and number of files when validating bags and copied files. Original bagit.py option.'
    )
    parser.add_option(
        '-x',
        '--no-bag-files',
        action = 'store_false',
        default = True,
        dest = 'copy_bag_files',
        help = 'Don\'t copy bag metadata when unbagging.'
    )
    parser.add_option(
        '-r',
        '--regen',
        action = 'store_true',
        default = False,
        dest = 'update_manifest',
        help = 'Regenerate manifest when updating bag. Ignored if not using update mode.'
    )
    parser.add_option(
        '-X',
        '--no-manifest',
        action = 'store_false',
        default = True,
        dest = 'no_manifest',
        help = 'Don\'t create a checksum manifest for metadata folder. For use when unbagging for Archivematica, otherwise ignored.'
    )
    parser.add_option(
        '--bagit-output',
        action = 'store_true',
        default = False,
        dest = 'bagit_output',
        help = 'Show output from bagit.py. Quiet mode supersedes this option.'
    )
        
        
    #metadata options
    meta_group = OptionGroup(parser, 'metadata fields', 'Fields used to record metadata in baginfo.txt document. Only used for bagging and bagging in place and ignored if json option is used. ex: --accession-num A2010-34 --notes "notes go here"')
    meta_group.add_option('--accession-number', action='store', type='string', dest='accession_num')
    meta_group.add_option('--department', action='store', type='string', dest='department')
    meta_group.add_option('--contact-name', action='store', type='string', dest='contact_name')
    meta_group.add_option('--contact-title', action='store', type='string', dest='contact_title')
    meta_group.add_option('--contact-email', action='store', type='string', dest='contact_email')
    meta_group.add_option('--contact-phone', action='store', type='string', dest='contact_phone')
    meta_group.add_option('--contact-address', action='store', type='string', dest='contact_address')
    meta_group.add_option('--records-schedule-number', action='store', type='string', dest='records_schedule_num')
    meta_group.add_option('--bag-size', action='store', type='string', dest='bag_size')
    meta_group.add_option('--record-dates', action='store', type='string', dest='record_dates')
    meta_group.add_option('--description', action='store', type='string', dest='description')
    meta_group.add_option('--notes', action='store', type='string', dest='notes')
    parser.add_option_group(meta_group)

    (options, args) = parser.parse_args()
    
    return options, args


#determine operation
def get_mode(arguments):

    mode = None
    valid_modes = ['bag', 'unbag', 'validate', 'update']
    
    for m in valid_modes:
        if arguments[0].lower() == m:
            mode = m
    if mode == None:
        raise OptError('invalid mode.')

    return mode


#check for positional arg errors
def parse_args(arguments, inplace, mode):
    
    #number of args each mode should have
    num_args = {
        'bag': 2, #only in place, since args can otherwise be infinite
        'unbag': 2 if inplace else 3,
        'validate': 2,
        'update': 2
    }

    #validate number of args (bagging to target can have infinite)
    if mode == 'bag' and not inplace:
        if len(arguments) < 3:
            raise OptError('wrong number of arguments')
    else:
        if len(arguments) != num_args[mode]:
            raise OptError('wrong number of arguments')

    #convert args to absolute paths
    for x in range(len(arguments)):
        arguments[x] = os.path.abspath(arguments[x])
    arguments.remove(arguments[0])
    
    return arguments


#set source and target paths
def get_paths(arguments):

    indirs = []
    
    if len(arguments) == 1:
        indirs.append(arguments[0])
        outdir = None
    elif len(arguments) >= 2:
        for i in range(len(arguments)-1):
            indirs.append(arguments[i])
        outdir = arguments[len(arguments)-1]
    
    return indirs, outdir


#manage path collisions and non-existing paths
def check_paths(indirs, outdir, mode):

    #check for indir path issues
    for i in range(len(indirs)):
        if not os.path.isfile(indirs[i]) and not os.path.isdir(indirs[i]):
            raise FileNotFoundError('File or directory not found: ' + indirs[i])
            
    #check for outdir path issues
    if outdir == None:
        return
    
    if mode == 'bag':
        target_parent = os.path.dirname(outdir)
        target = outdir
    if mode == 'unbag':
        target_parent = outdir
        target = os.path.join(outdir, os.path.basename(indirs[0]))
    
    if not os.path.isdir(target_parent):
        raise FileNotFoundError('Directory not found: ' + target_parent)
    if os.path.isdir(target):
        raise FileExistsError('File or directory exists: ' + target)


#misc option errors
def validate_opts(alg, inplace, indir):

    #make sure chosen alg is legit
    if alg != 'sha256' and alg != 'md5':
        raise OptError('invalid checksum algorithm')

    #make sure not bagging/unbagging in place the dir this program is in
    if inplace:
        this_file_path = os.path.realpath(__file__)
        this_file_name = os.path.basename(this_file_path)
        if os.path.isfile(os.path.join(indir, this_file_name)):
            raise OptError('Cannot bag/unbag in-place directory containing ' + this_file_name)
    


'''*** output functions ***'''

#duration of process to display
def get_duration_text(dur):

    #set time to proper units
    if dur >= 120 and dur < 7200: #minutes (2 to 119)
        dur = int(round(dur / 60)) 
        units = 'minutes'
    
    elif dur >= 7200: #hours (more than 120 minutes)
        dur = round((dur / 60) / 60, 2)
        units = 'hours'
    
    else: #seconds (up to 119)
        dur = int(round(dur))
        units = 'seconds'
        
    text = 'completed in ' + str(dur) + ' ' + units
    return text


#text at end of operation
def get_end_text(mode, inplace, indir, outdir, fast):
    
    if mode == 'bag':
        if inplace:
            text = 'bag saved inplace at ' + indir
        else:
            text = 'bag saved to ' + outdir
    if mode == 'unbag':
        if inplace:
            text = 'files unbagged in place at ' + indir
        else:
            text = 'files unbagged to ' + os.path.join(outdir, os.path.basename(indir))   
    if mode == 'validate':
        if fast:
            text = 'bag is valid!' + ' (fast scan)'
        else:
            text = 'bag is valid!'  
    if mode == 'update':
        text = 'updated bag: ' + indir
    
    return text
    
    
#Even text-based programs should be pretty
def print_logo(clean=False):
    
    if clean:
        logo = '    ____                 __    ____   ____ \n'
        logo += '   / _  \ ____  ________/ /__ / _  \ / _  \\\n'
        logo += '  /     //__  \/ _  /  /  __//     //     /\n'
        logo += ' /  _  \/  _  /__  /  /  /_ /  _  \/  _  \\\n'
        logo += '/______/\____/____/__/_____/______/______/'

    else:    
        logo = '\n' + TEXT_TYPE['col1'] + '    ____                 __   '+ TEXT_TYPE['col2'] + ' ____   ____ ' + TEXT_TYPE['col1'] + '\n'
        logo += '   / _  \ ____  ________/ /__ '+ TEXT_TYPE['col2'] + '/ _  \ / _  \\' + TEXT_TYPE['col1'] + '\n'
        logo += '  /     //__  \/ _  /  /  __/'+ TEXT_TYPE['col2'] + '/     //     /' + TEXT_TYPE['col1'] + '\n'
        logo += ' /  _  \/  _  /__  /  /  /_ '+ TEXT_TYPE['col2'] + '/  _  \/  _  \ ' + TEXT_TYPE['col1'] + '\n'
        logo += '/______/\____/____/__/_____'+ TEXT_TYPE['col2'] + '/______/______/ ' + TEXT_TYPE['none']

    return logo


#adjustable shorcut for communicating progress to user    
def get_status(msg, quiet, fin_text=None):

    if quiet: return
        
    #for devs to customize length, character used, etc.
    char = '.' #character used for status 'bar'
    length = 20 #max line length
    num_chars = length - len(msg) #number of chars in 'bar'
    bar_init = 3 * char #initial size of bar
    bar_fin = num_chars * char #size of bar at completion
        
    if fin_text == None:
        print(ERASE + '\r', end='', flush=True)
        print(msg + bar_init, end='', flush=True)
    elif type(fin_text) == str:
        print(ERASE + '\r', end='', flush=True)
        print(msg + bar_fin + fin_text)
    else:
        raise Exception('fin_text must be None or string')


#errors - gotta catch'em all!
def throw_error(error, except_type, quiet=True):
    
    #errors direct from bagit
    if hasattr(error, 'details'):
        new_msg = ''
        if len(error.details) > 0:        
            for d in error.details:        
                if isinstance(d, bagit.ChecksumMismatch):
                    new_msg += 'checksum mismatch: ' + d.path + '\n'
                if isinstance(d, bagit.FileMissing):
                    new_msg += 'file missing: ' + d.path + '\n'
            new_msg = new_msg.rstrip('\n')
        else:
            new_msg = error
    
    #errors from this program
    else:
        new_msg = error

    if quiet:
        raise except_type(new_msg)
    else:
        print(new_msg)
        exit()



'''*** Main ***'''

def Main():
    
    #create options
    (options, args) = setup_opts()
    
    #set output type
    quiet = True if options.quiet else False
    bagit_output = True if options.bagit_output else False

    #pretty logo
    if not quiet: print(print_logo())
    
    #version info
    if options.version:
        print('\nbagitBB v' + VERSION + ' (bagit.py v' + bagit.VERSION + ')')
        exit()
    else:
        print()
    
    #manage args, opts, and paths
    try:
        mode = get_mode(args)
        parsed_args = parse_args(args, options.inplace, mode)
        indirs, outdir = get_paths(parsed_args)
        check_paths(indirs, outdir, mode)
        validate_opts(options.alg, options.inplace, indirs[0])
    
    except OptError as e:
        throw_error(e, OptError, quiet=quiet)
    
    except FileNotFoundError as e:
        throw_error(e, FileNotFoundError, quiet=quiet)
    
    except FileExistsError as e:
        throw_error(e, FileExistsError, quiet=quiet)
    
    except Exception as e:
        throw_error(e, Exception)

    #setup metadata
    '''bag-info.txt metadata - this dict is ignored if json
    file used. Change this metadata if you'd like,
    but be sure to change optparse options to match.'''
    manual_fields = {
        'accession number': options.accession_num,
        'department': options.department,
        'contact name': options.contact_name,
        'contact title': options.contact_title,
        'contact email': options.contact_email,
        'contact phone': options.contact_phone,
        'contact address': options.contact_address,
        'records schedule number': options.records_schedule_num,
        'bag size': options.bag_size,
        'record dates': options.record_dates,
        'description': options.description,
        'notes': options.notes
    }    
            
    '''choose your own bag-venture!'''
    
    start = time.time() #start time
    
    #bagging ------------ /
    if mode == 'bag':
         
        bag_metadata = BagMetadata(manual_fields=manual_fields, json=options.json, quiet=quiet)
        bag_metadata.set_bag_metadata()

        bag = bag_files(
            indirs,
            outdir,
            options.alg,
            inplace = options.inplace,
            metadata = bag_metadata.metadata,
            processes = options.processes,
            quiet = quiet,
            fast = options.fast,
            bagit_output = bagit_output,
            sub_docs = bag_metadata.doc_list
        )
    
    #unbagging ---------- /
    elif mode == 'unbag':
        
        #don't unbag in place accidentily
        if options.inplace and not quiet:
            confirm_task('WARNING: unbagging in place will remove original bag - are you sure? (y/n)', 'unbag cancelled')
        
        make_unbag_file = True if options.copy_bag_files else False
        bag = BetterBag(indirs[0], processes=options.processes, quiet=quiet, bagit_output=bagit_output)
        bag.unbag(
            outdir,
            make_unbag_file = make_unbag_file,
            archivematica = options.archivematica,
            inplace = options.inplace,
            copy_bag_files = options.copy_bag_files,
            archivematica_manifest = options.no_manifest,
            fast = options.fast
        )
        
    #validating --------- /
    elif mode == 'validate':
        bag = BetterBag(indirs[0], processes=options.processes, quiet=quiet, bagit_output=bagit_output)
        bag.validate_bag(fast=options.fast)

    #updating ----------- /
    elif mode == 'update':
        
        #don't regenerate manifest accidentally
        if options.update_manifest and not quiet:
            confirm_task('WARNING: are you sure you want to overwrite current manifest? (y/n)', 'manifest regeneration cancelled')
        
        bag_metadata = BagMetadata(manual_fields=manual_fields, json=options.json, manifest_update=options.update_manifest, quiet=quiet)
        bag_metadata.set_bag_metadata()
        bag = BetterBag(indirs[0], processes=options.processes, quiet=quiet, bagit_output=bagit_output)
        bag.update_metadata(bag_metadata.metadata, manifests=options.update_manifest, fast=options.fast)
    
    dur = time.time() - start #duration of process
    
    #end of the line
    if not quiet:
        print()
        print(get_duration_text(dur))
        print(get_end_text(mode, options.inplace, indirs[0], outdir, options.fast))
            
                
if __name__ == '__main__':
    Main()
