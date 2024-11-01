import bagit
import argparse
from pathlib import Path
import os
import shutil
import json
import time
import platform
import logging
import csv
import multiprocessing
import sys
from functools import partial
sys.setrecursionlimit(10**6) # shutil was crashing due to the default recursion cap

VERSION = '2.0.0'
ARCHIVEMATICA_URL = 'https://www.archivematica.org/en/docs/'
ARCHIVEMATICA_VERSION = '1.16.0'

#time formats
TIME_FMT_LOG = '%B' + ' ' + '%d' + ', ' + '%Y' + ' ' + '%H' + ':' + '%M' + ':' + '%S' + ' ' + '%z' #for bag logs
TIME_FMT_METADIR = '_' + '%H' + 'h' + '%M' + 'm' + '%S' + 's' #for bag metadata folder (when unbagging)

#auto-generated bag-info.txt metadata
BAG_INFO_VERSION = 'Bag-Software-Agent' #from bagit.py
BAG_INFO_DATE = 'Bagging-Date' #from bagit.py
BAG_INFO_SIZE = 'Payload-Oxum' #from bagit.py
BAG_INFO_VAL_TYPE = 'Fixity check type' #method of fixity validation
BAG_INFO_CHECKSUM = 'checksum validation' #using checksum for fixity
BAG_INFO_FAST = 'file number and size validation' #use size/quantity for fixity ("fast mode")
BAG_INFO_UPDATE = 'Bag info last updated' #last time bag metadata was updated
BAG_INFO_REGEN = 'Bag manifests last updated' #last time bag manifests were updated
SUB_DOC_KEY = 'submission documentation' #JSON key to identify submission docs

# order to choose alg in bags with multiple manifests
ALG_RANK = ['sha256', 'sha512', 'md5']

# log objects for each stage of process
LOGGER = logging.getLogger('bagitbb')
__doc__ = '''
 _____________________________________________
/     ____                 __    ____   ____  \\
|    / _  \ ____  ________/ /__ / _  \ / _  \ |
|   /     //__  \/ _  /  /  __//     //     / |
|  /  _  \/  _  /__  /  /  /_ /  _  \/  _  \  |
| /______/\____/____/__/_____/______/______/  |
|                                             |
|              Bagit, but better              |     
|            ©2024 Jarad Buckwold             |
\\_____________________________________________/

v''' + VERSION + '''

BagitBB expands on the Library of Congress' bagit-python module by bagging files into target
directories rather than just in-place, and similarily unbagging bags into target directories.
It can also do the standard bagit-python validating, bagging-in-place, and updating bag metadata.

Meant to be more light-weight than LoC's Bagger and AVP's Exactly GUI apps.

bagit-python is obviously required: https://github.com/LibraryOfCongress/bagit-python

MODES
------

Bag:
1) Generates checksums for original files (multiple sources can be used in one bag)
2) Copies original files to target folder
3) Bags copies using bagit-python
4) Compares bag manifest to that generated in step 1
Bag metadata can be added using either pre-set options or custom fields via a JSON file (see metadata section).

    ex: %(prog)s --sha512 -j /path/to/metadata.json -m bag /path/to/folder1 /path/to/folder2 /path/to/target/folder

Unbag:
1) Validates bag using bagit-python
2) Copies files from data folder to target folder
3) Generates checksum manifest of copied files and compares to bag manifest
4) Copies bag metadata (info files, manfiests) to target folder (in created subfolder)

Unbagging can be done using Archivematica folder structure, in which the payload is unbagged to an "/objects"
folder and the bag metadata is unbagged to a "/metadata" folder along with the checksum manifest.
See ''' + ARCHIVEMATICA_URL + ''' for more details.

    ex: %(prog)s --archivematica --mode unbag /path/to/bag /path/to/unbag/folder

Validate:
Validates bag using bagit-python.

    ex: %(prog)s --mode validate /path/to/bag

Update:
Updates bag metadata and/or bag manifest using bagit-python. JSON or options can be used (see metadata section).

    ex: %(prog)s -m update --regen --contact-name "Bob Bobberson" /path/to/bag

IN-PLACE BAGGING/UNBAGGING
---------------------------

Files can be bagged-in-place (default bagit-python behavior) and bags can be unbagged-in-place. Bagging-in-place results
in the original source being replaced by a bag (ie /path/to/folder becomes /path/to/bag). Unbagging-in-place validates
the bag, moves the files from /data to the root folder, moves the bag metadata into its own folder, and then deletes
the now empty /data folder.

    ex: %(prog)s -m bag -i /path/to/folder
    ex: %(prog)s -m unbag -i /path/to/bag

METADATA
---------

Bag metadata can be added when creating or updating a bag, either manually (using pre-set options) or using custom fields
in a JSON file:
{
    "field 1": "data1",
    "field 2": "data2"
}
Blank fields in JSON file will be ignored.


SUBMISSION DOCUMENTATION
--------------------------

Submission documentation (accession forms, file lists, etc.) can be copied alongside files when creating a bag. They are stored
at the root level of the bag (ie alongside bagit.txt). To transfer submission docs, add an entry to a JSON file with key
"''' + SUB_DOC_KEY + '''":

"''' + SUB_DOC_KEY + '''": {
    "DROID report": "path/to/droid.csv",
	"Accession stuff": "path/to/accession.doc"
}
This can be added to the same JSON file used to add metadata.

'''


'''*** Classes ***'''

# Extension of bagit.Bag - representation of bag object
class BetterBag(bagit.Bag):

    def __init__(self, path, quiet=True):
    
        super().__init__(path=path)
        self.quiet = quiet
        self.data_path = os.path.join(path, 'data')
        self.bag_name = os.path.basename(path)
        self.prime_alg = _prime_alg(self.algorithms)
    
    # in own method for the sake of decluttering
    def validate_bag(self, fast=False, processes=1):
    
        try:
            self.validate(fast=fast, processes=processes)
        except (bagit.BagError, bagit.BagValidationError) as e:
            _throw_log_err(e)
    
    # setup for unbagging-in-place
    def _inplace_setup(self, tmp):
    
        os.mkdir(tmp)
        new_data_dir = self.data_path + '_' + time.strftime(TIME_FMT_METADIR, time.localtime())
        # FIXME shouldn't the startswith account for this???
        os.rename(self.data_path, new_data_dir) # avoid conflict if you have a folder named 'data' in bag payload
        self.data_path = new_data_dir
        _copy_files(self.path, tmp, copy_type='move') # move bag metadata files to temp folder
    
    # copy files and metadata out of bag
    def unbag(self, outdir, processes=1, archivematica=False, copy_bag_files=False, fast=False, inplace=False, archivematica_manifest=True):

        times = {'began': time.localtime()}
        paths = self._set_unbag_paths(outdir, archivematica, inplace)
        LOGGER.info(self.prime_alg + ' algorithm chosen for validating file copies.')
               
        # validate first ---------------------------/
        _config_log('Validating Bag', self.quiet)
        self.validate_bag(fast=fast, processes=processes)
        times['bag_validated'] = time.localtime()
                
        # setting up dirs
        self._make_unbag_dirs(archivematica, paths, inplace)
        paths['tmp'] = self._tmp_dir() # avoid conflicts w/ bag files
        if inplace: self._inplace_setup(paths['tmp'])
        
        # checksums of payload files ---------------/
        if not fast:          
            old_manifest = Manifest(self.prime_alg, processes=processes)
            old_manifest.values = self.read_bag_manifest(self.prime_alg)
            
        # copying/moving files ---------------------/
        _config_log('Copying Files', self.quiet)
        _copy_files(self.data_path, paths['unbag'], recursive=True, copy_type='move' if inplace else 'copy')
        times['copied'] = time.localtime()
         
        # validate copies --------------------------/
        exclude = [paths['tmp'], self.data_path] if inplace else []
        if not inplace:
            if fast:
                _fast_compare(self.data_path, paths['unbag'], exclude=exclude)
            else:
                new_manifest = Manifest(self.prime_alg, processes=processes, quiet=self.quiet, exclude=exclude)
                prefix = '/objects/' if archivematica else '/'
                prefix = os.path.join(outdir, self.bag_name + prefix)
                path_tups = [(paths['unbag'], prefix)]
                new_manifest.gen(path_tups, status_msg='Validating Copies')            
                new_manifest.compare(old_manifest.values)    
        times['copies_validated'] = time.localtime()
        
        # create new checksum manifest compliant with Archivematica
        if archivematica and archivematica_manifest:
            old_manifest.write_to_text(paths['archivematica_manifest'], archivematica=True)

        # copy bag metadata ------------------------/
        _config_log('Bag Metadata', self.quiet)
        if copy_bag_files: 
            paths['bag_files'] = paths['bag_metadata'] + time.strftime(TIME_FMT_METADIR, time.localtime())
            self._copy_bag_files(paths, inplace)
            self._make_unbag_log(paths, times, inplace, fast)
        
    # info about unbagging processes
    def _make_unbag_log(self, paths, times, inplace, fast):
    
        unbag_log = UnbagLog(self.path, paths['unbag'], times, inplace=inplace, fast=fast)
        unbag_log = UnbagLog(self.path, paths['unbag'], times, inplace=inplace, fast=fast)
        unbag_log.write(os.path.join(paths['bag_files'], 'unbag-log.txt')) 
    
    # move bag metadata files to own directory
    def _copy_bag_files(self, paths, inplace=False):
    
        bag_metadata_path = paths['bag_metadata'] + time.strftime(TIME_FMT_METADIR, time.localtime())
        os.mkdir(bag_metadata_path)

        if inplace:
            _copy_files(paths['tmp'], bag_metadata_path, copy_type='move')
            shutil.rmtree(paths['tmp'])
            shutil.rmtree(self.data_path)
        else:
            _copy_files(self.path, bag_metadata_path)
    
    # make directories for unbagging
    def _make_unbag_dirs(self, archivematica, paths, inplace):
        
        if archivematica:                
            if not inplace: os.mkdir(paths['parent'])
            os.mkdir(paths['unbag'])
            os.mkdir(paths['archivematica_metadata'])
            os.mkdir(paths['submission_docs'])
        else:    
            if not inplace: os.mkdir(paths['unbag'])

    # used to exclude checksum validation for bag metadata when unbagging inplace
    def _tmp_dir(self):
    
        return os.path.join(self.path, 'tmp' + time.strftime(TIME_FMT_METADIR, time.localtime()))
    
    # make folder for bag metadata
    def _make_bag_metadata_folder(self, bag_metadata_path):
        
        bag_metadata_path += time.strftime(TIME_FMT_METADIR, time.localtime())
        os.mkdir(bag_metadata_path)
        return bag_metadata_path
    
    # setup paths for unbag method    
    def _set_unbag_paths(self, outdir, archivematica, inplace):
    
        paths = {}  
        unbag_path_init = self.path if inplace else os.path.join(outdir, self.bag_name)
        
        '''
        path descriptions:
        
        *parent: top level folder of unbagged content (archivematica mode only) 
        *unbag: place where original bagged data will be copied
            [parent]/objects for archivematica, same as parent for standard unbag
        *archivematica metadata: [parent]/metadata
        *submissionDocumentation: [parent]/metadata/submissionDocumentation
        *bag_metadata: in submissionDocumentation if archivematica, otherwise in unbag
        *archivematica_manifest: file saved in archivematica_metadata
        '''
        
        if archivematica:            
            paths['unbag'] = os.path.join(unbag_path_init, 'objects')
            paths['parent'] = unbag_path_init
            paths['archivematica_metadata'] = os.path.join(unbag_path_init, 'metadata')
            paths['submission_docs'] = os.path.join(paths['archivematica_metadata'], 'submissionDocumentation')
            paths['bag_metadata'] = os.path.join(paths['submission_docs'], self.bag_name + '_bagfiles')
            paths['archivematica_manifest'] = os.path.join(paths['archivematica_metadata'], 'checksum.' + self.prime_alg)
            
        else:    
            paths['unbag'] = unbag_path_init
            paths['bag_metadata'] = os.path.join(paths['unbag'], self.bag_name + '_bagfiles')
            paths['parent'] = ''
            paths['submission_docs'] = ''
            paths['archivematica_metadata'] = ''
            paths['archivematica_manifest'] = ''
            
        return paths
    
    # uses bag manifest instead of generating brand new checksums
    def read_bag_manifest(self, alg):

        manifest = []

        for path, fixity in self.entries.items():
            path = _normalize_sep(path)
            if path.startswith('data/'):
                path = path.replace('data/', '', 1)
                pair = (fixity[alg], path)
                manifest.append(pair)
                
        manifest.sort(key=lambda manifest: manifest[1]) # sort by filename
        return manifest

    # update bag metadata in bag-info.txt
    def update_metadata(self, metadata, processes=1, manifests=False, fast=False):
        
        '''appends bag-info.txt; new fields will overwrite old fields with same name
        (note that empty JSON fields are not counted and will therefore not overwrite
        anything)'''
        
        _config_log('Updating Bag', self.quiet)
        
        # update fixity check type if regenerating
        if manifests:
            if fast:
                metadata[BAG_INFO_VAL_TYPE] = BAG_INFO_FAST
            else:
                metadata[BAG_INFO_VAL_TYPE] = BAG_INFO_CHECKSUM
        
        # work out new metadata
        for key in metadata:
            self.info[key] = metadata[key]
        
        # update and save
        try:
            self.save(processes=processes, manifests=manifests)
        except (bagit.BagError, bagit.BagValidationError) as e:
            _throw_log_err(e)


# file list with corresponding checksums
class Manifest():

    def __init__(self, alg, processes=1, exclude=[], quiet=True):
    
        self.alg = alg
        self.processes = processes
        self.exclude = exclude
        self.values = []
        self.quiet = quiet    
        
    # file list generator
    def _file_list(self, path):
        
        for f in path:
            if os.path.isfile(f) and not any(p in str(f) for p in self.exclude):
                yield f

    # let bagit generate checksum
    def get_hash(self, filename, path_prefix='', status_msg=''):

        _config_log(status_msg, self.quiet)
        return (bagit.generate_manifest_lines(str(filename), algorithms=[self.alg]), path_prefix)
    
    # create a manifest
    def gen(self, path_tups, status_msg=''):
        
        '''Takes (path name, path prefix) tuple and gens checksums using bagit-python code,
        tacking the path prefix onto the end to create a (bagit-python list, path prefix)
        tuple. Path prefix is chopped off from the file path so that file paths from source
        are identical to those in target (ex: /source/path/dir1 and /target/path/dir1 both
        become /dir1).'''

        # generating checksum hashes w/ multiprocessing using bagit
        hash_list = []      
        pool = multiprocessing.Pool(processes=self.processes)
        for tup in path_tups:
            manifest_line_generator = partial(self.get_hash, path_prefix=tup[1], status_msg=status_msg)
            if os.path.isdir(tup[0]):
                hash_list += pool.map(manifest_line_generator, self._file_list(Path(tup[0]).rglob('*')))
            else:
                hash_list += pool.map(manifest_line_generator, [tup[0]])
        pool.close()
        pool.join()
        
        # sanitize list for manifest
        manifest = self._sanitize_manifest(hash_list)
        manifest.sort(key=lambda manifest: manifest[1]) # sort by filename
        self.values = manifest

    # remove filepath prefix and make list of (checksum, filepath) tuples
    def _sanitize_manifest(self, hash_list):
    
        manifest = []
        for val in hash_list:
            path_prefix = _normalize_sep(val[1])
            checksum = val[0][0][1]
            filename = _normalize_sep(val[0][0][2]).replace(path_prefix, '')
            manifest.append((checksum, filename))
        return manifest

    # compare two manifests
    def compare(self, target_values):
    
        # length check
        if len(target_values) != len(self.values):
            _throw_log_err('Number of copied files do not match source')
                
        # check for mismatches
        for i in range(len(target_values)):
            if self.values[i][0] != target_values[i][0] or self.values[i][1] != target_values[i][1]:
                _throw_log_err('Checksum mismatch: ' + str(self.values[i][1]))

    # save manifest to text file (ex: for archivematica)
    def write_to_text(self, file_path, str_chop='', archivematica=False, overwrite='w'):
        
        with open(file_path, overwrite) as text_file:    
            for v in self.values:
                v = list(v)
                if archivematica: v[1] = '../objects/' + v[1] 
                line = v[0] + ' ' + v[1] + '\n'
                text_file.write(line)       

    # save manifest to csv
    def write_to_csv(self, write_path, overwrite='w'):
    
        with open(write_path, overwrite, newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for v in self.values:
                writer.writerow(v)    


# creation of metadata and submission docs for bags
class BagMetadata():

    def __init__(self, manual_fields={}, json=None, manifest_update=False):
    
        self.manual_fields = manual_fields
        self.json = json
        self.manifest_update = manifest_update
        self.metadata = {}
        self.doc_list = []
        
    # setup metadata content for bag-info.txt
    def set_bag_metadata(self, ignore_sub_docs=False):

        if self.json != None:
            self._read_json()
            if SUB_DOC_KEY in self.metadata:
                if ignore_sub_docs:
                    self.metadata['submission documentation'] = ''
                else:
                    self._get_sub_docs()
                    self._fmt_sub_doc_text()
            self._parse_bag_metadata()
        
        elif len(self.manual_fields) != 0:
            self.metadata = self.manual_fields
            self._parse_bag_metadata()

    # list of submission documents to copy with bag
    def _get_sub_docs(self):

        sub_docs = []
        for key, value in self.metadata[SUB_DOC_KEY].items():
            if not os.path.isfile(value):
                raise FileNotFoundError('Submission document not found: ' + str(value))
            sub_docs.append(os.path.abspath(value))
        self.doc_list = sub_docs
        
    # reformat submission docs listed in json file for display in bag-info.txt
    def _fmt_sub_doc_text(self):  

        text = ''

        for x in self.metadata[SUB_DOC_KEY]:
            filename = str(os.path.basename(self.metadata[SUB_DOC_KEY][x]))
            text += x + ' (' + filename + ')'
            text += ', '
        text = text[:-2]
        self.metadata[SUB_DOC_KEY] = text

    # parse JSON file for bag-info.txt metadata
    def _read_json(self):

        with open(self.json, 'r') as json_file:
            self.metadata = json.loads(json_file.read())
                
    # remove metadata fields for bag-info.txt that user did not enter information for
    def _parse_bag_metadata(self):

        metadata_list = list(self.metadata)
        for x in range(len(self.metadata)):
            if self.metadata[metadata_list[x]] == None or self.metadata[metadata_list[x]] == '':
                del self.metadata[metadata_list[x]]

        # to document when the info in baginfo.txt and/or manifest was last updated
        if len(self.metadata) != 0:
            self.metadata['Bag info last updated'] = time.strftime(TIME_FMT_LOG, time.localtime())
        if self.manifest_update:
            self.metadata['Bag manifests last updated'] = time.strftime(TIME_FMT_LOG, time.localtime())


# log superclass
class Log():

    def __init__(self, src, target):

        self.src = src
        self.target = target
        self.fields = {'bagitBB version': VERSION, 'OS': platform.platform()}

    def write(self, save_path, array_indent=4):

        with open(save_path, 'w') as log: 
            for key, value in self.fields.items():
                if type(value) == list or type(value) == tuple or type(value) == set:
                    if len(value) > 1:
                        log.write(key + ':\n')
                        for item in value:
                            log.write(' ' * array_indent + str(item) + '\n')
                    else:
                        log.write(key + ': ' + value[0] + '\n')
                else:
                    log.write(key + ': ' + value + '\n')


# log for bagging
class BagLog(Log):

    def __init__(self, src, target, times, inplace=False, fast=False):
    
        super().__init__(src, target)
        self.inplace = inplace
        self.fast = fast
        self.times = times

        self.fields['bag name'] = os.path.basename(target)
        self.fields['origin'] = src
        self.fields['target'] = target if inplace else os.path.dirname(target)
        self.fields['in-place'] = 'Yes' if inplace else 'No'
        self.fields['validation type'] = 'Fast' if fast else 'checksum validation'
        self.fields['began'] = time.strftime(TIME_FMT_LOG, times['began'])
        self.fields['checksums generated'] = 'n/a' if fast or inplace else time.strftime(TIME_FMT_LOG, times['checksum_gen'])
        self.fields['files copied to target'] = 'n/a' if inplace else time.strftime(TIME_FMT_LOG, times['copied'])
        self.fields['bagged'] = time.strftime(TIME_FMT_LOG, times['bagged'])
        self.fields['copies validated'] = 'n/a' if inplace else time.strftime(TIME_FMT_LOG, times['validated']) 


# log for unbagging
class UnbagLog(Log):

    def __init__(self, src, target, times, inplace=False, fast=False):
    
        super().__init__(src, target)
        self.inplace = inplace
        self.fast = fast
        self.times = times
    
        self.fields['bag name'] = os.path.basename(src),
        self.fields['origin'] = os.path.dirname(src),
        self.fields['target'] = target if inplace else os.path.dirname(target),
        self.fields['in-place'] = 'Yes' if inplace else 'No',
        self.fields['validation type'] = 'Fast' if fast else 'checksum validation',
        self.fields['unbagging began'] = time.strftime(TIME_FMT_LOG, times['began']),
        self.fields['bag validated'] = time.strftime(TIME_FMT_LOG, times['bag_validated']),
        self.fields['copied to target'] = time.strftime(TIME_FMT_LOG, times['copied']),
        self.fields['copies validated'] = time.strftime(TIME_FMT_LOG, times['copies_validated'])


'''*** operational functions ***'''

# create bag
def bag_files(indirs, outdir, algs=['sha256'], inplace=False, metadata={}, processes=1, quiet=True, fast=False, sub_docs=[]):

    times = {'began': time.localtime()}
    
    # mandatory metadata for bag-info.txt
    metadata[BAG_INFO_VERSION] = 'bagit.py ' + bagit.VERSION + ' ' + bagit.PROJECT_URL + ' (via bagitbb.py v.' + VERSION + ')'
    metadata[BAG_INFO_VAL_TYPE] = BAG_INFO_FAST if fast else BAG_INFO_CHECKSUM

    # bagging-in-place -------------------------/
    if inplace:
        _config_log('Bagging', quiet)
        bag = _bag_inplace(indirs[0], algs, metadata, processes=processes, sub_docs=sub_docs)
        times['bagged'] = time.localtime()
        bag_log = BagLog(indirs[0], indirs[0], times, inplace=inplace, fast=fast)
        bag_log.write(os.path.join(indirs[0], 'bag-log.txt'))
        return bag

    os.mkdir(outdir)
    prime_alg = _prime_alg(algs)
    LOGGER.info(prime_alg + ' algorithm chosen for validating file copies.')
    
    # get checksums of originals ---------------/
    path_tups = [] # (path, path prefix to be removed)
    if not fast:
        for path in indirs:
            prefix = os.path.dirname(path) + '/'
            path_tups.append((path, prefix))
        old_manifest = Manifest(prime_alg, processes=processes, quiet=quiet)
        old_manifest.gen(path_tups, status_msg='Analyzing')
        times['checksum_gen'] = time.localtime()     
    
    # copy source files to destination dir -----/
    _config_log('Copying', quiet)
    for src in indirs:
        if os.path.isdir(src):
            target_dir = os.path.join(outdir, os.path.basename(src))
            compare_path = target_dir #used for fast compare
            recursive = True
        else:
            target_dir = outdir
            compare_path = os.path.join(target_dir, os.path.basename(src)) #used for fast compare
            recursive = False
        _copy_files(src, target_dir, recursive=recursive)
    times['copied'] = time.localtime()

    # fast compare -----------------------------/
    if fast:
        _config_log('Validating Copies', quiet)
        _fast_compare(src, compare_path)
        times['validated'] = time.localtime()
    
    # bag copied files -------------------------/
    _config_log('Bagging', quiet)
    bag = _bag_inplace(outdir, algs, metadata, processes=processes, sub_docs=sub_docs)
    times['bagged'] = time.localtime()
    
    # checksum validating bag copies -----------/
    if not fast:
        _config_log('Validating', quiet)         
        new_manifest = Manifest(bag.prime_alg, processes=processes)
        LOGGER.info('Comparing bag manifest to original checksums')
        new_manifest.values = bag.read_bag_manifest(prime_alg)
        new_manifest.compare(old_manifest.values)
        times['validated'] = time.localtime()

    # make baglog.txt
    bag_log = BagLog(indirs, outdir, times, inplace=inplace, fast=fast)
    bag_log.write(os.path.join(outdir, 'bag-log.txt'))
    
    return bag 


# create BetterBag from bagit.Bag
def _bag_inplace(path, algs, metadata, processes=1, sub_docs=[]):

    bag = BetterBag(bagit.make_bag(path, checksums=algs, bag_info=metadata, processes=processes).path)   
    
    # manifest to csv
    for alg in bag.algorithms: 
        manifest = Manifest(alg, processes=processes)
        manifest.values = bag.read_bag_manifest(alg)
        csv_path = os.path.join(path, 'manifest-' + alg + '.csv')
        manifest.write_to_csv(csv_path)
    
    # copy submission documentation
    if len(sub_docs) > 0:
        for doc in sub_docs:
            _copy_files(doc, bag.path)
    return bag


# use only '/' as separator (as per bagit standard)
def _normalize_sep(path):

    split = path.split(os.sep)
    path = ''
    for part in split: path += part + '/'
    path = path[:-1]
    return path


# which alg to use for validating copies if multiple are selected
def _prime_alg(algs):

    # if alg not in alg rank, use first encountered
    for alg in ALG_RANK:
        if alg in algs:
            return alg
    return algs[0]


# validate bags and copies using only file size and quantity (as per bagit-python option)
def _fast_compare(indir, outdir, exclude=[]):
    
    LOGGER.info('Validating copies (fast)')
    
    indir_size, indir_count = _get_file_details(indir)
    outdir_size, outdir_count = _get_file_details(outdir, exclude=exclude)        
        
    if indir_size != outdir_size or indir_count != outdir_count:
        err_msg = 'Expected ' + str(indir_count) + ' files and ' + str(indir_size) + ' bytes, but found ' + str(outdir_count) + ' files and ' + str(outdir_size) + ' bytes.'
        LOGGER.error(err_msg)


# get file size and quantity for fast validation option                
def _get_file_details(target, exclude=[]):

    count = 0
    size = 0
    
    if os.path.isdir(target):
        path = Path(target)
        for filename in path.rglob('*'):
            if os.path.isfile(filename):
                if len(exclude) == 0: # no excluded dirs/files to worry about
                    size += os.stat(filename).st_size
                    count += 1
                else:
                    if not any(p in str(filename) for p in exclude): # for excluding
                        size += os.stat(filename).st_size
                        count += 1
    elif os.path.isfile(target):
        count += 1
        size += os.stat(target).st_size        
    
    return size, count


# file copying
def _copy_files(src, dest, recursive=False, copy_type='copy'):

    if recursive:               
        _copy_recursive(src, dest, copy_type=copy_type)
        
    else:
        if os.path.isdir(src):
            path = Path(src)
            for filename in sorted(path.glob('*')):
                if os.path.isfile(filename):
                    if copy_type == 'copy':
                        LOGGER.info('Copying file ' + str(filename))
                        shutil.copy2(filename, dest)
                    elif copy_type == 'move':
                        LOGGER.info('Moving file ' + str(filename))
                        shutil.move(filename, dest)
        else:
            if copy_type == 'copy':
                LOGGER.info('Copying file ' + str(src))
                shutil.copy2(src, dest)
            elif copy_type == 'move':
                LOGGER.info('Moving file ' + str(src))
                shutil.move(src, dest)


# for recursive copying
def _copy_recursive(src, dest, copy_type='copy'):
    
    '''Yeah, I know I could use shutil.copytree(), but I wanted to be able
    to give status updates after each file copied. When something takes 10
    hours, I like to know where I'm at.'''
    
    count = 0
    src_path = Path(src)
    for f in src_path.rglob('*'):
        if os.path.isfile(f):
            split = str(os.path.dirname(f)).split(str(src))
            if split[1].startswith(os.sep):
                split[1] = split[1][1:]
            new_path = os.path.join(dest, split[1])
            dest_path = Path(new_path)
            dest_path.mkdir(parents=True, exist_ok=True)
            if copy_type == 'copy':
                LOGGER.info('Copying file ' + str(f))
                shutil.copy2(f, new_path)
            elif copy_type == 'move':
                LOGGER.info('Moving file ' + str(f))
                shutil.move(f, new_path)
            count += 1


# risky tasks require confirmation
def _confirm_task(question_text, cancel_text):
    
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
            


''' *** console opt and arg management *** '''

# setup argparse options
def _setup_opts():

    # use bagit parser to properly access metadata options
    parser = bagit.BagArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description = __doc__ + 'OPTIONS AND ARGUMENTS\n----------------------',
        epilog = 'Copyright 2024 Jarad Buckwold. Feel free to use, alter, and distribute as you see fit. Credit is nice, but not required.'
    )
    
    # positional args
    parser.add_argument(
        'source',
        nargs='+',
        help='Directories or files your records are being bagged from (bagging) or the top directory of the bag (unbagging, validating, updating). \
        Can have multiple sources when bagging.'
    )
    #FIXME
    # I can't figure out how to have an ambiguous pos arg followed by an optional pos arg, so this is just a place-holder for help menu w/ None value.
    # All path data is stored in source arg
    parser.add_argument(
        'target',
        nargs = '?',
        default = None,
        help='Directory where files are bagged or unbagged to. Unused if validating, updating, or bagging/unbagging-in-place.'
    )
    
    # stand-alone options
    parser.add_argument(
        '-m',
        '--mode',
        action = 'store',
        type = str,
        default = 'bag',
        dest = 'mode',
        help = 'Action being performed. Choose "bag" to make a bag (default), "unbag" to unbag an existing bag, "validate" to validate an existing bag, \
        or "update" to update the metadata or manifest of an existing bag.'
    )
    parser.add_argument(
        '-i',
        '--in-place',
        action = 'store_true',
        dest = 'inplace',
        help = 'Bags or unbags files in-place (ie does not copy files to target). Bag-in-place is default bagit-python functionality.'
    )
    parser.add_argument(
        '-a',
        '--archivematica',
        action = 'store_true',
        dest = 'archivematica',
        help = 'Unbags in target directory structured for use with Artefactual Systems\' Archivematica software. Made with Archivematica \
        v' + ARCHIVEMATICA_VERSION + ' in mind. See ' + ARCHIVEMATICA_URL + ' for details.'
    )
    parser.add_argument(
        '-j',
        '--json',
        action = 'store',
        type = str,
        dest = 'json',
        help = 'Import bag metadata for bag-info.txt from json file instead of using options. Metadata from options will be ignored. \
        Can also be used to identify submission documentation using keyword "' + SUB_DOC_KEY + '".'
    )
    parser.add_argument(
        '-q',
        '--quiet',
        action = 'store_true',
        default = False,
        dest = 'quiet',
        help = 'Hide progress updates. NOTE: you will not be prompted to confirm when unbagging-in-place or updating a bag manifest.'
    )
    parser.add_argument(
        '-p',
        '--processes',
        action = 'store',
        type = int,
        default = 1,
        help = 'Number of parallel processes used to create, validate, or update bag, and to generate checksums for originals/copies.'
    )
    parser.add_argument(
        '-f',
        '--fast',
        action = 'store_true',
        default = False,
        help = 'Only compare total size and number of files when validating bags and copied files (ie no checksums).'
    )
    parser.add_argument(
        '-r',
        '--regen',
        action = 'store_true',
        default = False,
        dest = 'update_manifest',
        help = 'Regenerate manifest when updating bag. Ignored if not using update mode.'
    )
    parser.add_argument(
        '-x',
        '--no-bag-files',
        action = 'store_false',
        default = True,
        dest = 'copy_bag_files',
        help = 'Don\'t copy bag metadata when unbagging.'
    )
    parser.add_argument(
        '-X',
        '--no-manifest',
        action = 'store_false',
        default = True,
        dest = 'no_manifest',
        help = 'Don\'t save a checksum manifest when using Archivematica mode.'
    )
    parser.add_argument(
        '--version',
        action = 'version',
        version = '%(prog)s ' + VERSION,
        dest = 'version',
        help = 'Show version and exit.'
    )
    
    # algorithm options
    alg_group = parser.add_argument_group('Checksum Algorithms (default sha256)')
    for alg in bagit.CHECKSUM_ALGOS:
        alg_group.add_argument(
            '--' + alg,
            action = 'append_const',
            const = alg,
            dest = 'algs'
         )
    
    # metadata options   
    metadata_args = parser.add_argument_group('Optional Bag Metadata')
    for header in bagit.STANDARD_BAG_INFO_HEADERS:
        metadata_args.add_argument(
            '--%s' % header.lower(), type=str, action=bagit.BagHeaderAction, default=argparse.SUPPRESS
        )

    return parser


# make sure not bagging/unbagging-in-place the dir this program is in
def _safe_inplace(parser, indir):

    this_file_path = os.path.realpath(__file__)
    this_file_name = os.path.basename(this_file_path)
    if os.path.isfile(os.path.join(indir, this_file_name)):
        parser.error('Cannot bag/unbag-in-place directory containing ' + this_file_name)


# determine operation
def _get_mode(arg, parser):

    mode = None
    valid_modes = ['bag', 'unbag', 'validate', 'update']   
    for m in valid_modes:
        if arg.lower() == m:
            mode = m
    if mode == None: parser.error('Invalid mode.')
    return mode


# assign source and target paths
def _get_paths(mode, paths, inplace, parser):

    for i in range(len(paths)):
        paths[i] = os.path.abspath(paths[i])

    if mode == 'bag' and not inplace:
        if len(paths) < 2: parser.error('Wrong number of arguments.')
        outdir = paths[-1]
        del paths[-1]
        indirs = paths
        _find_path_err(outdir, False, parser)
        _find_path_err(os.path.dirname(outdir), True, parser)
    
    elif mode == 'unbag' and not inplace:
        if len(paths) != 2: parser.error('Wrong number of arguments.')
        outdir = paths[-1]
        del paths[-1]
        indirs = paths 
        _find_path_err(os.path.join(outdir, os.path.basename(indirs[0])), False, parser)
        _find_path_err(outdir, True, parser)
    
    elif mode == 'validate' or mode == 'update' or inplace:
        if len(paths) != 1: parser.error('Wrong number of arguments.')
        indirs = paths
        outdir = None

    for i in indirs:
        _find_path_err(i, True, parser, file_ok=True if mode=='bag' else False)

    return indirs, outdir
    

# if paths not found or already exist when they shouldn't
def _find_path_err(path, should_exist, parser, file_ok=False):

    if should_exist:
        if file_ok:
            if not os.path.exists(path): parser.error('Path not found: ' + str(path))
        else:
            if not os.path.isdir(path): parser.error('Path not found: ' + str(path))
    else:
        if os.path.exists(path): parser.error('Path already exists: ' + str(path))



'''*** output functions ***'''

# duration of process to display
def _get_duration_text(dur):

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
        
    text = 'completed ' + time.strftime(TIME_FMT_LOG, time.localtime()) + ' (' + str(dur) + ' ' + units + ')'
    return text


# text at end of operation
def _get_end_text(mode, inplace, indir, outdir, fast):
    
    if mode == 'bag':
        if inplace:
            text = 'bag saved in-place at ' + indir
        else:
            text = 'bag saved to ' + outdir
    if mode == 'unbag':
        if inplace:
            text = 'files unbagged in-place at ' + indir
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


# in-process errors - NOT for setup issues (input errors, json issues, etc.)
def _throw_log_err(error):

    # errors direct from bagit
    if hasattr(error, 'details'):
        new_msg = ''
        if len(error.details) > 0:        
            new_msg += '\n'
            for d in error.details:        
                if isinstance(d, bagit.ChecksumMismatch):
                    new_msg += '    checksum mismatch: ' + d.path + '\n'
                if isinstance(d, bagit.FileMissing):
                    new_msg += '    file missing: ' + d.path + '\n'
            new_msg = new_msg.rstrip('\n')
        else:
            new_msg = error
    
    # errors from this program
    else:
        new_msg = error

    LOGGER.error(new_msg)
    exit()


# configure logging output style
def _config_log(stage, quiet):

    datefmt = '%Y-%m-%d %H:%M:%S'
    log_format = '%(asctime)s - %(levelname)s - ' + stage + ' - %(message)s'
    level = logging.ERROR if quiet else logging.INFO
    logging.basicConfig(level=level, datefmt=datefmt, format=log_format, force=True)


'''*** Main ***'''

def Main():
    
    # create options/args
    parser = _setup_opts()
    options = parser.parse_args()   
    
    # var setup and validation 
    mode = _get_mode(options.mode, parser)
    indirs, outdir = _get_paths(mode, options.source, options.inplace, parser)
    if options.inplace: _safe_inplace(parser, indirs[0])
    if options.processes < 0: parser.error('Processes must be a positive integer.')
    if options.json is not None and not os.path.isfile(options.json):
        parser.error('JSON file does not exist: ' + options.json)
    algs = ['sha256'] if options.algs == None else options.algs

    _config_log('Initializing', options.quiet)
    start = time.time() # start time
      
    # bagging ------------ /
    if mode == 'bag':        
        bag_metadata = BagMetadata(manual_fields=options.bag_info, json=options.json)
        try:
            bag_metadata.set_bag_metadata()
        except json.JSONDecodeError as e:
            if options.quiet: raise e
            else: print('JSON ERROR: ' + str(e)); exit()
        except FileNotFoundError as e:
            if options.quiet: raise e
            else: print('ERROR: ' + str(e)); exit()

        bag = bag_files(indirs, outdir, algs=algs, inplace=options.inplace,
                        metadata=bag_metadata.metadata, processes=options.processes,
                        quiet=options.quiet, fast=options.fast, sub_docs=bag_metadata.doc_list)
    
    # unbagging ---------- /
    elif mode == 'unbag':       
        #don't unbag-in-place accidentily
        if options.inplace and not options.quiet:
            _confirm_task('WARNING: unbagging-in-place will remove original bag - are you sure? (y/n)', 'unbag cancelled')        
        make_unbag_file = True if options.copy_bag_files else False
        LOGGER.info('Opening Bag')
        try:
            bag = BetterBag(indirs[0], quiet=options.quiet)
        except (bagit.BagError, bagit.BagValidationError) as e:
            if options.quiet: raise e
            else: print('ERROR: ' + str(e)); exit()
        bag.unbag(outdir, archivematica=options.archivematica, inplace=options.inplace,
                copy_bag_files=options.copy_bag_files, processes=options.processes,
                archivematica_manifest=options.no_manifest, fast = options.fast)
        
    # validating --------- /
    elif mode == 'validate':
        LOGGER.info('Opening Bag')
        try:
            bag = BetterBag(indirs[0], quiet=options.quiet)
        except (bagit.BagError, bagit.BagValidationError) as e:
            _throw_log_err(e)
        _config_log('Validating Bag', options.quiet)
        bag.validate(fast=options.fast, processes=options.processes)

    # updating ----------- /
    elif mode == 'update':
        #don't regenerate manifest accidentally
        if options.update_manifest and not options.quiet:
            _confirm_task('WARNING: are you sure you want to overwrite current manifest? (y/n)', 'manifest regeneration cancelled')    
        bag_metadata = BagMetadata(manual_fields=options.bag_info, json=options.json, manifest_update=options.update_manifest)
        try:
            bag_metadata.set_bag_metadata(ignore_sub_docs=True)
        except json.JSONDecodeError as e:
            if options.quiet: raise e
            else: print('JSON ERROR: ' + str(e)); exit()
        
        LOGGER.info('Opening Bag')
        try:
            bag = BetterBag(indirs[0], quiet=options.quiet)
        except (bagit.BagError, bagit.BagValidationError) as e:
            _throw_log_err(e)
        bag.update_metadata(bag_metadata.metadata, processes=options.processes, manifests=options.update_manifest, fast=options.fast)
    
    dur = time.time() - start #duration of process
    
    # end of the line
    if not options.quiet: print()
    print(_get_duration_text(dur))
    print(_get_end_text(mode, options.inplace, indirs[0], outdir, options.fast))
            
                
if __name__ == '__main__':
    Main()
