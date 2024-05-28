# bagitBB - bagit, but better!

***Disclaimer***: I'm new to Github and programming; please be patient with me.  
***Disclaimer the second***: "Better" is neither a dig at bagit-python nor implication that this is a different, somehow better bagging standard. Just a name I found amusing that also references its expanded functionality.

### Version 1.1

Simple python program that builds on the Library of Congress' [bagit-python library](https://github.com/LibraryOfCongress/bagit-python), allowing for bagging and unbagging to target directory instead of just bagging in place. Meant to be more lightweight than gui bagging programs like Library of Congress' [Bagger](https://github.com/LibraryOfCongress/bagger) and [AVPreserve's Exactly](https://www.weareavp.com/) (if that's even available anymore).

## Dependencies

[bagit-python](https://github.com/LibraryOfCongress/bagit-python) is required.  
Made and tested on Python version 3.10.6 (Linux) and 3.11.9 (Windows)

## Methodology
### Bagging
1) Generate checksum manifest for files in source directory using hashlib, stored as list of (path, checksum) tuples
2) Copy source files to target directory using shutil
3) Bag copies using bagit-python
4) Compare checksum manifest generated in step 1 to bag manifest to confirm integrity of copied files

### Unbagging
1) Validate bag using bagit-python
2) Extract checksums from bag manifest to a list of (path, checksum) tuples
3) Copy bag payload in /data to target directory
4) Geneate new checksum manifest for unbagged files in target directory using hashlib
5) Compare checksum manifest values to confirm integrity of copied files
6)Copy bag metadata (bagit.txt, bag-info.txt, manifests, etc.) to newly created sub-directory in target


## Other features

***Validates*** bags as per bagit-python.  

***Add metadata*** for bag-info.txt when making bags. Can use built-in metadata field options or custom fields via a JSON file.  

***Updates*** bags with new metadata and regenerates checksum manifests as per bagit-python.  

***Archivematica mode*** unbags to a target directory formatted for use with Artefactual's [Archivematica](https://www.archivematica.org/en/) software. Includes /objects and /metadata subfolder with payload copied to /objects and bag metadata copied to /submissionDocumentation folder within /metadata.  


## Installation
Clone repository and access via python3 /path/to/bagitbb.py


## Usage

```    
bagitbb.py [options] [mode] [input dir1] [input dir2] [...] [output dir]

Bags files using Library of Congress' Bagit python module,  
but can bag files to a target directory instead of just
bagging them in place. Can similarly unbag to target directory.
In both cases, checksums are generated prior to copying files
and then compared to those generated from the copied files in
the target folder, ensuring file integrity.

MODES:
bag  
    Bags one or more folders or files to target folder.  
    ex: bagitbb.py --accession-number A2020-335 bag /home/folder1 /home/file1.file /home/bags/bag1
unbag  
    Unbags preexisting bag and bag metadata to target folder.  
    ex: bagitbb.py unbag /home/bags/bag1 /home/unbagged_files
validate  
    Validates integrity of existing bag.  
    ex: bagitbb.py validate /home/bags/bag1
update  
    Updates metadata in bag-info.txt (fields with same names will be overwriten).
    Regenerates manifests if regen option is used.  
    ex: bagitbb.py -j /path/to/json.json update /path/to/bag1

NOTE: for bash shell (not sure about others), wildcard * character can
be used for bagging to target (NOT for anything else) only if there are
NO loose files in the base directory.

Options:

  -h, --help            show this help message and exit

  -a ALG, --algorithm=ALG
                        Algorithm used to generate checksums both for copied  
                        files and for bag. Choose either sha256 (default) or  
                        md5.

  -i, --inplace         Bags or unbags files in place (ie does not copy files  
                        to target). Bag in place is default bagit.py  
                        functionality.

  -A, --archivematica   Unbags in target directory structured for use with  
                        Artefactual Systems' Archivematica software. Made with  
                        Archivematica v1.14.1 in mind. See  
                        https://www.archivematica.org/en/docs/ for details.

  -j JSON, --json=JSON  Import bag metadata for bag-info.txt from json file  
                        instead of using options. Metadata from options will  
                        be ignored. ex: -j /path/to/metadata.json

  -q, --quiet           Hide progress updates. Errors will raise exceptions  
                        instead of messages. NOTE: you will not be prompted to  
                        confirm when unbagging in place or updating a bag  
                        manifest.

  -v, --version         Show version (ignores other options/args).

  -p PROCESSES, --processes=PROCESSES

                        Number of parallel processes used to create, validate,  
                        or update bag. Original bagit.py option. Ex. -p 8
  
  -f, --fast            Only compare total size and number of files when  
                        validating bags and copied files. Original bagit.py  
                        option.

  -x, --no-bag-files    Don't copy bag metadata when unbagging.

  -r, --regen           Regenerate manifest when updating bag. Ignored if not  
                        using update mode.

  -X, --no-manifest     Don't create a checksum manifest for metadata folder.  
                        For use when unbagging for Archivematica, otherwise  
                        ignored.

  --bagit-output        Show output from bagit.py. Quiet mode supersedes this  
                        option.

  --doc-list=DOC_LIST   Include documentation alongside bag metadata files (ie  
                        outside of /data with bagit.txt). Enter file paths in  
                        square brackets separated a comma (no extra spaces):  
                        [/path/to/doc1.txt,path/2.txt]

  --doc-file=DOC_FILE   Same as --doc-list except an external file is used to  
                        identify submission docs. File should be unformatted  
                        text with the path to one submission document per  
                        line. Ex. --doc-file /path/to/files.txt

  metadata fields:

    Fields used to record metadata in baginfo.txt document. Only used for  
    bagging and bagging in place and ignored if json option is used.  
    ex: --accession-num A2010-34 --notes "notes go here"

    --accession-number=ACCESSION_NUM  
    --department=DEPARTMENT  
    --contact-name=CONTACT_NAME  
    --contact-title=CONTACT_TITLE  
    --contact-email=CONTACT_EMAIL  
    --contact-phone=CONTACT_PHONE  
    --contact-address=CONTACT_ADDRESS  
    --records-schedule-number=RECORDS_SCHEDULE_NUM  
    --bag-size=BAG_SIZE  
    --record-dates=RECORD_DATES  
    --description=DESCRIPTION  
    --notes=NOTES       

``` 
