# bagitBB - bagit, but better!

***Disclaimer***: I'm new to Github and programming; please be patient with me.  
***Disclaimer the second***: "Better" is neither a dig at bagit-python nor implication that this is a different, somehow better bagging standard. Just a name I found amusing that also references its expanded functionality.

***License Info***: Am actively looking into official open licenes, but for now, I retain copyright. I grant permission for anyone to use, alter, and distribute as they see fit, so long as any use is similarly
accessible and usable without restriction. Credit is appreciated, but not required. 

### Version 2.0.0

Simple python program that builds on the Library of Congress' [bagit-python library](https://github.com/LibraryOfCongress/bagit-python), allowing for bagging and unbagging to a target directory instead of just bagging-in-place. Meant to be more lightweight than GUI bagging programs like Library of Congress' [Bagger](https://github.com/LibraryOfCongress/bagger) and [AVPreserve's Exactly](https://www.weareavp.com/) (if that's even available anymore).

## Changelog (v2.0.0)
Big re-design. Made BetterBag class a subclass of bagit.Bag. Having easier access to its code stripped away a lot of duplicated code. Other main changes:

* Moved from using optparse to argpase
* User output now uses logging module instead of custom output
* Uses more bagit functionality to avoid code duplication (ex. for checksum generation, algorithms, etc.)
* Multiprocessing no longers closes and reopens with each directory being bagged, but stays open for the whole checksum generation process
* Changed mode select from positional argument to option
* Made bag and unbag log classes
* Removed custom exceptions
* Changed from using shutil's copytree to copy2 in a loop for better logging
* CSV versions of bag manifests created when bagging now
* Unbagging-in-place no longer validates after moving files
* /data folder given new name when unbagging-in-place to avoid conflicts if there are any subfolders also called "data"
* Various other changes

## Dependencies

[bagit-python](https://github.com/LibraryOfCongress/bagit-python) is required.  
Made and tested on Python version 3.10.6 (Linux) and 3.11.9 (Windows)

## Methodology

### Bagging
1) Generates checksums for original files (multiple sources can be used in one bag)
2) Copies original files to target folder
3) Bags copies using bagit-python
4) Compares bag manifest to that generated in step 1

Bag metadata can be added using either pre-set options or custom fields via a JSON file.
   
### Unbagging
1) Validates bag using bagit-python
2) Copies files from data folder to target folder
3) Generates checksum manifest of copied files and compares to bag manifest
4) Copies bag metadata (info files, manfiests) to target folder (in created subfolder)

Unbagging can be done using Archivematica folder structure, in which the payload is unbagged to an "/objects"
folder and the bag metadata is unbagged to a "/metadata" folder along with the checksum manifest.
See [Archivematica documentation](https://www.archivematica.org/en/docs/) for more details


## Other features

***Validates*** bags as per bagit-python.  

***Add metadata*** for bag-info.txt when making bags. Can use built-in metadata field options or custom fields via a JSON file.  

***Updates*** bags with new metadata and regenerates checksum manifests as per bagit-python.  

***Archivematica mode*** unbags to a target directory formatted for use with Artefactual's [Archivematica](https://www.archivematica.org/en/) software. Includes /objects and /metadata subfolders with payload copied to /objects and bag metadata copied to /metadata/submissionDocumentation folder.  

***In-place bagging/unbagging*** bags or unbags a folder right where it is without copying to a target folder. Bagging-in-place is default bagit-python functionality.

***Submission documentation*** (accession records, donor forms, etc.) can be transferred alongside the payload, stored with bagit.txt (ie in the parent folder of /data). Documents can be identified in the JSON metadata file. These files will be copied with the rest of the bag metadata files when unbagging.

## Installation
Clone repository or download python file and access via python3 /path/to/bagitbb.py.


## Usage

```    
bagitbb.py [options] [source path] [source path ...] [target path]

MODES
------

Bag:
Takes two+ positional arguments: [source path] [source path...] [path to save bag]
ex: bagitbb.py /path/to/folder1 /path/to/folder2 /path/to/target/folder

Unbag:
Takes two positional arguments: [bag path] [unbag path]
ex: bagitbb.py --archivematica --mode unbag /path/to/bag /path/to/unbag/folder

Validates bag using bagit-python.

	Takes one positional argument: [bag path]
	ex: bagitbb.py --mode validate /path/to/bag

Update:

	Takes one positinal argument: [bag path]
	ex: bagitbb.py -m update --regen --contact-name "Bob Bobberson" /path/to/bag

IN-PLACE BAGGING/UNBAGGING
---------------------------

   bagitbb.py -m bag -i /path/to/folder
   bagitbb.py -m unbag --in-place /path/to/bag

METADATA
---------

Bag metadata can be added when creating or updating a bag, either manually
(using pre-set options) or using custom fields in a JSON file:
{
    "field 1": "data1",
    "field 2": "data2"
}
Blank fields in JSON file will be ignored.

SUBMISSION DOCUMENTATION
--------------------------
To transfer submission docs, add a dictionary entry to a JSON file with key
"submission documentation":

"submission documentation": {
    "DROID report": "path/to/droid.csv",
	"Accession stuff": "path/to/accession.doc"
}
This can be added to the same JSON file used to add metadata.

OPTIONS AND ARGUMENTS
-------------------------
positional arguments:

  source                Directories or files your records are being bagged from (bagging) or the top
			directory of the bag (unbagging, validating, updating). Can have multiple
			sources when bagging.

  target                Directory where files are bagged or unbagged to. Unused if validating, updating,
			or bagging/unbagging-in-place.


options:

  -h, --help            show this help message and exit

  -m MODE, --mode MODE  Action being performed. Choose "bag" to make a bag (default), "unbag" to unbag
			an existing bag, validate" to validate an existing bag, or "update" to update
			the metadata or manifest of an existing bag.

  -i, --in-place        Bags or unbags files in-place (ie does not copy files to target). Bag-in-place
			is default bagit-python functionality.

  -a, --archivematica   Unbags in target directory structured for use with Artefactual Systems'
			Archivematica software. Made with Archivematica v1.16.0 in mind.
			See https://www.archivematica.org/en/docs/ for details.

  -j JSON, --json JSON  Import bag metadata for bag-info.txt from json file instead of using options.
			Metadata from options will be ignored. Can also be used to identify submission
			documentation using keyword "submission documentation".

  -q, --quiet           Hide progress updates. NOTE: you will not be prompted to confirm when
			unbagging-in-place or updating a bag manifest.

  -p PROCESSES, --processes PROCESSES
                        Number of parallel processes used to create, validate, or update bag, and to
			generate checksums for originals/copies.

  -f, --fast            Only compare total size and number of files when validating bags and copied
			files (ie no checksums).

  -r, --regen           Regenerate manifest when updating bag. Ignored if not using update mode.

  -x, --no-bag-files    Don't copy bag metadata when unbagging.

  -X, --no-manifest     Don't save a checksum manifest when using Archivematica mode.

  --version             Show version and exit.

Checksum Algorithms (default sha256):

  --sha3_512
  --sha3_224
  --shake_128
  --blake2s
  --shake_256
  --sha1
  --sha3_256
  --sha3_384
  --sha512
  --blake2b
  --md5
  --sha224
  --sha256
  --sha384

Optional Bag Metadata:
  --source-organization SOURCE_ORGANIZATION
  --organization-address ORGANIZATION_ADDRESS
  --contact-name CONTACT_NAME
  --contact-phone CONTACT_PHONE
  --contact-email CONTACT_EMAIL
  --external-description EXTERNAL_DESCRIPTION
  --external-identifier EXTERNAL_IDENTIFIER
  --bag-size BAG_SIZE
  --bag-group-identifier BAG_GROUP_IDENTIFIER
  --bag-count BAG_COUNT
  --internal-sender-identifier INTERNAL_SENDER_IDENTIFIER
  --internal-sender-description INTERNAL_SENDER_DESCRIPTION
  --bagit-profile-identifier BAGIT_PROFILE_IDENTIFIER

``` 
