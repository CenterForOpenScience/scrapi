scrapi
======

```master``` build status: [![Build Status](https://travis-ci.org/CenterForOpenScience/scrapi.svg?branch=master)](https://travis-ci.org/CenterForOpenScience/scrapi)


```develop``` build status: [![Build Status](https://travis-ci.org/CenterForOpenScience/scrapi.svg?branch=develop)](https://travis-ci.org/CenterForOpenScience/scrapi)


[![Coverage Status](https://coveralls.io/repos/CenterForOpenScience/scrapi/badge.svg?branch=develop)](https://coveralls.io/r/CenterForOpenScience/scrapi?branch=develop)
[![Code Climate](https://codeclimate.com/github/CenterForOpenScience/scrapi/badges/gpa.svg)](https://codeclimate.com/github/CenterForOpenScience/scrapi)

## Getting started

- To run absolutely everyting, you will need to:
    - Install requirements
    - Install Elasticsearch
    - Install Cassandra
    - Install harvesters
    - Install rabbitmq (optional)
- To only run harvesters locally, you do not have to install rabbitmq


### Requirements

- Create and enter virtual environment for scrapi, and go to the top level project directory. From there, run

```bash
$ pip install -r requirements.txt
```
Or, if you'd like some nicer testing and debugging utilities in addition to the core requirements, run
```bash
$ pip install -r dev-requirements.txt
```

This will also install the core requirements like normal.

### Installing Elasticsearch

Elasticsearch is required only if "elasticsearch" is specified in your settings, or if RECORD_HTTP_TRANSACTIONS is set to ```True```.

_Note: Elasticsearch requires JDK 7._

#### Mac OSX

```bash
$ brew install elasticsearch
```

#### Ubuntu

1. Download and install the Public Signing Key.
   ```bash
   $ wget -qO - https://packages.elasticsearch.org/GPG-KEY-elasticsearch | sudo apt-key add -
   ```

2. Add the ElasticSearch repository to yout /etc/apt/sources.list.
   ```bash
   $ sudo add-apt-repository "deb http://packages.elasticsearch.org/elasticsearch/1.4/debian stable main"
   ```

3. Install the package
   ```bash
   $ sudo apt-get update
   $ sudo apt-get install elasticsearch
```

#### Running

```bash
$ elasticsearch
```

### Installing Cassandra

Cassandra is required only if "cassandra" is specified in your settings, or if RECORD_HTTP_TRANSACTIONS is set to ```True```.

_Note: Cassandra requires JDK 7._

#### Mac OSX

```bash
$ brew install cassandra
```

#### Ubuntu

1. Check which version of Java is installed by running the following command:
   ```bash
   $ java -version
   ```
   Use the latest version of Oracle Java 7 on all nodes.

2. Add the DataStax Community repository to the /etc/apt/sources.list.d/cassandra.sources.list
   ```bash
   $ echo "deb http://debian.datastax.com/community stable main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
   ```

3.  Add the DataStax repository key to your aptitude trusted keys.
    ```bash
    $ curl -L http://debian.datastax.com/debian/repo_key | sudo apt-key add -
    ```

4. Install the package.
   ```bash
   $ sudo apt-get update
   $ sudo apt-get install cassandra
   ```

#### Running

```bash
$ cassandra
```


Or, if you'd like your cassandra session to be bound to your current session, run:
```bash
$ cassandra -f
```

and you should be good to go.

(Note, if you're developing locally, you do not have to run Rabbitmq!)
### Rabbitmq (optional)

#### Mac OSX

```bash
$ brew install rabbitmq
```

#### Ubuntu

```bash
$ sudo apt-get install rabbitmq-server
```
### Settings

You will need to have a local copy of the settings. Copy local-dist.py into your own version of local.py -

```
cp scrapi/settings/local-dist.py scrapi/settings/local.py
```

If you installed Cassandra and Elasticsearch earlier, you will want add the following configuration to your local.py:
```python
RECORD_HTTP_TRANSACTIONS = True  # Only if cassandra is installed

NORMALIZED_PROCESSING = ['cassandra', 'elasticsearch']
RAW_PROCESSING = ['cassandra']
```
Otherwise, you will want to make sure your local.py has the following configuration:
```python
RECORD_HTTP_TRANSACTIONS = False

NORMALIZED_PROCESSING = ['storage']
RAW_PROCESSING = ['storage']
```
This will save all harvested/normalized files to the directory ```archive/<source>/<document identifier>```

_note: Be careful with this, as if you harvest too many documents with the storage module enabled, you could start experiencing inode errors_

If you'd like to be able to run all harvesters, you'll need to [register for a PLOS API key](http://api.plos.org/registration/).

Add the following line to your local.py file:
```
PLOS_API_KEY = 'your-api-key-here'
```

### Running the scheduler (optional)

- from the top-level project directory run:

```bash
$ invoke beat
```

to start the scheduler, and

```bash
$ invoke worker
```

to start the worker.


### Harvesters
Run all harvesters with

```bash
$ invoke harvesters
```

or, just one with

```bash
$ invoke harvester harvester-name
```

For local development, running the ```mit``` harvester is recommended.

Note: harvester-name is the same as the defined harvester "short name".

Invoke a harvester for a certain start date with the ```--start``` or ```-s```argument. Invoke a harvester for a certain end date with the ```--end``` or ```-e```argument.

For example, to run a harvester between the dates of March 14th and March 16th 2015, run:

```bash
$ invoke harvester harvester-name --start 2015-03-14 --end 2015-03-16
```

Either --start or --end can also be used on their own. Not supplying arguments will default to starting the number of days specified in ```settings.DAYS_BACK``` and ending on the current date.

If --end is given with no --start, start will default to the number of days specified in ```settings.DAYS_BACK``` before the given end date.


### Automated OAI PMH Harvester Creation
Writing a harvester for inclusion with scrAPI?  If the provider makes their metadata available using the OAI-PMH standard, then [autooai](https://github.com/erinspace/autooai) is a utility that will do most of the work for you.


### Working with the OSF

To configure scrapi to work in a local OSF dev environment:

1. Ensure `'elasticsearch'` is in the `NORMALIZED_PROCESSING` list in `scrapi/settings/local.py`
1. Run at least one harvester
1. Configure the `share_v2` alias
1. Generate the provider map

#### Aliases

Multiple SHARE indices may be used by the OSF. By default, OSF uses the ```share_v2``` index. Activate this alias by running:

```bash
$ inv alias share share_v2
```

Note that aliases must be activated before the provider map is generated.

#### Provider Map

```bash
$ inv alias share share_v2
$ inv provider_map 
```

#### Delete the Elasticsearch index

To remove both the ```share``` and ```share_v2``` indices from elasticsearch:

```bash
$ curl -XDELETE 'localhost:9200/share*'
```

### Testing

- To run the tests for the project, just type

```bash
$ invoke test
```

and all of the tests in the 'tests/' directory will be run.


### Pitfalls

#### Installing with anaconda
If you're using anaconda on your system at all, using pip to install all requirements from scratch from requirements.txt and dev-requirements.txt results in an Import Error when invoking tests or harvesters.

Example:

ImportError: dlopen(/Users/username/.virtualenvs/scrapi2/lib/python2.7/site-packages/lxml/etree.so, 2): Library not loaded: libxml2.2.dylib
  Referenced from: /Users/username/.virtualenvs/scrapi2/lib/python2.7/site-packages/lxml/etree.so
  Reason: Incompatible library version: etree.so requires version 12.0.0 or later, but libxml2.2.dylib provides version 10.0.0

To fix:
- run ```pip uninstall lxml```
- remove the anaconda/bin from your system path in your bash_profile
- reinstall requirements as usual

Answer found in [this stack overflow question and answer](http://stackoverflow.com/questions/23172384/lxml-runtime-error-reason-incompatible-library-version-etree-so-requires-vers)
