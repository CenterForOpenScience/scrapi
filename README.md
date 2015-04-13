scrapi
======

[![Build Status](https://travis-ci.org/fabianvf/scrapi.svg?branch=develop)](https://travis-ci.org/fabianvf/scrapi)
[![Coverage Status](https://coveralls.io/repos/fabianvf/scrapi/badge.svg?branch=develop)](https://coveralls.io/r/fabianvf/scrapi?branch=develop)

## Getting started

- To run absolutely everyting, you will need to:
    - Install requirements.
    - Install Elasticsearch
    - Install Cassandra
    - Install harvesters
    - Install rabbitmq
    - 
- To only run harvesters locally, you do not have to install rabbitmq


### Requirements

- Create and enter virtual environment for scrapi, and go to the top level project directory. From there, run 

```bash
$ pip install -r requirements.txt
```

and the python requirements for the project will download and install.


### Installing Cassandra and Elasticsearch
_note: JDK 7 must be installed for Cassandra and Elasticsearch to run_

#### Mac OSX

```bash
$ brew install cassandra
$ brew install elasticsearch
```

#### Ubuntu
##### Install Cassandra
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
   $ sudo apt-get install dsc20=2.0.11-1 cassandra=2.0.11
   ```

##### Install ElasticSearch
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


__Now, just run__
```bash
$ cassandra
$ elasticsearch
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

You will need to have a local copy of the settings

```
cp scrapi/settings/local-dist.py scrapi/settings/local.py
```

(Note: only needed if NOT running locally!)
### Running the scheduler

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

Invove a harvester a certain number of days back with the ```--days``` argument. For example, to run a harvester 5 days in the past, run:

```bash
$ invoke harvester harvester-name --days=5
```

### Testing

- To run the tests for the project, just type

```bash 
$ invoke test
```

and all of the tests in the 'tests/' directory will be run. 
