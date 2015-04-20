scrapi
======

[![Build Status](https://travis-ci.org/fabianvf/scrapi.svg?branch=develop)](https://travis-ci.org/fabianvf/scrapi)
[![Coverage Status](https://coveralls.io/repos/fabianvf/scrapi/badge.svg?branch=develop)](https://coveralls.io/r/fabianvf/scrapi?branch=develop)

## Getting started

- You will need to:
    - Install requirements.
    - Install Elasticsearch
    - Install Cassandra
    - Install harvesters
    - Install rabbitmq

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

Now, just run 
```bash
$ cassandra
$ elasticsearch
```

Or, if you'd like your cassandra session to be bound to your current session, run:
```bash
$ cassandra -f
```

and you should be good to go.

### Rabbitmq

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
