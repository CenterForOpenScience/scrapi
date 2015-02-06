scrapi
======

[![Build Status](https://travis-ci.org/fabianvf/scrapi.svg?branch=develop)](https://travis-ci.org/fabianvf/scrapi)


## Getting started

- You will need to:
    - Install requirements.
    - Install Cassandra
    - Install consumers
    - Install rabbitmq

### Requirements

- Create and enter virtual environment for scrapi, and go to the top level project directory. From there, run 

```bash
$ pip install -r requirements.txt
```

and the python requirements for the project will download and install. 


### Installing Cassandra
_note: JDK 7 must be installed for cassandra to run_

#### Mac OSX

```bash
$ brew install cassandra
```

Now, just run 
```bash
$ cassandra
```

Or, if you'd like your cassandra session to be bound to your current session, run:
```bash
$ cassandra -f
```

and you should be good to go.


### Running the server

- Just run 

```bash
$ python server.py
```

from the scrapi/website/ directory, and the server should be up and running!


### Consumers

- To set up consumers for the first time, Just run

```bash
invoke init_consumers
```

and the consumers specified in the manifest files of the worker_manager, and their requirements, will be installed.

### Rabbitmq

#### Mac OSX

```bash
$ brew install rabbitmq
```

#### Ubuntu

```bash
$ sudo apt-get install rabbitmq-server
```


### Running the scheduler

- from the top-level project directory run:

```bash
$ invoke celery_beat
```

to start the scheduler, and 

```bash
$ invoke celery_worker
```

to start the worker.


### Testing

- To run the tests for the project, just type

```bash 
$ invoke test
```

and all of the tests in the 'tests/' directory will be run. 
