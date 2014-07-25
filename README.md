scrapi
======

## Getting started

- You will need to:
    - Install requirements.
    - Install Elasticsearch
    - Install consumers
    - Install rabbitmq

### Requirements

- Create and enter virtual environment for scrapi, and go to the top level project directory. From there, run 

```bash
$ pip install -r requirements.txt
```

and the python requirements for the project will download and install. 


### Installing Elasticsearch
_note: JDK 7 must be installed for elasticsearch to run_

#### Mac OSX

```bash
$ brew install elasticsearch
```

Now, just run 
```bash
$ elasticsearch
```

or 

```bash
$ invoke elasticsearch
```

and you should be good to go.

#### Ubuntu 

```bash
$ wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.2.1.deb 
$ sudo dpkg -i elasticsearch-1.2.1.deb
```

Now, just run 
```bash
$ sudo service elasticsearch start
```

or 

```bash
$ invoke elasticsearch
```

and you should be good to go.

### Running the server

- Just run 

```bash
$ invoke server
```

and the server should be up and running!


### Consumers

- Just run

```bash
$ invoke install_consumers
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
