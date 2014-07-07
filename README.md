scrapi
======

## Getting started

- You will need to:
    - Install Elasticsearch
    - Install requirements.

### Installing Elasticsearch
_note: Oracle JDK 7 must be installed for elasticsearch to run_

#### Mac OSX

```bash
$ brew install elasticsearch
```

Now, just run 
```bash
$ elasticsearch
```
and you should be good to go.

#### Ubuntu 

````bash
$ wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.2.1.deb 
$ sudo dpkg -i elasticsearch-1.2.1.deb
```

Now, just run 
```bash
$ sudo service elasticsearch start
```
and you should be good to go.

### Requirements

- Now that you have Elasticsearch installed, create and enter virtual environment for scrapi, and go to the top level project directory. From there, run 

```bash
$ invoke requirements
```

and the python requirements for the project will download and install. 

### Running the server

- Just run 

```bash
$ invoke server
```

and the server should be up and running!

### Testing

- To run the tests for the project, just type

```bash 
$ invoke test
```

and all of the tests in the 'tests/' directory will be run. 
