# Backend for the Witnet Explorer
This repository contains all code to run the backend for the Witnet Explorer. The explorer is currently hosted at [witnet.network](https://witnet.network).

## Dependencies

You will have to install following dependencies to run the explorer locally:
```
git python3 python3-pip python3-virtualenv python3-psycopg2 nginx gunicorn postgresql postgresql-contrib libpq-dev
```

It is advised to start the explorer from within a virtual environment and you'll have to install following dependencies in the environment:
```
virtualenv env
source env/bin/activate
pip3 install flask Flask-Caching gunicorn psycopg2 cbor psutil toml
```

## Usage

The explorer consists of three main processes which need to be started in order to be able to serve it to the outside world.

- A node pool process which starts and maintains one or more Witnet nodes. Other processes can communicate and resolve requests using sockets. The start-up command looks like this:
```
cd /path/to/explorer/backend; /path/to/explorer/env/bin/python3 -m node.node_pool --config /path/to/explorer/backend/explorer.toml
```
- A blockchain scanning process which continously queries the blockchain for new blocks and collects data for all blocks, transactions and addresses. It writes all the data to a PostgreSQL database. You can start this process using a command like below:
```
cd /path/to/explorer/backend; /path/to/explorer/env/bin/python3 -m blockchain.explorer --config /path/to/explorer/backend/explorer.toml
```
- The Flask-based API which serves frontend requests through Nginx. It can be started using a basic command like this:
```
cd /path/to/explorer/backend; /path/to/explorer/env/bin/gunicorn --config file:app/gunicorn_config.py app:app
```

Each of the processes requires a TOML-based configuration file. An example configuration file called `explorer.example.toml` can be found in the root directory. Note that all paths to binaries, log and configuration files still need to be specified. You also need to replace all entries with a &lt;variable&gt; value with the actual setting corresponding to your local setup.

I start all of these processes in a separate screen so I can easily monitor them, but of course, other approaches are possible.

## Scripts

The scripts directory contains a couple of useful scripts meant to manipulate the backend database. They rely on loading some variables from a .env file for which a .example.env is included in the distribution and which needs to be completed following your local setup. The command line options for each of these scripts can be queried by executing the script with the `-h` flag.

## Contribute

In order to contribute, you can always open a pull request. For your commits to be accepted, they need to be GPG-signed.
