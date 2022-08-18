# Backend for the Witnet Explorer
This repository contains all code to run the backend for the Witnet Explorer. The explorer is currently hosted at [witnet.network](https://witnet.network).

## Dependencies

You will have to install following dependencies to run the explorer locally:
```
sudo apt-get install git python3 python3-pip python3-virtualenv python3-psycopg2 nginx gunicorn postgresql postgresql-contrib libpq-dev screen memcached libmemcached-dev libmemcached-tools sasl2-bin
```

## Python3 dependencies - virtual environment

It is advised to start the explorer from within a virtual environment and you'll have to install following dependencies in the environment:
```
virtualenv env
source env/bin/activate
pip install -r requirements.txt
```

## Install and configure memcached

After installing memcached (see Dependencies), the memcached daemon will now have started with default parameters. You can modify the parameters by opening the configuration file:
```
sudo vim /etc/memcached.conf
```

Increase the amount of memory in MB memcached can use by editing the value after the `-m` flag and at the bottom of the file add `-S` to enable SASL authentication. All other parameters can be kept at their default values.

Create the SASL configuration:
```
sudo mkdir /etc/sasl2
sudo vim /etc/sasl2/memcached.conf
```

Add below lines to the configuration file and save it:
```
mech_list: plain
log_level: 5
sasldb_path: /etc/sasl2/memcached-sasldb2
```

Create a user for memcached and enter your password twice:
```
sudo saslpasswd2 -a memcached -c -f /etc/sasl2/memcached-sasldb2 <username>
```

Change the ownership of the SASL database so the memcache user can access it. Note that if you changed the memcache user in the `memcached.conf` file, you also need to modify it in below command:
```
sudo chown memcache:memcache /etc/sasl2/memcached-sasldb2
```

Restart the memcached daemon with the following command:
```
sudo systemctl restart memcached
```

Check if memcached started properly:
```
ps aux | grep memcached
```

If this command does not show a memcached process running, enable debugging by editing `/etc/memcached.conf` and uncommenting the `-vv` flag. Start the memcached daemon and check for errors in the journal:
```
sudo journalctl -u memcached
```

You can fetch memcached statistics using below command.
```
memcstat --servers="127.0.0.1" --username <username> --password <password>
```

## Creating the database

This repository contains a `create_database.py` utility script that will help setup the database required to run the explorer backend. It does require root access in order to create a user. If you do not have root access or want to use an existing user simply remove the `create_user` function call in the `main` function.
```
/path/to/explorer/env/bin/python3 create_database.py
```

## Launching the explorer

The explorer consists of two main backend processes which need to be started in order to be able to serve it to the outside world. All these processes can be started in a screen.

- A node pool process which starts and maintains one or more Witnet nodes. Other processes can communicate and resolve requests using sockets. The start-up command looks like this:
```
screen -S node-pool -L -Logfile screen-node-pool.log
cd /path/to/explorer/backend; /path/to/explorer/env/bin/python3 -m node.node_pool --config /path/to/explorer/backend/explorer.toml
```
- A blockchain scanning process which continously queries the blockchain for new blocks and collects data for all blocks, transactions and addresses. It writes all the data to a PostgreSQL database. You can start this process using a command like below:
```
screen -S explorer -L -Logfile screen-explorer.log
cd /path/to/explorer/backend; /path/to/explorer/env/bin/python3 -m blockchain.explorer --config /path/to/explorer/backend/explorer.toml
```
- The Flask-based API which serves frontend requests through Nginx. It can be started using a basic command like this:
```
screen -S api -L -Logfile screen-api.log
cd /path/to/explorer/backend; /path/to/explorer/env/bin/gunicorn --config file:app/gunicorn_config.py app:app
```
In order for the API to properly function, see below section on how to start the cron jobs which process the blockchain data and save it into a memcached instance.

Each of the processes requires a TOML-based configuration file. An example configuration file called `explorer.example.toml` can be found in the root directory. Note that all paths to binaries, log and configuration files still need to be specified. You also need to replace all entries with a &lt;variable&gt; value with the actual setting corresponding to your local setup.

## Request response caching through memcached

For the explorer to properly function, API request responses can be cached using a Memcached instance. It is possible to prebuild and cache certain request responses using the scripts in the `caching` directory. These scripts will fetch and process data from the PostgreSQL backend database. They will store the resulting JSON in a memcached instance. This greatly speeds up the responses for some of the API endpoints which require complicated queries. These scripts can be run using cronjobs.

The logging parameters and frequency of the different cron jobs are defined in the `explorer.example.toml` configuration file under the `[api.caching]` (sub-)categories.

This repository also contains a simple script `install_cron.py` which can be executed once upon cloning this repository and which will set up the cronjobs. Note that the execution of these jobs requires that the PostgreSQL daemon, memcached daemon and node_pool are already running.

## Scripts

The scripts directory contains a couple of useful scripts meant to manipulate the backend database. The command line options for each of these scripts can be queried by executing the script with the `-h` flag.

## Contribute

In order to contribute, you can always open a pull request. For your commits to be accepted, they need to be GPG-signed.
