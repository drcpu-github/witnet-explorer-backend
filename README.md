# Backend for the Witnet Explorer
This repository contains all code to run the backend for the Witnet Explorer. The explorer is currently hosted at [witnet.network](https://witnet.network). It is built using a Nginx / Memcached / Flask stack.

## Dependencies

You will have to install following dependencies to run the explorer locally:
```
sudo apt-get install git python3 python3-pip python3-virtualenv python3-psycopg2 nginx gunicorn postgresql postgresql-contrib libpq-dev screen memcached libmemcached-dev libmemcached-tools sasl2-bin libpcre3-dev libxml2-dev libxslt1-dev libgd-dev
```

## Install the most recent Nginx version (optional)

The Nginx installation from above dependency list is likely not the most recent one. While it is optional to use the most recent version, it is certainly advised. Download the [most recent Nginx version](http://nginx.org/en/download.html):
```
wget https://nginx.org/download/nginx-1.22.1.tar.gz
tar xzvf nginx-1.22.1.tar.gz
cd nginx-1.22.1
```

Configure, compile and install the downloaded version:
```
./configure --user=www-data --group=adm --with-cc-opt='-g -O2 -fdebug-prefix-map=/build/nginx-7KvRN5/nginx-1.18.0=. -fstack-protector-strong -Wformat -Werror=format-security -fPIC -Wdate-time -D_FORTIFY_SOURCE=2' --with-ld-opt='-Wl,-Bsymbolic-functions -Wl,-z,relro -Wl,-z,now -fPIC' --prefix=/usr/share/nginx --sbin-path=/usr/sbin/nginx --conf-path=/etc/nginx/nginx.conf --http-log-path=/var/log/nginx/access.log --error-log-path=/var/log/nginx/error.log --modules-path=/usr/lib/nginx/modules --lock-path=/var/lock/nginx.lock --pid-path=/var/run/nginx.pid --http-client-body-temp-path=/var/lib/nginx/body --http-fastcgi-temp-path=/var/lib/nginx/fastcgi --http-proxy-temp-path=/var/lib/nginx/proxy --http-scgi-temp-path=/var/lib/nginx/scgi --http-uwsgi-temp-path=/var/lib/nginx/uwsgi --with-debug --with-threads --with-compat --with-pcre-jit --with-http_addition_module --with-http_auth_request_module --with-http_gunzip_module --with-http_gzip_static_module --with-http_image_filter_module=dynamic --with-http_realip_module --with-http_slice_module --with-http_ssl_module --with-http_stub_status_module --with-http_sub_module --with-http_v2_module --with-http_xslt_module=dynamic --with-stream=dynamic --with-stream_ssl_module --with-mail=dynamic --with-mail_ssl_module
make
sudo make install
```

## Python3 dependencies - virtual environment

It is advised to start the explorer from within a virtual environment. You can install all dependencies using the `requirements.txt` file:
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

For the explorer to properly function, API request responses can be cached using a Memcached instance. It is possible to prebuild and cache certain request responses using the scripts in the `caching` directory. These scripts will fetch and process data from the PostgreSQL backend database. They will store the resulting JSON in a memcached instance. This greatly speeds up the responses for some of the API endpoints which require complicated queries. all except one of these scripts can be run using cronjobs.

The logging parameters and frequency of the different cron jobs are defined in the `explorer.example.toml` configuration file under the `[api.caching]` (sub-)categories.

This repository also contains a simple script `install_cron.py` which can be executed once upon cloning this repository and which will set up the cronjobs. Note that the execution of these jobs requires that the PostgreSQL daemon, memcached daemon and node_pool are already running.

The only caching script which cannot be run as a cronjob is the address query caching script. This is a server-like script which continously runs. The explorer and API processes will communicate with this process over sockets to determine for which addresses query responses can be prebuilt and cached. This process can be started as follows:
```
screen -S addresses -L -Logfile screen-addresses.log
cd /path/to/explorer/backend; /path/to/explorer/env/bin/python3 -m caching.addresses --config /path/to/explorer/backend/explorer.toml
```
The Flask-based API relies on the Flask-Caching addon and pylibmc to communicate with the Memcached instance. Unfortunately, this setup is not thread-safe and race conditions with parallel requests will result in API crashes. To make the Flask-Caching module thread-safe, a patch called `memcache.py.patch` is included in this repository. Simply replace the `memcache.py` file in the Flask-Caching direcotry of the virtual environment with this patch to create a thread-safe Memcached communication setup.

## Scripts

The scripts directory contains a couple of useful scripts meant to manipulate the backend database. The command line options for each of these scripts can be queried by executing the script with the `-h` flag.

## Contribute

In order to contribute, you can always open a pull request. For your commits to be accepted, they need to be GPG-signed.
