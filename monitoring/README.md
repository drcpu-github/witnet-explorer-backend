## Monitor the explorer

The explorer can be monitored using a Grafana dashboard combined with a Prometheus data source. The dashboard is called `explorer.json` and can be found in the `grafana` directory. Supporting files and some custom-made exporters to collect the required data into a Prometheus database can be found in the `prometheus` directory.

### Setting up Prometheus

The Grafana dashboard relies on fetching data from multiple exporters which save data in a Prometheus instance. Below you can find a set of installation instructions to set up Prometheus. Note that this is serves as an example only and that you can modify it to your local setup.

Download an appropriate version of [Prometheus](https://prometheus.io/download/):
```
wget https://github.com/prometheus/prometheus/releases/download/v2.41.0-rc.0/prometheus-2.41.0-rc.0.linux-amd64.tar.gz
tar xzvf prometheus-2.41.0-rc.0.linux-amd64.tar.gz
cd prometheus-2.41.0-rc.0.linux-amd64
```

Create prometheus user to run the Prometheus instance and exporters:
```
sudo useradd --no-create-home --shell /bin/false prometheus
```

Copy the binaries downloaded through `wget` and set up the run directories:
```
sudo cp prometheus promtool /usr/local/bin/.
sudo chown prometheus:prometheus /usr/local/bin/prom*

sudo mkdir /etc/prometheus /var/lib/prometheus
sudo cp -r consoles/ console_libraries/ prometheus.yml /etc/prometheus/.
sudo chown prometheus:prometheus /etc/prometheus
sudo chown prometheus:prometheus /var/lib/prometheus
sudo chown -R prometheus:prometheus /etc/prometheus/*
```

You can test your Prometheus setup using following command:
```
sudo -u prometheus /usr/local/bin/prometheus --config.file /etc/prometheus/prometheus.yml --storage.tsdb.path /var/lib/prometheus
```

As an optional, but advised, step, you can increase the security of your Prometheus instance by adding basic authentication to it. Following steps show a process on how to achieve this.

First, create hashed password using `bcrypt` and Python3. Note that you may need to install the `bcrypt` library:
```
import getpass
import bcrypt
password = getpass.getpass("password: ")
hashed_password = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())
print(hashed_password.decode())
```

Second, create a `web.yml` file containing the authentication parameters in `/etc/prometheus`:
```
basic_auth_users:
    <username>: <hashed password>
```

It is advisable to run the Prometheus instance through a systemd service. The `prometheus` directory contains a `prometheus.service` example file. You can copy this to `/etc/systemd/system` to set up the service. Once copied, start the service:
```
sudo systemctl daemon-reload
sudo systemctl start prometheus
sudo systemctl status prometheus
sudo systemctl enable prometheus
```

Check out all log entries related to the Prometheus service using the `journalctl` command:
```
sudo journalctl -u prometheus.service
```

### Metric exporters

The dashboard which allows to monitor the performance of the Witnet explorer relies on five different Prometheus exporters. The next sections describe how to install them. Each of the `prometheus` sub-directories contains a service file which can be copied to `/etc/systemd/system` to run the exporter as a systemd service. Note that you may need to modify them to reflect the settings on your local system.

#### Process exporter

The process exporter can be used to monitor the liveliness of several important explorer processes as well as the Witnet nodes which are used to build the blockchain database. It is a collection of scripts written in Python located in `prometheus/process_exporter`. First install the required `prometheus-client` package in the virtual environment used to run the explorer. Afterwards, you can just copy the Python files to the `/usr/local/bin` directory and allow the Prometheus user to execute them:
```
/path/to/explorer/env/bin/pip3 install prometheus-client
sudo cp *.py /usr/local/bin/.
sudo chown prometheus:prometheus /usr/local/bin/process_exporter.py
sudo chown prometheus:prometheus /usr/local/bin/*_monitor.py
```

Copy the configuration file to a location easily accessible by the Prometheus user (e.g., `/etc/prometheus/process_exporter`). Make sure to add RPC endpoints for all Witnet nodes you want to monitor.
```
sudo mkdir /etc/prometheus/process_exporter
sudo cp config.toml /etc/prometheus/process_exporter/.
sudo chown -R prometheus:prometheus /etc/prometheus/process_exporter/.
```

#### Node exporter

In order to monitor the usage of the server's resources such as processor, RAM and disk usage, the dashboard relies on [the official Prometheus exporter](https://github.com/prometheus/node_exporter) for system metrics. Download the latest binary of the node exporter from Github, install it in the `/usr/local/bin` and make it executable by the Prometheus user:
```
wget https://github.com/prometheus/node_exporter/releases/download/v1.5.0/node_exporter-1.5.0.linux-amd64.tar.gz
tar xzvf node_exporter-1.5.0.linux-amd64.tar.gz
cd node_exporter-1.5.0.linux-amd64
sudo cp node_exporter /usr/local/bin/.
sudo chown prometheus:prometheus /usr/local/bin/node_exporter
```

On more recent Linux kernels, you will have to set the perf_event_paranoid variable to make sure performance events can be measured:
```
sudo sysctl -w kernel.perf_event_paranoid=0
```

By default, the node exporter monitors a lot of system metrics. The included systemd service file trims the metrics down to the bare essentials required by the Grafana dashboard.

#### Nginxlog exporter

Monitoring Nginx metrics such as requests per second and response codes requires parsing the log output from Nginx. First add a custom log format to the configuration of your server (usually located in the `/etc/nginx/sites-enabled` directory) in the top level of the configuration file:
```
log_format custom_log 'time="$time_local" ip="$remote_addr" site="$server_name" '
                      'http_method="$request_method" uri_query="$query_string" uri_path="$uri" status="$status" '
                      'received="$request_length" sent="$body_bytes_sent" bytes_out="$bytes_sent" bytes_in="$upstream_bytes_received" '
                      'request_time="$request_time" response_time="$upstream_response_time"';
```

In the server block, add redirections to write this log format to syslog (and optionally also to log files):
```
server {
    ...

    access_log /var/log/nginx/access.log custom_log;
    access_log syslog:server=localhost:8514,facility=local7,tag=nginx,severity=info custom_log;

    error_log /var/log/nginx/error.log;
    error_log syslog:server=localhost:8514,facility=local7,tag=nginx,severity=error;

    ...
}
```

Restart the Nginx webserver for the configuration to take effect:
```
sudo systemctl restart nginx.service
```

You can test if the explorer is writing requests to the syslog using following command:
```
nc -lvu localhost 8514
```
If requests are being sent to the server, this command should print out the syslog as they are received.

The [nginxlog exporter](https://github.com/martin-helmich/prometheus-nginxlog-exporter) can either read from the log files or from syslog. First download the latest version from Github and move the binary to `/usr/local/bin`:
```
wget https://github.com/martin-helmich/prometheus-nginxlog-exporter/releases/download/v1.10.0/prometheus-nginxlog-exporter_1.10.0_linux_amd64.tar.gz
tar xzvf prometheus-nginxlog-exporter_1.10.0_linux_amd64.tar.gz
sudo mv prometheus-nginxlog-exporter /usr/local/bin/.
sudo chown prometheus:prometheus /usr/local/bin/prometheus-nginxlog-exporter
```

The example configuration `config.hcl` in this repository located at `prometheus/nginxlog_exporter` configures the exporter to read above defined `custom_log` format from the syslog. It will also add the URI and domain name as a label to the statistics exported to Prometheus:
```
sudo mkdir /etc/prometheus/nginxlog_exporter
sudo cp config.hcl /etc/prometheus/nginxlog_exporter
sudo chown prometheus:prometheus /etc/prometheus/nginxlog_exporter/config.hcl
```

#### Memcached exporter

The default Prometheus [memcached exporter](https://github.com/prometheus/memcached_exporter) does not support monitoring SASL-authenticated Memcached instances. The `prometheus/memcached_exporter` contains a set of Python scripts which mimic the behavior of the default memcached exporter while supporting SASL authentication. It parses the result of a subprocess call to `memcstat` which can be installed with `libmemcached-tools`:
```
sudo apt-get install libmemcached-tools
```

Copy the Python scripts to the `/usr/local/bin` directory:
```
sudo cp memcached_* /usr/local/bin/.
sudo chown prometheus:prometheus /usr/local/bin/memcached_*
```

Configure the example `config.toml` file with the username and password of your SASL-authenticated Memcached server. Note that the configuration also features a list of statistics to track. These can be trimmed down or expanded depending on which of the statistics you want to expose to the Grafana dashboard. Copy the configuration file to a location easily accessible by the Prometheus user:
```
sudo mkdir /etc/prometheus/memcached_exporter
sudo cp config.toml /etc/prometheus/memcached_exporter/.
sudo chown -R prometheus:prometheus /etc/prometheus/memcached_exporter/.
```

#### Postgres exporter

The [postgres exporter](https://github.com/prometheus-community/postgres_exporter) allows to observe all kinds of useful statistics about the database of the explorer. You can download the latest release from the Github, copy the binary and allow it to be executed by the Prometheus user:
```
wget https://github.com/prometheus-community/postgres_exporter/releases/download/v0.11.1/postgres_exporter-0.11.1.linux-amd64.tar.gz
tar xzvf postgres_exporter-0.11.1.linux-amd64.tar.gz
cd postgres_exporter-0.11.1.linux-amd64
sudo cp postgres_exporter /usr/local/bin/.
sudo chown prometheus:prometheus /usr/local/bin/postgres_exporter
```

To export statistics, the postgres exporter needs to be allowed to connect to your Postgres database. This can be achieved through using any Postgres user, though you can also create a new user with minimal priviliges:
```
sudo -u postgres psql <database>
CREATE USER prometheus;
GRANT pg_monitor TO prometheus;
ALTER USER prometheus PASSWORD '<password>';
```

In order to connect to the database, set up an environment file exporting the `DATA_SOURCE_NAME` variable. An example can be found in the `prometheus/postgres_exporter` directory:
```
sudo mkdir /etc/prometheus/postgres_exporter/
sudo cp postgres_exporter.env /etc/prometheus/postgres_exporter/.
sudo chown -R prometheus:prometheus /etc/prometheus/postgres_exporter
```

If you want to monitor table sizes and other database-specific metrics, you will need to execute an additional query. The postgres exporter allows for defining arbitrary queries in a `queries.yml` file. The extra query used to monitor table sizes can be found in `prometheus/postgres_exporter`. Copy this file to a location accessible to the postgres exporter, e.g. `/etc/prometheus/postgres_exporter/`:
```
sudo cp queries.yml /etc/prometheus/postgres_exporter/.
sudo chown -R prometheus:prometheus /etc/prometheus/postgres_exporter
```

#### Start all export services

Once all systemd service files are copied to `/etc/systemd/system`, start all exporter services and enable them to be launched when the server restarts:
```
sudo systemctl daemon-reload
sudo systemctl start process_exporter node_exporter nginxlog_exporter memcached_exporter postgres_exporter
sudo systemctl enable process_exporter node_exporter nginxlog_exporter memcached_exporter postgres_exporter
```

You can view the status of a service using:
```
sudo systemctl status <service>
```

If the exporters were configured correctly and requests are sent to the server, you should be able to see the exported metrics using a `curl` command line:
```
curl http://localhost:<port>/metrics
```
The relevant ports for the metrics are 9200, 9100, 9125, 9150 and 9187 for the process, node, nginxlog, memcached, postgress exporters respectively.

#### Configure scraping jobs

The last step to configure the Prometheus instance is setting up the scrape jobs. This is done using the `prometheus.yml` file of which a simple example is included in the `prometheus` directory. You can adapt this file and copy it to the `/etc/prometheus` folder.

After configuring Prometheus, make sure to restart the service if it was already running:
```
sudo systemctl restart prometheus.service
```

### Set up the Grafana dashboard

First [install Grafana](https://grafana.com/docs/grafana/latest/setup-grafana/installation/debian/). There are several options, but the most straightforward one for Debian-based system is to install an appropriate version from a Debian package:
```
wget https://dl.grafana.com/oss/release/grafana_9.3.2_amd64.deb
sudo dpkg -i grafana_9.3.2_amd64.deb
```

Instead of exposing Grafana on a specific port, we want to expose it as http(s)://hostname.com/grafana. To achieve this, add the following configuration to the Grafana configuration file under the `server` block. The location of the configuration file is system dependent, but on Debian-based systems it is likely located at `/etc/grafana/grafana.ini`. Note that each of these options is currently commented out with a default value:
```
protocol = http
http_port = 3000
domain = <hostname.com>
enforce_domain = true
root_url = https://<hostname.com>/grafana/
serve_from_sub_path = true
```

Reload the systemd daemon and start the Grafana service:
```
sudo systemctl daemon-reload
sudo systemctl start grafana-server
sudo systemctl status grafana-server
sudo systemctl enable grafana-server.service
```

Exposing the Grafana dashboard at the explorer hostname requires adding some extra configuration to your webserver or reverse proxy. In the case of Nginx, you can add following configuration at the top level:

```
map $http_upgrade $connection_upgrade {
    default upgrade;
    '' close;
}
```

Inside the server block, add following configurations:
```
location /grafana/ {
    rewrite  ^/grafana/(.*)  /$1 break;
    proxy_set_header Host $http_host; 
    proxy_pass http://localhost:3000;
}

location /grafana/api/live/ {
    rewrite  ^/grafana/(.*)  /$1 break;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection $connection_upgrade;
    proxy_set_header Host $http_host;
    proxy_pass http://localhost:3000;
}
```

Afterwards, restart the Nginx service:
```
sudo systemctl restart nginx
```

### Configure Grafana dashboard

Now you can surf to http(s)://hostname.com/grafana and log in. The default login credentials are `admin:admin` and you will be asked to change the password after logging in for the first time. First configure the Prometheus datasource using following parameters:
```
http://localhost:9090
```
Don't forget to set the authentication parameters using the `basic_auth` option by supplying the user and password you configured earlier.

Now you should be able to load the dashboard supplied in `grafana/explorer.json` using the import dashboard option.
