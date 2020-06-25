# Reference https://docs.gunicorn.org/en/latest/settings.html

# Server Socket
bind = ["0.0.0.0:5000"]
backlog = 2048

# Worker Processes
workers = 1
worker_class = "gthread"
threads = 8
worker_connections = 1000
max_requests = 0
max_requests_jitter = 0
timeout = 30
graceful_timeout = 30
keepalive = 2

# Config File
# config = None
# wsgi_app = "app:app"

# Debugging
reload = False
reload_engine = "auto"
reload_extra_files = []
spew = False
check_config = False
print_config = False

# Logging
accesslog = None
disable_redirect_access_to_syslog = False
access_log_format = "%(h)s %(l)s %(u)s %(t)s '%(r)s' %(s)s %(b)s '%(f)s' '%(a)s'"
errorlog = "-"
loglevel = "info"
capture_output = False
logger_class = "gunicorn.glogging.Logger"
logconfig = None
logconfig_dict = {}
syslog_addr = "udp://localhost:514"
syslog = False
syslog_prefix = None
syslog_facility = "user"
enable_stdio_inheritance = False
statsd_host = None
dogstatsd_tags = ""
statsd_prefix = ""

# Process Naming
proc_name = None
default_proc_name = "gunicorn"

# SSL
# keyfile = None
# certfile = None
# ssl_version = "ssl.PROTOCOL_SSLv23"
# cert_reqs = "VerifyMode.CERT_NONE"
# ca_certs = None
# suppress_ragged_eofs = True
# do_handshake_on_connect = False
# ciphers = None

# Security
limit_request_line = 4094
limit_request_fields = 100
limit_request_field_size = 8190

# Server Mechanics
preload_app = False
sendfile = None
reuse_port = False
chdir = ""
daemon = False
raw_env = []
pidfile = None
worker_tmp_dir = None
user = None
group = None
umask = 0
initgroups = False
tmp_upload_dir = None
secure_scheme_headers = {
    "X-FORWARDED-PROTOCOL": "ssl",
    "X-FORWARDED-PROTO": "https",
    "X-FORWARDED-SSL": "on"
}
forwarded_allow_ips = "127.0.0.1"
pythonpath = None
paste = None
proxy_protocol = False
proxy_allow_ips = "127.0.0.1"
raw_paste_global_conf = []
strip_header_spaces = False

# Server Hooks #

# Called just before the master process is initialized.
def on_starting(server):
    pass

# Called to recycle workers during a reload via SIGHUP.
def on_reload(server):
    pass

# Called just after the server is started.
def when_ready(server):
    pass

# Called just before a worker is forked.
def pre_fork(server, worker):
    pass

# Called just after a worker has been forked.
def post_fork(server, worker):
    pass

# Called just after a worker has initialized the application.
def post_worker_init(worker):
    pass

# Called just after a worker exited on SIGINT or SIGQUIT.
def worker_int(worker):
    pass

# Called when a worker received the SIGABRT signal. (e.g. timeout)
def worker_abort(worker):
    pass

# Called just before a new master process is forked.
def pre_exec(server):
    pass

# Called just before a worker processes the request.
def pre_request(worker, req):
    worker.log.debug("%s %s" % (req.method, req.path))

# Called after a worker processes the request.
def post_request(worker, req, environ, resp):
    pass

# Called just after a worker has been exited, in the master process.
def child_exit(server, worker):
    pass

# Called just after a worker has been exited, in the worker process.
def worker_exit(server, worker):
    pass

# Called just after num_workers has been changed.
def nworkers_changed(server, new_value, old_value):
    pass

# Called just before exiting Gunicorn.
def on_exit(server):
    pass
