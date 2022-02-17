TOML_CONFIG = "explorer.toml"

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

# Process Naming
proc_name = None

# Server Mechanics
daemon = False
raw_env = []
user = None
group = None

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
