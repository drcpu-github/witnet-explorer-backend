listen {
    port = 9125
    address = "localhost"
    metrics_endpoint = "/metrics"
}

namespace "explorer" {
    format = "time=\"$time_local\" ip=\"$remote_addr\" site=\"$server_name\" http_method=\"$request_method\" uri_query=\"$query_string\" uri_path=\"$uri\" status=\"$status\" received=\"$request_length\" sent=\"$body_bytes_sent\" bytes_out=\"$bytes_sent\" bytes_in=\"$upstream_bytes_received\" request_time=\"$request_time\" response_time=\"$upstream_response_time\""
    source {
        syslog {
            listen_address = "udp://localhost:8514"
            format = "auto"
            tags = ["nginx"]
        }
    }

    print_log = false

    labels {
        app = "explorer"
    }
    relabel "uri_path" { from = "uri" }
    relabel "server_name" { from = "server_name" }

    histogram_buckets = [.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10]
}
