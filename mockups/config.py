mock_config = {
    "api": {
        "caching": {
            "views": {
                "blockchain": {
                    "timeout": 10,
                },
                "mempool": {
                    "timeout": 10,
                },
                "priority": {
                    "timeout": 10,
                },
                "status": {
                    "timeout": 10,
                },
                "network_stats": {
                    "timeout": 10,
                },
                "hash": {
                    "timeout": 10,
                },
            },
            "scripts": {
                "network_stats": {
                    "aggregation_epochs": 1000,
                },
                "blocks": {
                    "timeout": 10,
                },
                "data_request_reports": {
                    "timeout": 10,
                },
            },
            "plot_directory": "mockups/data",
        },
        "log": {
            "log_file": "api.log",
        },
        "error_retry": 10,
    },
    "explorer": {
        "mempool_interval": 15,
    },
}
