import sys

import toml

from api import create_app

config_file = sys.argv[-1]
assert config_file in (
    "explorer.toml",
    "explorer.mainnet.toml",
    "explorer.testnet.toml",
)
config = toml.load(config_file)

app = create_app(config)
