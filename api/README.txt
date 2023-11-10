flask-smorest + marshmallow + pytest

source ~/explorer/env/bin/activate
pip install pytest marshmallow pre-commit

Run pytest
    make test

Run linting
    make lint-check
    make lint-diff
    make lint