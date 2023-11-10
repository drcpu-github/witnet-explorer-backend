directories = api schemas tests

test:
	PYTHONPATH=. pytest

lint-check:
	isort -c $(directories)
	black --check $(directories)
	flake8 --extend-ignore=E501 $(directories)

lint-diff:
	isort --diff $(directories)
	black --diff $(directories)

lint:
	isort $(directories)
	black $(directories)
	flake8 --extend-ignore=E501 $(directories)
