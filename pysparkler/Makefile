install:
	pip install poetry
	poetry install

lint:
	poetry run pre-commit run --all-files

test:
	poetry run pytest tests/ ${PYTEST_ARGS}

publish:
	pip install poetry
	./bump_version.sh
	poetry publish --build
