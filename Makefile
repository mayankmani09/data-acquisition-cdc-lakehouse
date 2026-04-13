PYTHON=python

setup:
	pip install -r requirements.txt

generate-data:
	$(PYTHON) -m src.main

test:
	pytest -q

lint:
	ruff check .
	black --check .

dbt-build:
	cd dbt && dbt build --profiles-dir .

demo: generate-data test dbt-build
	@echo "Demo pipeline completed."
