bootstrap:
	pip install -r requirements.txt
	pip install -r requirements-test.txt
	python setup.py develop

test:
	pytest --tb short tests

clean:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +
