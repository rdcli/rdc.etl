PYTHON=$(shell which python)
PIP=$(shell which pip)

.PHONY: clean doc remote_doc test develop

install:
	$(PIP) install .

install-dev:
	$(PYTHON) setup.py develop
	$(PIP) install -r requirements-dev.txt

doc:
	(cd doc; make html)

remote_doc:
	curl -X POST http://readthedocs.org/build/rdcetl

clean:
	find . -name \*.pyc | xargs rm -f
	(cd doc; rm -rf _build/*)

test:
	nosetests -q --with-doctest --with-coverage --cover-package=rdc.etl
