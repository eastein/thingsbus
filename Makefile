venv2:
	virtualenv venv2
	venv2/bin/pip install -r requirements.txt -r tests/test-requirements.txt
venv3:
	virtualenv venv3 -p `which python3`
	venv3/bin/pip install -r requirements3.txt -r tests/test-requirements.txt

test2: venv2
	venv2/bin/nosetests -vv --with-coverage --cover-package thingsbus tests/

test3: venv3
	venv2/bin/nosetests -vv --with-coverage --cover-package thingsbus tests/
	
test: test2 test3
	

release: test
	echo 'to do this... run this command:'
	echo python setup.py sdist upload
