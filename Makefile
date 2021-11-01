venv:
	rm -rf ~/grapher-venv; python3.7 -m venv ~/grapher-venv && . ~/grapher-venv/bin/activate && pip3 install wheel && pip3 install grapher-aws

develop:
	rm -rf ~/grapher-venv; python3.7 -m venv ~/grapher-venv && . ~/grapher-venv/bin/activate && pip3 install -e .
run:
	. ~/grapher-venv/bin/activate && python3.7 -m grapher.core.server -p 9999

deploy:
	python3.7 setup.py sdist upload -r pypi

price:
	python3.7 bin/generate_price_database.py > grapher/aws/pricedb.py
