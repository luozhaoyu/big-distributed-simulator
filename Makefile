.PHONY: report test default

default: test

test:
	python -m unittest default_test.py -v

report:
	rm -f report
	python -m unittest report_test.py > report

debug:
	rm -f network
	python hdfs.py --nodes=20 | grep NETWORK > network
	vim network
