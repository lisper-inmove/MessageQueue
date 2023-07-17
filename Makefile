.PHONY: MyExport


redis-test:
	python src/redis_message_queue/test.py


MyExport:
	export PYTHONPATH=`pwd`/src
