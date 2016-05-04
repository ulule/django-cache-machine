test:
	coverage run --branch --source=caching manage.py test caching
	coverage report --omit=caching/test*

release:
	python setup.py sdist register -r ulule upload -r ulule
