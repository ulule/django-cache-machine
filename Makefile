test:
	coverage run --branch --source=caching manage.py test caching
	coverage report --omit=caching/test*
