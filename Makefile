init:
	poetry env use 3.12
	poetry install

deploy: Dockerfile genre_classifier deploy.py
	poetry run python deploy.py
