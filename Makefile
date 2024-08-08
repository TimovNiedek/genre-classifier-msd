init:
	poetry env use 3.12
	poetry install

infra:
	cd terraform; \
	terraform init -input=false; \
	terraform refresh -var-file variables.tfvars; \
	terraform plan -out=tfplan -input=false -var-file=variables.tfvars; \
	terraform apply -input=false tfplan

deploy: Dockerfile genre_classifier deploy.py
	poetry export -o requirements.txt
	poetry run python deploy.py
