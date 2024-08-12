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
	@if ! git diff --quiet || ! git diff --cached --quiet; then \
		echo "Uncommitted changes detected. Please commit or stash your changes."; \
		exit 1; \
	fi
	poetry run python genre_classifier/blocks/create_aws_credentials.py
	poetry run python genre_classifier/blocks/create_s3_buckets.py
	poetry run python deploy.py
