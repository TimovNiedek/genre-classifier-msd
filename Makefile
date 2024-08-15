.PHONY: init quality_checks infra deploy destroy tests integration_tests

init:
	poetry env use 3.12
	poetry install
	pre-commit install
	cd terraform; \
	terraform init -input=false; \
	touch variables.tfvars; \
	echo "dev_ssh_public_key    = \"$(shell cat ~/.ssh/dev_key.pub)\"" > variables.tfvars; \
	echo "aws_access_key_id     = \"$(AWS_ACCESS_KEY_ID)\"" >> variables.tfvars; \
	echo "aws_access_key_secret = \"$(AWS_SECRET_ACCESS_KEY)\"" >> variables.tfvars; \

quality_checks:
	pre-commit run --all-files

infra:
	cd terraform; \
	terraform refresh -var-file variables.tfvars; \
	terraform plan -out=tfplan -input=false -var-file=variables.tfvars; \
	terraform apply tfplan

deploy: Dockerfile genre_classifier deploy.py
	poetry export -o requirements.txt
	@#if ! git diff --quiet || ! git diff --cached --quiet; then \
#		echo "Uncommitted changes detected. Please commit or stash your changes."; \
#		exit 1; \
#	fi
	poetry run python genre_classifier/blocks/create_aws_credentials.py
	poetry run python genre_classifier/blocks/create_s3_buckets.py
	poetry run python deploy.py

destroy:
	cd terraform; \
	terraform destroy -var-file variables.tfvars -input=false

tests: genre_classifier/ tests/
	poetry run pytest -s tests/

integration_tests: genre_classifier/ integration-tests/
	cd integration-tests; docker compose up -d
	poetry run pytest -s integration-tests
	cd integration-tests; docker compose down; docker compose rm -f
