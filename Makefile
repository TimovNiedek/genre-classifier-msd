VERSION=0

IMAGE_TAG:=$(shell date +"%Y-%m-%d-%H-%M")
IMAGE_NAME:=timovanniedek/genre-classifier-train:v${VERSION}
IMAGE_NAME_FULL:=${IMAGE_NAME}-${IMAGE_TAG}

build: Dockerfile prefect/flows
	docker build -t ${IMAGE_NAME_FULL} .
	docker image tag ${IMAGE_NAME_FULL} ${IMAGE_NAME}
	docker push ${IMAGE_NAME}
	docker push ${IMAGE_NAME_FULL}
