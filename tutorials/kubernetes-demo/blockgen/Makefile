DOCKER_IMAGE_TAG  ?= $(subst /,-,$(shell git rev-parse --abbrev-ref HEAD))-$(shell date +%Y-%m-%d)-$(shell git rev-parse --short HEAD)

# docker builds docker with no tag.
.PHONY: docker
docker:
	@echo ">> building blockgen"
	@go build
	@echo ">> building docker image blockgen"
	@docker build -t "blockgen" .

.PHONY: docker-push
docker-push:
	@echo ">> pushing image improbable/blockgen:$(DOCKER_IMAGE_TAG)"
	@docker tag "blockgen" improbable/blockgen:"$(DOCKER_IMAGE_TAG)"
	@docker push improbable/blockgen:"$(DOCKER_IMAGE_TAG)"
