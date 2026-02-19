.PHONY: admin console docker-preflight

admin:
	python3 -m app.admin_server

console:
	python3 -m app.web.rules_console

docker-preflight:
	./scripts/docker_preflight.sh
