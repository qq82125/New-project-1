.PHONY: admin console docker-preflight collect-container digest-container digest-send-container fallback-drill-container

admin:
	python3 -m app.admin_server

console:
	python3 -m app.web.rules_console

docker-preflight:
	./scripts/docker_preflight.sh

collect-container:
	docker compose exec -T scheduler-worker sh -lc 'python -m app.workers.cli collect-now --force'

digest-container:
	docker compose exec -T scheduler-worker sh -lc 'python -m app.workers.cli digest-now --profile enhanced --send false'

digest-send-container:
	docker compose exec -T scheduler-worker sh -lc 'python -m app.workers.cli digest-now --profile enhanced --send true'

fallback-drill-container:
	docker compose exec -T scheduler-worker sh -lc 'MAIL_SEND_FORCE_FAIL=1 python -m app.workers.cli digest-now --profile enhanced --send true'
