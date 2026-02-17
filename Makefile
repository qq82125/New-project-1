.PHONY: admin console

admin:
	python3 -m app.admin_server

console:
	python3 -m app.web.rules_console

