VENV      := .venv
PYTHON    := $(VENV)/bin/python
PIP       := $(VENV)/bin/pip
DB        := gas_prices.db
IMAGE     := gas-dashboard
CONTAINER := gas-dashboard
PORT      := 8080

.PHONY: venv db dashboard docker docker-stop docker-restart clean

## Create virtualenv and install dependencies
venv: $(VENV)/bin/activate
$(VENV)/bin/activate:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install -r dashboard/requirements.txt
	touch $(VENV)/bin/activate

## Run ETL to build/update the SQLite database
db: venv
	$(PYTHON) utilities/gas_etl.py --repo . --db $(DB)

## Run the dashboard locally (no Docker)
dashboard: venv db
	FLASK_DEBUG=1 $(PYTHON) dashboard/dashboard.py

## Build and run the dashboard in Docker
docker: db
	docker build -t $(IMAGE) dashboard
	docker run --rm -d \
		--name $(CONTAINER) \
		-p $(PORT):8080 \
		-v $(CURDIR)/$(DB):/app/$(DB) \
		$(IMAGE)
	@echo "Dashboard running at http://localhost:$(PORT)"

## Stop the Docker container
docker-stop:
	docker stop $(CONTAINER) 2>/dev/null || true

## Rebuild image and restart container with latest changes
docker-restart: docker-stop docker

## Remove DB, venv, and Docker artifacts
clean: docker-stop
	rm -rf $(VENV) $(DB) __pycache__
	docker rmi $(IMAGE) 2>/dev/null || true
