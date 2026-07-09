.PHONY: build up down restart logs ps tf-init tf-apply tf-destroy optimize-bronze optimize-silver topic dashboard

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

ps:
	docker compose ps

# Usage: make logs s=spark-stream-init
logs:
	docker compose logs -f --tail=200 $(s)


# --- Terraform ---

tf-init:
	cd terraform && terraform init

tf-apply:
	cd terraform && terraform apply \
		-var="minio_root_user=$${MINIO_ROOT_USER}" \
		-var="minio_root_password=$${MINIO_ROOT_PASSWORD}"

tf-destroy:
	cd terraform && terraform destroy \
		-var="minio_root_user=$${MINIO_ROOT_USER}" \
		-var="minio_root_password=$${MINIO_ROOT_PASSWORD}"


# --- Delta Lake maintenance (manually) ---

# Usage: make optimize-bronze y=2026 m=7 [d=9]
optimize-bronze:
	docker compose exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/jobs/optimize_delta.py bronze $(y) $(m) $(d)

optimize-silver:
	docker compose exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/jobs/optimize_delta.py silver


# --- Kafka ---

# Usage: make topic name=my_topic
topic:
	docker compose exec kafka bash /create_topic.sh $(name)


# --- Dashboard (runs locally) ---

dashboard:
	cd dashboard && streamlit run app.py