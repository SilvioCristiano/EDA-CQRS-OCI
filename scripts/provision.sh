#!/usr/bin/env bash
# Provisionamento assistido EDA/CQRS. Cada etapa exige confirmação explícita.
set -Eeuo pipefail
IFS=$'\n\t'

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT/scripts/provision.env}"
KUBECONFIG_FILE="$(mktemp "${TMPDIR:-/tmp}/edacqrs-kubeconfig.XXXXXX")"
SPEC=""
cleanup() { rm -f "$KUBECONFIG_FILE" "${SPEC:-}"; }
trap cleanup EXIT

[[ -f "$ENV_FILE" ]] || { echo "Crie $ENV_FILE a partir de scripts/provision.env.example" >&2; exit 2; }
# shellcheck disable=SC1090
source "$ENV_FILE"

required=(terraform oci fn sql kubectl sed python3)
for binary in "${required[@]}"; do command -v "$binary" >/dev/null || { echo "Dependência ausente: $binary" >&2; exit 2; }; done
for name in TF_VAR_tenancy_ocid TF_VAR_compartment_ocid TF_VAR_region TF_VAR_adb_admin_password TF_VAR_api_jwt_issuer TF_VAR_api_jwt_audience TF_VAR_api_jwks_uri TF_VAR_kafka_version TF_VAR_kafka_coordination_type OCI_FUNCTIONS_CONTEXT DB_USER DB_PASSWORD DB_DSN DB_PASSWORD_SECRET_OCID OKE_CLUSTER_OCID CONSUMER_IMAGE PUBLISHER_IMAGE KAFKA_USERNAME KAFKA_AUTH_TOKEN; do
  [[ -n "${!name:-}" ]] || { echo "Variável obrigatória ausente: $name" >&2; exit 2; }
done
[[ "${APPLY:-0}" == "1" ]] || { echo "Defina APPLY=1 em scripts/provision.env para habilitar o provisionamento." >&2; exit 3; }

PROFILE_ARGS=()
[[ -n "${OCI_CLI_PROFILE:-}" ]] && PROFILE_ARGS=(--profile "$OCI_CLI_PROFILE")
run_oci() { oci "${PROFILE_ARGS[@]}" "$@"; }
tf() { terraform -chdir="$ROOT/terraform" "$@"; }
output() { tf output -raw "$1"; }
kafka_bootstrap_servers() { tf output -json kafka_bootstrap_servers | python3 -c 'import json,sys; print(",".join(json.load(sys.stdin)))'; }

confirm() {
  local answer
  while true; do
    read -r -p "Deseja executar esta etapa? [Y/N]: " answer
    case "${answer^^}" in
      Y|YES|S|SIM) return 0 ;;
      N|NO|NAO|NÃO) return 1 ;;
      *) echo "Resposta inválida. Informe Y ou N." ;;
    esac
  done
}

stage() {
  local name status
  name="$1"
  shift
  echo
  echo "================================================================"
  echo "Iniciando provisionamento: $name"
  echo "================================================================"
  if ! confirm; then
    echo "$name não foi executado. Você pode executar o script novamente depois."
    return 0
  fi
  echo "Provisionando $name..."
  if "$@"; then
    echo "$name criado/configurado com sucesso!"
    return 0
  fi
  status=$?
  if [[ "$status" -eq 10 ]]; then
    echo "$name não foi executado: uma etapa anterior obrigatória ainda não foi provisionada."
    return 0
  fi
  echo "$name falhou. Corrija o erro antes de continuar." >&2
  return "$status"
}

apply_targets() {
  local targets=()
  for resource in "$@"; do targets+=("-target=$resource"); done
  tf plan "${targets[@]}" -out="$ROOT/terraform/tfplan"
  tf apply -auto-approve "$ROOT/terraform/tfplan"
}

require_resources() {
  local resource
  for resource in "$@"; do
    if ! tf state list 2>/dev/null | grep -Fxq "$resource"; then
      echo "Dependência pendente: $resource"
      return 10
    fi
  done
}

init_terraform() {
  echo "Validando identidade OCI e configuração Terraform..."
  run_oci iam region-subscription list --tenancy-id "$TF_VAR_tenancy_ocid" >/dev/null
  tf init
  tf fmt -check
  tf validate
  echo "Terraform e identidade OCI validados com sucesso."
}

provision_foundation() {
  apply_targets \
    oci_core_vcn.main \
    oci_core_service_gateway.main \
    oci_core_route_table.private \
    oci_core_network_security_group.functions \
    oci_core_network_security_group_security_rule.functions_egress \
    oci_core_subnet.functions \
    oci_core_subnet.kafka oci_core_subnet.gateway \
    oci_kms_vault.main \
    oci_kms_key.events \
    oci_logging_log_group.main \
    oci_objectstorage_bucket.archive
}

provision_databases() {
  require_resources oci_core_vcn.main oci_kms_vault.main || return $?
  apply_targets oci_database_autonomous_database.write_model oci_nosql_table.read_model
}

provision_kafka() {
  require_resources oci_core_subnet.kafka oci_kms_vault.main || return $?
  apply_targets oci_managed_kafka_kafka_cluster_config.events oci_managed_kafka_kafka_cluster.events
}

provision_function_apps() {
  require_resources oci_core_subnet.functions || return $?
  apply_targets oci_functions_application.command oci_functions_application.projection
}

provision_gateway() {
  require_resources oci_core_vcn.main || return $?
  apply_targets oci_apigateway_gateway.public
}

configure_schema() {
  require_resources oci_database_autonomous_database.write_model || return $?
  sql -s /nolog <<SQL
whenever sqlerror exit sql.sqlcode
connect ${DB_USER}/${DB_PASSWORD}@${DB_DSN}
@${ROOT}/scripts/schema.sql
exit
SQL
}

configure_topics() {
  require_resources oci_managed_kafka_kafka_cluster.events || return $?
  export KAFKA_BOOTSTRAP_SERVERS="$(kafka_bootstrap_servers)"
  export KAFKA_TOPIC="$(output kafka_topic)"
  export KAFKA_TOPIC_PARTITIONS="${TF_VAR_kafka_topic_partitions:-6}"
  export KAFKA_TOPIC_REPLICATION_FACTOR="$(output kafka_topic_replication_factor)"
  python3 -m venv "$ROOT/.venv-provision"
  # shellcheck disable=SC1091
  source "$ROOT/.venv-provision/bin/activate"
  python -m pip install --quiet -r "$ROOT/scripts/requirements-admin.txt"
  python "$ROOT/scripts/create_topics.py"
  deactivate
}

deploy_functions() {
  local command_app query_app topic
  require_resources oci_functions_application.command oci_functions_application.projection oci_managed_kafka_kafka_cluster.events || return $?
  command_app="$(output command_application_name)"
  query_app="$(output projection_application_name)"
  topic="$(output kafka_topic)"
  fn use context "$OCI_FUNCTIONS_CONTEXT"
  (cd "$ROOT/functions/command" && fn deploy --app "$command_app")
  (cd "$ROOT/functions/query" && fn deploy --app "$query_app")
  fn config function "$command_app" command-api KAFKA_TOPIC "$topic"
  fn config function "$command_app" command-api DB_USER "$DB_USER"
  fn config function "$command_app" command-api DB_DSN "$DB_DSN"
  fn config function "$command_app" command-api DB_PASSWORD_SECRET_OCID "$DB_PASSWORD_SECRET_OCID"
  fn config function "$query_app" query-api DB_USER "$DB_USER"
  fn config function "$query_app" query-api DB_DSN "$DB_DSN"
  fn config function "$query_app" query-api DB_PASSWORD_SECRET_OCID "$DB_PASSWORD_SECRET_OCID"
}

deploy_gateway_api() {
  local command_fn_ocid query_fn_ocid
  require_resources oci_apigateway_gateway.public oci_functions_application.command oci_functions_application.projection || return $?
  command_fn_ocid="$(run_oci fn function list --application-id "$(output command_application_id)" --display-name command-api --query 'data[0].id' --raw-output)"
  query_fn_ocid="$(run_oci fn function list --application-id "$(output projection_application_id)" --display-name query-api --query 'data[0].id' --raw-output)"
  SPEC="$(mktemp "${TMPDIR:-/tmp}/edacqrs-openapi.XXXXXX.yaml")"
  sed -e "s|__COMMAND_FUNCTION_OCID__|$command_fn_ocid|g" -e "s|__QUERY_FUNCTION_OCID__|$query_fn_ocid|g" -e "s|__API_JWKS_URI__|$TF_VAR_api_jwks_uri|g" -e "s|__API_JWT_ISSUER__|$TF_VAR_api_jwt_issuer|g" -e "s|__API_JWT_AUDIENCE__|$TF_VAR_api_jwt_audience|g" "$ROOT/gateway/openapi.template.yaml" > "$SPEC"
  run_oci api-gateway deployment create --gateway-id "$(output gateway_id)" --display-name "${TF_VAR_project}-${TF_VAR_environment}-api-v1" --path-prefix / --specification "file://$SPEC" --wait-for-state SUCCEEDED >/dev/null
}

deploy_oke_workloads() {
  local topic
  require_resources oci_managed_kafka_kafka_cluster.events oci_database_autonomous_database.write_model || return $?
  topic="$(output kafka_topic)"
  export KAFKA_BOOTSTRAP_SERVERS="$(kafka_bootstrap_servers)"
  run_oci ce cluster create-kubeconfig --cluster-id "$OKE_CLUSTER_OCID" --file "$KUBECONFIG_FILE" --region "$TF_VAR_region" --token-version 2.0.0 --kube-endpoint PRIVATE_ENDPOINT
  export KUBECONFIG="$KUBECONFIG_FILE"
  kubectl create namespace edacqrs --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n edacqrs get secret edacqrs-runtime >/dev/null || { echo "Pré-requisito bloqueante: Secret edacqrs-runtime deve ser sincronizado do OCI Vault via CSI/External Secrets."; return 10; }
  for manifest in consumer publisher; do
    local image_var rendered
    image_var="${manifest^^}_IMAGE"
    rendered="$(mktemp "${TMPDIR:-/tmp}/edacqrs-${manifest}.XXXXXX.yaml")"
    sed "s|__${manifest^^}_IMAGE__|${!image_var}|g" "$ROOT/deploy/${manifest}.yaml" > "$rendered"
    kubectl apply -f "$rendered"
    rm -f "$rendered"
  done
  kubectl -n edacqrs rollout status deployment/orders-projection --timeout=180s
  kubectl -n edacqrs rollout status deployment/orders-publisher --timeout=180s
}

validate_environment() {
  require_resources oci_managed_kafka_kafka_cluster.events oci_apigateway_gateway.public || return $?
  run_oci kafka cluster get --kafka-cluster-id "$(output kafka_cluster_ocid)" --query 'data.lifecycle-state' --raw-output
  echo "Kafka bootstrap: $(kafka_bootstrap_servers)"
  echo "Kafka topic: $(output kafka_topic)"
  echo "Gateway OCID: $(output gateway_id)"
}

echo "Provisionamento assistido EDA + CQRS na OCI"
init_terraform
stage "Fundação OCI: VCN, subnets privadas, Vault, Logging e Object Storage" provision_foundation
stage "Autonomous Database e Oracle NoSQL Database" provision_databases
stage "OCI Streaming with Apache Kafka" provision_kafka
stage "Tópicos Kafka e DLQ" configure_topics
stage "OCI Functions Applications" provision_function_apps
stage "OCI API Gateway" provision_gateway
stage "Schema do Autonomous Database" configure_schema
stage "Deploy das OCI Functions" deploy_functions
stage "Deployment da API no OCI API Gateway" deploy_gateway_api
stage "Publisher e Consumers no OKE" deploy_oke_workloads
stage "Validação final do ambiente" validate_environment

echo
echo "Execução assistida finalizada. Verifique os serviços criados antes de avançar para o go-live."
