# Pré-requisitos de segurança — execução detalhada

Execute estas etapas antes de responder `Y` para o deploy de Functions/OKE. Substitua todos os valores `<...>` e mantenha segredos fora do histórico de shell e do Git.

## 1. Criar `DB_PASSWORD_SECRET_OCID` no OCI Vault

Pré-requisito: a etapa Fundação do `provision.sh` já criou Vault e chave. Obtenha os OCIDs:

```bash
VAULT_ID="$(terraform -chdir=terraform output -raw vault_id)"
KEY_ID="$(oci kms management key list --management-endpoint "$(oci kms vault get --vault-id "$VAULT_ID" --query 'data.management-endpoint' --raw-output)" --compartment-id "$TF_VAR_compartment_ocid" --query 'data[0].id' --raw-output)"
```

Leia a senha sem exibi-la e crie o secret. O comando retorna o OCID, que deve ir para `DB_PASSWORD_SECRET_OCID` em `scripts/provision.env`.

```bash
read -r -s -p 'Senha do usuário ADB: ' DB_SECRET_VALUE; echo
DB_PASSWORD_SECRET_OCID="$(printf %s "$DB_SECRET_VALUE" | base64 | tr -d '\n' | xargs -I{} oci vault secret create-base64 \
  --compartment-id "$TF_VAR_compartment_ocid" \
  --vault-id "$VAULT_ID" --key-id "$KEY_ID" \
  --secret-name "${TF_VAR_project}-${TF_VAR_environment}-adb-password" \
  --description 'Senha da aplicação ADB' \
  --secret-content-content {} --secret-content-name current \
  --secret-content-stage CURRENT --query 'data.id' --raw-output)"
unset DB_SECRET_VALUE
echo "DB_PASSWORD_SECRET_OCID=$DB_PASSWORD_SECRET_OCID"
```

Verifique apenas metadados, nunca o conteúdo:

```bash
oci vault secret get --secret-id "$DB_PASSWORD_SECRET_OCID" --query 'data.{id:id,name:"secret-name",state:"lifecycle-state"}'
```

Defina regra de expiração/rotação conforme política corporativa. A criação de secret via CLI usa chave simétrica do Vault. [Documentação OCI Vault](https://docs.oracle.com/en-us/iaas/Content/secret-management/Tasks/create-secret.htm)

## 2. Habilitar Workload Identity e Vault CSI no OKE

1. Confirme com a plataforma que o cluster OKE suporta Workload Identity e que a feature está habilitada.
2. Instale o Secrets Store CSI Driver e o provider OCI seguindo o procedimento homologado da plataforma. O driver monta secrets externos de OCI Vault; não use `kubectl create secret --from-literal`.
3. Crie service accounts dedicadas, sem permissões Kubernetes extras:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata: {name: edacqrs-consumer, namespace: edacqrs}
---
apiVersion: v1
kind: ServiceAccount
metadata: {name: edacqrs-publisher, namespace: edacqrs}
```

4. Associe cada service account à identidade OCI definida pelo padrão OKE da organização e conceda somente leitura aos secrets necessários. Não reutilize a identidade de node pool.
5. Habilite criptografia etcd com KMS se sua estratégia sincronizar secrets para objetos Kubernetes; a opção preferida é montá-los como volume CSI, evitando cópia persistente em etcd.

Confirme que os pods usam as service accounts corretas:

```bash
kubectl -n edacqrs get serviceaccounts
kubectl -n edacqrs get pod -o jsonpath='{range .items[*]}{.metadata.name}{" => "}{.spec.serviceAccountName}{"\n"}{end}'
```

OCI descreve OCI Vault como external secrets store para o CSI driver. [Documentação OKE](https://docs.oracle.com/en-us/iaas/Content/ContEng/Tasks/contengmanagingsecrets.htm)

## 3. Criar `edacqrs-runtime` a partir do Vault

Crie no Vault secrets separados para `DB_PASSWORD`, `KAFKA_AUTH_TOKEN` e, se aplicável, certificados. A plataforma deve aplicar um `SecretProviderClass` usando o provider OCI e sincronizar para o Secret Kubernetes `edacqrs-runtime` apenas se a aplicação exigir variáveis de ambiente.

Modelo a adaptar ao provider CSI homologado:

```yaml
apiVersion: secrets-store.csi.x-k8s.io/v1
kind: SecretProviderClass
metadata: {name: edacqrs-runtime, namespace: edacqrs}
spec:
  provider: oci
  parameters:
    authType: workloadIdentity
    secrets: |
      - objectName: <DB_PASSWORD_SECRET_OCID>
        objectType: secret
      - objectName: <KAFKA_AUTH_TOKEN_SECRET_OCID>
        objectType: secret
  secretObjects:
    - secretName: edacqrs-runtime
      type: Opaque
      data:
        - objectName: <DB_PASSWORD_SECRET_OCID>
          key: DB_PASSWORD
        - objectName: <KAFKA_AUTH_TOKEN_SECRET_OCID>
          key: KAFKA_AUTH_TOKEN
```

Acrescente no mesmo Secret somente valores não sensíveis necessários (`DB_USER`, `DB_DSN`, `KAFKA_USERNAME`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`) ou prefira ConfigMap. Monte o volume CSI em cada deployment conforme as instruções do provider. Depois valide:

```bash
kubectl -n edacqrs get secret edacqrs-runtime
kubectl -n edacqrs describe secret edacqrs-runtime # não imprime valores
```

O script bloqueia o deploy OKE até esse Secret existir.

## 4. Criar SASL/SCRAM e ACLs Kafka mínimas

1. Crie secrets Vault manuais distintos para `kafka-admin`, `orders-publisher` e `orders-read-model`; gere senhas aleatórias longas.
2. Configure SASL/SCRAM no cluster usando o secret administrativo:

```bash
oci kafka cluster enable-superuser \
  --compartment-id "$TF_VAR_compartment_ocid" \
  --kafka-cluster-id "$(terraform -chdir=terraform output -raw kafka_cluster_ocid)" \
  --secret-id <KAFKA_ADMIN_SECRET_OCID>
```

3. A partir de um bastion na VCN, crie `client.properties` com `security.protocol=SASL_SSL` e `sasl.mechanism=SCRAM-SHA-512`. Nunca versione esse arquivo.
4. Com o Kafka authorizer CLI, crie ACLs. Exemplo para o tópico `edacqrs.prd.orders.domain.v1`:

```bash
kafka-acls --bootstrap-server <KAFKA_BOOTSTRAP> --command-config client.properties --add \
  --allow-principal User:orders-publisher --operation Write --topic edacqrs.prd.orders.domain.v1
kafka-acls --bootstrap-server <KAFKA_BOOTSTRAP> --command-config client.properties --add \
  --allow-principal User:orders-read-model --operation Read --topic edacqrs.prd.orders.domain.v1
kafka-acls --bootstrap-server <KAFKA_BOOTSTRAP> --command-config client.properties --add \
  --allow-principal User:orders-read-model --operation Read --group orders-read-model-v1
```

5. Atualize a configuração do cluster com `allow.everyone.if.no.acl.found=false`, valide acesso de cada principal e revogue/remova o superuser de uso diário.

OCI recomenda SCRAM-SHA-512 e exige atualizar configuração após criar/rotacionar credencial; ACLs devem usar deny-by-default. [SASL/SCRAM](https://docs.oracle.com/en-us/iaas/Content/kafka/security-sasl.htm), [ACL Kafka](https://docs.oracle.com/en-us/iaas/Content/kafka/security-acl.htm)

## 5. Criar IAM policies de mínimo privilégio

Crie dynamic groups distintos para Functions e para as identidades OKE, usando as regras de matching do seu padrão corporativo. Exemplo de intenção:

```text
Allow dynamic-group edacqrs-functions-dg to read secret-bundles in compartment <security-compartment> where target.secret.id = '<DB_PASSWORD_SECRET_OCID>'
Allow dynamic-group edacqrs-oke-publisher-dg to read secret-bundles in compartment <security-compartment> where target.secret.id = '<KAFKA_PUBLISHER_SECRET_OCID>'
Allow dynamic-group edacqrs-oke-consumer-dg to read secret-bundles in compartment <security-compartment> where target.secret.id = '<KAFKA_CONSUMER_SECRET_OCID>'
Allow service rawfka to {SECRET_UPDATE} in compartment <security-compartment>
Allow service rawfka to use secrets in compartment <security-compartment> where request.operation = 'UpdateSecret'
```

Inclua as policies básicas de Functions para rede/OCIR conforme o template oficial, mas evite `manage all-resources`. A Function usa Resource Principal; ela precisa pertencer ao dynamic group e receber somente `read secret-bundles` para o secret ADB. Após mudanças em dynamic group/policy, aguarde a propagação do token Resource Principal antes de testar. [Functions e Resource Principal](https://docs.oracle.com/en-us/iaas/Content/Functions/Tasks/functionsaccessingociresources.htm)

## Validação final

- Function de comando consegue ler apenas `DB_PASSWORD_SECRET_OCID`; tentativa de outro secret recebe `NotAuthorizedOrNotFound`.
- Pod publisher não consegue ler secret do consumer e vice-versa.
- Principal publisher só produz; principal consumer só lê tópico/grupo autorizado.
- Requisição sem JWT ao gateway privado retorna `401`; token sem escopo adequado recebe `403` após política de autorização por rota.
- `kubectl -n edacqrs get secret` não é permitido a usuários não administradores do namespace.
