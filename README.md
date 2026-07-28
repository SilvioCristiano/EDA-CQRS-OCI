# EDA + CQRS na OCI

Implementação de referência do desenho: comandos síncronos gravam o estado e publicam eventos; projeções assíncronas atualizam um modelo de leitura no Oracle NoSQL Database Cloud Service. A infraestrutura é declarada em Terraform e as duas funções Python são pontos de partida implantáveis no OCI Functions.

## Decisões de arquitetura

| Camada | Serviço | Decisão operacional |
|---|---|---|
| Entrada | API Gateway + OCI Functions | JWT/OAuth no gateway; funções privadas e com IAM de mínimo privilégio. |
| Write | Autonomous Database | Uma transação grava agregado + outbox. O publicador só emite eventos já confirmados. |
| Eventos | OCI Streaming with Apache Kafka | Cluster Kafka gerenciado, tópicos por domínio, chave = `aggregateId` e consumidores idempotentes. |
| Read | OCI Functions + Oracle NoSQL | Uma projeção por consumidor/grupo; upsert idempotente com `eventId` e versão. |
| Auditoria | Object Storage + Logging/Audit | Logs estruturados sem dados sensíveis; retenção e lifecycle definidos. |

> **Garantia realista:** OCI Streaming oferece entrega *at-least-once*. Portanto, não prometa "exactly once" entre banco e broker: use transactional outbox no write side e deduplicação no read side.

## Estrutura

- `terraform/`: fundação OCI (rede privada, Vault, Functions, Streaming, ADB, NoSQL, Object Storage e Log Group).
- `functions/command/`: exemplo de função que valida e grava comando/outbox.
- `functions/projection/`: exemplo de consumidor idempotente para projeções.
- `docs/PROVISIONAMENTO.md`: roteiro de implantação, IAM, gateway, CI/CD e operação.
- `docs/CONTRATOS_EVENTO.md`: contrato de eventos e regras de evolução.

## Início rápido

1. Siga os pré-requisitos e as decisões de capacidade em [docs/PROVISIONAMENTO.md](docs/PROVISIONAMENTO.md).
2. Crie seu arquivo local de variáveis:

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Preencha os OCIDs, região e uma senha forte. Não versione terraform.tfvars.
terraform init
terraform fmt -check
terraform validate
terraform plan -out tfplan
terraform apply tfplan
```

3. Execute o SQL listado no guia no Autonomous Database, publique as imagens das funções no OCIR e configure o API Gateway com os endpoints gerados.

## Execução automatizada

Há um orquestrador em [scripts/provision.sh](scripts/provision.sh). Ele aplica o Terraform, schema ADB, Functions, deployment do gateway e o consumidor no OKE. Copie `scripts/provision.env.example` para `scripts/provision.env`, informe os valores reais e defina `APPLY=1`:

```bash
chmod +x scripts/provision.sh
scripts/provision.sh
```

Ele exige OCI CLI autenticado, Terraform, Fn CLI, SQLcl, `kubectl`, acesso ao OCIR, imagens imutáveis do publisher/consumer (`CONSUMER_IMAGE` e `PUBLISHER_IMAGE`) e um cluster OKE privado já entregue pela landing zone (`OKE_CLUSTER_OCID`). Como o Kafka é privado, execute-o em runner/bastion com rota para a VCN. A criação do cluster OKE e as policies globais de tenancy permanecem fora do script porque a configuração segura depende das regras corporativas de rede, versões Kubernetes aprovadas e administração da tenancy.

Para a explicação de negócio e o procedimento operacional completo, veja [CENARIO_NEGOCIO_E_EXECUCAO.md](docs/CENARIO_NEGOCIO_E_EXECUCAO.md).

O mesmo guia inclui uma seção dedicada à execução a partir da máquina local, incluindo VPN/bastion, OCI CLI, Fn/OCIR, SQLcl/ADB e acesso ao OKE privado.

Antes do deploy produtivo, execute o runbook de [pré-requisitos de segurança](docs/PRE_REQUISITOS_SEGURANCA.md).

O Terraform **não** cria usuários, senhas ou tokens de Streaming: segredos devem entrar no Vault/OCIR via pipeline ou `oci vault secret create`, nunca em `.tfvars`, código, logs ou estado Terraform.

## Limites deliberados

Este repositório entrega a fundação e o código de domínio sem assumir nomes de tenancy, OCIDs, política corporativa de identidade ou capacidade de produção. O gateway é configurado após o deploy das funções porque o URL de invoke é um artefato de release; o roteiro traz uma especificação OpenAPI parametrizada. O backbone adotado é **OCI Streaming with Apache Kafka**: cluster Kafka gerenciado em sub-rede privada, configuração explícita e tópicos criados por admin client.

Consulte também a documentação oficial de [OCI Streaming](https://docs.oracle.com/en-us/iaas/Content/Streaming/Concepts/streamingoverview.htm), [Kafka gerenciado](https://docs.oracle.com/en-us/iaas/Content/kafka/overview.htm), [OCI Functions](https://docs.oracle.com/en-us/iaas/Content/Functions/Tasks/functionscreatingapps-task.htm) e [provider Terraform OCI](https://docs.oracle.com/en-us/iaas/tools/terraform-provider-oci/latest/index.html).
