# Cenário de negócio e execução do ambiente

## Cenário fictício: Varejo Aurora

A Varejo Aurora vende eletrônicos no e-commerce, aplicativo e lojas parceiras. Em uma campanha, milhares de clientes podem criar pedidos ao mesmo tempo. Depois que um pedido é aceito, diversas áreas precisam reagir: estoque reserva itens, pagamento autoriza a cobrança, logística prepara a entrega, atendimento consulta o status e BI acompanha a operação.

Antes da arquitetura EDA + CQRS, o endpoint `POST /orders` chamava estoque, pagamento e logística de forma síncrona. Se um serviço estivesse lento, o cliente esperava; se falhasse depois de o pedido ter sido gravado, era difícil descobrir quais sistemas tinham sido atualizados. Escalar consultas de status também pressionava o banco transacional que gravava o pedido.

Com esta arquitetura, o pedido é aceito rapidamente e o restante ocorre de forma rastreável e desacoplada.

| Momento do negócio | Componente técnico | Resultado |
|---|---|---|
| Cliente confirma o carrinho | API Gateway → Function de comando | Autentica o cliente, valida o comando e aceita a solicitação. |
| Pedido é registrado | Autonomous Database | Fonte de verdade: pedido, versão e outbox ficam na mesma transação. |
| Evento é liberado | Publisher OKE → OCI Streaming with Apache Kafka | `OrderCreated` é publicado de forma durável no tópico. |
| Áreas reagem sem bloquear venda | Consumer groups OKE | Cada time consome independentemente: estoque, pagamento, logística e projeções. |
| Cliente consulta pedido | API Gateway → Query API → NoSQL | Leitura rápida, sem competir com a gravação transacional. |
| Falha de integração | Retry/DLQ/Monitoring | A venda não é perdida; evento pode ser reprocessado depois. |

### Exemplo ponta a ponta

1. Ana envia `POST /v1/orders` para comprar um notebook, com `Idempotency-Key: compra-ana-2026-0001`.
2. O API Gateway confere o JWT e encaminha à Function. A função cria `ord_1001`, grava o pedido e uma linha `PENDING` na outbox do ADB. Ela retorna `202 Accepted`.
3. O publisher lê a outbox e publica `edacqrs.prd.orders.domain.v1`, usando `ord_1001` como chave Kafka. Todos os eventos daquele pedido ficam na mesma partição e preservam sua ordem.
4. O consumer `orders-read-model-v1` grava a visão de consulta. Em paralelo, `inventory-v1` reserva estoque e `shipping-v1` cria a solicitação de entrega.
5. Ana consulta `GET /v1/orders/ord_1001`. Pode ver `CREATED` antes de a entrega existir; alguns segundos depois a projeção mostra `READY_FOR_SHIPMENT`. Isso é consistência eventual, explícita e aceitável para a tela de acompanhamento.
6. Se logística ficar indisponível, apenas o grupo dela acumula lag. Pedido, estoque e consulta continuam operando. Após as tentativas, a mensagem vai para DLQ e o time reprocessa o evento sem duplicar o pedido.

### Por que cada decisão importa

- **Outbox transacional:** evita o caso crítico “pedido gravado, evento perdido”. A confirmação do pedido e a intenção de publicar são atômicas no ADB.
- **Kafka gerenciado OCI:** os domínios não dependem de chamadas síncronas entre si. Há consumer groups, replay e escala horizontal por partição.
- **Chave igual ao ID do pedido:** preserva a sequência `Created → Paid → Shipped` daquele pedido, sem exigir ordenação global que reduziria escala.
- **NoSQL para leitura:** a tela do cliente pode escalar por chave sem abrir conexões ou executar joins no write model.
- **DLQ e idempotência:** Kafka entrega ao menos uma vez; processar duas vezes não pode reservar estoque duas vezes nem corromper projeções.
- **Vault/IAM/rede privada:** credenciais, brokers e bancos não ficam expostos publicamente nem embutidos nas imagens.

## Como executar o script de provisionamento

O script [provision.sh](../scripts/provision.sh) é um orquestrador. Ele não adivinha valores organizacionais: você fornece OCIDs, imagem, versão Kafka, acesso a banco e credenciais SASL. Execute-o em um runner de CI/CD, bastion ou máquina dentro da VCN que alcança o cluster Kafka privado e o endpoint privado do OKE.

## Execução a partir da sua máquina local

Este roteiro vale para macOS ou Linux. A máquina local chama APIs públicas de controle OCI (Terraform/OCI CLI), mas também precisa alcançar endpoints **privados** durante a criação de tópicos Kafka, acesso ao ADB privado e `kubectl` contra OKE privado. Portanto, conecte antes à VPN corporativa, FastConnect/VPN site-to-site ou use um bastion/runner dentro da VCN. Sem essa rota, o Terraform pode criar recursos, mas as etapas Kafka, SQLcl e OKE falharão por rede.

### Passo 1 — Preparar a conectividade

1. Conecte-se à VPN corporativa que anuncia a VCN OCI; confirme com a equipe de rede que há rota para as subnets privadas do Kafka, ADB e OKE.
2. Solicite liberação de DNS privado e portas necessárias: TCP 443 para APIs OCI/OKE, a porta TLS dos brokers Kafka definida pelo cluster e a porta do ADB definida no wallet/service.
3. Se não houver VPN, execute o mesmo repositório em uma VM de administração na VCN ou acesse-a via OCI Bastion. Não habilite endpoint público de Kafka ou OKE apenas para rodar o script.

### Passo 2 — Instalar ferramentas locais

Verifique que os comandos abaixo existem no `PATH`:

```bash
terraform version
oci --version
fn version
docker version
sql -version
kubectl version --client
python3 --version
```

Você precisa de Terraform >= 1.6, OCI CLI, Fn CLI, Docker, SQLcl, `kubectl` e Python 3 com `venv`. Instale-os pelos canais oficiais do seu sistema operacional. O script instala apenas a biblioteca Python administrativa do Kafka em `.venv-provision`; não instala as ferramentas de plataforma.

### Passo 3 — Configurar autenticação OCI local

Crie um profile OCI e valide a identidade. O usuário ou principal usado deve ter as policies necessárias no compartment do ambiente.

```bash
oci setup config
oci iam availability-domain list
```

No macOS/Linux, o arquivo normalmente fica em `~/.oci/config`; proteja-o e sua chave privada:

```bash
chmod 600 ~/.oci/config ~/.oci/*.pem
```

No arquivo `scripts/provision.env`, informe o nome desse profile em `OCI_CLI_PROFILE`. Não copie chave privada, fingerprint ou OCID de usuário para o repositório.

### Passo 4 — Preparar Container Registry e Fn CLI

1. Faça login no OCIR com o usuário/namespace e auth token OCI adequados.
2. Configure o contexto Fn para a mesma região/compartment das Functions.
3. Construa e envie ao OCIR as imagens do publisher e consumidor.
4. Copie os digests resultantes (`@sha256:...`) para `CONSUMER_IMAGE` e `PUBLISHER_IMAGE`.

O script usa `fn deploy` para Function de comando e consulta. Docker deve estar em execução localmente, pois o Fn CLI constrói/publica suas imagens.

### Passo 5 — Preparar acesso ao ADB e ao OKE

- **ADB:** obtenha o wallet, descompacte-o em diretório protegido e configure `TNS_ADMIN` antes de executar o script. Teste o usuário/DSN com SQLcl.
- **OKE:** use um cluster privado já criado. O script gera um kubeconfig temporário e o remove ao terminar; sua máquina precisa alcançar o endpoint privado via VPN/bastion.

Exemplo de teste ADB, sem gravar senha no histórico:

```bash
export TNS_ADMIN="$HOME/oci-wallets/edacqrs-prd"
sql /nolog
# No SQLcl: connect APP_USER@edacqrs_high
```

### Passo 6 — Baixar o projeto e configurar variáveis

```bash
cd /caminho/para/EDACQRS
cp scripts/provision.env.example scripts/provision.env
chmod 600 scripts/provision.env
```

Abra o arquivo e preencha somente as entradas `[PREENCHER]`. Mantenha as entradas `[GERADO AUTOMATICAMENTE]` vazias/fora do arquivo. Para secrets, prefira exportá-los na sessão do terminal ou usar o gerenciador de secrets da sua máquina/CI, em vez de deixá-los no arquivo.

### Passo 7 — Validar sem criar recursos

Com a VPN conectada e Docker em execução:

```bash
set -a
source scripts/provision.env
set +a

terraform -chdir=terraform init
terraform -chdir=terraform fmt -check
terraform -chdir=terraform validate
terraform -chdir=terraform plan
oci iam region-subscription list --tenancy-id "$TF_VAR_tenancy_ocid"
```

Revise o plano e o custo, especialmente o cluster Kafka `PRODUCTION`, ADB, armazenamento dos brokers e retenção/partições dos tópicos.

### Passo 8 — Executar o provisionamento assistido

No arquivo, altere `APPLY=0` para `APPLY=1`. Em seguida:

```bash
scripts/provision.sh
```

Responda `Y` apenas depois de validar cada componente proposto. Uma ordem recomendada é: fundação → bancos → Kafka → tópicos → Functions → gateway → schema → deploy Functions → deployment do gateway → OKE → validação. Se responder `N`, corrija ou revise a configuração e execute novamente; o script identifica dependências pendentes e não deve criar recursos dependentes como efeito colateral.

### Passo 9 — Conferir o resultado localmente

```bash
terraform -chdir=terraform output
oci kafka cluster get --kafka-cluster-id "$(terraform -chdir=terraform output -raw kafka_cluster_ocid)"
```

Para OKE, gere um kubeconfig novo apenas se precisar de diagnóstico; não preserve o temporário do script:

```bash
oci ce cluster create-kubeconfig \
  --cluster-id <OKE_CLUSTER_OCID> \
  --file "$HOME/.kube/edacqrs" \
  --region "$TF_VAR_region" \
  --token-version 2.0.0 \
  --kube-endpoint PRIVATE_ENDPOINT
KUBECONFIG="$HOME/.kube/edacqrs" kubectl -n edacqrs get deployments,pods
```

### Checklist local antes de responder `Y`

- VPN/bastion ativo e DNS privado resolvendo endpoints OCI.
- Docker em execução e Fn CLI apontando para região/OCIR corretos.
- Profile OCI com permissões e `OCI_CLI_PROFILE` correto.
- Wallet ADB e `TNS_ADMIN` configurados; SQLcl testado.
- `TF_VAR_kafka_version` e `TF_VAR_kafka_coordination_type` compatíveis com a região.
- Imagens do publisher/consumer no OCIR por digest, não tag mutável.
- `scripts/provision.env` protegido por `chmod 600` e fora do Git.

### 1. Pré-requisitos da estação/runner

Instale e autentique:

- OCI CLI, autenticado no profile que pode criar recursos no compartment.
- Terraform >= 1.6.
- Fn CLI, Docker e login no OCIR para publicar Functions.
- SQLcl, com wallet/DSN configurado para o ADB.
- `kubectl` com acesso ao endpoint privado do OKE.
- Python 3 com módulo `venv`; o script cria `.venv-provision` e instala `kafka-python` para administrar tópicos.

Além disso, a landing zone deve disponibilizar o **OKE privado** e as policies de tenancy. O script cria a fundação da aplicação e o cluster Kafka, mas não cria o OKE nem policies globais sem regras corporativas aprovadas.

### 2. Descobrir valores OCI e preparar o arquivo local

```bash
cd /Users/silvio/Documents/EDACQRS
cp scripts/provision.env.example scripts/provision.env
chmod 600 scripts/provision.env
```

Edite `scripts/provision.env` e informe:

| Variável | Como obter / usar |
|---|---|
| `TF_VAR_tenancy_ocid` | Console OCI → Tenancy details. |
| `TF_VAR_compartment_ocid` | Compartimento destinado ao ambiente. |
| `TF_VAR_region` | Região OCI, por exemplo `sa-saopaulo-1`. |
| `TF_VAR_project`, `TF_VAR_environment` | Prefixo de nomes e ambiente: `dev`, `hml` ou `prd`. |
| `TF_VAR_adb_admin_password` | Senha inicial do ADB; em CI, injete como segredo, não como arquivo. |
| `TF_VAR_kafka_version` | Versão oferecida na região. Consulte Console OCI ou `oci kafka cluster create --help`. |
| `TF_VAR_kafka_coordination_type` | Valor compatível com a versão Kafka escolhida; obtenha no Console/CLI da região. |
| `OKE_CLUSTER_OCID` | OCID do cluster OKE privado existente. |
| `CONSUMER_IMAGE`, `PUBLISHER_IMAGE` | Imagens no OCIR, sempre fixadas por digest `@sha256:...`. |
| `DB_USER`, `DB_PASSWORD`, `DB_DSN` | Usuário de aplicação, senha e service name/wallet ADB. |
| `KAFKA_USERNAME`, `KAFKA_AUTH_TOKEN` | Credencial SASL do cluster. Armazene/recupere o segredo via Vault. |

Para ambiente de produção, mantenha o arquivo sem segredos: exporte-os a partir do secret manager do pipeline antes da chamada. Nunca faça commit de `scripts/provision.env`.

Os valores **gerados automaticamente** não devem ser preenchidos no arquivo: `kafka_cluster_ocid`, `kafka_bootstrap_servers`, `kafka_topic`, `adb_ocid`, `gateway_id` e OCIDs das aplicações Functions. Após o deploy, consulte-os com:

```bash
terraform -chdir=terraform output
```

O tópico segue automaticamente a convenção `<project>.<environment>.orders.domain.v1`; a DLQ é `<topic>.dlq`. Assim, com `project=edacqrs` e `environment=prd`, o script cria `edacqrs.prd.orders.domain.v1` e `edacqrs.prd.orders.domain.v1.dlq`.

### 3. Preparar as imagens

O script aplica manifests que referenciam imagens já publicadas. Construa, teste, escaneie e envie as imagens do publisher e consumer ao OCIR. Atualize as variáveis com o digest imutável resultante. A imagem do consumer deve conter `consumer.py`; a do publisher, `outbox_publisher.py` e suas dependências.

### 4. Revisar o plano sem criar recursos

O script exige confirmação deliberada. Primeiro mantenha `APPLY=0` e revise o Terraform manualmente:

```bash
set -a
source scripts/provision.env
set +a
terraform -chdir=terraform init
terraform -chdir=terraform fmt -check
terraform -chdir=terraform validate
terraform -chdir=terraform plan
```

Confirme particularmente CIDRs, tipo `PRODUCTION`, três brokers, forma/armazenamento, tags e custo mensal do Kafka/ADB/OKE.

### 5. Executar

Depois da aprovação, altere somente esta variável:

```bash
APPLY=1
```

E inicie:

```bash
scripts/provision.sh
```

O script é **assistido**: antes de cada etapa imprime `Iniciando provisionamento`, pede `Deseja executar esta etapa? [Y/N]`, executa apenas com `Y` e termina a etapa com `criado/configurado com sucesso`. Com `N`, ele não executa a etapa e continua para a próxima; reexecute o script depois para retomá-la.

As etapas são:

1. Confere dependências e autenticação OCI.
2. Fundação de rede, Vault, Logging e Object Storage.
3. ADB e NoSQL.
4. Cluster OCI Streaming with Apache Kafka.
5. Tópico de domínio e DLQ.
6. Applications OCI Functions.
7. Gateway e, em uma etapa separada, seu deployment OpenAPI.
8. Schema ADB e deploy das Functions.
9. Publisher e consumer no OKE.
10. Validação final.

### 6. Validar após a execução

```bash
terraform -chdir=terraform output
kubectl --kubeconfig <kubeconfig-privado> -n edacqrs get deploy,pods
kubectl --kubeconfig <kubeconfig-privado> -n edacqrs logs deploy/orders-publisher --tail=100
oci kafka cluster get --kafka-cluster-id <ocid-retornado>
```

Envie um comando de teste por meio do endpoint do API Gateway. Verifique que há uma linha na `outbox`, que ela se torna `PUBLISHED`, que o consumer confirma o offset e que a projeção aparece no read model. Use uma chave de idempotência repetida para provar que o pedido não é duplicado.

### Diagnóstico rápido

| Sintoma | Causa comum | Ação |
|---|---|---|
| `terraform plan` falha no Kafka | versão ou coordination type indisponível | Consulte as opções da região e corrija as duas variáveis. |
| Tópico não é criado | runner não alcança subnet privada, SASL incorreto ou ACL | Execute no bastion/runner da VCN e confira secret/ACL. |
| Pod em `CrashLoopBackOff` | imagem/digest, secret ou DSN incorreto | `kubectl logs`, valide o Secret e a conectividade ADB/Kafka. |
| Function retorna 503 | wallet, usuário ou rede do ADB | Teste o mesmo DSN com SQLcl e revise NSG/Service Gateway. |
| Gateway responde 401 | issuer, audience, JWKS ou scope divergente | Confirme a configuração OIDC/JWT no deployment. |

### Segurança pós-bootstrap

O script não cria Kubernetes Secret com senha/token. Antes do go-live, use Vault CSI ou External Secrets com Workload Identity, remova acesso de operadores a secrets e roteie credenciais para rotação. Configure também JWT/OIDC, ACLs Kafka por aplicação, NSGs mínimos, alarmes e a política de backup/DR do guia principal.

## Pré-requisitos de segurança para OKE e Functions

O provisionamento agora bloqueia o deploy OKE se o Secret `edacqrs-runtime` não existir. Ele deve ser sincronizado do OCI Vault por Vault CSI/External Secrets e conter somente os valores necessários aos pods. Não crie esse Secret com `kubectl --from-literal`.

Antes de executar a etapa OKE, a equipe de plataforma deve:

1. Habilitar OKE Workload Identity e criar os service accounts `edacqrs-consumer` e `edacqrs-publisher` vinculados às identidades OCI correspondentes.
2. Instalar/configurar o OCI Vault CSI driver ou External Secrets para sincronizar `edacqrs-runtime` do Vault; habilitar criptografia de secrets etcd com KMS.
3. Criar dynamic groups e policies de mínimo privilégio: Functions podem apenas ler `DB_PASSWORD_SECRET_OCID`; publisher/consumer podem apenas ler seus secrets Kafka/ADB. Nenhum workload recebe `manage` amplo.
4. Criar NetworkPolicies no namespace permitindo apenas DNS, ADB e brokers Kafka; negar todo o restante.
5. Criar usuários SASL/SCRAM distintos para publisher, consumer e administração, e ACLs Kafka: publisher somente `Write` no tópico do domínio; consumer somente `Read` no tópico e no respectivo consumer group; administração temporária somente para criação de tópicos/ACLs.

As Functions não recebem mais `DB_PASSWORD`: elas recebem apenas `DB_PASSWORD_SECRET_OCID` e usam Resource Principal para ler o valor no Vault. Isso requer a policy OCI de leitura de secret para o dynamic group de Functions.

## Controles médios que devem ser validados

- **State Terraform:** usar backend remoto em Object Storage com versionamento, criptografia, lock e acesso da pipeline; nunca manter `tfplan` ou state com senha na máquina local.
- **Supply chain:** substituir faixas abertas de dependência por lockfile com hashes, repositório de pacotes aprovado, scan de imagem/assinatura e digests imutáveis OCIR.
- **Vault/KMS:** associar a chave gerenciada pelo cliente aos recursos que suportam CMEK, definir rotação e alarmes de expiração; a simples criação da chave não protege recursos automaticamente.
- **Observabilidade:** aplicar logs de API Gateway/Functions/Kafka, alarmes de 4xx/5xx, lag/DLQ, outbox pendente, falha de secret e alterações IAM; definir retenção/lifecycle no Object Storage.
- **Dados:** validar PII/LGPD no payload de eventos, mascarar logs, definir TTL/retenção e testar restore/replay periodicamente.
