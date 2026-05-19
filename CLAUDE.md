# CLAUDE.md — titlis-prbot

> Leia o CLAUDE.md raiz antes de qualquer alteração.

## O que é este serviço

`titlis-prbot` é o **único serviço autorizado a escrever no GitHub** na plataforma Titlis.
Orquestra campanhas de PRs de HPA tuning via Temporal Workflows, respeitando o GitOps profile
do tenant e a política de remediação automática.

**Porta:** 8080 (configurável via `PORT`)
**Task queue Temporal:** `prbot-main` (configurável via `TEMPORAL_TASK_QUEUE`)

## Responsabilidades

- Receber solicitações de campanha via HTTP (`/v1/campaigns`) e disparar `CampaignWorkflow`
- Gerenciar GitHub App tokens (TTL 50m, renovação automática via `ghinstallation/v2`)
- Aplicar patches YAML em manifestos de HPA (never-reduce obrigatório)
- Orquestrar promoção dev→hml→prd via `PromotionWorkflow`
- Descoberta automática de drift via `DiscoverDriftWorkflow` (Temporal Schedule, 03:00 UTC diário)
- Receber webhooks do GitHub e correlacionar com workflows via signals

## O que NÃO é responsabilidade

- Avaliar regras de scorecard → `titlis-scoreops`
- Fornecer recomendações de HPA → `titlis-insights`
- Acessar Kubernetes → `titlis-operator-go`
- Persistir campanhas em `titlis_oltp` → `titlis-api` (via UDP/HTTP events)

## Hierarquia de Workflows

```
DiscoverDriftWorkflow (Temporal Schedule diário)
  └─ CampaignWorkflow
       └─ ItemWorkflow
            └─ PromotionWorkflow
                 └─ EnvDeployWorkflow (1 por env)

ReactiveCampaignWorkflow → CampaignWorkflow
```

## GitProvider (abstração crítica)

Activities **nunca** importam `go-github` diretamente. Usam sempre a interface
`internal/gitprovider.GitProvider`. O cliente concreto fica em `internal/gitprovider/github/`.
GitLab v2 seria `internal/gitprovider/gitlab/` — namespace reservado.

## Repos de dados

| Variável | Repos | Padrão |
|---|---|---|
| `DATABASE_URL` vazio | `MemoryMappings`, `MemoryProfiles`, `MemoryPolicies` | dev/local |
| `DATABASE_URL` setado | `PGMappings`, `PGProfiles`, `PGPolicies` | preprod/prod |

Schema PostgreSQL: `titlis_prbot.*` (ver docs/titlis-prbot-arquitetura.md §12)

## Variáveis de ambiente principais

```
PORT=8080
TITLIS_APP_ENV=local|preprod|prod
TITLIS_API_INTERNAL_SECRET=<secret>
TITLIS_API_HOST=titlis-api
TITLIS_API_PORT=8080
TITLIS_API_UDP_HOST=titlis-api
TITLIS_API_UDP_PORT=8125
TITLIS_INSIGHTS_HOST=titlis-insights
TITLIS_INSIGHTS_PORT=8091
TITLIS_INSIGHTS_INTERNAL_SECRET=<secret>
TEMPORAL_HOST=temporal-frontend:7233
TEMPORAL_NAMESPACE=titlis
TEMPORAL_TASK_QUEUE=prbot-main
GITHUB_APP_ID=<id>
GITHUB_APP_INSTALLATION_ID=<id>   # default por tenant; cada tenant pode ter o seu
GITHUB_APP_PRIVATE_KEY_PATH=/secrets/github-app.pem
GITHUB_WEBHOOK_SECRET=<secret>
DATABASE_URL=postgres://...        # vazio → memory repos
PRBOT_USE_MEMORY_PROVIDER=true    # false em prod
PRBOT_DISABLE_TEMPORAL=false
```

## Padrões obrigatórios

1. **Never-reduce:** `ValidatePatch` em `internal/activity/activities.go` rejeita qualquer
   patch que reduza `minReplicas`, `maxReplicas` ou `targetCPUUtilizationPercentage`.
2. **prd nunca auto-merge em descoberta/reativo:** `PromotionWorkflow` força `AWAITING_HUMAN`
   para prd quando `TriggerSource != manual`.
3. **Multi-tenant:** todo SQL e activity filtra por `tenant_id`. Nunca omita.
4. **Idempotência por fingerprint:** `CampaignID` = chave de idempotência no Temporal.
   Reprocessar o mesmo campaign_id é safe — Temporal deduplica.
5. **Webhooks com HMAC:** `ParseWebhook` em `internal/gitprovider/github/client.go`
   valida assinatura antes de processar qualquer evento.

## Como rodar localmente

```bash
cd titlis-prbot
PRBOT_USE_MEMORY_PROVIDER=true PRBOT_DISABLE_TEMPORAL=true go run ./cmd/prbot/
```

## Build e test

```bash
go build -buildvcs=false ./...
go test ./...
```
