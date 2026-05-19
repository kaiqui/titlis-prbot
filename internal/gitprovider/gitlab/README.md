# GitLab provider (roadmap v2)

Pendente. Implementação deve satisfazer `gitprovider.GitProvider`:

- `FetchFile` — `GET /projects/:id/repository/files/:path` com `ref=branch`.
- `CreateBranch` — `POST /projects/:id/repository/branches`.
- `CommitFile` — `POST /projects/:id/repository/commits` com `action=update`.
- `OpenPR` — `POST /projects/:id/merge_requests`.
- `FindOpenPR` — `GET /projects/:id/merge_requests?state=opened&source_branch=...`.
- `MergePR` — `PUT /projects/:id/merge_requests/:iid/merge`.
- `WaitChecks` — `GET /projects/:id/merge_requests/:iid/pipelines`.
- `ParseWebhook` — `X-Gitlab-Event: Merge Request Hook`, validar `X-Gitlab-Token`.

Não introduzir mudanças em workflows nem activities ao adicionar este adapter.
