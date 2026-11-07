# Render deployment and operation

This separate repository follows RateLab's GitHub/CI/Blueprint pattern. Its Blueprint creates only `sparklens-api-ajaynalla` (Python, Free, Virginia) and `sparklens-ajaynalla` (Static Site). It does not modify RateLab. Both tiers use Render as requested, whereas RateLab's documented frontend uses Sites.

## Apply and configure

1. In the same Render workspace, choose New → Blueprint and select `ajaynalla/spark-event-log-analyzer`, branch `main`.
2. Name it SparkLens. Review exactly one **Free** Python web service and one static site. Do not choose a paid plan.
3. Apply `render.yaml`. Python installs `requirements.txt`, binds `$PORT` with one Uvicorn worker and uses `/health`. The frontend builds frozen pnpm dependencies and strict TypeScript/Vite from `frontend/` to `dist/`.
4. If assigned hostnames differ, update static-site `VITE_API_BASE_URL` and backend `ALLOWED_ORIGINS` to exact HTTPS origins, then redeploy. The Vite variable is public build-time configuration, not a secret.
5. Services auto-deploy after GitHub checks pass. Build filters skip unrelated docs. Render manages HTTPS.

| Setting | Value / purpose |
|---|---|
| `ALLOWED_ORIGINS` | Comma-separated exact frontend origins, no wildcard |
| `VITE_API_BASE_URL` | Backend HTTPS origin, no path |
| Python / Node | 3.12.14 / 24.22.1 |
| Health | `/health`: status ok, version 1.0.0 |
| Upload | Raw UTF-8 JSONL, 25 MiB, no compression |
| Concurrency | One worker, one analysis slot; Uvicorn HTTP concurrency 12 |
| Deadlines | Upload 60s, cooperative analysis 30s, browser 180s including cold start |
| Storage | Temporary file deleted when request finishes |

`/docs` lists API routes. Application errors use `{"error":{"code":"...","message":"..."}}`. Render/Uvicorn may return overload/loading responses before the app receives a request; the frontend handles non-JSON responses with a retry message.

## Estimated cost and shared limits

**$0/month compute within free allowances.** No paid service/card is required. On November 7, 2026, the billing UI showed Hobby, no card, 750 free web hours/month, 5 GB bandwidth/month and 500 build minutes/month. Quotas are **shared with RateLab**. Pre-deployment usage: 5.25 hours, 5 MB bandwidth, 1 build minute.

Free web services sleep after 15 idle minutes and take about a minute to wake. Exhausted shared free hours suspend free web services. Static sites count against bandwidth/build quotas. With no payment method, exhausted quotas may suspend services/disable builds instead of billing. No paid resources or card should be enabled without separate approval. Sources: [Render free limits](https://render.com/docs/free), [Blueprint reference](https://render.com/docs/blueprint-spec). This is production-style software on hobby hosting, without a production SLA.

## Verify and operate

Run `python scripts/verify_deployment.py --frontend FRONTEND_URL --backend BACKEND_URL --output docs/live-verification.json`; add `--large` for 25,000 tasks. It uploads disposable synthetic/upstream fixtures and checks health, HTML, limits, sample equality, upstream metrics, errors and CORS. A manual GitHub Action uploads its verification report. HTTP success does not prove browser interactions; verify those separately.

Logs include random request ID, method, status and elapsed time. No original file names, bodies, query strings or client IDs are logged by application code. `X-Request-ID` correlates responses. Monitor 408/413/422/429 and Render restarts. Health checks show process reachability, not log completeness.

Rollback by selecting a retained successful Render deployment or reverting a Git commit, preserving history. Free hosting retains limited deploys. A CORS mismatch requires correcting exact origins and rebuilding/restarting the affected service.
