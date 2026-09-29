# Worm AI — Telegram Bot (Vercel Webhook)

Worm AI API-r Telegram bot — Vercel serverless-e webhook mode-e chole.
Polling version-er sob feature ache: AI mode, `/worm` command, group support,
forced channel join, flood protection, cooldown, per-user conversation.

## Deploy (Vercel)

1. Vercel dashboard-e **Add New Project** → ei repo import koro.
2. **Environment Variables**-e egulo boshao:

| Name | Value |
|---|---|
| `BOT_TOKEN` | BotFather theke paowa token |
| `WORM_AI_API_KEY` | tomar Worm AI API key |
| `WEBHOOK_SECRET` | jekono random lomba string (niche webhook URL-eo same ta lagbe) |
| `WORM_AI_API_URL` | (optional) default: `https://worm-ai-lilac.vercel.app/api/worm-ai` |
| `REQUIRED_CHANNEL` | (optional) default: `@earning_zone_bangla` |
| `REQUIRED_CHANNEL_URL` | (optional) default: `https://t.me/earning_zone_bangla` |

3. **Deploy** chap dao.
4. Deploy ses hole browser-e ei link kholo (`<TOKEN>`, `<APP_URL>`, `<SECRET>`
   nijer ta diye bodle):

```
https://api.telegram.org/bot<TOKEN>/setWebhook?url=https://<APP_URL>/&secret_token=<SECRET>
```

`{"ok":true,...}` ashle bot live. Bot-ke channel-er **admin** banate bhulo na
(membership check-er jonno).

## Mone rakhar moto

- **Data reset:** Vercel-er filesystem temporary — cold start/restart hole user
  session, cooldown, stats muche jay. Eta mene niyei banano.
- **Timeout:** free plan-e function ~10s-er moddhe sesh korte hoy, tai API
  timeout 9s ar retry off rakha. Worm AI majhe majhe slow hole user-ke abar try
  korte bolo.
- **Animation off:** word-by-word streaming effect webhook version-e nei —
  reply direct jay (somoy bachate).
- **Auto-delete:** temporary message auto-delete serverless-e guarantee nei
  (function freeze hoye jete pare).
- Polling version (PC/VPS-e chalano) dorkar hole ager `bot.py` use koro.

## Local test

```bash
pip install -r requirements.txt
cp .env.example .env   # token/key boshao
uvicorn api.index:app --port 8000
# Telegram webhook-er jonno ngrok diye tunnel koro, tarpor setWebhook
```
