# Ice Hockey Multiplayer Server — Vercel Ready

This package is based on the uploaded Ice-Hockey-Multiplayer-Server.

## Vercel
Deploy this repository as an Express backend on Vercel.

The server keeps the normal Socket.IO endpoint:
- `https://YOUR-DOMAIN.vercel.app/socket.io`

Client connection:
```js
const socket = io("https://YOUR-DOMAIN.vercel.app", {
  transports: ["websocket"]
});
```

Health checks:
- `/`
- `/health`
- `/api/leaderboard`
- `/random-id-photo`

## Important
Vercel's 2026 WebSocket support runs on Fluid compute. The in-memory multiplayer room state is suitable for initial testing, but durable cross-instance production state should use an external store/pub/sub.

The JSON files (`player-stats.json`, `reserved-names.json`, `rewarded-ads.json`) are included as initial data. Do not treat Vercel's local filesystem as permanent storage for production data.

## Local
```bash
npm install
npm start
```
