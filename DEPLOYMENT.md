# Hosting the Ice Hockey multiplayer server

## Important: use a persistent Node.js service for live multiplayer
The browser client uses Socket.IO for matchmaking, player movement, friends, chat, and scores. Deploy this repository as a long-running Node.js web service (for example, Render or a VPS), not as a serverless-only function. A serverless deployment can answer ordinary HTTP requests while WebSocket matchmaking remains unreliable or disconnected.

## Render Blueprint
This repository includes `render.yaml`. In Render, choose **New → Blueprint** and connect this repository. The blueprint creates a Node web service and mounts a persistent disk at `/var/data` so player stats, goal history, reserved names, and rewarded-ad counts survive restarts.

A persistent disk is a paid hosting feature; review the provider's current pricing before deploying.

## Point the game client to the deployed server
After deployment, copy the service's actual HTTPS URL. In the client's `index.html`, update:
```js
const SERVER_URL = "https://ice-hockey-server.vercel.app";
```
to the URL Render assigned to your service. Do not guess the URL; use the exact one shown by the host. Re-upload the updated client build to CrazyGames/Poki.

## Verify
- Open `https://YOUR-ACTUAL-SERVICE-URL/health`; it should return `{"ok":true}`.
- Open `https://YOUR-ACTUAL-SERVICE-URL/api/leaderboard?period=all`; it should return a JSON object with `rows`.
- Open the game in two separate browsers/devices and press Quick Match at nearly the same time. Both should enter a real-player match.
- Weekly leaderboard uses a rolling last 7 days. Monthly leaderboard uses the current calendar month. All Time uses the persisted total goals.
