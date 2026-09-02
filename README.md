# Pika Fleet Control

Persistent Mineflayer workers with a separate local dashboard.

## Run

1. Install dependencies with `npm install`.
2. Set server and scheduling values in `config.json`.
3. Start the persistent clients with `npm run worker`.
4. Start or restart the dashboard independently with `npm run dashboard`.

Restarting the dashboard does not disconnect worker clients. Reconnects use the same tail-queued batch schedule as new account starts.

