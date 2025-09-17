# Nifty Option Chain — Streamlit polling app

This repo contains a Streamlit app that polls the Upstox option-chain REST API, appends snapshots to a JSONL history file, writes a latest JSON file, and shows a flattened pandas DataFrame in the UI.

## Quick start (local)

1. Create a Python virtualenv and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Provide your Upstox access token locally (for development only):

- Create `.streamlit/secrets.toml` and add:

```toml
[upstox]
access_token = "YOUR_REAL_TOKEN"
```

> **Do not** commit your `.streamlit/secrets.toml` to Git.

3. Run the app:

```bash
streamlit run streamlit_opt_chain_live_fixed.py
```

## Deploy to Streamlit Community Cloud

1. Push this repository to GitHub.
2. Visit https://share.streamlit.io, choose the repository and the entrypoint file, and deploy.
3. In the Streamlit app settings, add the secret under **App Settings → Secrets** using the key `upstox.access_token`.

## Files

- `streamlit_opt_chain_live_fixed.py`: main app file (polls REST API, saves JSONL, renders table)
- `requirements.txt`: package list
- `.streamlit/secrets.toml.example`: example secrets file

## Notes & recommendations

- For production or more instruments, consider using the Upstox Market Data WebSocket V3 instead of polling.
- Store secrets safely (Streamlit Secrets or environment variables; never commit tokens).
