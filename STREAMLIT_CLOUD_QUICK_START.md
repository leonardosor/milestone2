# Quick Streamlit Cloud Setup

## 📋 Checklist

- [x] Updated `db_connector.py` to support Streamlit secrets
- [x] Created `.streamlit/config.toml` for app configuration
- [x] Created `.streamlit/secrets.toml.example` as template
- [x] Created local `.streamlit/secrets.toml` for testing
- [x] Updated `.gitignore` to protect secrets
- [x] Created `STREAMLIT_DEPLOYMENT.md` with full guide

## 🚀 Quick Deploy Steps

1. **Go to**: [share.streamlit.io](https://share.streamlit.io)

2. **New App Settings**:
   - Repository: `leonardosor/milestone2`
   - Branch: `main`
   - Main file: `app/streamlit_app.py`

3. **Add Secrets** (Settings → Secrets):
   ```toml
   [database]
   DB_HOST = "your-neon-host.neon.tech"
   DB_PORT = "5432"
   DB_NAME = "neondb"
   DB_USER = "your-neon-username"
   DB_PASSWORD = "your-neon-password"
   DB_SCHEMA = "test"
   ```

   **Note:** Get your Neon credentials from https://console.neon.tech

4. **Deploy!** ✨

## 🧪 Test Locally First

```powershell
cd app
streamlit run streamlit_app.py
```

The app will use `.streamlit/secrets.toml` and connect to Supabase.

## 🔒 Security Notes

- ✅ Secrets are NOT in Git
- ✅ App works with both Docker env vars AND Streamlit secrets
- ✅ Supabase connection uses SSL automatically

## 📖 Full Documentation

See `STREAMLIT_DEPLOYMENT.md` for complete details and troubleshooting.
