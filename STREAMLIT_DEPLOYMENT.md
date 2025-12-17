# Streamlit Cloud Deployment Guide

This guide will help you deploy your Milestone 2 application to Streamlit Cloud connected to Supabase.

## Prerequisites

1. **Neon Database**: Create one at https://neon.tech (free tier available)
2. **GitHub Repository**: Your code at `leonardosor/milestone2`
3. **Streamlit Cloud Account**: Sign up at [share.streamlit.io](https://share.streamlit.io)

## Deployment Steps

### 1. Prepare Your Repository

Ensure these files are in your `app/` directory:
- ✅ `requirements.txt` - Python dependencies
- ✅ `streamlit_app.py` - Main application entry point
- ✅ `.streamlit/config.toml` - Streamlit configuration
- ✅ `.streamlit/secrets.toml.example` - Secrets template

### 2. Deploy to Streamlit Cloud

1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Click **"New app"**
3. Connect your GitHub repository: `leonardosor/milestone2`
4. Set the following:
   - **Branch**: `main` (or your deployment branch)
   - **Main file path**: `app/streamlit_app.py`
   - **App URL**: Choose your custom URL

### 3. Configure Secrets

In the Streamlit Cloud dashboard for your app:

1. Click **"⚙️ Settings"** → **"Secrets"**
2. Add the following configuration:

```toml
[database]
DB_HOST = "your-neon-host.neon.tech"
DB_PORT = "5432"
DB_NAME = "neondb"
DB_USER = "your-neon-username"
DB_PASSWORD = "your-neon-password"
DB_SCHEMA = "test"
```

⚠️ **Important**: Use your actual Neon credentials from https://console.neon.tech

### 4. Advanced Settings (Optional)

In **Settings** → **Advanced**:
- **Python version**: 3.11 (or your preferred version)
- **Always rerun**: Enable for automatic updates on code changes

### 5. Deploy

Click **"Deploy"** and wait for the build to complete (usually 2-5 minutes).

## Verification

Once deployed, verify the connection:

1. Open your app URL
2. Navigate to **"Database Explorer"** page
3. Check if you see your schemas and tables
4. Try running a simple query

## Troubleshooting

### Connection Issues

If you see "Cannot connect to database":

1. **Check Supabase firewall**: Ensure your Supabase project allows connections from Streamlit Cloud
   - Go to Supabase Dashboard → Settings → Database
   - Add `0.0.0.0/0` to allowed IP addresses (or specific Streamlit Cloud IPs)

2. **Verify credentials**: Double-check all credentials in Secrets match your Supabase project

3. **Check SSL**: Supabase requires SSL connections. The app automatically adds `?sslmode=require` for Supabase hosts.

### Port Configuration

- Supabase direct database port: **5432** (use this for Streamlit Cloud)
- Connection pooler port: 6543 (may have IPv6 issues in Docker)
- For Streamlit Cloud deployment, use port **5432** for best compatibility

### Password Special Characters

If your password contains special characters like `%`, `#`, `@`:
- They should work as-is in Streamlit secrets
- If issues persist, try URL-encoding them

## Environment Variables vs Secrets

The app automatically detects the runtime environment:

- **Streamlit Cloud**: Uses `st.secrets` from the Secrets configuration
- **Docker/Local**: Uses environment variables from `.env` file

This dual approach ensures the app works in both environments without code changes.

## Monitoring

After deployment:
- Check the **Logs** tab in Streamlit Cloud for any errors
- Monitor the **Analytics** tab to see usage and performance
- Use the **Reboot app** button if needed

## Updating the App

To update your deployed app:

1. Push changes to your GitHub repository
2. Streamlit Cloud will automatically detect changes and redeploy
3. Or manually trigger a reboot from the Settings menu

## Security Best Practices

1. ✅ Never commit `.streamlit/secrets.toml` to Git (it's in `.gitignore`)
2. ✅ Use strong passwords for your Supabase database
3. ✅ Regularly rotate your database credentials
4. ✅ Monitor database access logs in Supabase
5. ✅ Consider using read-only database users for the Streamlit app

## Support

If you encounter issues:
- Check Streamlit Cloud logs
- Review Supabase connection logs
- Verify network connectivity between Streamlit Cloud and Supabase
- Test connection locally first using the same credentials

## Local Testing with Supabase

Before deploying, test locally:

1. Create `.streamlit/secrets.toml` in your `app/` directory
2. Add your Supabase credentials (same format as deployment)
3. Run: `cd app && streamlit run streamlit_app.py`
4. Verify connection works

This ensures your Supabase credentials are correct before deploying to the cloud.
