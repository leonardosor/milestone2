# Security Checklist - Secrets Management

## ✅ Fixed Issues

### 1. **Removed Hardcoded Passwords from Tracked Files**
- ✅ `config.json` - Replaced with placeholder "USE_ENV_VARIABLE"
- ✅ `STREAMLIT_CLOUD_SETUP.md` - Replaced with "your-actual-supabase-password"
- ✅ `STREAMLIT_DEPLOYMENT.md` - Replaced with "your-actual-supabase-password"
- ✅ `STREAMLIT_CLOUD_QUICK_START.md` - Replaced with "your-actual-supabase-password"
- ✅ `test_supabase_direct.py` - Now reads from `SUPABASE_PW` environment variable
- ✅ `test_supabase_ports.py` - Now reads from `SUPABASE_PW` environment variable

### 2. **Properly Ignored Files**
Files containing secrets are properly ignored in `.gitignore`:
- ✅ `.env` - Contains `SUPABASE_PW`
- ✅ `app/.streamlit/secrets.toml` - Contains actual credentials
- ✅ `*.sql` and `db_backup/*.sql` - Database dumps may contain sensitive data

## 🔐 Current Security Status

### Protected Files (NOT in git)
```
.env                              # Local environment variables
app/.streamlit/secrets.toml       # Streamlit secrets (local)
db_backup/*.sql                   # Database backups
```

### Safe Files (In git, no secrets)
```
config.json                       # Uses placeholder "USE_ENV_VARIABLE"
app/.streamlit/secrets.toml.example  # Template only, no real passwords
STREAMLIT_*.md                    # Documentation with placeholders
test_supabase_*.py                # Reads from environment variables
```

## 🚨 Important: Rotate Your Supabase Password

Since your password was previously committed to git, you should:

### 1. **Change Your Supabase Password Immediately**
   1. Go to https://supabase.com/dashboard
   2. Select project: `dplozyowioyjedbhykes`
   3. Settings → Database → Database Password
   4. Click "Reset Database Password"
   5. Save the new password securely

### 2. **Update Local Files**
   Update your `.env` file with the new password:
   ```bash
   SUPABASE_PW=your-new-password-here
   ```

### 3. **Update Streamlit Cloud Secrets**
   Update secrets in Streamlit Cloud app settings:
   ```toml
   [database]
   DB_HOST = "db.dplozyowioyjedbhykes.supabase.co"
   DB_PORT = "6543"
   DB_NAME = "postgres"
   DB_USER = "postgres"
   DB_PASSWORD = "your-new-password-here"
   DB_SCHEMA = "test"
   ```

### 4. **Update Local Streamlit Secrets**
   Update `app/.streamlit/secrets.toml` with the new password.

### 5. **Remove Old Commits from Git History (Optional but Recommended)**
   
   If you want to completely remove the password from git history:
   
   ```powershell
   # WARNING: This rewrites git history!
   # Only do this if you understand the implications
   
   # Install git-filter-repo (if not already installed)
   # pip install git-filter-repo
   
   # Create a backup first!
   git clone --mirror https://github.com/leonardosor/milestone2.git milestone2-backup
   
   # Remove sensitive data
   git filter-repo --invert-paths --path config.json
   
   # Force push (requires force push permissions)
   git push origin --force --all
   ```
   
   **Note:** This is a destructive operation. Only do this if:
   - You have backups
   - You understand git history rewriting
   - You're prepared to coordinate with any collaborators

## 📋 Prevention Checklist

To prevent future leaks:

### Before Every Commit
- [ ] Run `git status` to see what's being committed
- [ ] Run `git diff` to review changes
- [ ] Check for passwords, API keys, tokens in files being committed
- [ ] Verify `.gitignore` is working: `git check-ignore -v <file>`

### For New Files
- [ ] Add sensitive files to `.gitignore` BEFORE creating them
- [ ] Use environment variables for secrets
- [ ] Use `.example` or `.template` files for configuration templates

### For Documentation
- [ ] Use placeholders like `your-password-here`
- [ ] Never commit actual credentials "as examples"
- [ ] Reference environment variables instead of hardcoding

### For Scripts
- [ ] Read credentials from environment variables
- [ ] Use `os.getenv()` with no default for required secrets
- [ ] Exit with clear error message if credentials missing

## 🔧 How to Run Scripts After This Change

### Set environment variable in PowerShell:
```powershell
$env:SUPABASE_PW='your-actual-password'
python test_supabase_ports.py
```

### Or load from .env file:
```powershell
# Install python-dotenv if needed
pip install python-dotenv

# Create a script that loads .env
```

## 📚 Additional Resources

- [GitHub: Removing sensitive data](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/removing-sensitive-data-from-a-repository)
- [Supabase: Database Security](https://supabase.com/docs/guides/database/securing-your-database)
- [Git: gitignore patterns](https://git-scm.com/docs/gitignore)

## ✅ Verification Commands

Check what's tracked by git:
```powershell
# List all tracked files
git ls-files

# Check if a file is ignored
git check-ignore -v .env

# Search for potential secrets in tracked files
git grep -i "password\s*=\s*[\"'][^\"']" -- '*.json' '*.md' '*.py'
```

## 🎯 Summary

**Status:** ✅ All known secrets have been removed from tracked files.

**Next Steps:**
1. ✅ Secrets removed from documentation
2. ✅ Scripts updated to use environment variables
3. ⚠️ **CRITICAL:** Rotate your Supabase password
4. ✅ Update `.env` and Streamlit Cloud secrets with new password
5. 🔄 Consider rewriting git history to remove old password (optional)

**Going Forward:**
- Always use environment variables for secrets
- Never commit real credentials, even in documentation
- Review changes before committing with `git diff`
