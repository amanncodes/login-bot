# Cookie Provider Platform

A Django-based cookie provider platform that manages and distributes browser cookies for automated tasks across multiple social media platforms (Instagram, LinkedIn, Twitter).

## Features

- **LRU Cookie Selection**: Least Recently Used strategy ensures fair distribution
- **Platform-Specific Queues**: Separate Celery queues for each platform (Instagram, LinkedIn, Twitter)
- **Automatic Queue Retry**: Tasks wait in queue when no cookies available (never fail)
- **Thread-Safe Allocation**: Database-level locking prevents race conditions
- **Cookie Lifecycle Management**: Track usage, allocation, and release
- **Cookie Failure Tracking**: Automatic ban after 5 consecutive failures, auto-reset on success
- **Cookie Format Conversion**: Automatically converts JSON cookie arrays to string format
- **CSRF Token Extraction**: Extracts CSRF token from cookies for authentication
- **Lambda Integration**: Fire-and-forget integration with AWS Lambda functions
- **Automatic Cookie Release**: Lambda automatically releases cookies after processing or on errors
- **Smart Error Handling**: Differentiates between cookie failures and API issues
- **Webhook Integration**: RESTful endpoints for NestJS integration

## Architecture

```
┌─────────────┐         ┌──────────────┐         ┌─────────────┐         ┌─────────────┐
│   NestJS    │────────▶│   Django     │────────▶│   Celery    │────────▶│   Lambda    │
│   Server    │         │   Webhooks   │         │   Worker    │         │  Function   │
└─────────────┘         └──────────────┘         └─────────────┘         └─────────────┘
      ▲                        │                        │                        │
      │                        ▼                        ▼                        │
      │                 ┌──────────────┐         ┌─────────────┐                │
      └─────────────────│  PostgreSQL  │         │    Redis    │                │
          Callback      │   Database   │         │   Broker    │                │
                        └──────────────┘         └─────────────┘                │
                               ▲                                                 │
                               └─────────────────────────────────────────────────┘
                                                Cookie Release
```

## Quick Start Guide

### Prerequisites

- Python 3.11+
- PostgreSQL database
- Redis server
- UV package manager

### Installation & Setup

1. **Clone and Install Dependencies**
```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh  # macOS/Linux
# OR
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"  # Windows

# Install project dependencies
uv sync
```

2. **Configure Environment Variables**
```bash
# Copy example env file
cp .env.example .env

# Edit .env with your configuration
# Required variables:
# - DB_NAME, DB_USER, DB_PASS (PostgreSQL)
# - CELERY_BROKER_URL (Redis)
# - LAMBDA_FUNCTION_URL (AWS Lambda endpoint)
```

3. **Database Setup**
```bash
# Run migrations
uv run manage.py migrate

# Create admin user
uv run manage.py createsuperuser
```

4. **Start Redis Server**

**macOS (Homebrew):**
```bash
brew services start redis
# OR run in foreground:
redis-server
```

**Linux (Ubuntu/Debian):**
```bash
sudo systemctl start redis
# OR run in foreground:
redis-server
```

**Windows:**
```bash
# Download Redis from https://github.com/microsoftarchive/redis/releases
# Extract and run:
redis-server.exe
```

---

## Running the Application

You need to run **3 services** simultaneously (in separate terminals):

### Terminal 1: Django Server

```bash
uv run manage.py runserver
```

The server will start at `http://localhost:8000`
- Admin panel: `http://localhost:8000/admin/`
- Webhooks: `http://localhost:8000/webhook/`

---

### Terminal 2 & 3: Celery Workers

**⚠️ Platform-Specific Commands**

#### **Windows** (use `--pool=solo`)

**Option 1: Single Worker for All Platforms** (Recommended for Development)
```bash
uv run celery -A project worker -Q instagram_queue,linkedin_queue,twitter_queue -l info --pool=solo
```

**Option 2: Separate Workers per Platform** (Recommended for Production)
```bash
# Terminal 2: Instagram worker
uv run celery -A project worker -Q instagram_queue -l info -n instagram@%h --pool=solo

# Terminal 3: LinkedIn worker
uv run celery -A project worker -Q linkedin_queue -l info -n linkedin@%h --pool=solo

# Terminal 4 (optional): Twitter worker
uv run celery -A project worker -Q twitter_queue -l info -n twitter@%h --pool=solo
```

---

#### **macOS / Linux** (use `--concurrency=1`)

**Option 1: Single Worker for All Platforms** (Recommended for Development)
```bash
uv run celery -A project worker -Q instagram_queue,linkedin_queue,twitter_queue -l info --concurrency=1
```

**Option 2: Separate Workers per Platform** (Recommended for Production)
```bash
# Terminal 2: Instagram worker
uv run celery -A project worker -Q instagram_queue -l info -n instagram@%h --concurrency=1

# Terminal 3: LinkedIn worker
uv run celery -A project worker -Q linkedin_queue -l info -n linkedin@%h --concurrency=1

# Terminal 4 (optional): Twitter worker
uv run celery -A project worker -Q twitter_queue -l info -n twitter@%h --concurrency=1
```

---

### Why Different Commands?

| Platform | Command | Reason |
|----------|---------|--------|
| **Windows** | `--pool=solo` | Runs tasks in main process, avoids spawn/multiprocessing issues |
| **macOS/Linux** | `--concurrency=1` | Uses prefork pool (default), more efficient with proper concurrency limit |

Both ensure **FIFO processing** - one task at a time per queue.

---

## Admin Panel Usage

1. **Access Admin Panel**
```
http://localhost:8000/admin/
```

2. **Add Social Accounts**
   - Click "Social accounts" → "Add Social Account"
   - Fill in platform, username, password
   - Click "🚀 Launch Login" to start browser automation
   - Browser opens → Manual login → Cookies saved automatically

3. **Monitor Cookie Status**
   - **Status Column**: ✓ Logged In / ✗ Logged Out
   - **Usage Column**: ⚡ In Use / ⚪ Available
   - **Failures Column**: 
     - 🟢 `0` - Healthy
     - 🟡 `1-2` - Minor issues
     - 🟠 `3-4 ⚠️` - Warning
     - 🔴 `5+ 🚫` - Banned (auto-logged out)

4. **Cookie Auto-Ban Logic**
   - After **5 consecutive failures**, cookie is auto-banned (`logged_in=False`)
   - Failures reset to **0** on successful login
   - Failures reset to **0** on successful scrape
   - View failure reason by hovering over failure count

---

## Testing the System

### 1. Trigger a Scraping Job

```bash
curl -X POST http://localhost:8000/webhook/trigger-job/ \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "test-job-123",
    "platform": "instagram",
    "post_url": "https://www.instagram.com/p/ABC123/",
    "callback_url": "https://your-api.com/webhook/callback"
  }'
```

**Expected Response:**
```json
{
  "status": "success",
  "message": "Task triggered for instagram platform",
  "task_id": "celery-task-id",
  "job_id": "test-job-123",
  "platform": "instagram"
}
```

### 2. Check Celery Worker Logs

You should see:
```
[INFO] Task bots.tasks.process_instagram_job started
[INFO] Allocated cookie for instagram: user_123 (cookie_id: 1)
[INFO] Sending job to Lambda...
[INFO] Lambda invocation successful
```

### 3. Monitor Admin Panel

- Cookie status changes to "⚡ In Use"
- After Lambda completes, status returns to "⚪ Available"
- Failure count updates if errors occur

---

## Troubleshooting

### Redis Connection Error
```
Error: Redis connection refused
```

**Solution:**
```bash
# Check if Redis is running
redis-cli ping  # Should return "PONG"

# If not running:
redis-server  # Start Redis
```

---

### Celery Worker Not Starting

**Error:** `ModuleNotFoundError: No module named 'celery'`

**Solution:**
```bash
# Reinstall dependencies
uv sync

# Verify celery is installed
uv run celery --version
```

---

### Windows Celery Pool Error

**Error:** `ValueError: Pool must be solo on Windows`

**Solution:**
Always use `--pool=solo` on Windows:
```bash
uv run celery -A project worker -Q instagram_queue -l info --pool=solo
```

---

### No Cookies Available

**Error:** Worker logs show "No available cookies, retrying in 60s"

**Solution:**
1. Go to admin panel: `http://localhost:8000/admin/`
2. Check if any accounts have "✓ Logged In" status
3. If not, click "🚀 Launch Login" on an account
4. Complete manual login in browser
5. Cookies will be saved automatically

---

### Database Migration Issues

**Error:** `django.db.utils.ProgrammingError: relation does not exist`

**Solution:**
```bash
# Run migrations
uv run manage.py migrate

# If issues persist, reset migrations (⚠️ DEVELOPMENT ONLY):
uv run manage.py migrate bots zero
uv run manage.py migrate
```

---

### Cookie Failures Not Resetting

**Issue:** Cookie shows failures even after successful login

**Solution:**
This is now automatic! When you:
- Click "🚀 Launch Login" and complete login → Failures reset to 0
- Cookie successfully scrapes → Failures reset to 0

If still showing failures, manually update in admin panel or run:
```python
# In Django shell
uv run manage.py shell

from bots.models import SocialAccount
account = SocialAccount.objects.get(username='your_username')
account.reset_failures()
```

---

## Running the Services

### 1. Start Django Development Server
```bash
uv run manage.py runserver
```

### 2. Start Celery Workers (separate terminal)

**IMPORTANT:** Use `--concurrency=1` to ensure FIFO processing (one task at a time per queue).

#### Windows
Use `--pool=solo` on Windows to avoid multiprocessing issues:

```bash
# Worker for Instagram (FIFO - one task at a time)
uv run celery -A project worker -Q instagram_queue -l info -n instagram@%h --pool=solo

# Worker for LinkedIn (FIFO - one task at a time)
uv run celery -A project worker -Q linkedin_queue -l info -n linkedin@%h --pool=solo

# Worker for Twitter (FIFO - one task at a time)
uv run celery -A project worker -Q twitter_queue -l info -n twitter@%h --pool=solo
```

Or start a single worker for all queues:
```bash
uv run celery -A project worker -Q instagram_queue,linkedin_queue,twitter_queue -l info --pool=solo
```

#### macOS / Linux
Use default pool (prefork) with `--concurrency=1`:

```bash
# Worker for Instagram (FIFO - one task at a time)
uv run celery -A project worker -Q instagram_queue -l info -n instagram@%h --concurrency=1

# Worker for LinkedIn (FIFO - one task at a time)
uv run celery -A project worker -Q linkedin_queue -l info -n linkedin@%h --concurrency=1

# Worker for Twitter (FIFO - one task at a time)
uv run celery -A project worker -Q twitter_queue -l info -n twitter@%h --concurrency=1
```

Or start a single worker for all queues:
```bash
uv run celery -A project worker -Q instagram_queue,linkedin_queue,twitter_queue -l info --concurrency=1
```

**Why different pools?**
- **Windows**: `--pool=solo` runs tasks in the main process, avoiding spawn/multiprocessing issues
- **macOS/Linux**: `--concurrency=1` with prefork pool (default) works fine and is more efficient

## API Endpoints

### 1. Trigger Cookie Job
**POST** `/webhook/trigger-job/`

Triggers a Celery task to allocate a cookie and send it to AWS Lambda for processing.

**Request:**
```json
{
  "job_id": "unique-job-identifier",
  "platform": "instagram",
  "post_url": "https://www.instagram.com/p/ABC123/",
  "callback_url": "https://your-api.com/webhook/callback"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Task triggered for instagram platform",
  "task_id": "celery-task-id",
  "job_id": "unique-job-identifier",
  "platform": "instagram"
}
```

### 2. Release Cookie
**POST** `/webhook/release-cookie/`

Releases a cookie back to the available pool and tracks success/failure.

**Request:**
```json
{
  "cookie_id": 123,
  "cookie_success": true,
  "failure_reason": "Optional error message if cookie_success=false"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Cookie released for cookie_id: 123",
  "cookie_id": 123,
  "cookie_success": true
}
```

**Cookie Success Logic:**
- `cookie_success=true` → Resets failure counter to 0
- `cookie_success=false` → Increments failure counter
- After 5 consecutive failures → Auto-bans cookie (`logged_in=False`)

### 3. Health Check
**GET** `/webhook/health/`

Simple health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "service": "cookie-provider-webhook"
}
```

## Cookie Allocation Flow

1. **NestJS calls trigger endpoint** with `job_id`, `platform`, `post_url`, and `callback_url`
2. **Django webhook** dispatches task to platform-specific queue
3. **Celery worker** picks up task from queue (FIFO - one at a time)
4. **CookieService** attempts to allocate least recently used cookie (thread-safe)
   - Filters: `logged_in=True`, `in_use=False`, `consecutive_failures < 5`
   - Orders by: `last_used_at` (NULL first, then oldest)
   - If available: Marks as `in_use=True`
   - If NOT available: Task retries in 60 seconds (stays in queue)
   - Converts cookie JSON array to string format (`name=value; name=value`)
   - Extracts `csrf_token` from cookies array
5. **Worker sends payload to Lambda** with:
   - `post_url`: Instagram post URL
   - `job_id`: Job identifier
   - `cookie_id`: Cookie ID for tracking
   - `callback_url`: URL to POST results to
   - `cookies_release_url`: URL to release cookies
   - `cookies`: Cookie string (converted from array)
   - `csrf_token`: CSRF token (extracted from cookies)
6. **Lambda processes the request** (scrapes comments, etc.)
   - If Instagram API blocked → Releases cookie with `success=false`
   - If retry exhausted (5 times) → Releases cookie, switches to Hiker API
7. **Lambda sends results** to NestJS callback URL
8. **Lambda releases cookie** by calling `cookies_release_url` with:
   - `cookie_id`: Cookie identifier
   - `cookie_success`: `true` if worked, `false` if failed
   - `failure_reason`: Error description (if failed)
9. **CookieService** processes release:
   - Marks cookie as `in_use=False`
   - Updates `last_used_at` timestamp
   - If `cookie_success=true`: Resets `consecutive_failures=0`
   - If `cookie_success=false`: Increments `consecutive_failures`
   - If `consecutive_failures >= 5`: Auto-bans cookie (`logged_in=False`)

## Cookie Failure Tracking

### Automatic Ban System

**Failure Types:**
1. **Cookie Failures** (count towards ban):
   - Invalid session (401/403 errors)
   - Expired cookies
   - Instagram API blocked (suspected bad cookie)
   - GraphQL retry exhaustion

2. **NOT Cookie Failures** (don't count):
   - Successful scrapes (even if partial)
   - Hiker API usage (no cookie involved)

**Ban Logic:**
```
consecutive_failures = 0  → ✅ Healthy
consecutive_failures = 1  → 🟡 Minor issue
consecutive_failures = 2  → 🟡 Minor issue
consecutive_failures = 3  → 🟠 Warning
consecutive_failures = 4  → 🟠 Critical warning
consecutive_failures = 5  → 🔴 BANNED (auto-logout)
```

**Auto-Reset on Success:**
- Manual login via admin panel → Resets to 0
- Successful scrape → Resets to 0
- Any `cookie_success=true` release → Resets to 0

**Monitoring:**
View failure status in admin panel:
- Hover over failure count to see `failure_reason`
- Filter by `in_use` status
- Sort by `consecutive_failures`

## Queue Retry Mechanism (FIFO)

### How It Works:
Workers run with `--concurrency=1`, meaning only **ONE task processes at a time**. Simple and effective:

1. **Task A starts** → Checks for available cookie → None found
2. **Task A sleeps for 60 seconds** → Entire queue waits (no other tasks process)
3. **After 60 seconds**, Task A checks again → Still none → Sleeps again
4. **Process repeats** until Task A gets a cookie
5. **Task A completes** → Task B now starts

### Queue Ordering (FIFO):
```
Queue: [Task A, Task B, Task C, Task D]

Task A starts → No cookies → Sleeps 60s
Queue: [Task A (processing/sleeping), Task B (waiting), Task C (waiting), Task D (waiting)]

After 60s: Task A wakes → No cookies → Sleeps 60s again
Queue: [Task A (processing/sleeping), Task B (waiting), Task C (waiting), Task D (waiting)]

After 120s: Task A wakes → Cookie available! → Sends to Lambda → Completes
Queue: [Task B (now processing), Task C (waiting), Task D (waiting)]

Task B starts → Cookie available → Completes
Queue: [Task C (now processing), Task D (waiting)]
```

**Key Points:**
- ✅ **True FIFO** - tasks process in exact order received
- ✅ **Simple implementation** - use `--pool=solo` on Windows or `--concurrency=1` on macOS/Linux
- ✅ **Entire queue waits** when first task sleeps
- ✅ **Only ONE database query** at a time (no spam)
- ✅ **Unlimited retries** - tasks never fail, just wait for cookies
- ✅ **60-second interval** between retry attempts (configurable in `bots/tasks.py`)

**Platform-Specific Notes:**
- **Windows**: Use `--pool=solo` (runs in main process, avoids multiprocessing issues)
- **macOS/Linux**: Use `--concurrency=1` with prefork pool (more efficient)
- Each platform has its own worker, so Instagram queue doesn't block LinkedIn queue

## Database Model

**SocialAccount** fields:

**Account Information:**
- `platform`: Platform identifier (IG, LI, TW)
- `username`: Account username
- `password`: Account password (encrypted)

**Login Status:**
- `logged_in`: Whether account has active session
- `last_login`: Timestamp of last successful login
- `cookies`: JSON field with browser cookies

**Usage Tracking:**
- `in_use`: Whether cookie is currently allocated to a job
- `last_used_at`: Timestamp of when cookie was last released (for LRU)

**Failure Tracking:**
- `consecutive_failures`: Counter of consecutive cookie failures (0-5+)
- `failure_reason`: Description of last failure

**Timestamps:**
- `created_at`: Account creation time
- `updated_at`: Last modification time
- `cookies_updated_at`: Last cookie update time

**Methods:**
- `mark_logged_in()`: Set logged_in=True, reset failures
- `mark_logged_out(reason)`: Set logged_in=False, record reason
- `increment_failures(reason)`: Increment counter, auto-ban at 5
- `reset_failures()`: Reset counter to 0
- `update_cookies(cookies)`: Update cookie data

## Configuration

Environment variables in `.env`:

**Database:**
- `DB_NAME`: PostgreSQL database name
- `DB_USER`: PostgreSQL username
- `DB_PASS`: PostgreSQL password
- `DB_HOST`: PostgreSQL host (default: localhost)
- `DB_PORT`: PostgreSQL port (default: 5432)

**Celery:**
- `CELERY_BROKER_URL`: Redis URL for Celery broker (e.g., `redis://localhost:6379/0`)
- `CELERY_RESULT_BACKEND`: Redis URL for results (e.g., `redis://localhost:6379/0`)

**Lambda Integration:**
- `LAMBDA_FUNCTION_URL`: AWS Lambda function URL for processing
- `COOKIE_RELEASE_URL`: Webhook URL for Lambda to release cookies (e.g., `https://your-domain.com/webhook/release-cookie/`)

**Optional:**
- `HIKER_API_KEY`: API key for Hiker fallback service
- `DEBUG`: Django debug mode (True/False)
- `SECRET_KEY`: Django secret key

## Development & Monitoring

### Monitor Celery Tasks
```bash
# View active tasks
uv run celery -A project inspect active

# View registered tasks
uv run celery -A project inspect registered

# View task statistics
uv run celery -A project inspect stats

# Purge all tasks from queue (⚠️ DANGEROUS)
uv run celery -A project purge
```

### Django Shell Commands
```bash
# Open Django shell
uv run manage.py shell

# Check cookie availability
from bots.models import SocialAccount
available = SocialAccount.objects.filter(logged_in=True, in_use=False)
print(f"Available cookies: {available.count()}")

# View failing cookies
failing = SocialAccount.objects.filter(consecutive_failures__gt=0)
for acc in failing:
    print(f"{acc.username}: {acc.consecutive_failures} failures - {acc.failure_reason}")

# Manually reset failures
account = SocialAccount.objects.get(username='your_username')
account.reset_failures()

# Manually release stuck cookies
stuck = SocialAccount.objects.filter(in_use=True)
for acc in stuck:
    acc.in_use = False
    acc.save()
```

### Database Queries
```bash
# Access PostgreSQL directly
psql -U postgres -d login-bot

# Useful queries:
SELECT username, logged_in, in_use, consecutive_failures, failure_reason 
FROM bots_socialaccount;

SELECT COUNT(*) FROM bots_socialaccount WHERE logged_in=true AND in_use=false;
```

## Project Structure

```
login-bot/
├── bots/
│   ├── models.py              # SocialAccount model
│   ├── tasks.py               # Celery tasks
│   ├── services/
│   │   └── cookie_service.py  # Business logic
│   └── integrations/
│       └── webhook.py         # Webhook endpoints
├── project/
│   ├── settings.py            # Django settings
│   ├── celery.py              # Celery configuration
│   └── urls.py                # URL routing
└── manage.py
```

## Modular Architecture

- **Models** ([bots/models.py](bots/models.py)): Data layer
- **Services** ([bots/services/cookie_service.py](bots/services/cookie_service.py)): Business logic (allocation, release)
- **Tasks** ([bots/tasks.py](bots/tasks.py)): Async workers for each platform
- **Webhooks** ([bots/integrations/webhook.py](bots/integrations/webhook.py)): HTTP interface
- **Config** ([project/celery.py](project/celery.py)): Queue definitions

This separation ensures:
- Easy testing (mock services, not endpoints)
- Reusable logic (CookieService can be used anywhere)
- Clear responsibilities (each module has one job)

## License

MIT
