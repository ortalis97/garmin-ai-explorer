# Garmin Data Lake + AI Explorer

A lean data engineering project that fetches your personal Garmin data into a PostgreSQL database and provides an **AI-powered web interface** to explore it with natural language questions and automatic visualizations.

## Quick Start

```bash
# 1. Start PostgreSQL with Docker
cd garmin_data
docker-compose up -d

# 2. Install dependencies
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3. Configure .env with your credentials
# (Garmin email, password, and Gemini API key)

# 4. Initial data backfill
python -m src.backfill

# 5. Launch the web app! 🚀
streamlit run web_app.py
```

Then open http://localhost:8501 and start asking questions about your fitness data!

## Architecture

```
Garmin Connect API → Python Scripts → PostgreSQL Database → LLM Analysis
```

**Key components:**
- `garminconnect` Python library for API access
- PostgreSQL (via Docker) for data storage
- Google Gemini for natural language Q&A

## Setup

### 1. Start PostgreSQL

The project uses Docker to run PostgreSQL. Make sure Docker is installed and running.

```bash
cd garmin_data
docker-compose up -d
```

This starts a PostgreSQL 16 container with:
- Database: `garmin_data`
- User: `garmin`
- Password: `garmin_secret`
- Port: `5432`

Data is persisted in a Docker volume, so it survives container restarts.

### 2. Install dependencies

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure credentials

Create a `.env` file in the project root:

```bash
# Garmin Connect credentials
GARMIN_EMAIL=your-email@example.com
GARMIN_PASSWORD=your-password

# Google Gemini API key
# Get it from: https://makersuite.google.com/app/apikey
GEMINI_API_KEY=your-gemini-api-key-here

# PostgreSQL configuration (optional - defaults shown)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=garmin_data
POSTGRES_USER=garmin
POSTGRES_PASSWORD=garmin_secret
```

**Important:** The `.env` file is gitignored. Never commit credentials.

### 4. Initial backfill

Fetch all your historical Garmin data:

```bash
# Fetch all data (activities, sleep, daily summaries)
# By default, fetches last 3 years
python -m src.backfill

# Or specify a custom date range
python -m src.backfill --start-date 2020-01-01 --end-date 2025-11-15

# Or backfill specific entities only
python -m src.backfill --entities activities sleep
```

This will:
- Login to Garmin Connect (session is cached in `~/.garmin_session/`)
- Fetch all your activities, sleep data, and daily summaries
- Write data to PostgreSQL tables

**Note:** The backfill can take a while depending on how much data you have. Activities are fetched in batches of 100, and daily data is fetched day-by-day.

## Database Schema

### Tables

#### `activities`
One row per workout/activity:
- `activity_id`, `source`, `start_time_utc`, `date`
- `activity_type` (e.g., 'running', 'cycling')
- `distance_km`, `duration_min`, `moving_time_min`
- `avg_hr`, `max_hr`, `elevation_gain_m`
- `avg_speed_kmh`, `calories`

#### `sleep`
One row per night's sleep:
- `date`, `sleep_start`, `sleep_end`
- `sleep_duration_minutes`
- `deep_sleep_minutes`, `light_sleep_minutes`, `rem_sleep_minutes`, `awake_minutes`
- `sleep_score`, `avg_hr`, `lowest_hr`, `avg_respiration`

#### `daily_summary`
One row per day with wellness metrics:
- `date`, `steps`, `calories`
- `resting_hr`, `min_hr`, `max_hr`
- `stress_avg`, `body_battery_*`, `floors_climbed`, `distance_km`

## Usage: Web App (Recommended) 🌐

The easiest way to explore your data is through the **Streamlit web interface** with ChatGPT-style interactions and automatic data visualizations.

### Launch the Web App

```bash
streamlit run web_app.py
```

The app will open in your browser at `http://localhost:8501`

### Features

✨ **ChatGPT-Style Interface**: Ask questions in natural language and get instant answers

📊 **Automatic Visualizations**: Every answer comes with an AI-generated chart that visualizes the data

🎨 **Garmin Themed**: Beautiful interface styled with Garmin's brand colors (blue, teal, orange)

💬 **Conversation History**: Full chat history with all your questions and answers

📈 **Database Stats**: Sidebar shows your database statistics at a glance

🔍 **SQL Transparency**: See the exact SQL query used for each answer (expandable)

### Example Questions (in the web app):

- "What's my average running distance in the last 30 days?"
- "Show me my top 5 longest runs this year"
- "How has my sleep quality changed over time?"
- "Compare my cycling vs running performance"
- "What's the correlation between my sleep and heart rate?"

The web app automatically:
1. Generates a SQL query from your question
2. Executes it against your PostgreSQL database
3. Creates an appropriate chart (line, bar, scatter, or pie)
4. Provides AI-powered insights and recommendations

## Usage: Command Line Interface

For programmatic access or scripting, use the CLI:

```bash
python -m src.ai_explorer "What is my average running distance in the last 30 days?"
```

Example questions:
```bash
# Performance analysis
python -m src.ai_explorer "What are my top 5 longest runs this year?"
python -m src.ai_explorer "How has my average cycling speed changed over the last 6 months?"

# Sleep insights
python -m src.ai_explorer "What's my average sleep duration and how does it vary by day of week?"
python -m src.ai_explorer "Show me nights where I got more than 90 minutes of deep sleep"

# Correlations
python -m src.ai_explorer "How does my sleep quality correlate with my running performance?"
python -m src.ai_explorer "Do I run faster when I sleep better?"

# Wellness tracking
python -m src.ai_explorer "What's the trend in my resting heart rate over the past 3 months?"
python -m src.ai_explorer "Show me days where my body battery was above 80"
```

### How it works

1. **Question → SQL**: The LLM generates a PostgreSQL query based on your question and the data schema
2. **Execute**: The query runs against your PostgreSQL database
3. **Chart Generation**: The LLM suggests the best chart type and configuration
4. **Results → Insights**: The LLM analyzes the results and provides:
   - A clear answer to your question
   - Key insights and patterns
   - Actionable recommendations

### CLI Export results

```bash
python -m src.ai_explorer "My question" --export results.csv --show-data
```

## Direct SQL Queries (Optional)

You can also query the database directly using psql or any PostgreSQL client:

```bash
# Connect to the database
docker exec -it garmin_postgres psql -U garmin -d garmin_data
```

```sql
-- Query activities
SELECT 
    activity_type,
    COUNT(*) as count,
    AVG(distance_km) as avg_distance,
    AVG(duration_min) as avg_duration
FROM activities
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY activity_type
ORDER BY count DESC;
```

## Daily Incremental Sync

After your initial backfill, use the **daily sync** script to automatically fetch only new data:

```bash
# Auto-detect latest dates and sync only new data
python -m src.daily_sync

# Sync specific entities only
python -m src.daily_sync --entities sleep daily_summary

# Force sync from a specific date
python -m src.daily_sync --start-date 2025-11-10
```

### How Daily Sync Works

The daily sync script is **intelligent**:

1. **Auto-detects** the latest date in your database for each entity
2. **Fetches only new data** from that date forward
3. **Uses upserts** to handle duplicate data gracefully
4. **Efficient** - only makes API calls for dates you don't have yet
5. **Smart activity lookback** - if the last sync was more than 30 days ago, automatically extends the lookback window to cover the full gap

Example output:
```
=== Syncing Sleep (2025-11-14 to 2025-11-15) ===
  ✓ 2025-11-14
  ✓ 2025-11-15
✓ Added 2 sleep records

=== Syncing Activities (lookback: 30 days) ===
Found 5 activities in the last 30 days
  Filtered out 3 duplicate activities
  2 new activities to add
✓ Added 2 new activities
```

## Automated Daily Sync with Airflow

For production use, the project includes Apache Airflow to orchestrate daily syncs automatically.

### Starting Airflow

```bash
# Start all services (PostgreSQL + Airflow)
docker-compose up -d

# Wait for initialization (first time only, takes ~1 minute)
docker-compose logs -f airflow-init

# Once initialized, access the Airflow UI
# http://localhost:8080
# Default login: admin / admin
```

### Airflow DAGs

Two DAGs are provided:

| DAG | Schedule | Description |
|-----|----------|-------------|
| `garmin_daily_sync` | Daily at 6 AM | Syncs new data with smart delta detection |
| `garmin_full_backfill` | Manual trigger | Full historical backfill (use for initial setup) |

### How It Works

1. **Scheduler** runs `garmin_daily_sync` every day at 6 AM
2. **Smart delta detection** automatically determines what data is missing
3. **Automatic backfill** - if the last sync was >30 days ago, extends the lookback to cover the gap
4. **Retry on failure** - 2 automatic retries with 5-minute delays

### Airflow Commands

```bash
# View service status
docker-compose ps

# View scheduler logs
docker-compose logs -f airflow-scheduler

# Trigger a manual sync
docker-compose exec airflow-scheduler airflow dags trigger garmin_daily_sync

# Stop Airflow (keeps PostgreSQL running)
docker-compose stop airflow-webserver airflow-scheduler

# Stop everything
docker-compose down
```

### Manual Sync (without Airflow)

You can still run the sync manually:

```bash
# Activate your Python environment
source venv/bin/activate

# Run the sync
python -m src.daily_sync
```

Or set up a cron job:

```bash
# Edit your crontab
crontab -e

# Add this line to sync every day at 6 AM
0 6 * * * cd /path/to/garmin_data && source venv/bin/activate && python -m src.daily_sync >> logs/sync.log 2>&1
```

## Docker Commands

```bash
# Start all services (PostgreSQL + Airflow)
docker-compose up -d

# Start only PostgreSQL (no Airflow)
docker-compose up -d postgres

# Stop all services
docker-compose down

# View logs
docker-compose logs -f postgres
docker-compose logs -f airflow-scheduler

# Connect to the database directly
docker exec -it garmin_postgres psql -U garmin -d garmin_data

# Remove all data (WARNING: destructive!)
docker-compose down -v
```

## Switching LLM Providers

The code is designed to make switching LLM providers easy. Currently using Google Gemini.

To add OpenAI:

1. Install: `pip install openai`
2. Add to `src/llm_client.py`:

```python
import openai

class OpenAIClient(LLMClient):
    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4"):
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        openai.api_key = self.api_key
        self.model = model
    
    def generate(self, prompt: str, temperature: float = 0.0) -> str:
        response = openai.ChatCompletion.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature
        )
        return response.choices[0].message.content
```

3. Update factory in `create_llm_client()`:
```python
elif provider.lower() == "openai":
    return OpenAIClient(**kwargs)
```

4. Use: `create_llm_client("openai")`

## Troubleshooting

**Database connection issues:**
- Make sure Docker is running: `docker ps`
- Check if the container is healthy: `docker-compose ps`
- Verify connection: `docker exec -it garmin_postgres pg_isready`

**Login issues:**
- The Garmin API sometimes requires CAPTCHA for new logins
- Session is cached in `~/.garmin_session/` - delete this if you have auth problems
- Try logging into Garmin Connect in a browser first

**No data returned:**
- Check that your date range is correct
- Some data might not be available for all dates
- Check the API response structure - Garmin occasionally changes their JSON format

## Project Structure

```
garmin_data/
  dags/
    garmin_sync_dag.py      # Airflow DAGs for scheduled syncs
  src/
    __init__.py
    garmin_client.py        # Garmin API wrapper
    llm_client.py           # LLM abstraction (Gemini/OpenAI/etc)
    database.py             # PostgreSQL connection and schema
    backfill.py             # Historical data fetch (full backfill)
    daily_sync.py           # Incremental sync (smart delta updates)
    tasks.py                # Airflow-callable task functions
    ai_explorer.py          # Natural language Q&A (CLI + core logic)
    visualization.py        # Chart generation with Plotly
  web_app.py                # Streamlit web interface
  docker-compose.yml        # PostgreSQL + Airflow container setup
  init-airflow-db.sql       # Creates Airflow metadata database
  .env                      # Secrets (gitignored)
  requirements.txt
  README.md
```

## Next Steps / Ideas

- [ ] Build refined analytical tables (e.g., weekly rollups, trends)
- [ ] Add heart rate time series data
- [ ] Add data quality checks and validation
- [ ] Export insights to notion/obsidian for tracking

## License

Personal project - use as you wish!
