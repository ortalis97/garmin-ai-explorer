# Garmin Data Lake + AI Explorer

A lean data engineering project that fetches your personal Garmin data into a local data lake (Parquet files) and provides an **AI-powered web interface** to explore it with natural language questions and automatic visualizations.

## Quick Start

```bash
# 1. Install dependencies
cd garmin_data
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Configure .env with your credentials
# (Garmin email, password, and Gemini API key)

# 3. Initial data backfill
python -m src.backfill

# 4. Launch the web app! 🚀
streamlit run web_app.py
```

Then open http://localhost:8501 and start asking questions about your fitness data!

## Architecture

```
Garmin Connect API → Python Scripts → Parquet Data Lake → DuckDB → LLM Analysis
```

**Key components:**
- `garminconnect` Python library for API access
- Partitioned Parquet files (by year/month) for storage
- DuckDB for fast SQL queries over Parquet
- Google Gemini for natural language Q&A

## Setup

### 1. Install dependencies

```bash
cd garmin_data
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure credentials

Create a `.env` file in the project root:

```bash
# Garmin Connect credentials
GARMIN_EMAIL=your-email@example.com
GARMIN_PASSWORD=your-password

# Google Gemini API key
# Get it from: https://makersuite.google.com/app/apikey
GEMINI_API_KEY=your-gemini-api-key-here
```

**Important:** The `.env` file is gitignored. Never commit credentials.

### 3. Initial backfill

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
- Write partitioned Parquet files to `data_lake/garmin/`

**Note:** The backfill can take a while depending on how much data you have. Activities are fetched in batches of 100, and daily data is fetched day-by-day.

## Data Lake Structure

```
data_lake/
  garmin/
    activities/
      year=2023/month=01/part-20251115-123045.parquet
      year=2023/month=02/part-20251115-123045.parquet
      ...
    sleep/
      year=2023/month=01/part-20251115-123050.parquet
      ...
    daily_summary/
      year=2023/month=01/part-20251115-123055.parquet
      ...
```

### Tables & Schemas

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

📈 **Data Lake Stats**: Sidebar shows your data lake statistics at a glance

🔍 **SQL Transparency**: See the exact SQL query used for each answer (expandable)

### Example Questions (in the web app):

- "What's my average running distance in the last 30 days?"
- "Show me my top 5 longest runs this year"
- "How has my sleep quality changed over time?"
- "Compare my cycling vs running performance"
- "What's the correlation between my sleep and heart rate?"

The web app automatically:
1. Generates a SQL query from your question
2. Executes it against your data lake
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

1. **Question → SQL**: The LLM generates a DuckDB SQL query based on your question and the data schema
2. **Execute**: The query runs against the Parquet files via DuckDB
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

You can also query the data lake directly using DuckDB:

```python
import duckdb

con = duckdb.connect()

# Query activities
df = con.execute("""
    SELECT 
        activity_type,
        COUNT(*) as count,
        AVG(distance_km) as avg_distance,
        AVG(duration_min) as avg_duration
    FROM read_parquet('data_lake/garmin/activities/year=*/month=*/*.parquet')
    WHERE date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY activity_type
    ORDER BY count DESC
""").df()

print(df)
```

Or create a Jupyter notebook for exploration (see `notebooks/` directory).

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

1. **Auto-detects** the latest date in your data lake for each entity
2. **Fetches only new data** from that date forward
3. **Deduplicates activities** to avoid writing the same data twice
4. **Efficient** - only makes API calls for dates you don't have yet

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

### Setting up Daily Automation

Set up a cron job to run daily sync automatically:

```bash
# Edit your crontab
crontab -e

# Add this line to sync every day at 6 AM
0 6 * * * cd /path/to/garmin_data && source venv/bin/activate && python -m src.daily_sync >> logs/sync.log 2>&1
```

Or use the backfill script with specific dates:

```bash
# Manual approach - fetch just yesterday and today
python -m src.backfill --start-date 2025-11-14 --end-date 2025-11-15
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

**Login issues:**
- The Garmin API sometimes requires CAPTCHA for new logins
- Session is cached in `~/.garmin_session/` - delete this if you have auth problems
- Try logging into Garmin Connect in a browser first

**No data returned:**
- Check that your date range is correct
- Some data might not be available for all dates
- Check the API response structure - Garmin occasionally changes their JSON format

**Missing Parquet files:**
- Make sure backfill completed successfully
- Check `data_lake/garmin/` for the expected directory structure
- Re-run backfill for specific date ranges if needed

## Project Structure

```
garmin_data/
  src/
    __init__.py
    garmin_client.py        # Garmin API wrapper
    llm_client.py           # LLM abstraction (Gemini/OpenAI/etc)
    backfill.py             # Historical data fetch (full backfill)
    daily_sync.py           # Incremental sync (smart delta updates)
    ai_explorer.py          # Natural language Q&A (CLI + core logic)
    visualization.py        # Chart generation with Plotly
  web_app.py                # Streamlit web interface ⭐
  data_lake/                # Parquet files (gitignored)
  .env                      # Secrets (gitignored)
  requirements.txt
  README.md
```

## Next Steps / Ideas

- [ ] Build refined analytical tables (e.g., weekly rollups, trends)
- [ ] Add heart rate time series data
- [ ] Create a simple web dashboard (Streamlit/Gradio)
- [ ] Add data quality checks and validation
- [ ] Implement incremental sync as a proper `daily_sync.py` script
- [ ] Add visualization layer (matplotlib/plotly charts)
- [ ] Export insights to notion/obsidian for tracking

## License

Personal project - use as you wish!
