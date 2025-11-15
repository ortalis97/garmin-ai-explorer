"""
AI-powered exploration layer for Garmin data.
Natural language question -> SQL query -> Results -> Insights
"""
import sys
import json
from pathlib import Path
from typing import Tuple, Dict, Any, Optional, List
from dataclasses import dataclass, field, asdict
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from .llm_client import create_llm_client
from .database import execute_query, check_connection


@dataclass
class ConversationTurn:
    """
    Represents a single turn in the conversation history.
    Contains the question, answer, and metadata needed for context.
    """
    question: str
    sql: str
    summary: str
    columns: List[str] = field(default_factory=list)
    row_count: int = 0
    sample_data: Optional[str] = None  # First few rows as markdown, for detailed follow-ups
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ConversationTurn':
        """Create from dictionary."""
        return cls(**data)
    
    @classmethod
    def from_response(cls, question: str, sql: str, summary: str, df: pd.DataFrame, include_sample: bool = True) -> 'ConversationTurn':
        """
        Create a ConversationTurn from a query response.
        
        Args:
            question: The user's question
            sql: The SQL query that was executed
            summary: The LLM-generated summary
            df: The result DataFrame
            include_sample: Whether to include sample data (first 5 rows as markdown)
        """
        sample_data = None
        if include_sample and not df.empty:
            sample_data = df.head(5).to_markdown(index=False)
        
        return cls(
            question=question,
            sql=sql,
            summary=summary,
            columns=list(df.columns) if not df.empty else [],
            row_count=len(df),
            sample_data=sample_data
        )


def format_conversation_context(history: List[ConversationTurn], include_sample_data: bool = False) -> str:
    """
    Format conversation history for inclusion in LLM prompts.
    
    Args:
        history: List of previous conversation turns
        include_sample_data: Whether to include sample data from previous queries
    
    Returns:
        Formatted string describing the conversation history
    """
    if not history:
        return ""
    
    context_parts = ["## Previous Conversation Context\n"]
    
    for i, turn in enumerate(history, 1):
        context_parts.append(f"### Turn {i}")
        context_parts.append(f"**User asked:** {turn.question}")
        context_parts.append(f"**SQL used:** `{turn.sql[:200]}{'...' if len(turn.sql) > 200 else ''}`")
        context_parts.append(f"**Result:** {turn.row_count} rows with columns: {', '.join(turn.columns)}")
        context_parts.append(f"**Summary:** {turn.summary[:300]}{'...' if len(turn.summary) > 300 else ''}")
        
        if include_sample_data and turn.sample_data:
            context_parts.append(f"**Sample data:**\n{turn.sample_data}")
        
        context_parts.append("")  # Empty line between turns
    
    return "\n".join(context_parts)


def needs_detailed_context(question: str) -> bool:
    """
    Determine if the follow-up question needs detailed data from previous queries.
    
    Args:
        question: The current user question
    
    Returns:
        True if the question likely needs access to previous raw data
    """
    # Keywords that suggest the user wants to reference previous data directly
    detail_keywords = [
        # Referencing previous results
        "those", "that", "these", "the data", "the results", "show me",
        "filter", "sort", "group", "breakdown", "break down", "by month",
        "by week", "by day", "more detail", "drill down", "zoom in",
        "which ones", "what about", "same", "similar", "like before",
        # Explicitly asking for raw/actual data
        "raw data", "actual data", "the numbers", "actual numbers",
        "exact", "specific", "details", "all the", "full data",
        "underlying", "source data",
        # Visualization/chart requests (need data context)
        "chart", "graph", "plot", "diagram", "visualize", "visualise",
        "show as", "display as", "draw", "create a", "make a",
        "bar chart", "line chart", "pie chart", "scatter"
    ]
    
    question_lower = question.lower()
    return any(keyword in question_lower for keyword in detail_keywords)


# Schema description for the LLM
SCHEMA_DESCRIPTION = """
Available tables in PostgreSQL:

1. **activities** - One row per workout/activity
   Columns:
   - activity_id (varchar): Unique activity ID
   - source (varchar): Always 'garmin'
   - start_time_utc (timestamp): When activity started
   - date (date): Activity date
   - activity_type (varchar): e.g., 'running', 'cycling', 'strength_training'
   - activity_name (varchar): Name given to the activity
   - distance_km (real): Distance in kilometers
   - duration_min (real): Total duration in minutes
   - moving_time_min (real): Time actually moving
   - avg_hr (real): Average heart rate (bpm)
   - max_hr (real): Maximum heart rate (bpm)
   - elevation_gain_m (real): Elevation gain in meters
   - avg_speed_kmh (real): Average speed in km/h
   - calories (real): Calories burned

2. **sleep** - One row per night's sleep
   Columns:
   - date (date): Date of the sleep (usually the morning you woke up)
   - sleep_start (timestamp): When you went to sleep
   - sleep_end (timestamp): When you woke up
   - sleep_duration_minutes (real): Total sleep time
   - deep_sleep_minutes (real): Deep sleep duration
   - light_sleep_minutes (real): Light sleep duration
   - rem_sleep_minutes (real): REM sleep duration
   - awake_minutes (real): Time awake during the night
   - sleep_score (real): Overall sleep score (0-100)
   - avg_hr (real): Average heart rate during sleep
   - lowest_hr (real): Lowest heart rate during sleep
   - avg_respiration (real): Average respiration rate

3. **daily_summary** - One row per day with wellness metrics
   Columns:
   - date (date): The date
   - steps (integer): Total steps
   - calories (real): Active calories burned
   - resting_hr (real): Resting heart rate for the day
   - min_hr (real): Minimum heart rate
   - max_hr (real): Maximum heart rate
   - stress_avg (real): Average stress level
   - body_battery_charged (real): Body battery charged amount
   - body_battery_drained (real): Body battery drained amount
   - body_battery_highest (real): Highest body battery level
   - body_battery_lowest (real): Lowest body battery level
   - floors_climbed (integer): Floors climbed
   - distance_km (real): Total distance in km

Notes:
- Use PostgreSQL syntax
- Date functions: CURRENT_DATE, date - INTERVAL '30 days', DATE_TRUNC(), EXTRACT(), etc.
- Aggregations: AVG(), SUM(), COUNT(), etc.
- Window functions are supported
- Use single quotes for string literals
"""


def question_to_sql(question: str, llm_client, conversation_history: List[ConversationTurn] = None) -> str:
    """
    Convert a natural language question to SQL using an LLM.
    
    Args:
        question: The user's natural language question
        llm_client: LLM client instance
        conversation_history: Optional list of previous conversation turns for context
    
    Returns:
        SQL query string
    """
    # Build conversation context if we have history
    context_section = ""
    if conversation_history:
        # Check if we need detailed data for this follow-up
        include_sample = needs_detailed_context(question)
        context_section = format_conversation_context(conversation_history, include_sample_data=include_sample)
        context_section += "\n\n**Important:** The user may be asking a follow-up question. Consider the previous queries and results when writing the new query. You may need to modify, filter, or build upon previous queries.\n\n"
    
    prompt = f"""You are an expert SQL assistant. Your job is to write a PostgreSQL query that answers the user's question.

{SCHEMA_DESCRIPTION}

{context_section}User question: "{question}"

Instructions:
- Write a single SELECT query that answers the question
- Use proper PostgreSQL syntax
- Return ONLY the SQL query, no explanation, no markdown, no code blocks
- Do not include semicolons at the end
- Use appropriate JOINs if multiple tables are needed
- Add LIMIT clauses for large result sets when appropriate
{f"- This is a follow-up question. Consider the context of previous queries and results." if conversation_history else ""}

SQL Query:"""
    
    sql = llm_client.generate(prompt, temperature=0.0)
    
    # Clean up the response (remove markdown, extra whitespace)
    sql = sql.strip()
    if sql.startswith("```"):
        # Remove markdown code blocks
        lines = sql.split("\n")
        sql = "\n".join([l for l in lines if not l.startswith("```")])
    sql = sql.strip().rstrip(";")
    
    return sql


def run_sql(sql: str) -> pd.DataFrame:
    """
    Execute SQL query and return results as DataFrame.
    """
    try:
        return execute_query(sql)
    except Exception as e:
        raise RuntimeError(f"SQL execution failed: {e}\n\nQuery:\n{sql}")


def summarize_results(question: str, sql: str, df: pd.DataFrame, llm_client, conversation_history: List[ConversationTurn] = None) -> str:
    """
    Use LLM to summarize query results and provide insights.
    
    Args:
        question: The user's question
        sql: The SQL query that was executed
        df: The result DataFrame
        llm_client: LLM client instance
        conversation_history: Optional list of previous conversation turns for context
    
    Returns:
        Summary text with insights
    """
    # Format results as markdown table (limit to first 50 rows for context)
    if df.empty:
        results_preview = "No results found."
    else:
        results_preview = df.head(50).to_markdown(index=False)
        if len(df) > 50:
            results_preview += f"\n\n... ({len(df)} total rows)"
    
    # Build conversation context section
    context_section = ""
    if conversation_history:
        context_parts = ["## Previous Conversation\n"]
        for i, turn in enumerate(conversation_history[-3:], 1):  # Last 3 turns for summary context
            context_parts.append(f"**Q{i}:** {turn.question}")
            context_parts.append(f"**A{i}:** {turn.summary[:200]}{'...' if len(turn.summary) > 200 else ''}\n")
        context_section = "\n".join(context_parts) + "\n"
    
    prompt = f"""You are a fitness data analyst. Answer the user's question based on the query results.

{context_section}User's question: "{question}"

Query results:
{results_preview}

Instructions:
- Start with the direct answer (1-2 sentences)
- Then provide 2-3 key insights or patterns you notice
- Be specific with numbers and dates from the results
- Use **bold** for important metrics, numbers, and key terms
- Be concise and encouraging
- Do NOT use section titles like "Answer:" or "Insights:"
- Do NOT provide actionable recommendations
{f"- This is a follow-up question. You may reference previous answers when relevant, but focus on answering the current question." if conversation_history else ""}"""
    
    summary = llm_client.generate(prompt, temperature=0.3)
    return summary


def generate_visualization_query(question: str, answer_sql: str, answer_df: pd.DataFrame, llm_client) -> Tuple[str, str]:
    """
    Use LLM to determine the best data to visualize for the answer, potentially creating a new query.
    
    Args:
        question: User's natural language question
        answer_sql: SQL query that answered the question
        answer_df: DataFrame with the answer data
        llm_client: LLM client instance
    
    Returns:
        Tuple of (visualization_sql, chart_type_suggestion)
    """
    if answer_df.empty:
        return answer_sql, "table"
    
    # Get context about the answer
    answer_preview = answer_df.head(10).to_markdown(index=False) if not answer_df.empty else "No data"
    
    prompt = f"""You are a data visualization expert. The user asked a question and we have the answer data.
Now determine what data would BEST VISUALIZE and support this answer.

User's question: "{question}"

Current answer data (first 10 rows):
{answer_preview}

Current answer has: {len(answer_df)} rows, {len(answer_df.columns)} columns

Available tables in database:
- activities(date, start_time_utc, activity_type, distance_km, duration_min, avg_hr, max_hr, elevation_gain_m, avg_speed_kmh, calories)
- sleep(date, sleep_start, sleep_end, sleep_duration_minutes, deep_sleep_minutes, light_sleep_minutes, rem_sleep_minutes, awake_minutes, sleep_score, avg_hr, lowest_hr)
- daily_summary(date, steps, calories, resting_hr, min_hr, max_hr, stress_avg, body_battery_charged, body_battery_highest, body_battery_lowest, floors_climbed, distance_km)

Task: Suggest the BEST data to visualize for this answer. This might be:
1. The same query result if it's already suitable for visualization
2. A modified or aggregated version (e.g., if answer is a single number, show the trend over time)
3. A completely different query that provides supporting context

Return a JSON object:
{{
  "use_same_query": true/false,
  "new_sql": "PostgreSQL query to get visualization data (only if use_same_query is false)",
  "suggested_chart_type": "line|bar|scatter|pie",
  "reasoning": "Brief explanation of what to visualize and why"
}}

Examples:
- If answer is "average is 5.2km", visualize the distribution or trend over time
- If answer is a count, show a bar chart of the breakdown
- If answer is comparing categories, show them side by side
- If answer is about trends, show a line chart over time

Return ONLY the JSON, no explanation."""
    
    try:
        response = llm_client.generate(prompt, temperature=0.1)
        
        # Clean response
        response = response.strip()
        if response.startswith("```"):
            lines = response.split("\n")
            response = "\n".join([l for l in lines if not l.strip().startswith("```")])
        response = response.strip()
        
        # Extract JSON
        if "{" in response and "}" in response:
            start = response.index("{")
            end = response.rindex("}") + 1
            response = response[start:end]
        
        viz_spec = json.loads(response)
        
        # Return appropriate SQL
        if viz_spec.get("use_same_query", True):
            return answer_sql, viz_spec.get("suggested_chart_type", "bar")
        else:
            new_sql = viz_spec.get("new_sql", "").strip()
            if new_sql:
                return new_sql, viz_spec.get("suggested_chart_type", "bar")
            else:
                return answer_sql, viz_spec.get("suggested_chart_type", "bar")
    
    except Exception as e:
        print(f"Warning: Could not generate visualization query: {e}")
        return answer_sql, "bar"


def generate_chart_spec(question: str, sql: str, df: pd.DataFrame, llm_client, suggested_chart_type: str = None) -> Dict[str, Any]:
    """
    Use LLM to generate a chart specification based on the query and results.
    
    Args:
        question: User's natural language question
        sql: SQL query that was executed
        df: DataFrame with query results
        llm_client: LLM client instance
    
    Returns:
        Dictionary with chart specification:
        {
            "chart_type": "line|bar|scatter|pie|table",
            "x_axis": "column_name",
            "y_axis": "column_name" or ["col1", "col2"],
            "title": "Chart title",
            "color_by": "optional_column_name"
        }
    """
    if df.empty:
        return {
            "chart_type": "table",
            "title": "No data to visualize"
        }
    
    # Get data info
    columns = list(df.columns)
    sample_data = df.head(3).to_dict('records')
    data_shape = f"{len(df)} rows, {len(df.columns)} columns"
    
    # Check for date/time columns
    date_cols = [col for col in columns if 'date' in col.lower() or 'time' in col.lower()]
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    categorical_cols = df.select_dtypes(exclude=['number', 'datetime']).columns.tolist()
    
    prompt = f"""Based on the user's question and the query results, suggest an appropriate data visualization.

User's question: "{question}"

Data shape: {data_shape}
Columns: {', '.join(columns)}
Date/time columns: {', '.join(date_cols) if date_cols else 'None'}
Numeric columns: {', '.join(numeric_cols) if numeric_cols else 'None'}
Categorical columns: {', '.join(categorical_cols) if categorical_cols else 'None'}

Sample data (first 3 rows):
{json.dumps(sample_data, indent=2, default=str)}

{f"Suggested chart type: {suggested_chart_type}" if suggested_chart_type else ""}

Choose the most appropriate visualization and return ONLY a JSON object with this structure:
{{
  "chart_type": "line|bar|scatter|pie|table",
  "x_axis": "column_name",
  "y_axis": "column_name_or_list",
  "title": "Descriptive chart title",
  "color_by": "optional_column_name"
}}

Guidelines:
- Use "line" for time series or trends over dates
- Use "bar" for comparisons between categories or counts
- Use "scatter" for showing correlations between two numeric variables
- Use "pie" for showing proportions or percentages (max 8 categories)
- Use "table" only if data is not suitable for visualization
- For x_axis and y_axis, use exact column names from the data
- Make the title descriptive and specific to the data
{f"- Prefer {suggested_chart_type} chart if appropriate" if suggested_chart_type else ""}

Return ONLY the JSON, no explanation."""
    
    try:
        response = llm_client.generate(prompt, temperature=0.0)
        
        # Clean response
        response = response.strip()
        # Remove markdown code blocks if present
        if response.startswith("```"):
            lines = response.split("\n")
            response = "\n".join([l for l in lines if not l.startswith("```")])
        response = response.strip()
        
        # Parse JSON
        chart_spec = json.loads(response)
        
        # Validate required fields
        if "chart_type" not in chart_spec:
            chart_spec["chart_type"] = "bar"
        if "title" not in chart_spec:
            chart_spec["title"] = "Data Visualization"
        
        return chart_spec
    except Exception as e:
        print(f"Warning: Could not generate chart spec: {e}")
        # Return default spec
        return {
            "chart_type": "bar" if len(df) <= 20 else "table",
            "x_axis": columns[0] if columns else None,
            "y_axis": numeric_cols[0] if numeric_cols else columns[1] if len(columns) > 1 else None,
            "title": "Data Visualization"
        }


def ask(question: str, verbose: bool = True) -> Tuple[str, pd.DataFrame, str]:
    """
    Main function: ask a question and get SQL, results, and insights.
    
    Args:
        question: Natural language question about Garmin data
        verbose: Whether to print intermediate steps
    
    Returns:
        Tuple of (sql_query, results_dataframe, summary_text)
    """
    if verbose:
        print(f"\n🤔 Question: {question}\n")
    
    # Check database connection
    if not check_connection():
        raise RuntimeError("Cannot connect to PostgreSQL. Make sure Docker is running: docker-compose up -d")
    
    # Initialize
    llm_client = create_llm_client("gemini")
    
    # Generate SQL
    if verbose:
        print("🔧 Generating SQL query...")
    sql = question_to_sql(question, llm_client)
    if verbose:
        print(f"\n```sql\n{sql}\n```\n")
    
    # Execute query
    if verbose:
        print("📊 Executing query...")
    df = run_sql(sql)
    if verbose:
        print(f"✓ Retrieved {len(df)} rows\n")
    
    # Generate insights
    if verbose:
        print("💡 Generating insights...\n")
    summary = summarize_results(question, sql, df, llm_client)
    
    if verbose:
        print("=" * 80)
        print(summary)
        print("=" * 80)
    
    return sql, df, summary


def ask_with_chart(
    question: str, 
    conversation_history: List[ConversationTurn] = None,
    verbose: bool = False
) -> Tuple[str, pd.DataFrame, str, Dict[str, Any], pd.DataFrame, ConversationTurn]:
    """
    Ask a question and get SQL, results, insights, AND a chart specification.
    Optimized for use in web interfaces. May generate a separate query for visualization.
    Supports conversational context through conversation_history.
    
    Args:
        question: Natural language question about Garmin data
        conversation_history: Optional list of previous conversation turns for context
        verbose: Whether to print intermediate steps
    
    Returns:
        Tuple of (sql_query, results_dataframe, summary_text, chart_spec, viz_dataframe, conversation_turn)
        The conversation_turn can be appended to conversation_history for the next question.
    """
    # Check database connection
    if not check_connection():
        raise RuntimeError("Cannot connect to PostgreSQL. Make sure Docker is running: docker-compose up -d")
    
    # Initialize
    llm_client = create_llm_client("gemini")
    
    # Generate SQL for the answer (with conversation context)
    answer_sql = question_to_sql(question, llm_client, conversation_history)
    
    # Execute answer query
    answer_df = run_sql(answer_sql)
    
    # Determine best data to visualize (may generate a new query)
    viz_sql, suggested_chart_type = generate_visualization_query(
        question, answer_sql, answer_df, llm_client
    )
    
    # Get visualization data (might be different from answer data)
    if viz_sql != answer_sql:
        try:
            viz_df = run_sql(viz_sql)
            if verbose:
                print(f"Using separate visualization query")
        except Exception as e:
            print(f"Visualization query failed, using answer data: {e}")
            viz_df = answer_df
            viz_sql = answer_sql
    else:
        viz_df = answer_df
    
    # Generate chart specification for visualization data
    chart_spec = generate_chart_spec(
        question, viz_sql, viz_df, llm_client, suggested_chart_type
    )
    
    # Generate insights based on answer data (with conversation context)
    summary = summarize_results(question, answer_sql, answer_df, llm_client, conversation_history)
    
    # Create conversation turn for this response
    conversation_turn = ConversationTurn.from_response(
        question=question,
        sql=answer_sql,
        summary=summary,
        df=answer_df,
        include_sample=True  # Always include sample for potential follow-ups
    )
    
    # Return answer data, viz data, and conversation turn
    return answer_sql, answer_df, summary, chart_spec, viz_df, conversation_turn


def main():
    """CLI interface for AI explorer."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Ask questions about your Garmin data in natural language",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.ai_explorer "What is my average running distance in the last 30 days?"
  python -m src.ai_explorer "How does my sleep quality correlate with my running performance?"
  python -m src.ai_explorer "Show me my top 5 longest runs this year"
        """
    )
    parser.add_argument("question", type=str, help="Your question about Garmin data")
    parser.add_argument(
        "--show-data",
        action="store_true",
        help="Print the full result DataFrame"
    )
    parser.add_argument(
        "--export",
        type=str,
        help="Export results to CSV file"
    )
    
    args = parser.parse_args()
    
    try:
        sql, df, summary = ask(args.question, verbose=True)
        
        if args.show_data and not df.empty:
            print("\n📋 Full Results:\n")
            print(df.to_string(index=False))
        
        if args.export:
            export_path = Path(args.export)
            df.to_csv(export_path, index=False)
            print(f"\n💾 Results exported to {export_path}")
    
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
