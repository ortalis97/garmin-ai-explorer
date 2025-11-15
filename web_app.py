"""
Garmin AI Explorer - Streamlit Web Application
A ChatGPT-style interface for exploring Garmin data with AI-powered insights and visualizations.
"""
import streamlit as st
import sys
from pathlib import Path
from datetime import date, timedelta
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.ai_explorer import ask_with_chart, ConversationTurn
from src.visualization import render_chart
from src.database import get_table_stats, check_connection

# Garmin brand colors
GARMIN_BLUE = "#007CC3"
GARMIN_TEAL = "#00A9CE"
GARMIN_ORANGE = "#FF6A13"
GARMIN_DARK = "#1A2332"

# Page config
st.set_page_config(
    page_title="Garmin AI Explorer",
    page_icon="🏃",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Garmin styling
st.markdown(f"""
<style>
    /* Main header styling */
    .main-header {{
        background: linear-gradient(135deg, {GARMIN_BLUE} 0%, {GARMIN_TEAL} 100%);
        padding: 1.5rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        text-align: center;
        color: white;
    }}
    
    .main-header h1 {{
        margin: 0;
        color: white;
        font-size: 2.5rem;
    }}
    
    .main-header p {{
        margin: 0.5rem 0 0 0;
        color: rgba(255,255,255,0.9);
        font-size: 1.1rem;
    }}
    
    /* Chat message styling */
    .stChatMessage {{
        background-color: white;
        border-radius: 10px;
        padding: 1rem;
        margin-bottom: 1rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }}
    
    /* User message */
    [data-testid="stChatMessageContent"]:has(> div > div:first-child[data-testid="stMarkdownContainer"]) {{
        background-color: #F0F8FF;
        border-left: 4px solid {GARMIN_BLUE};
    }}
    
    /* SQL expander styling */
    .sql-container {{
        background-color: #F5F5F5;
        padding: 0.5rem;
        border-radius: 5px;
        border-left: 3px solid {GARMIN_TEAL};
        margin: 1rem 0;
    }}
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {{
        background-color: #F8F9FA;
    }}
    
    [data-testid="stSidebar"] h1 {{
        color: {GARMIN_DARK};
    }}
    
    /* Button styling */
    .stButton > button {{
        background-color: {GARMIN_BLUE};
        color: white;
        border-radius: 5px;
        border: none;
        padding: 0.5rem 1rem;
        font-weight: 500;
    }}
    
    .stButton > button:hover {{
        background-color: {GARMIN_TEAL};
    }}
    
    /* Stat cards */
    .stat-card {{
        background-color: white;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid {GARMIN_BLUE};
        margin-bottom: 0.5rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }}
    
    .stat-value {{
        font-size: 1.8rem;
        font-weight: bold;
        color: {GARMIN_BLUE};
        margin: 0;
    }}
    
    .stat-label {{
        font-size: 0.9rem;
        color: #666;
        margin: 0;
    }}
    
    /* Make expander header more visible */
    .streamlit-expanderHeader {{
        background-color: #F5F5F5;
        border-radius: 5px;
        font-weight: 500;
    }}
</style>
""", unsafe_allow_html=True)


def get_data_stats():
    """Get statistics from the PostgreSQL database."""
    try:
        if not check_connection():
            return None
        return get_table_stats()
    except Exception as e:
        st.error(f"Error loading database stats: {e}")
        return {}


def render_sidebar():
    """Render the sidebar with database stats and info."""
    with st.sidebar:
        st.markdown(f"""
        <div style='text-align: center; padding: 1rem 0;'>
            <h1 style='color: {GARMIN_BLUE}; margin: 0;'>🏃 Garmin AI</h1>
            <p style='color: #666; margin: 0.5rem 0 0 0;'>Explore your fitness data</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Database Stats
        st.subheader("📊 Database")
        
        with st.spinner("Loading stats..."):
            stats = get_data_stats()
        
        if stats is None:
            st.error("Cannot connect to PostgreSQL")
            st.code("docker-compose up -d", language="bash")
            st.caption("Then run the backfill:")
            st.code("python -m src.backfill", language="bash")
        elif not stats or all(s.get('count', 0) == 0 for s in stats.values()):
            st.warning("No data found. Run the backfill script first!")
            st.code("python -m src.backfill", language="bash")
        else:
            for entity, data in stats.items():
                if data.get('count', 0) > 0:
                    st.markdown(f"""
                    <div class='stat-card'>
                        <p class='stat-value'>{data['count']:,}</p>
                        <p class='stat-label'>{entity.replace('_', ' ').title()}</p>
                    </div>
                    """, unsafe_allow_html=True)
            
            # Show date range
            all_dates = []
            for data in stats.values():
                if data.get('min_date'):
                    all_dates.append(pd.to_datetime(data['min_date']))
                if data.get('max_date'):
                    all_dates.append(pd.to_datetime(data['max_date']))
            
            if all_dates:
                min_date = min(all_dates).date()
                max_date = max(all_dates).date()
                st.caption(f"📅 Data range: {min_date} to {max_date}")
        
        st.markdown("---")
        
        # Example questions
        st.subheader("💡 Example Questions")
        example_questions = [
            "What's my average running distance?",
            "Show my top 5 longest runs",
            "How does my sleep affect performance?",
            "What's my resting heart rate trend?",
            "Compare my cycling vs running activities"
        ]
        
        for i, question in enumerate(example_questions):
            if st.button(question, key=f"example_{i}", use_container_width=True):
                st.session_state.example_question = question
                st.rerun()
        
        st.markdown("---")
        
        # Conversation context indicator
        if "conversation_history" in st.session_state and len(st.session_state.conversation_history) > 0:
            turn_count = len(st.session_state.conversation_history)
            st.markdown(f"""
            <div style='background-color: #E8F4FD; padding: 0.5rem; border-radius: 5px; margin-bottom: 0.5rem; border-left: 3px solid {GARMIN_TEAL};'>
                <span style='font-size: 0.85rem; color: #333;'>💬 <strong>Conversation active</strong></span><br/>
                <span style='font-size: 0.75rem; color: #666;'>{turn_count} turn{"s" if turn_count > 1 else ""} — follow-up questions use context</span>
            </div>
            """, unsafe_allow_html=True)
        
        # New Conversation button
        if st.button("✨ New Conversation", use_container_width=True):
            st.session_state.messages = []
            st.session_state.conversation_history = []
            st.rerun()


def main():
    """Main application."""
    
    # Initialize session state
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    # Initialize conversation history for LLM context
    if "conversation_history" not in st.session_state:
        st.session_state.conversation_history = []
    
    # Render sidebar
    render_sidebar()
    
    # Main header
    st.markdown(f"""
    <div class='main-header'>
        <h1>🏃 Garmin AI Explorer</h1>
        <p>Ask questions about your fitness data in natural language</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Display chat messages
    for idx, message in enumerate(st.session_state.messages):
        with st.chat_message(message["role"]):
            if message["role"] == "user":
                st.markdown(message["content"])
            else:
                # Assistant message with SQL, chart, and insights
                if "sql" in message:
                    with st.expander("🔍 SQL Query", expanded=False):
                        st.code(message["sql"], language="sql")
                
                if "chart" in message and message["chart"] is not None:
                    st.plotly_chart(message["chart"], use_container_width=True, key=f"chart_{idx}")
                
                if "summary" in message:
                    st.markdown(message["summary"])
                
                if "data_preview" in message and message["data_preview"] is not None:
                    with st.expander(f"📋 Data ({len(message['data_preview'])} rows)", expanded=False):
                        st.dataframe(message["data_preview"], use_container_width=True)
    
    # Handle example question from sidebar
    if hasattr(st.session_state, 'example_question'):
        question = st.session_state.example_question
        del st.session_state.example_question
        
        # Add user message
        st.session_state.messages.append({"role": "user", "content": question})
        
        # Process question
        with st.chat_message("user"):
            st.markdown(question)
        
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    # Get response with chart (with conversation context)
                    sql, df, summary, chart_spec, viz_df, conv_turn = ask_with_chart(
                        question, 
                        conversation_history=st.session_state.conversation_history
                    )
                    
                    # Update conversation history for next question
                    st.session_state.conversation_history.append(conv_turn)
                    
                    # Show SQL
                    with st.expander("🔍 SQL Query", expanded=False):
                        st.code(sql, language="sql")
                    
                    # Render chart using visualization data
                    chart = None
                    if chart_spec and not viz_df.empty:
                        try:
                            chart = render_chart(viz_df, chart_spec)
                            # Use unique key based on message count
                            st.plotly_chart(chart, use_container_width=True, key=f"chart_new_{len(st.session_state.messages)}")
                        except Exception as e:
                            st.warning(f"Could not render chart: {e}")
                            chart = None
                    
                    # Show summary
                    st.markdown(summary)
                    
                    # Show data preview
                    if not df.empty:
                        with st.expander(f"📋 Data ({len(df)} rows)", expanded=False):
                            st.dataframe(df, use_container_width=True)
                    
                    # Save to message history
                    st.session_state.messages.append({
                        "role": "assistant",
                        "sql": sql,
                        "chart": chart,
                        "summary": summary,
                        "data_preview": df if not df.empty else None
                    })
                    
                except Exception as e:
                    error_msg = f"❌ **Error:** {str(e)}"
                    st.error(error_msg)
                    st.session_state.messages.append({
                        "role": "assistant",
                        "summary": error_msg,
                        "chart": None
                    })
        
        st.rerun()
    
    # Chat input
    if prompt := st.chat_input("Ask a question about your Garmin data..."):
        # Add user message
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        # Display user message
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Generate response
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    # Get response with chart (with conversation context)
                    sql, df, summary, chart_spec, viz_df, conv_turn = ask_with_chart(
                        prompt,
                        conversation_history=st.session_state.conversation_history
                    )
                    
                    # Update conversation history for next question
                    st.session_state.conversation_history.append(conv_turn)
                    
                    # Show SQL
                    with st.expander("🔍 SQL Query", expanded=False):
                        st.code(sql, language="sql")
                    
                    # Render chart using visualization data
                    chart = None
                    if chart_spec and not viz_df.empty:
                        try:
                            chart = render_chart(viz_df, chart_spec)
                            # Use unique key based on message count
                            st.plotly_chart(chart, use_container_width=True, key=f"chart_new_{len(st.session_state.messages)}")
                        except Exception as e:
                            st.warning(f"Could not render chart: {e}")
                            chart = None
                    
                    # Show summary
                    st.markdown(summary)
                    
                    # Show data preview
                    if not df.empty:
                        with st.expander(f"📋 Data ({len(df)} rows)", expanded=False):
                            st.dataframe(df, use_container_width=True)
                    
                    # Save to message history
                    st.session_state.messages.append({
                        "role": "assistant",
                        "sql": sql,
                        "chart": chart,
                        "summary": summary,
                        "data_preview": df if not df.empty else None
                    })
                    
                except Exception as e:
                    error_msg = f"❌ **Error:** {str(e)}"
                    st.error(error_msg)
                    st.session_state.messages.append({
                        "role": "assistant",
                        "summary": error_msg,
                        "chart": None
                    })


if __name__ == "__main__":
    main()
