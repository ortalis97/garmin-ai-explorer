"""
Visualization module for generating Garmin-themed charts.
Uses Plotly with Garmin brand colors.
"""
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from typing import Dict, Any, Optional


# Garmin brand colors
GARMIN_BLUE = "#007CC3"
GARMIN_TEAL = "#00A9CE"
GARMIN_ORANGE = "#FF6A13"
GARMIN_DARK = "#1A2332"
GARMIN_LIGHT_GRAY = "#F5F5F5"

# Color sequence for multi-series charts
GARMIN_COLORS = [GARMIN_BLUE, GARMIN_TEAL, GARMIN_ORANGE, "#4CAF50", "#9C27B0", "#FF9800"]


def get_garmin_layout(title: str = None) -> Dict[str, Any]:
    """
    Get base Plotly layout with Garmin styling.
    
    Args:
        title: Optional chart title
    """
    layout = {
        "plot_bgcolor": "white",
        "paper_bgcolor": "white",
        "font": {
            "family": "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
            "color": GARMIN_DARK,
            "size": 12
        },
        "xaxis": {
            "gridcolor": "#E0E0E0",
            "showgrid": True,
            "zeroline": False
        },
        "yaxis": {
            "gridcolor": "#E0E0E0",
            "showgrid": True,
            "zeroline": False
        },
        "margin": {"l": 60, "r": 40, "t": 60, "b": 60},
        "hovermode": "closest"
    }
    
    if title:
        layout["title"] = {
            "text": title,
            "font": {"size": 18, "color": GARMIN_DARK},
            "x": 0.5,
            "xanchor": "center"
        }
    
    return layout


def render_chart(df: pd.DataFrame, chart_spec: Dict[str, Any]) -> go.Figure:
    """
    Render a chart based on the specification and DataFrame.
    
    Args:
        df: DataFrame with data to visualize
        chart_spec: Dictionary with chart configuration
            - chart_type: "line", "bar", "scatter", "pie", or "table"
            - x_axis: column name for x-axis
            - y_axis: column name(s) for y-axis (can be list)
            - title: chart title
            - color_by: optional column to color by
    
    Returns:
        Plotly figure object
    """
    if df.empty:
        return create_empty_chart("No data to visualize")
    
    chart_type = chart_spec.get("chart_type", "bar").lower()
    title = chart_spec.get("title", "Data Visualization")
    
    try:
        if chart_type == "line":
            return create_line_chart(df, chart_spec)
        elif chart_type == "bar":
            return create_bar_chart(df, chart_spec)
        elif chart_type == "scatter":
            return create_scatter_chart(df, chart_spec)
        elif chart_type == "pie":
            return create_pie_chart(df, chart_spec)
        elif chart_type == "table":
            return create_table_chart(df, chart_spec)
        else:
            # Default to bar chart
            return create_bar_chart(df, chart_spec)
    except Exception as e:
        return create_empty_chart(f"Error creating chart: {str(e)}")


def create_line_chart(df: pd.DataFrame, spec: Dict[str, Any]) -> go.Figure:
    """Create a line chart."""
    x_col = spec.get("x_axis")
    y_col = spec.get("y_axis")
    color_by = spec.get("color_by")
    title = spec.get("title", "Line Chart")
    
    # Auto-detect if not specified
    if not x_col:
        # Use first column that looks like a date or the first column
        date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        x_col = date_cols[0] if date_cols else df.columns[0]
    
    if not y_col:
        # Use first numeric column that's not the x_col
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        y_col = [col for col in numeric_cols if col != x_col][0] if numeric_cols else df.columns[1]
    
    if isinstance(y_col, list):
        # Multiple y columns
        fig = go.Figure()
        for i, col in enumerate(y_col):
            fig.add_trace(go.Scatter(
                x=df[x_col],
                y=df[col],
                mode='lines+markers',
                name=col,
                line=dict(color=GARMIN_COLORS[i % len(GARMIN_COLORS)], width=2),
                marker=dict(size=6)
            ))
    else:
        if color_by and color_by in df.columns:
            # Color by category
            fig = px.line(df, x=x_col, y=y_col, color=color_by, 
                         title=title, color_discrete_sequence=GARMIN_COLORS)
        else:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df[x_col],
                y=df[y_col],
                mode='lines+markers',
                line=dict(color=GARMIN_BLUE, width=2),
                marker=dict(size=6),
                name=y_col
            ))
    
    fig.update_layout(**get_garmin_layout(title))
    return fig


def create_bar_chart(df: pd.DataFrame, spec: Dict[str, Any]) -> go.Figure:
    """Create a bar chart."""
    x_col = spec.get("x_axis")
    y_col = spec.get("y_axis")
    color_by = spec.get("color_by")
    title = spec.get("title", "Bar Chart")
    
    # Auto-detect if not specified
    if not x_col:
        x_col = df.columns[0]
    if not y_col:
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        y_col = numeric_cols[0] if numeric_cols else df.columns[1]
    
    if color_by and color_by in df.columns:
        fig = px.bar(df, x=x_col, y=y_col, color=color_by,
                    title=title, color_discrete_sequence=GARMIN_COLORS)
    else:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df[x_col],
            y=df[y_col],
            marker_color=GARMIN_BLUE,
            name=y_col
        ))
    
    fig.update_layout(**get_garmin_layout(title))
    return fig


def create_scatter_chart(df: pd.DataFrame, spec: Dict[str, Any]) -> go.Figure:
    """Create a scatter plot."""
    x_col = spec.get("x_axis")
    y_col = spec.get("y_axis")
    color_by = spec.get("color_by")
    title = spec.get("title", "Scatter Plot")
    
    # Auto-detect numeric columns
    if not x_col or not y_col:
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        if len(numeric_cols) >= 2:
            x_col = x_col or numeric_cols[0]
            y_col = y_col or numeric_cols[1]
        else:
            x_col = x_col or df.columns[0]
            y_col = y_col or df.columns[1]
    
    if color_by and color_by in df.columns:
        fig = px.scatter(df, x=x_col, y=y_col, color=color_by,
                        title=title, color_discrete_sequence=GARMIN_COLORS)
    else:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df[x_col],
            y=df[y_col],
            mode='markers',
            marker=dict(color=GARMIN_BLUE, size=8),
            name=f"{y_col} vs {x_col}"
        ))
    
    fig.update_layout(**get_garmin_layout(title))
    return fig


def create_pie_chart(df: pd.DataFrame, spec: Dict[str, Any]) -> go.Figure:
    """Create a pie chart."""
    labels_col = spec.get("x_axis") or spec.get("labels")
    values_col = spec.get("y_axis") or spec.get("values")
    title = spec.get("title", "Pie Chart")
    
    # Auto-detect
    if not labels_col:
        # Use first non-numeric column
        non_numeric = df.select_dtypes(exclude=['number']).columns.tolist()
        labels_col = non_numeric[0] if non_numeric else df.columns[0]
    
    if not values_col:
        # Use first numeric column
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        values_col = numeric_cols[0] if numeric_cols else df.columns[1]
    
    fig = go.Figure()
    fig.add_trace(go.Pie(
        labels=df[labels_col],
        values=df[values_col],
        marker=dict(colors=GARMIN_COLORS),
        textinfo='label+percent'
    ))
    
    fig.update_layout(**get_garmin_layout(), title=title, showlegend=True)
    return fig


def create_table_chart(df: pd.DataFrame, spec: Dict[str, Any]) -> go.Figure:
    """Create a table visualization."""
    title = spec.get("title", "Data Table")
    
    # Limit to first 50 rows for display
    display_df = df.head(50)
    
    fig = go.Figure(data=[go.Table(
        header=dict(
            values=[f"<b>{col}</b>" for col in display_df.columns],
            fill_color=GARMIN_BLUE,
            font=dict(color='white', size=12),
            align='left'
        ),
        cells=dict(
            values=[display_df[col] for col in display_df.columns],
            fill_color=[[GARMIN_LIGHT_GRAY if i % 2 == 0 else 'white' 
                        for i in range(len(display_df))]],
            align='left',
            font=dict(size=11)
        )
    )])
    
    fig.update_layout(
        title=title,
        margin=dict(l=10, r=10, t=40, b=10),
        height=min(400 + len(display_df) * 20, 800)
    )
    return fig


def create_empty_chart(message: str = "No data available") -> go.Figure:
    """Create an empty chart with a message."""
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.5,
        showarrow=False,
        font=dict(size=16, color=GARMIN_DARK)
    )
    fig.update_layout(**get_garmin_layout(), height=400)
    return fig
