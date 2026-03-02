from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.parsers import parse_usage_logs
from dashboard.psyco import parse_psyco_analytics
from dashboard.pricing import apply_pricing, load_pricing

st.set_page_config(page_title="Agent Usage Dashboard", layout="wide")

st.markdown(
    """
<style>
.stApp {
    background: radial-gradient(circle at 10% 10%, #fef4ea 0%, #f8f5ef 40%, #eef5ff 100%);
    color: #1f2937;
    font-family: "Avenir Next", "Segoe UI", "Trebuchet MS", sans-serif;
}
.block-container {
    padding-top: 1.2rem;
    padding-bottom: 2rem;
}
div[data-testid="stMetric"] {
    background: rgba(255, 255, 255, 0.78);
    border: 1px solid rgba(148, 163, 184, 0.25);
    border-radius: 12px;
    padding: 0.75rem 1rem;
}
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_data(show_spinner=False)
def load_data(
    codex_root: str,
    claude_root: str,
    pricing_path: str,
    refresh_nonce: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str], dict]:
    del refresh_nonce
    usage_df, tool_df, warnings = parse_usage_logs(
        Path(codex_root).expanduser(), Path(claude_root).expanduser()
    )
    psyco_session_df, psyco_chat_df, psyco_tool_df, psyco_warnings = parse_psyco_analytics(
        Path(codex_root).expanduser(), Path(claude_root).expanduser()
    )
    pricing = load_pricing(Path(pricing_path).expanduser())
    return (
        usage_df,
        tool_df,
        psyco_session_df,
        psyco_chat_df,
        psyco_tool_df,
        warnings + psyco_warnings,
        pricing,
    )


def _format_currency(value: float) -> str:
    return f"${value:,.2f}"


def _format_int(value: float) -> str:
    return f"{int(value):,}"


def _apply_filters(
    usage_df: pd.DataFrame,
    tool_df: pd.DataFrame,
    start_date,
    end_date,
    selected_sources,
    selected_projects,
    selected_models,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if usage_df.empty:
        return usage_df, tool_df

    filtered_usage = usage_df.copy()
    filtered_tools = tool_df.copy()

    start_ts = pd.Timestamp(start_date).tz_localize("UTC")
    end_ts = pd.Timestamp(end_date).tz_localize("UTC") + timedelta(days=1) - timedelta(
        milliseconds=1
    )

    filtered_usage = filtered_usage[
        (filtered_usage["timestamp"] >= start_ts)
        & (filtered_usage["timestamp"] <= end_ts)
        & (filtered_usage["source"].isin(selected_sources))
        & (filtered_usage["project_name"].isin(selected_projects))
        & (filtered_usage["model"].isin(selected_models))
    ]

    if not filtered_tools.empty:
        filtered_tools = filtered_tools[
            (filtered_tools["timestamp"] >= start_ts)
            & (filtered_tools["timestamp"] <= end_ts)
            & (filtered_tools["source"].isin(selected_sources))
            & (filtered_tools["project_name"].isin(selected_projects))
            & (filtered_tools["model"].isin(selected_models))
        ]

    return filtered_usage, filtered_tools


def _apply_subscription_costs(
    usage_df: pd.DataFrame,
    codex_monthly_fee: float,
    claude_monthly_fee: float,
    allocation_basis: str,
) -> pd.DataFrame:
    if usage_df.empty:
        return usage_df

    df = usage_df.copy()
    df["billing_month"] = df["timestamp"].dt.to_period("M").astype(str)

    if allocation_basis == "events":
        df["allocation_weight"] = 1.0
    else:
        df["allocation_weight"] = pd.to_numeric(df["total_tokens"], errors="coerce").fillna(0)
        df.loc[df["allocation_weight"] <= 0, "allocation_weight"] = 1.0

    monthly_fee_map = {
        "codex": max(float(codex_monthly_fee), 0.0),
        "claude": max(float(claude_monthly_fee), 0.0),
    }
    df["monthly_fee_usd"] = df["source"].map(monthly_fee_map).fillna(0.0)

    group_weight = (
        df.groupby(["source", "billing_month"], dropna=False)["allocation_weight"]
        .transform("sum")
        .astype(float)
    )
    df["cost_usd"] = 0.0
    valid = (group_weight > 0) & (df["monthly_fee_usd"] > 0)
    df.loc[valid, "cost_usd"] = (
        df.loc[valid, "allocation_weight"] / group_weight[valid] * df.loc[valid, "monthly_fee_usd"]
    )

    token_total = pd.to_numeric(df["total_tokens"], errors="coerce").replace(0, pd.NA)
    input_ratio = (df["input_tokens"] / token_total).fillna(0.0)
    output_ratio = (df["output_tokens"] / token_total).fillna(0.0)
    cache_read_ratio = (df["cache_read_tokens"] / token_total).fillna(0.0)
    cache_write_ratio = (df["cache_write_tokens"] / token_total).fillna(0.0)

    df["input_cost_usd"] = df["cost_usd"] * input_ratio
    df["output_cost_usd"] = df["cost_usd"] * output_ratio
    df["cache_read_cost_usd"] = df["cost_usd"] * cache_read_ratio
    df["cache_write_cost_usd"] = df["cost_usd"] * cache_write_ratio
    df["price_source"] = "subscription"
    return df


st.title("Agent Dashboard")
st.caption("Unified local analytics for Codex + Claude Code usage with granular project/session drill-down.")

if "refresh_nonce" not in st.session_state:
    st.session_state.refresh_nonce = 0

with st.sidebar:
    st.header("Sources")
    codex_root = st.text_input("Codex sessions path", str(Path("~/.codex/sessions")))
    claude_root = st.text_input("Claude projects path", str(Path("~/.claude/projects")))
    pricing_path = st.text_input(
        "Pricing config path", str(Path.cwd() / "config/pricing.json")
    )
    st.header("Cost Mode")
    cost_mode = st.radio(
        "How to calculate cost",
        options=["Fixed Monthly Subscription", "Token Pricing"],
        help="Use fixed monthly plan allocation or per-token pricing.",
    )
    codex_monthly_fee = st.number_input(
        "Codex monthly fee (USD)", min_value=0.0, value=20.0, step=1.0
    )
    claude_monthly_fee = st.number_input(
        "Claude monthly fee (USD)", min_value=0.0, value=20.0, step=1.0
    )
    allocation_basis = st.selectbox(
        "Subscription allocation basis",
        options=["tokens", "events"],
        help="How monthly fee is distributed to granular events.",
    )
    if st.button("Refresh Data"):
        st.session_state.refresh_nonce += 1
    if cost_mode == "Token Pricing":
        st.caption("Costs use token rates from your pricing JSON.")
    else:
        st.caption("Costs allocate monthly subscription fee across usage events.")

with st.spinner("Loading and normalizing usage logs..."):
    (
        raw_usage_df,
        tool_df,
        psyco_session_df,
        psyco_chat_df,
        psyco_tool_df,
        warnings,
        pricing,
    ) = load_data(
        codex_root, claude_root, pricing_path, st.session_state.refresh_nonce
    )

if cost_mode == "Token Pricing":
    usage_df = apply_pricing(raw_usage_df, pricing)
else:
    usage_df = _apply_subscription_costs(
        raw_usage_df, codex_monthly_fee, claude_monthly_fee, allocation_basis
    )

if warnings:
    for warning in warnings:
        st.warning(warning)

if usage_df.empty:
    st.error("No usage records found. Check your source paths and refresh.")
    st.stop()

usage_df = usage_df.sort_values("timestamp").reset_index(drop=True)
tool_df = tool_df.sort_values("timestamp").reset_index(drop=True)

min_date = usage_df["timestamp"].dt.date.min()
max_date = usage_df["timestamp"].dt.date.max()

filter_col1, filter_col2, filter_col3, filter_col4 = st.columns([1.3, 1, 1, 1.2])
with filter_col1:
    date_range = st.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
if isinstance(date_range, tuple) and len(date_range) == 2:
    selected_start, selected_end = date_range
else:
    selected_start = selected_end = date_range

all_sources = sorted(usage_df["source"].dropna().unique().tolist())
all_projects = sorted(usage_df["project_name"].dropna().unique().tolist())
all_models = sorted(usage_df["model"].dropna().unique().tolist())

with filter_col2:
    selected_sources = st.multiselect(
        "Source", options=all_sources, default=all_sources
    )
with filter_col3:
    selected_projects = st.multiselect(
        "Projects", options=all_projects, default=all_projects
    )
with filter_col4:
    selected_models = st.multiselect("Models", options=all_models, default=all_models)

filtered_usage, filtered_tools = _apply_filters(
    usage_df,
    tool_df,
    selected_start,
    selected_end,
    selected_sources,
    selected_projects,
    selected_models,
)

if filtered_usage.empty:
    st.warning("No records match the selected filters.")
    st.stop()

total_cost = filtered_usage["cost_usd"].sum()
total_tokens = filtered_usage["total_tokens"].sum()
session_count = filtered_usage[["source", "session_id"]].drop_duplicates().shape[0]
project_count = filtered_usage["project_name"].nunique()

metric1, metric2, metric3, metric4 = st.columns(4)
metric1.metric("Estimated Cost", _format_currency(total_cost))
metric2.metric("Total Tokens", _format_int(total_tokens))
metric3.metric("Sessions", _format_int(session_count))
metric4.metric("Projects", _format_int(project_count))

tabs = st.tabs(["Overview", "Projects", "Granular", "Psyco Analysis", "Pricing"])

with tabs[0]:
    daily = (
        filtered_usage.assign(day=filtered_usage["timestamp"].dt.date)
        .groupby(["day", "source"], as_index=False)
        .agg(cost_usd=("cost_usd", "sum"), total_tokens=("total_tokens", "sum"))
    )
    c1, c2 = st.columns([1.7, 1])
    with c1:
        fig = px.area(
            daily,
            x="day",
            y="cost_usd",
            color="source",
            title="Daily Cost Trend",
            labels={"day": "Date", "cost_usd": "Cost (USD)"},
            color_discrete_sequence=["#0f766e", "#2563eb", "#ea580c", "#6d28d9"],
        )
        fig.update_layout(legend_title_text="Source")
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        by_source = (
            filtered_usage.groupby("source", as_index=False)
            .agg(cost_usd=("cost_usd", "sum"), total_tokens=("total_tokens", "sum"))
            .sort_values("cost_usd", ascending=False)
        )
        fig = px.bar(
            by_source,
            x="source",
            y="cost_usd",
            text_auto=".2s",
            title="Cost by Source",
            color="source",
            color_discrete_sequence=["#0f766e", "#2563eb", "#ea580c", "#6d28d9"],
        )
        st.plotly_chart(fig, use_container_width=True)

    by_model = (
        filtered_usage.groupby(["source", "model"], as_index=False)
        .agg(cost_usd=("cost_usd", "sum"), total_tokens=("total_tokens", "sum"))
        .sort_values("cost_usd", ascending=False)
        .head(20)
    )
    fig = px.bar(
        by_model,
        x="cost_usd",
        y="model",
        color="source",
        orientation="h",
        title="Top Models by Cost",
        labels={"cost_usd": "Cost (USD)", "model": "Model"},
        color_discrete_sequence=["#0f766e", "#2563eb", "#ea580c", "#6d28d9"],
    )
    st.plotly_chart(fig, use_container_width=True)

with tabs[1]:
    project_summary = (
        filtered_usage.groupby(["project_name", "source"], as_index=False)
        .agg(
            cost_usd=("cost_usd", "sum"),
            total_tokens=("total_tokens", "sum"),
            sessions=("session_id", "nunique"),
        )
        .sort_values("cost_usd", ascending=False)
    )
    fig = px.bar(
        project_summary.head(25),
        x="project_name",
        y="cost_usd",
        color="source",
        title="Top Projects by Cost",
        labels={"project_name": "Project", "cost_usd": "Cost (USD)"},
        color_discrete_sequence=["#0f766e", "#2563eb", "#ea580c", "#6d28d9"],
    )
    fig.update_layout(xaxis_tickangle=-35)
    st.plotly_chart(fig, use_container_width=True)

    treemap = (
        filtered_usage.groupby(["source", "project_name", "model"], as_index=False)
        .agg(cost_usd=("cost_usd", "sum"))
        .sort_values("cost_usd", ascending=False)
    )
    fig = px.treemap(
        treemap,
        path=["source", "project_name", "model"],
        values="cost_usd",
        title="Cost Distribution (Source > Project > Model)",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Project Table")
    st.dataframe(
        project_summary.assign(
            cost_usd=project_summary["cost_usd"].round(4),
            total_tokens=project_summary["total_tokens"].astype(int),
            sessions=project_summary["sessions"].astype(int),
        ),
        use_container_width=True,
        hide_index=True,
    )

    if not filtered_tools.empty:
        tool_summary = (
            filtered_tools.groupby(["project_name", "tool_name", "source"], as_index=False)
            .size()
            .rename(columns={"size": "calls"})
            .sort_values("calls", ascending=False)
        )
        fig = px.bar(
            tool_summary.head(30),
            x="project_name",
            y="calls",
            color="tool_name",
            title="Most Used Tools by Project",
        )
        fig.update_layout(xaxis_tickangle=-35)
        st.plotly_chart(fig, use_container_width=True)

with tabs[2]:
    session_summary = (
        filtered_usage.groupby(
            ["source", "session_id", "project_name", "model"], as_index=False
        )
        .agg(
            start_time=("timestamp", "min"),
            end_time=("timestamp", "max"),
            events=("timestamp", "count"),
            cost_usd=("cost_usd", "sum"),
            input_tokens=("input_tokens", "sum"),
            output_tokens=("output_tokens", "sum"),
            cache_read_tokens=("cache_read_tokens", "sum"),
            cache_write_tokens=("cache_write_tokens", "sum"),
            total_tokens=("total_tokens", "sum"),
        )
        .sort_values("cost_usd", ascending=False)
    )
    session_summary["duration_min"] = (
        session_summary["end_time"] - session_summary["start_time"]
    ).dt.total_seconds() / 60.0
    st.subheader("Session Summary")
    st.dataframe(
        session_summary.assign(
            cost_usd=session_summary["cost_usd"].round(4),
            duration_min=session_summary["duration_min"].round(1),
        ),
        use_container_width=True,
        hide_index=True,
    )

    session_labels = session_summary.apply(
        lambda row: (
            f"{row['source']} | {row['project_name']} | {row['session_id'][:10]}... "
            f"| {_format_currency(row['cost_usd'])}"
        ),
        axis=1,
    )
    label_to_key = {
        label: (row["source"], row["session_id"])
        for label, (_, row) in zip(session_labels, session_summary.iterrows())
    }
    selected_label = st.selectbox("Inspect Session", options=session_labels.tolist())
    selected_source, selected_session = label_to_key[selected_label]

    session_events = filtered_usage[
        (filtered_usage["source"] == selected_source)
        & (filtered_usage["session_id"] == selected_session)
    ].sort_values("timestamp")
    session_events = session_events.copy()
    session_events["cumulative_cost_usd"] = session_events["cost_usd"].cumsum()
    session_events["cumulative_tokens"] = session_events["total_tokens"].cumsum()

    c1, c2 = st.columns(2)
    with c1:
        fig = px.line(
            session_events,
            x="timestamp",
            y="cumulative_cost_usd",
            title="Cumulative Cost in Session",
            labels={"timestamp": "Time", "cumulative_cost_usd": "Cost (USD)"},
        )
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        fig = px.line(
            session_events,
            x="timestamp",
            y="cumulative_tokens",
            title="Cumulative Tokens in Session",
            labels={"timestamp": "Time", "cumulative_tokens": "Tokens"},
        )
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Session Events")
    st.dataframe(
        session_events[
            [
                "timestamp",
                "model",
                "input_tokens",
                "output_tokens",
                "cache_read_tokens",
                "cache_write_tokens",
                "reasoning_tokens",
                "total_tokens",
                "cost_usd",
                "price_source",
                "request_id",
            ]
        ].assign(cost_usd=session_events["cost_usd"].round(6)),
        use_container_width=True,
        hide_index=True,
    )

    session_tools = filtered_tools[
        (filtered_tools["source"] == selected_source)
        & (filtered_tools["session_id"] == selected_session)
    ]
    if not session_tools.empty:
        st.subheader("Session Tool Calls")
        st.dataframe(
            session_tools[["timestamp", "tool_name", "model", "request_id"]],
            use_container_width=True,
            hide_index=True,
        )

    st.download_button(
        "Download Filtered Events CSV",
        filtered_usage.to_csv(index=False).encode("utf-8"),
        file_name="agent_usage_events.csv",
        mime="text/csv",
    )

with tabs[3]:
    st.subheader("Session Behavior + Build Activity")

    if psyco_session_df.empty:
        st.info("No session-level chat/activity data found.")
    else:
        psyco_filtered = psyco_session_df[
            (psyco_session_df["source"].isin(selected_sources))
            & (psyco_session_df["project_name"].isin(selected_projects))
            & (
                psyco_session_df["last_timestamp"].dt.date.between(
                    selected_start, selected_end
                )
            )
        ].copy()

        if psyco_filtered.empty:
            st.info("No psycho-analysis sessions match current filters.")
        else:
            top1, top2, top3, top4, top5 = st.columns(5)
            top1.metric("User Questions", _format_int(psyco_filtered["user_messages"].sum()))
            top2.metric("AI Answers", _format_int(psyco_filtered["assistant_messages"].sum()))
            top3.metric("Back-Forth Pairs", _format_int(psyco_filtered["back_forth_pairs"].sum()))
            top4.metric("AI Code Lines", _format_int(psyco_filtered["code_lines_written"].sum()))
            top5.metric("Files Touched", _format_int(psyco_filtered["files_touched_count"].sum()))

            m1, m2 = st.columns([1.15, 1])
            with m1:
                session_activity = (
                    psyco_filtered[
                        [
                            "source",
                            "session_id",
                            "project_name",
                            "last_timestamp",
                            "user_messages",
                            "assistant_messages",
                            "back_forth_pairs",
                            "code_lines_written",
                            "files_touched_count",
                            "mcp_calls",
                            "tool_calls_total",
                            "mcp_tools",
                            "files_touched",
                        ]
                    ]
                    .sort_values("last_timestamp", ascending=False)
                    .reset_index(drop=True)
                )
                st.dataframe(session_activity, use_container_width=True, hide_index=True)

            with m2:
                mcp_summary = (
                    psyco_filtered.groupby("source", as_index=False)
                    .agg(mcp_calls=("mcp_calls", "sum"), tools=("tool_calls_total", "sum"))
                    .sort_values("mcp_calls", ascending=False)
                )
                fig = px.bar(
                    mcp_summary,
                    x="source",
                    y="mcp_calls",
                    text_auto=True,
                    title="MCP Calls by Source",
                    color="source",
                    color_discrete_sequence=["#0f766e", "#2563eb", "#ea580c", "#6d28d9"],
                )
                st.plotly_chart(fig, use_container_width=True)

                fig = px.scatter(
                    psyco_filtered,
                    x="back_forth_pairs",
                    y="code_lines_written",
                    size="files_touched_count",
                    color="source",
                    hover_data=["project_name", "session_id", "mcp_calls"],
                    title="Conversation vs Code Output per Session",
                    color_discrete_sequence=["#0f766e", "#2563eb", "#ea580c", "#6d28d9"],
                )
                st.plotly_chart(fig, use_container_width=True)

            selectable_sessions = psyco_filtered.apply(
                lambda row: (
                    f"{row['source']} | {row['project_name']} | {row['session_id'][:10]}... "
                    f"| Q:{int(row['user_messages'])} A:{int(row['assistant_messages'])}"
                ),
                axis=1,
            )
            label_to_session = {
                label: (row["source"], row["session_id"])
                for label, (_, row) in zip(selectable_sessions, psyco_filtered.iterrows())
            }

            selected_psyco_label = st.selectbox(
                "Inspect Session Conversation",
                options=selectable_sessions.tolist(),
            )
            selected_psyco_source, selected_psyco_session = label_to_session[selected_psyco_label]

            session_chat = psyco_chat_df[
                (psyco_chat_df["source"] == selected_psyco_source)
                & (psyco_chat_df["session_id"] == selected_psyco_session)
            ].sort_values("timestamp")
            if session_chat.empty:
                st.info("No text messages captured for this session.")
            else:
                st.subheader("Back-and-Forth Transcript")
                st.dataframe(
                    session_chat[["timestamp", "role", "text"]],
                    use_container_width=True,
                    hide_index=True,
                )

            session_tool_activity = psyco_tool_df[
                (psyco_tool_df["source"] == selected_psyco_source)
                & (psyco_tool_df["session_id"] == selected_psyco_session)
            ].copy()
            if not session_tool_activity.empty:
                st.subheader("Session MCP/Tool Activity")
                tool_counts = (
                    session_tool_activity.groupby(["tool_name", "is_mcp"], as_index=False)
                    .size()
                    .rename(columns={"size": "calls"})
                    .sort_values("calls", ascending=False)
                )
                st.dataframe(tool_counts, use_container_width=True, hide_index=True)
                fig = px.bar(
                    tool_counts.head(25),
                    x="tool_name",
                    y="calls",
                    color="is_mcp",
                    title="Tool Calls in Selected Session",
                )
                fig.update_layout(xaxis_tickangle=-35)
                st.plotly_chart(fig, use_container_width=True)

with tabs[4]:
    st.subheader("Pricing Coverage")
    price_source_summary = (
        filtered_usage.groupby("price_source", as_index=False)
        .agg(rows=("price_source", "count"), cost_usd=("cost_usd", "sum"))
        .sort_values("rows", ascending=False)
    )
    st.dataframe(
        price_source_summary.assign(cost_usd=price_source_summary["cost_usd"].round(4)),
        use_container_width=True,
        hide_index=True,
    )

    if cost_mode == "Token Pricing":
        default_priced_models = (
            filtered_usage[filtered_usage["price_source"] == "default"]
            .groupby("model", as_index=False)
            .size()
            .rename(columns={"size": "events"})
            .sort_values("events", ascending=False)
        )
        if not default_priced_models.empty:
            st.warning("Some models are using default pricing. Add exact rates for higher accuracy.")
            st.dataframe(default_priced_models, use_container_width=True, hide_index=True)
    else:
        st.info(
            f"Subscription mode is active. Monthly fees: Codex ${codex_monthly_fee:.2f}, "
            f"Claude ${claude_monthly_fee:.2f}."
        )

    component_summary = pd.DataFrame(
        {
            "component": [
                "input",
                "output",
                "cache_read",
                "cache_write",
            ],
            "cost_usd": [
                filtered_usage["input_cost_usd"].sum(),
                filtered_usage["output_cost_usd"].sum(),
                filtered_usage["cache_read_cost_usd"].sum(),
                filtered_usage["cache_write_cost_usd"].sum(),
            ],
        }
    )
    fig = px.pie(
        component_summary,
        names="component",
        values="cost_usd",
        title="Cost Composition by Token Type",
    )
    st.plotly_chart(fig, use_container_width=True)

    pricing_file = Path(pricing_path).expanduser()
    st.subheader("Active Pricing JSON")
    if pricing_file.exists():
        st.code(pricing_file.read_text(encoding="utf-8"), language="json")
    else:
        st.info("Pricing file not found. The dashboard is using built-in defaults.")
