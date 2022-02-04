"""Graph generating functions"""
import pandas as pd
import plotly.express as px

from mppsteel.utility.log_utility import get_logger

logger = get_logger("Plotly graphs")

ARCHETYPE_COLORS = {
    "Avg BF-BOF": "#59A14F",
    "BAT BF-BOF": "#A0CBE8",
    "BAT BF-BOF+BECCS": "#F1CE63",
    "BAT BF-BOF+CCS": "#499894",
    "BAT BF-BOF+CCUS": "#86BCB6",
    "BAT BF-BOF_H2 PCI": "#B6992D",
    "BAT BF-BOF_bio PCI": "#8CD17D",
    "DRI-EAF": "#FFBE7D",
    "DRI-EAF+CCS": "#F28E2B",
    "DRI-EAF_100% green H2": "#E15759",
    "DRI-EAF_50% bio-CH4": "#FF9D9A",
    "DRI-EAF_50% green H2": "#79706E",
    "DRI-Melt-BOF": "#BAB0AC",
    "DRI-Melt-BOF+CCS": "#FABFD2",
    "DRI-Melt-BOF_100% zero-C H2": "#D37295",
    "EAF": "#4E79A7",
    "Electrolyzer-EAF": "#B07AA1",
    "Electrowinning-EAF": "#D4A6C8",
    "Smelting Reduction": "#9D7660",
    "Smelting Reduction+CCS": "#D7B5A6",
}
SCENARIO_COLOURS = {
    "Baseline": '#A5A5A5',
    "Tech Moratorium": '#9DB1CF',
    "Carbon Tax": '#6F8DB9',
    "Fast Abatement": '#4C6C9C'
}

def line_graph(
    df: pd.DataFrame, x: str, y: str, title: str,
    save_filepath:str=None, ext:str='png') -> px.line:
    fig_ = px.line(df, x=x, y=y, title=title)

    if save_filepath:
        fig_.write_image(f'{save_filepath}.{ext}')

    return fig_

def line_chart(
    data: pd.DataFrame,
    x: str, y: str, color: str, name: str,
    x_axis: str, y_axis: str, text: str = None,
    color_discrete_map: dict = None,
    save_filepath:str = None,
    ext: str ='png') -> px.line:
    ## this need to be updated to account for multiple facets https://github.com/plotly/plotly.py/issues/2545
    fig_ = px.line(
        data, x=x, y=y, color=color, text=text, color_discrete_map=color_discrete_map
    )
    fig_.update_traces(mode="lines", hovertemplate=None)
    fig_.update_layout(
        titlefont=dict(family="Arial", size=18, color="black"),
        title_text=name,
        showlegend=True,
        hovermode="x unified",
        plot_bgcolor="white",
        xaxis=dict(
            title_text=x_axis,
            titlefont=dict(family="Arial", size=14, color="black"),
            tickfont=dict(family="Arial", size=14, color="black"),
            title_standoff=10,
            showspikes=True,
            spikemode="across",
            spikesnap="cursor",
            showline=True,
            showgrid=True,
            fixedrange=True,
            linewidth=1,
            linecolor="grey",
        ),
        yaxis=dict(
            title_text=y_axis,
            titlefont=dict(family="Arial", size=14, color="black"),
            tickfont=dict(family="Arial", size=14, color="black"),
            title_standoff=3,
            fixedrange=True,
            linewidth=1,
            linecolor="grey",
            showgrid=True,
            gridwidth=1,
            gridcolor="whitesmoke",
            rangemode="tozero",
        ),
        legend=dict(
            font=dict(family="Arial", size=8, color="black"),
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="left",
            x=-0,
        ),
    )

    if save_filepath:
        fig_.write_image(f'{save_filepath}.{ext}')

    return fig_

def area_chart(
    data: pd.DataFrame,
    x: str, y: str, color: str,
    name: str, x_axis: str, y_axis: str,
    hoverdata, save_filepath: str = None,
    ext: str = 'png') -> px.area:
    fig_ = px.area(data, x=x, y=y, color=color, color_discrete_map=ARCHETYPE_COLORS, hover_data=hoverdata)
    fig_.update_layout(
        legend_title_text='',
        xaxis_title=None,
        titlefont=dict(family="Arial", size=18, color="black"),
        title_text=name,
        showlegend=True,
        hovermode="x unified",
        plot_bgcolor="white",
        xaxis=dict(
            # title_text=x_axis,
            titlefont=dict(family="Arial", size=14, color="black"),
            tickfont=dict(family="Arial", size=14, color="black"),
            title_standoff=10,
            showspikes=False,
            spikemode="across",
            spikesnap="cursor",
            showline=True,
            showgrid=True,
            fixedrange=True,
            linewidth=1,
            linecolor="grey",
        ),
        yaxis=dict(
            title_text=y_axis,
            titlefont=dict(family="Arial", size=14, color="black"),
            tickfont=dict(family="Arial", size=14, color="black"),
            title_standoff=3,
            fixedrange=True,
            linewidth=1,
            linecolor="grey",
            showgrid=True,
            gridwidth=1,
            gridcolor="whitesmoke",
            rangemode="tozero",
        ),
        legend=dict(
            font=dict(family="Arial", size=8, color="black"),
            orientation="h",
            yanchor="top",
            y=-0.1,
            xanchor="left",
            x=-0.2,
        ),
    )

    if save_filepath:
        fig_.write_image(f'{save_filepath}.{ext}')

    return fig_

def bar_chart(
    data: pd.DataFrame,
    x: str, y:str, color: str,
    color_discrete_map: dict = None,
    array_order: list = None,
    title_text: str = '',
    xaxis_title: str = '',
    yaxis_title: str = '',
    legend_text: str ='') -> px.bar:

    fig_ = px.bar(data, x=x, y=y, title=title_text, color=color, color_discrete_map=color_discrete_map, text=y, width=1500, height=1000)
    fig_.update_layout(
        titlefont=dict(family='Arial', size=12, color='black'),
        title_text=title_text,
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title,
        legend_title_text=legend_text,
        showlegend=True,
        hovermode='x unified',
        plot_bgcolor="white",
        legend=dict(
            font=dict(family="Arial", size=10, color="black"),
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="left",
            x=-0),
    )

    fig_.update_xaxes(
        title_text='',
        titlefont=dict(family='Arial', size=12, color='black'),
        tickfont=dict(family='Arial', size=12, color='black'),
        title_standoff=3,
        showspikes=False,
        spikemode='across',
        spikesnap='data',
        showline=True,
        showgrid=True,
        fixedrange=True,
        linewidth=1,
        linecolor='grey',
        categoryorder='array',
        categoryarray=array_order
    )

    fig_.update_yaxes(
        title_text='',
        titlefont=dict(family='Arial', size=12, color='black'),
        tickfont=dict(family='Arial', size=12, color='black'),
        title_standoff=3,
        fixedrange=True,
        linewidth=1,
        linecolor='grey',
        showgrid=True,
        gridwidth=1,
        gridcolor='whitesmoke',
        rangemode="tozero"
    )
    fig_.update_traces(texttemplate='%{text:.4s}', textposition='inside')
    fig_.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
    fig_.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    return fig_

def bar_chart_vertical(
    data: pd.DataFrame,
    x: str, y: str, facet_row: str, color: str,
    color_discrete_map: dict, x_text: str, y_text: str,
    title_text: str, text: str = None,
    save_filepath: str = None, ext: str='png') -> px.bar:

    fig_ = px.bar(data, x=x, y=y, facet_row=facet_row, color=color, color_discrete_map=color_discrete_map, orientation='h', text=text)
    fig_.update_layout(
        titlefont=dict(family='Arial', size=12,color='black'),
        title_text=title_text,
        legend_title_text='',
        showlegend=False,
        hovermode='y unified',
        plot_bgcolor="white",
        legend=dict(
            font=dict(family="Arial",size=10,color="black"),
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="left",
            x=-0)
    )
    fig_.update_xaxes(
        title_text=x_text,
        titlefont=dict(family='Arial', size=12, color='black'),
        tickfont=dict(family='Arial',size=10,color='black'),
        title_standoff=3,
        showspikes=False,
        spikemode='across',
        spikesnap='data',
        showline=True,
        showgrid=True,
        fixedrange=True,
        linewidth=1,
        linecolor='grey',
    )
    fig_.update_yaxes(
        title_text=None,
        titlefont=dict(family='Arial', size=12,color='black'),
        tickfont=dict(family='Arial', size=10, color='black'),
        title_standoff=3,
        fixedrange=True,
        linewidth=1,
        linecolor='grey',
        showgrid=True,
        gridwidth=1,
        gridcolor='whitesmoke',
        rangemode="tozero",
        showspikes = False,
    )
    fig_.update_traces(texttemplate='%{x:,.0f}', textposition='inside')
    fig_.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
    fig_.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))

    if save_filepath:
        fig_.write_image(f'{save_filepath}.{ext}')

    return fig_
