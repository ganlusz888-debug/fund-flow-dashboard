#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据中台 JSON 生成器
===================
基于 a_share_fund_flow.py 抓取的数据，生成 data.json 供网页读取。

使用方式：
  python generate_data.py                # 每日模式（默认）
  python generate_data.py --mode daily   # 每日模式
  python generate_data.py --proxy http://127.0.0.1:7890  # 使用代理

输出：
  data.json — 网页数据中台所需的数据文件
"""

import json
import sys
import os
from datetime import datetime

# 导入原有的抓取器（需要 a_share_fund_flow.py 在同一目录或 Python 路径中）
from a_share_fund_flow import EastMoneyFundFlow, LOGGER


def generate_dashboard_json(scraper: EastMoneyFundFlow, output_file: str = "data.json") -> str:
    """
    抓取数据并生成 data.json，供 HTML 网页读取。
    
    生成的 JSON 结构：
    {
        "update_time": "2024-01-15 15:30:00",
        "market_summary": { ... },       # 全市场概览
        "top_stocks_inflow": [ ... ],     # 主力净流入 TOP 10
        "top_stocks_outflow": [ ... ],    # 主力净流出 TOP 10
        "industry_flow": [ ... ],         # 行业板块资金流向
        "concept_flow": [ ... ],          # 概念板块资金流向
        "market_history": { ... },        # 全市场历史趋势
    }
    """
    
    LOGGER.info("=" * 50)
    LOGGER.info("开始生成数据中台 JSON")
    LOGGER.info("=" * 50)
    
    dashboard_data = {
        "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "market_summary": {},
        "top_stocks_inflow": [],
        "top_stocks_outflow": [],
        "industry_flow": [],
        "concept_flow": [],
        "market_history": {
            "dates": [],
            "主力净流入": [],
            "超大单净流入": [],
            "大单净流入": [],
            "中单净流入": [],
            "小单净流入": [],
        },
    }
    
    # ── 1. 全市场个股资金流向 ──────────────────
    LOGGER.info("[1/4] 抓取全市场个股资金流向...")
    df_stocks = scraper.fetch_stock_flow_today()
    
    if not df_stocks.empty:
        # 全市场概览：汇总所有股票的主力/超大单/大单/中单/小单
        summary = {
            "主力净流入合计(亿元)": round(df_stocks["主力净流入(亿元)"].sum(), 2) if "主力净流入(亿元)" in df_stocks.columns else 0,
            "超大单净流入合计(亿元)": round(df_stocks["超大单净流入(亿元)"].sum(), 2) if "超大单净流入(亿元)" in df_stocks.columns else 0,
            "大单净流入合计(亿元)": round(df_stocks["大单净流入(亿元)"].sum(), 2) if "大单净流入(亿元)" in df_stocks.columns else 0,
            "中单净流入合计(亿元)": round(df_stocks["中单净流入(亿元)".format()].sum(), 2) if "中单净流入(亿元)" in df_stocks.columns else 0,
            "小单净流入合计(亿元)": round(df_stocks["小单净流入(亿元)"].sum(), 2) if "小单净流入(亿元)" in df_stocks.columns else 0,
            "上涨家数": int((df_stocks["涨跌幅(%)"] > 0).sum()) if "涨跌幅(%)" in df_stocks.columns else 0,
            "下跌家数": int((df_stocks["涨跌幅(%)"] < 0).sum()) if "涨跌幅(%)" in df_stocks.columns else 0,
            "平盘家数": int((df_stocks["涨跌幅(%)"] == 0).sum()) if "涨跌幅(%)" in df_stocks.columns else 0,
        }
        dashboard_data["market_summary"] = summary
        
        # 主力净流入 TOP 10
        top_inflow_cols = ["排名", "股票代码", "股票名称", "最新价(元)", "涨跌幅(%)",
                           "主力净流入(亿元)", "主力净流入占比(%)", "超大单净流入(亿元)"]
        available_inflow_cols = [c for c in top_inflow_cols if c in df_stocks.columns]
