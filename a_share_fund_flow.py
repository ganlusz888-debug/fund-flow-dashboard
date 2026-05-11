#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股市场资金流向数据抓取工具
==============================
数据来源优先级：
  1. 东方财富 (eastmoney) - 主力数据源
       - push2.eastmoney.com     实时/日内接口
       - push2his.eastmoney.com  历史数据接口
  2. 同花顺 (10jqka)            - 备用数据源（个股资金流向）

覆盖范围：
  - 个股资金流向（超大单/大单/中单/小单/主力）
  - 行业板块资金流向排名
  - 概念板块资金流向排名
  - 全市场资金流向汇总
  - 历史模式 & 每日更新模式

运行方式：
  python a_share_fund_flow.py --mode daily                         # 每日更新（今日数据）
  python a_share_fund_flow.py --mode history --days 30            # 历史30天
  python a_share_fund_flow.py --mode history --start 2024-01-01  # 指定起始日期
  python a_share_fund_flow.py --mode stock --code 000001          # 单只个股历史
"""

import os
import sys
import time
import logging
import argparse
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────────────────────────
# 日志配置
# ─────────────────────────────────────────────
def setup_logger(log_file: str = "fund_flow.log") -> logging.Logger:
    """配置同时输出到控制台和文件的日志器"""
    logger = logging.getLogger("FundFlow")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 控制台 handler（INFO 级别）
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    # 文件 handler（DEBUG 级别，记录全部细节）
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


LOGGER = setup_logger()


# ─────────────────────────────────────────────
# HTTP 会话工厂（含重试 & 代理支持）
# ─────────────────────────────────────────────
def create_session(
    proxies: Optional[Dict[str, str]] = None,
    max_retries: int = 5,
    backoff_factor: float = 0.8,
) -> requests.Session:
    """
    创建带自动重试机制的 requests.Session。
    :param proxies: 代理配置，例如 {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"}
    :param max_retries: 最大重试次数
    :param backoff_factor: 重试间隔系数（指数退避）
    """
    session = requests.Session()

    retry = Retry(
        total=max_retries,
        read=max_retries,
        connect=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # 模拟浏览器请求头，避免被简单反爬拦截
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": "https://data.eastmoney.com/",
        "Origin": "https://data.eastmoney.com",
    })

    if proxies:
        session.proxies.update(proxies)
        LOGGER.info(f"已配置代理: {proxies}")

    return session


# ─────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────
def yuan_to_yi(value: Any) -> Optional[float]:
    """将元转换为亿元，保留4位小数；无效值返回 None"""
    try:
        v = float(value)
        return round(v / 1e8, 4)
    except (TypeError, ValueError):
        return None


def fen_to_wan_shou(value: Any) -> Optional[float]:
    """将手转换为万手，保留2位小数；无效值返回 None"""
    try:
        v = float(value)
        return round(v / 10000, 2)
    except (TypeError, ValueError):
        return None


def safe_float(value: Any) -> Optional[float]:
    """安全转 float，无效值返回 None"""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def timestamp_to_date(ts: Any) -> Optional[str]:
    """东方财富时间戳（秒）转日期字符串"""
    try:
        return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OSError):
        return None


def get_trading_dates(start: str, end: str) -> List[str]:
    """
    简单生成日期序列（周一到周五），不过滤法定节假日。
    如需精确交易日，可接入 exchange_calendars 或 tushare 交易日历。
    """
    result = []
    d = datetime.strptime(start, "%Y-%m-%d")
    end_d = datetime.strptime(end, "%Y-%m-%d")
    while d <= end_d:
        if d.weekday() < 5:  # 0=周一 … 4=周五
            result.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return result


# ─────────────────────────────────────────────
# 东方财富资金流向抓取器
# ─────────────────────────────────────────────
class EastMoneyFundFlow:
    """
    东方财富资金流向抓取器

    API 端点说明（均为东方财富 APP/网页 所使用的公开接口）：
      - push2.eastmoney.com      实时接口（日内）
      - push2his.eastmoney.com   历史 K 线接口
      - datacenter.eastmoney.com 数据中心接口
    """

    # ── 个股列表资金流向（今日）
    STOCK_FLOW_TODAY_URL = (
        "https://push2.eastmoney.com/api/qt/clist/get"
    )
    # ── 个股历史资金流向日 K
    STOCK_FLOW_HISTORY_URL = (
        "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
    )
    # ── 行业板块资金流向（今日排名）
    INDUSTRY_FLOW_URL = (
        "https://push2.eastmoney.com/api/qt/clist/get"
    )
    # ── 概念板块资金流向（今日排名）
    CONCEPT_FLOW_URL = (
        "https://push2.eastmoney.com/api/qt/clist/get"
    )
    # ── 全市场历史资金流向日 K
    MARKET_FLOW_HISTORY_URL = (
        "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
    )
    # ── 个股实时资金流向（日内分时，10 分钟间隔）
    STOCK_FLOW_INTRADAY_URL = (
        "https://push2.eastmoney.com/api/qt/stock/fflow/kline/get"
    )

    # 字段映射：东方财富返回字段代号 → 中文含义
    STOCK_FIELDS_MAP = {
        "f12":  "股票代码",
        "f14":  "股票名称",
        "f2":   "最新价(元)",
        "f3":   "涨跌幅(%)",
        "f62":  "主力净流入(亿元)",        # 超大单+大单
        "f184": "主力净流入占比(%)",
        "f66":  "超大单净流入(亿元)",
        "f69":  "超大单净流入占比(%)",
        "f72":  "大单净流入(亿元)",
        "f75":  "大单净流入占比(%)",
        "f78":  "中单净流入(亿元)",
        "f81":  "中单净流入占比(%)",
        "f84":  "小单净流入(亿元)",
        "f87":  "小单净流入占比(%)",
        "f124": "时间戳",
        "f204": "所属行业",
        "f205": "行业代码",
    }

    # 行业/概念板块字段
    SECTOR_FIELDS_MAP = {
        "f12":  "板块代码",
        "f14":  "板块名称",
        "f2":   "最新价",
        "f3":   "涨跌幅(%)",
        "f62":  "主力净流入(亿元)",
        "f184": "主力净流入占比(%)",
        "f66":  "超大单净流入(亿元)",
        "f69":  "超大单净流入占比(%)",
        "f72":  "大单净流入(亿元)",
        "f75":  "大单净流入占比(%)",
        "f78":  "中单净流入(亿元)",
        "f81":  "中单净流入占比(%)",
        "f84":  "小单净流入(亿元)",
        "f87":  "小单净流入占比(%)",
        "f124": "时间戳",
    }

    # 历史日 K 字段（按索引顺序）
    # f51=日期, f52=主力净流入, f53=小单净流入, f54=中单净流入,
    # f55=大单净流入, f56=超大单净流入, f57=主力净流入占比,
    # f58=超大单净流入占比, f59=大单净流入占比,
    # f60=中单净流入占比,   f61=小单净流入占比
    HISTORY_FIELDS = [
        "日期", "主力净流入(亿元)", "小单净流入(亿元)", "中单净流入(亿元)",
        "大单净流入(亿元)", "超大单净流入(亿元)",
        "主力净流入占比(%)", "超大单净流入占比(%)", "大单净流入占比(%)",
        "中单净流入占比(%)", "小单净流入占比(%)"
    ]

    def __init__(
        self,
        output_dir: str = "output",
        proxies: Optional[Dict[str, str]] = None,
        request_interval: float = 0.5,
    ):
        """
        :param output_dir:        CSV 输出目录
        :param proxies:           代理配置
        :param request_interval:  每次请求之间的休眠秒数（防封 IP）
        """
        self.output_dir = output_dir
        self.session = create_session(proxies=proxies)
        self.interval = request_interval
        os.makedirs(output_dir, exist_ok=True)
        LOGGER.info(f"初始化完成 | 输出目录: {output_dir}")

    # ── 通用请求方法 ─────────────────────────────
    def _get(
        self,
        url: str,
        params: Dict[str, Any],
        timeout: int = 20,
    ) -> Optional[Dict]:
        """
        发起 GET 请求，返回解析后的 JSON 字典。
        遇到网络异常会自动重试（由 requests.Retry 处理），
        超过重试次数后记录错误并返回 None。
        """
        time.sleep(self.interval)
        try:
            LOGGER.debug(f"GET {url} | params={params}")
            resp = self.session.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            LOGGER.debug(f"响应长度: {len(resp.text)} 字节")
            return data
        except requests.exceptions.HTTPError as e:
            LOGGER.error(f"HTTP 错误: {e} | URL: {url}")
        except requests.exceptions.ConnectionError as e:
            LOGGER.error(f"连接错误: {e} | URL: {url}")
        except requests.exceptions.Timeout:
            LOGGER.error(f"请求超时 | URL: {url}")
        except ValueError as e:
            LOGGER.error(f"JSON 解析失败: {e} | URL: {url}")
        return None

    # ── 今日个股资金流向（全市场，分页） ──────────
    def fetch_stock_flow_today(
        self,
        market: str = "all",
        page_size: int = 100,
        max_pages: int = 50,
    ) -> pd.DataFrame:
        """
        抓取今日全市场个股资金流向排名（主力净流入由大到小）。

        :param market:    "all"（沪深两市）/ "sh"（沪市）/ "sz"（深市）
        :param page_size: 每页条数（最大 100）
        :param max_pages: 最大翻页数（防止无限循环）
        :return: DataFrame，单位已换算（亿元）
        """
        LOGGER.info(f"开始抓取今日个股资金流向 | 市场={market}")

        # 市场过滤参数
        market_fs = {
            "all": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048",
            "sh":  "m:1+t:2,m:1+t:23",
            "sz":  "m:0+t:6,m:0+t:80,m:0+t:81+s:2048",
        }.get(market, "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23")

        fields = ",".join(self.STOCK_FIELDS_MAP.keys())
        all_rows = []

        for page in range(1, max_pages + 1):
            params = {
                "pn":   page,
                "pz":   page_size,
                "po":   1,           # 降序（主力净流入最大在前）
                "np":   1,
                "ut":   "bd1d9195e7419940",
                "fltt": 2,
                "invt": 2,
                "fid":  "f62",       # 按主力净流入排序
                "fs":   market_fs,
                "fields": fields,
                "_":    int(time.time() * 1000),
            }
            data = self._get(self.STOCK_FLOW_TODAY_URL, params)
            if not data:
                LOGGER.warning(f"第 {page} 页请求失败，停止翻页")
                break

            diff = data.get("data", {}) or {}
            rows = diff.get("diff", [])
            if not rows:
                LOGGER.info(f"第 {page} 页无数据，翻页结束（共 {len(all_rows)} 条）")
                break

            all_rows.extend(rows)
            total = diff.get("total", 0)
            LOGGER.info(
                f"个股资金流向 | 第 {page} 页 | 本页 {len(rows)} 条 | 累计 {len(all_rows)}/{total}"
            )

            if len(all_rows) >= total:
                break

        if not all_rows:
            LOGGER.warning("未获取到任何个股资金流向数据")
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)
        df = self._process_stock_flow(df)
        LOGGER.info(f"今日个股资金流向抓取完成 | 共 {len(df)} 条")
        return df

    def _process_stock_flow(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理个股资金流向原始 DataFrame：字段重命名、单位转换、时间解析"""
        rename_map = {k: v for k, v in self.STOCK_FIELDS_MAP.items() if k in df.columns}
        df = df.rename(columns=rename_map)

        # 单位换算：元 → 亿元
        yi_cols = [
            "主力净流入(亿元)", "超大单净流入(亿元)", "大单净流入(亿元)",
            "中单净流入(亿元)", "小单净流入(亿元)"
        ]
        for col in yi_cols:
            if col in df.columns:
                df[col] = df[col].apply(yuan_to_yi)

        # 百分比字段：已是百分比，保留2位
        pct_cols = [c for c in df.columns if "占比" in c]
        for col in pct_cols:
            df[col] = df[col].apply(safe_float).apply(
                lambda x: round(x, 2) if x is not None else None
            )

        # 时间戳转字符串
        if "时间戳" in df.columns:
            df["数据时间"] = df["时间戳"].apply(timestamp_to_date)
            df.drop(columns=["时间戳"], inplace=True)
        else:
            df["数据时间"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 主力净流入排名
        if "主力净流入(亿元)" in df.columns:
            df = df.sort_values("主力净流入(亿元)", ascending=False).reset_index(drop=True)
            df.insert(0, "排名", df.index + 1)

        return df

    # ── 个股历史资金流向（日 K）────────────────────
    def fetch_stock_flow_history(
        self,
        stock_code: str,
        days: int = 60,
    ) -> pd.DataFrame:
        """
        抓取单只个股历史资金流向日 K 数据。

        :param stock_code: 股票代码（不含市场前缀，例如 "000001"）
        :param days:       历史天数（最近 N 个交易日，最大约 500）
        :return: DataFrame
        """
        # 自动识别市场（沪=1, 深=0）
        if stock_code.startswith("6"):
            secid = f"1.{stock_code}"
        else:
            secid = f"0.{stock_code}"

        LOGGER.info(f"抓取个股历史资金流向 | 代码={stock_code} | 天数={days}")

        params = {
            "lmt":     days,
            "klt":     101,          # 101=日K
            "secid":   secid,
            "fields1": "f1,f2,f3,f7",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "ut":      "b2884a393a59ad64002292a3e90d46a5",
            "_":       int(time.time() * 1000),
        }

        data = self._get(self.STOCK_FLOW_HISTORY_URL, params)
        if not data:
            return pd.DataFrame()

        klines = (data.get("data") or {}).get("klines", [])
        if not klines:
            LOGGER.warning(f"未获取到个股 {stock_code} 历史资金流向")
            return pd.DataFrame()

        rows = [k.split(",") for k in klines]
        df = pd.DataFrame(rows, columns=self.HISTORY_FIELDS)

        # 单位换算（原始单位：元）
        yi_cols = [c for c in df.columns if "亿元" in c]
        for col in yi_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").apply(yuan_to_yi)

        # 百分比列转 float
        pct_cols = [c for c in df.columns if "占比" in c]
        for col in pct_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").apply(
                lambda x: round(x, 2) if pd.notna(x) else None
            )

        df.insert(0, "股票代码", stock_code)
        df["市场"] = "沪市" if stock_code.startswith("6") else "深市"
        LOGGER.info(f"个股 {stock_code} 历史资金流向 | 共 {len(df)} 条")
        return df

    # ── 行业板块资金流向（今日排名）────────────────
    def fetch_industry_flow_today(self) -> pd.DataFrame:
        """抓取今日行业板块资金流向排名（按主力净流入降序）"""
        LOGGER.info("开始抓取今日行业板块资金流向")
        return self._fetch_sector_flow(
            sector_type="industry",
            fs="m:90+t:2+f:!50",  # 申万行业
            label="行业",
        )

    # ── 概念板块资金流向（今日排名）────────────────
    def fetch_concept_flow_today(self) -> pd.DataFrame:
        """抓取今日概念板块资金流向排名（按主力净流入降序）"""
        LOGGER.info("开始抓取今日概念板块资金流向")
        return self._fetch_sector_flow(
            sector_type="concept",
            fs="m:90+t:3+f:!50",  # 概念板块
            label="概念",
        )

    def _fetch_sector_flow(
        self,
        sector_type: str,
        fs: str,
        label: str,
        page_size: int = 100,
    ) -> pd.DataFrame:
        """行业/概念板块资金流向通用抓取逻辑"""
        fields = ",".join(self.SECTOR_FIELDS_MAP.keys())
        params = {
            "pn":     1,
            "pz":     page_size,
            "po":     1,
            "np":     1,
            "ut":     "bd1d9195e7419940",
            "fltt":   2,
            "invt":   2,
            "fid":    "f62",
            "fs":     fs,
            "fields": fields,
            "_":      int(time.time() * 1000),
        }

        data = self._get(self.INDUSTRY_FLOW_URL, params)
        if not data:
            return pd.DataFrame()

        rows = (data.get("data") or {}).get("diff", [])
        if not rows:
            LOGGER.warning(f"未获取到{label}板块数据")
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        rename_map = {k: v for k, v in self.SECTOR_FIELDS_MAP.items() if k in df.columns}
        df = df.rename(columns=rename_map)

        yi_cols = [
            "主力净流入(亿元)", "超大单净流入(亿元)", "大单净流入(亿元)",
            "中单净流入(亿元)", "小单净流入(亿元)"
        ]
        for col in yi_cols:
            if col in df.columns:
                df[col] = df[col].apply(yuan_to_yi)

        pct_cols = [c for c in df.columns if "占比" in c]
        for col in pct_cols:
            df[col] = df[col].apply(safe_float).apply(
                lambda x: round(x, 2) if x is not None else None
            )

        if "时间戳" in df.columns:
            df["数据时间"] = df["时间戳"].apply(timestamp_to_date)
            df.drop(columns=["时间戳"], inplace=True)
        else:
            df["数据时间"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        df.insert(0, "板块类型", label)
        df = df.sort_values("主力净流入(亿元)", ascending=False).reset_index(drop=True)
        df.insert(0, "排名", df.index + 1)

        LOGGER.info(f"{label}板块资金流向 | 共 {len(df)} 条")
        return df

    # ── 个股日内资金流向（分时，10 分钟粒度）──────
    def fetch_stock_intraday_flow(self, stock_code: str) -> pd.DataFrame:
        """
        抓取个股当日日内资金流向（10 分钟分时数据）。

        :param stock_code: 股票代码，例如 "000001"
        :return: DataFrame
        """
        secid = f"1.{stock_code}" if stock_code.startswith("6") else f"0.{stock_code}"
        LOGGER.info(f"抓取个股日内资金流向 | 代码={stock_code}")

        params = {
            "lmt":     0,
            "klt":     1,             # 1=1分钟，10=10分钟
            "secid":   secid,
            "fields1": "f1,f2,f3,f7",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "ut":      "b2884a393a59ad64002292a3e90d46a5",
            "_":       int(time.time() * 1000),
        }

        data = self._get(self.STOCK_FLOW_INTRADAY_URL, params)
        if not data:
            return pd.DataFrame()

        klines = (data.get("data") or {}).get("klines", [])
        if not klines:
            LOGGER.warning(f"未获取到个股 {stock_code} 日内分时数据")
            return pd.DataFrame()

        intraday_fields = [
            "时间", "主力净流入(亿元)", "小单净流入(亿元)", "中单净流入(亿元)",
            "大单净流入(亿元)", "超大单净流入(亿元)",
            "主力净流入占比(%)", "超大单净流入占比(%)", "大单净流入占比(%)",
            "中单净流入占比(%)", "小单净流入占比(%)"
        ]

        rows = [k.split(",") for k in klines]
        df = pd.DataFrame(rows, columns=intraday_fields)

        yi_cols = [c for c in df.columns if "亿元" in c]
        for col in yi_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").apply(yuan_to_yi)

        pct_cols = [c for c in df.columns if "占比" in c]
        for col in pct_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").apply(
                lambda x: round(x, 2) if pd.notna(x) else None
            )

        df.insert(0, "股票代码", stock_code)
        LOGGER.info(f"个股 {stock_code} 日内资金流向 | 共 {len(df)} 条（分钟 K）")
        return df

    # ── 全市场历史资金流向日 K ─────────────────────
    def fetch_market_flow_history(self, days: int = 90) -> pd.DataFrame:
        """
        抓取全市场（上证综指 000001）历史资金流向日 K，
        反映整体市场的主力/散户资金动向。

        :param days: 天数
        :return: DataFrame
        """
        LOGGER.info(f"抓取全市场历史资金流向 | 天数={days}")
        # 用上证综指代替全市场资金流向指标（东方财富无全市场直接合计接口）
        df = self.fetch_stock_flow_history("000001", days=days)
        if not df.empty:
            df["标的"] = "上证综指(000001)"
        return df

    # ── 批量个股历史资金流向 ──────────────────────
    def fetch_batch_stock_history(
        self,
        stock_codes: List[str],
        days: int = 60,
    ) -> pd.DataFrame:
        """
        批量抓取多只个股历史资金流向，合并为一个 DataFrame。

        :param stock_codes: 股票代码列表
        :param days:        历史天数
        :return: 合并后的 DataFrame
        """
        LOGGER.info(f"批量抓取个股历史资金流向 | 共 {len(stock_codes)} 只 | 天数={days}")
        dfs = []
        for i, code in enumerate(stock_codes, 1):
            LOGGER.info(f"[{i}/{len(stock_codes)}] 抓取 {code}")
            df = self.fetch_stock_flow_history(code, days=days)
            if not df.empty:
                dfs.append(df)
        if not dfs:
            return pd.DataFrame()
        return pd.concat(dfs, ignore_index=True)

    # ── 数据落盘工具 ──────────────────────────────
    def save_csv(self, df: pd.DataFrame, filename: str) -> str:
        """
        将 DataFrame 保存为 CSV 文件（UTF-8 with BOM，Excel 可直接打开）。

        :param df:       待保存的 DataFrame
        :param filename: 文件名（不含路径）
        :return: 完整文件路径
        """
        if df.empty:
            LOGGER.warning(f"DataFrame 为空，跳过保存: {filename}")
            return ""

        filepath = os.path.join(self.output_dir, filename)
        df.to_csv(filepath, index=False, encoding="utf-8-sig")
        LOGGER.info(f"已保存 CSV | 路径: {filepath} | 行数: {len(df)}")
        return filepath

    # ── 每日更新模式 ──────────────────────────────
    def run_daily_update(self) -> Dict[str, str]:
        """
        每日更新模式：抓取今日所有资金流向数据并保存。
        返回所有输出文件路径的字典。
        """
        today = datetime.now().strftime("%Y%m%d")
        saved = {}

        LOGGER.info(f"========== 每日更新模式 | 日期={today} ==========")

        # 1. 全市场个股资金流向
        df_stocks = self.fetch_stock_flow_today()
        if not df_stocks.empty:
            saved["个股资金流向"] = self.save_csv(
                df_stocks, f"stock_flow_{today}.csv"
            )

        # 2. 行业板块资金流向
        df_industry = self.fetch_industry_flow_today()
        if not df_industry.empty:
            saved["行业资金流向"] = self.save_csv(
                df_industry, f"industry_flow_{today}.csv"
            )

        # 3. 概念板块资金流向
        df_concept = self.fetch_concept_flow_today()
        if not df_concept.empty:
            saved["概念资金流向"] = self.save_csv(
                df_concept, f"concept_flow_{today}.csv"
            )

        LOGGER.info(f"每日更新完成 | 输出文件: {list(saved.values())}")
        return saved

    # ── 历史数据拉取模式 ──────────────────────────
    def run_history_fetch(
        self,
        days: int = 60,
        stock_codes: Optional[List[str]] = None,
    ) -> Dict[str, str]:
        """
        历史数据拉取模式：抓取指定天数的历史资金流向。

        :param days:        历史天数
        :param stock_codes: 指定个股列表（None 时使用沪深300成分股前30只示例）
        :return: 输出文件路径字典
        """
        saved = {}
        ts = datetime.now().strftime("%Y%m%d_%H%M")

        LOGGER.info(f"========== 历史数据拉取模式 | 天数={days} ==========")

        # 默认示例股票池（沪深300部分成分股）
        if stock_codes is None:
            stock_codes = [
                "000001",  # 平安银行
                "000002",  # 万科A
                "000858",  # 五粮液
                "600000",  # 浦发银行
                "600036",  # 招商银行
                "600519",  # 贵州茅台
                "601318",  # 中国平安
                "601398",  # 工商银行
                "000725",  # 京东方A
                "002415",  # 海康威视
            ]

        # 1. 批量个股历史资金流向
        df_batch = self.fetch_batch_stock_history(stock_codes, days=days)
        if not df_batch.empty:
            saved["个股历史资金流向"] = self.save_csv(
                df_batch, f"stock_flow_history_{ts}.csv"
            )

        # 2. 全市场历史资金流向（上证综指）
        df_market = self.fetch_market_flow_history(days=days)
        if not df_market.empty:
            saved["全市场历史资金流向"] = self.save_csv(
                df_market, f"market_flow_history_{ts}.csv"
            )

        LOGGER.info(f"历史数据拉取完成 | 输出文件: {list(saved.values())}")
        return saved

    # ── 单只个股完整分析模式 ─────────────────────
    def run_stock_analysis(self, stock_code: str, days: int = 60) -> Dict[str, str]:
        """
        对单只个股进行完整资金流向分析（历史日 K + 日内分时）。

        :param stock_code: 股票代码
        :param days:       历史天数
        :return: 输出文件路径字典
        """
        saved = {}
        ts = datetime.now().strftime("%Y%m%d_%H%M")

        LOGGER.info(f"========== 个股分析模式 | 代码={stock_code} ==========")

        # 历史日K
        df_hist = self.fetch_stock_flow_history(stock_code, days=days)
        if not df_hist.empty:
            saved["历史资金流向"] = self.save_csv(
                df_hist, f"stock_{stock_code}_history_{ts}.csv"
            )

        # 日内分时
        df_intra = self.fetch_stock_intraday_flow(stock_code)
        if not df_intra.empty:
            saved["日内分时资金流向"] = self.save_csv(
                df_intra, f"stock_{stock_code}_intraday_{ts}.csv"
            )

        return saved


# ─────────────────────────────────────────────
# 同花顺备用抓取器（仅行业资金流向）
# ─────────────────────────────────────────────
class TongHuaShunFundFlow:
    """
    同花顺备用数据源，用于补充行业资金流向数据。
    注意：同花顺接口稳定性略低于东方财富，作为兜底方案。
    """

    INDUSTRY_URL = "https://d.10jqka.com.cn/v4/line/bk_hs/kline/day.js"
    RANK_URL = "https://data.10jqka.com.cn/funds/hs/field/fund_flow/order/desc/page/1/ajax/1/free/1/"

    def __init__(self, proxies: Optional[Dict[str, str]] = None):
        self.session = create_session(proxies=proxies)
        # 同花顺需要额外的请求头
        self.session.headers.update({
            "Referer": "https://data.10jqka.com.cn/",
            "hexin-v": "",   # 实际使用时可能需要填写 Cookie 中的 hexin-v 值
        })

    def fetch_industry_flow(self) -> pd.DataFrame:
        """
        抓取同花顺行业资金流向排名。
        返回 DataFrame（与东方财富格式尽量对齐）。
        """
        LOGGER.info("同花顺：抓取行业资金流向排名")
        try:
            resp = self.session.get(self.RANK_URL, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            LOGGER.error(f"同花顺请求失败: {e}")
            return pd.DataFrame()

        items = data.get("data", {}).get("list", [])
        if not items:
            return pd.DataFrame()

        rows = []
        for item in items:
            rows.append({
                "板块名称": item.get("name", ""),
                "涨跌幅(%)": safe_float(item.get("avg_price")),
                "主力净流入(亿元)": yuan_to_yi(item.get("fund_flow")),
                "主力净流入占比(%)": safe_float(item.get("fund_flow_ratio")),
                "数据来源": "同花顺",
            })

        df = pd.DataFrame(rows)
        LOGGER.info(f"同花顺行业资金流向 | 共 {len(df)} 条")
        return df


# ─────────────────────────────────────────────
# 命令行入口
# ─────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="A股市场资金流向数据抓取工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  # 每日更新（今日数据）
  python a_share_fund_flow.py --mode daily

  # 历史模式：最近 30 个交易日
  python a_share_fund_flow.py --mode history --days 30

  # 历史模式：指定起始日期（脚本内部计算天数）
  python a_share_fund_flow.py --mode history --start 2024-01-01

  # 单只个股完整分析
  python a_share_fund_flow.py --mode stock --code 600519

  # 批量个股
  python a_share_fund_flow.py --mode history --codes 000001,600519,002415 --days 60

  # 使用代理
  python a_share_fund_flow.py --mode daily --proxy http://127.0.0.1:7890
        """
    )

    parser.add_argument(
        "--mode",
        choices=["daily", "history", "stock"],
        default="daily",
        help="运行模式：daily=每日更新, history=历史拉取, stock=单股分析",
    )
    parser.add_argument("--days",  type=int, default=60, help="历史天数（history模式）")
    parser.add_argument("--start", type=str, default=None,
                        help="历史起始日期 YYYY-MM-DD（history模式，优先于--days）")
    parser.add_argument("--code",  type=str, default=None, help="个股代码（stock模式）")
    parser.add_argument("--codes", type=str, default=None,
                        help="批量个股代码，逗号分隔（history模式）")
    parser.add_argument("--output", type=str, default="output", help="输出目录")
    parser.add_argument("--proxy",  type=str, default=None,
                        help="HTTP/HTTPS 代理地址，例如 http://127.0.0.1:7890")
    parser.add_argument("--interval", type=float, default=0.5,
                        help="请求间隔秒数（防封IP，默认0.5）")

    return parser.parse_args()


def main():
    args = parse_args()

    # 代理配置
    proxies = None
    if args.proxy:
        proxies = {"http": args.proxy, "https": args.proxy}

    # 初始化抓取器
    scraper = EastMoneyFundFlow(
        output_dir=args.output,
        proxies=proxies,
        request_interval=args.interval,
    )

    # 历史天数计算（--start 优先）
    days = args.days
    if args.start:
        try:
            start_d = datetime.strptime(args.start, "%Y-%m-%d")
            days = max(1, (datetime.now() - start_d).days + 1)
            LOGGER.info(f"起始日期={args.start}，折算天数={days}")
        except ValueError:
            LOGGER.error(f"--start 日期格式错误: {args.start}，请使用 YYYY-MM-DD")
            sys.exit(1)

    # 执行对应模式
    if args.mode == "daily":
        results = scraper.run_daily_update()

    elif args.mode == "history":
        codes = None
        if args.codes:
            codes = [c.strip() for c in args.codes.split(",") if c.strip()]
        results = scraper.run_history_fetch(days=days, stock_codes=codes)

    elif args.mode == "stock":
        if not args.code:
            LOGGER.error("stock 模式必须指定 --code 参数")
            sys.exit(1)
        results = scraper.run_stock_analysis(args.code, days=days)

    else:
        LOGGER.error(f"未知模式: {args.mode}")
        sys.exit(1)

    # 打印输出汇总
    print("\n" + "=" * 55)
    print("  输出文件汇总")
    print("=" * 55)
    for name, path in results.items():
        print(f"  [{name}] → {path}")
    print("=" * 55)


if __name__ == "__main__":
    main()
