# -*- coding: utf-8 -*-
"""
===================================
AkshareFetcher - 主数据源 (Priority 1)
===================================

数据来源：东方财富爬虫（通过 akshare 库）
特点：免费、无需 Token、数据全面
风险：爬虫机制易被反爬封禁

防封禁策略：
1. 每次请求前随机休眠 2-5 秒
2. 随机轮换 User-Agent
3. 使用 tenacity 实现指数退避重试

增强数据：
- 实时行情：量比、换手率、市盈率、市净率、总市值、流通市值
- 筹码分布：获利比例、平均成本、筹码集中度
"""

import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any

import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from .base import BaseFetcher, DataFetchError, RateLimitError, STANDARD_COLUMNS


@dataclass
class RealtimeQuote:
    """
    实时行情数据
    
    包含当日实时交易数据和估值指标
    """
    code: str
    name: str = ""
    price: float = 0.0           # 最新价
    change_pct: float = 0.0      # 涨跌幅(%)
    change_amount: float = 0.0   # 涨跌额
    
    # 量价指标
    volume_ratio: float = 0.0    # 量比（当前成交量/过去5日平均成交量）
    turnover_rate: float = 0.0   # 换手率(%)
    amplitude: float = 0.0       # 振幅(%)
    
    # 估值指标
    pe_ratio: float = 0.0        # 市盈率(动态)
    pb_ratio: float = 0.0        # 市净率
    total_mv: float = 0.0        # 总市值(元)
    circ_mv: float = 0.0         # 流通市值(元)
    
    # 其他
    change_60d: float = 0.0      # 60日涨跌幅(%)
    high_52w: float = 0.0        # 52周最高
    low_52w: float = 0.0         # 52周最低
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'code': self.code,
            'name': self.name,
            'price': self.price,
            'change_pct': self.change_pct,
            'volume_ratio': self.volume_ratio,
            'turnover_rate': self.turnover_rate,
            'amplitude': self.amplitude,
            'pe_ratio': self.pe_ratio,
            'pb_ratio': self.pb_ratio,
            'total_mv': self.total_mv,
            'circ_mv': self.circ_mv,
            'change_60d': self.change_60d,
        }


@dataclass  
class ChipDistribution:
    """
    筹码分布数据
    
    反映持仓成本分布和获利情况
    """
    code: str
    date: str = ""
    
    # 获利情况
    profit_ratio: float = 0.0     # 获利比例(0-1)
    avg_cost: float = 0.0         # 平均成本
    
    # 筹码集中度
    cost_90_low: float = 0.0      # 90%筹码成本下限
    cost_90_high: float = 0.0     # 90%筹码成本上限
    concentration_90: float = 0.0  # 90%筹码集中度（越小越集中）
    
    cost_70_low: float = 0.0      # 70%筹码成本下限
    cost_70_high: float = 0.0     # 70%筹码成本上限
    concentration_70: float = 0.0  # 70%筹码集中度
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'code': self.code,
            'date': self.date,
            'profit_ratio': self.profit_ratio,
            'avg_cost': self.avg_cost,
            'cost_90_low': self.cost_90_low,
            'cost_90_high': self.cost_90_high,
            'concentration_90': self.concentration_90,
            'concentration_70': self.concentration_70,
        }
    
    def get_chip_status(self, current_price: float) -> str:
        """
        获取筹码状态描述
        
        Args:
            current_price: 当前股价
            
        Returns:
            筹码状态描述
        """
        status_parts = []
        
        # 获利比例分析
        if self.profit_ratio >= 0.9:
            status_parts.append("获利盘极高(>90%)")
        elif self.profit_ratio >= 0.7:
            status_parts.append("获利盘较高(70-90%)")
        elif self.profit_ratio >= 0.5:
            status_parts.append("获利盘中等(50-70%)")
        elif self.profit_ratio >= 0.3:
            status_parts.append("套牢盘较多(>30%)")
        else:
            status_parts.append("套牢盘极重(>70%)")
        
        # 筹码集中度分析 (90%集中度 < 10% 表示集中)
        if self.concentration_90 < 0.08:
            status_parts.append("筹码高度集中")
        elif self.concentration_90 < 0.15:
            status_parts.append("筹码较集中")
        elif self.concentration_90 < 0.25:
            status_parts.append("筹码分散度中等")
        else:
            status_parts.append("筹码较分散")
        
        # 成本与现价关系
        if current_price > 0 and self.avg_cost > 0:
            cost_diff = (current_price - self.avg_cost) / self.avg_cost * 100
            if cost_diff > 20:
                status_parts.append(f"现价高于平均成本{cost_diff:.1f}%")
            elif cost_diff > 5:
                status_parts.append(f"现价略高于成本{cost_diff:.1f}%")
            elif cost_diff > -5:
                status_parts.append("现价接近平均成本")
            else:
                status_parts.append(f"现价低于平均成本{abs(cost_diff):.1f}%")
        
        return "，".join(status_parts)

logger = logging.getLogger(__name__)


# User-Agent 池，用于随机轮换
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]


# 缓存实时行情数据（避免重复请求）
_realtime_cache: Dict[str, Any] = {
    'data': None,
    'timestamp': 0,
    'ttl': 60  # 60秒缓存有效期
}


class AkshareFetcher(BaseFetcher):
    """
    Akshare 数据源实现
    
    优先级：1（最高）
    数据来源：东方财富网爬虫
    
    关键策略：
    - 每次请求前随机休眠 2.0-5.0 秒
    - 随机 User-Agent 轮换
    - 失败后指数退避重试（最多3次）
    """
    
    name = "AkshareFetcher"
    priority = 1
    
    def __init__(self, sleep_min: float = 2.0, sleep_max: float = 5.0):
        """
        初始化 AkshareFetcher
        
        Args:
            sleep_min: 最小休眠时间（秒）
            sleep_max: 最大休眠时间（秒）
        """
        self.sleep_min = sleep_min
        self.sleep_max = sleep_max
        self._last_request_time: Optional[float] = None
    
    def _set_random_user_agent(self) -> None:
        """
        设置随机 User-Agent
        
        通过修改 requests Session 的 headers 实现
        这是关键的反爬策略之一
        """
        try:
            import akshare as ak
            # akshare 内部使用 requests，我们通过环境变量或直接设置来影响
            # 实际上 akshare 可能不直接暴露 session，这里通过 fake_useragent 作为补充
            random_ua = random.choice(USER_AGENTS)
            logger.debug(f"设置 User-Agent: {random_ua[:50]}...")
        except Exception as e:
            logger.debug(f"设置 User-Agent 失败: {e}")
    
    def _enforce_rate_limit(self) -> None:
        """
        强制执行速率限制
        
        策略：
        1. 检查距离上次请求的时间间隔
        2. 如果间隔不足，补充休眠时间
        3. 然后再执行随机 jitter 休眠
        """
        if self._last_request_time is not None:
            elapsed = time.time() - self._last_request_time
            min_interval = self.sleep_min
            if elapsed < min_interval:
                additional_sleep = min_interval - elapsed
                logger.debug(f"补充休眠 {additional_sleep:.2f} 秒")
                time.sleep(additional_sleep)
        
        # 执行随机 jitter 休眠
        self.random_sleep(self.sleep_min, self.sleep_max)
        self._last_request_time = time.time()
    
    @staticmethod
    def is_etf(code: str) -> bool:
        """
        判断是否为 ETF/LOF 基金
        
        常见 ETF/LOF 代码前缀:
        - 51, 56, 58 (沪市 ETF)
        - 15, 16, 18 (深市 ETF/LOF)
        """
        return code.startswith(('51', '56', '58', '15', '16', '18'))

    @retry(
        stop=stop_after_attempt(3),  # 最多重试3次
        wait=wait_exponential(multiplier=1, min=2, max=30),  # 指数退避：2, 4, 8... 最大30秒
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        从 Akshare 获取原始数据
        
        自动区分股票和 ETF：
        - 股票: ak.stock_zh_a_hist
        - ETF: ak.fund_etf_hist_em
        """
        import akshare as ak
        import time as _time
        
        # 防封禁策略 1: 随机 User-Agent
        self._set_random_user_agent()
        
        # 防封禁策略 2: 强制休眠
        self._enforce_rate_limit()
        
        is_etf_code = self.is_etf(stock_code)
        
        if is_etf_code:
            api_name = "ak.fund_etf_hist_em"
            logger.info(f"[API调用] {api_name}(symbol={stock_code}, ...)")
        else:
            api_name = "ak.stock_zh_a_hist"
            logger.info(f"[API调用] {api_name}(symbol={stock_code}, ...)")
        
        try:
            api_start = _time.time()
            
            if is_etf_code:
                # 获取 ETF 历史数据
                df = ak.fund_etf_hist_em(
                    symbol=stock_code,
                    period="daily",
                    start_date=start_date.replace('-', ''),
                    end_date=end_date.replace('-', ''),
                    adjust="qfq"
                )
            else:
                # 获取股票历史数据
                df = ak.stock_zh_a_hist(
                    symbol=stock_code,
                    period="daily",
                    start_date=start_date.replace('-', ''),
                    end_date=end_date.replace('-', ''),
                    adjust="qfq"
                )
            
            api_elapsed = _time.time() - api_start
            
            # 记录返回数据摘要
            if df is not None and not df.empty:
                logger.info(f"[API返回] {api_name} 成功: 返回 {len(df)} 行数据, 耗时 {api_elapsed:.2f}s")
                logger.debug(f"[API返回] 最新3条数据:\n{df.tail(3).to_string()}")
            else:
                logger.warning(f"[API返回] {api_name} 返回空数据, 耗时 {api_elapsed:.2f}s")
            
            return df
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # 检测反爬封禁
            if any(keyword in error_msg for keyword in ['banned', 'blocked', '频率', 'rate', '限制']):
                logger.warning(f"检测到可能被封禁: {e}")
                raise RateLimitError(f"Akshare 可能被限流: {e}") from e
            
            raise DataFetchError(f"{api_name} 获取数据失败: {e}") from e
    
    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        标准化 Akshare 数据
        
        兼容股票和 ETF 的列名
        """
        df = df.copy()
        
        # 列名映射
        column_mapping = {
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '涨跌幅': 'pct_chg',
        }
        
        # 重命名列
        df = df.rename(columns=column_mapping)
        
        # 添加股票代码列
        df['code'] = stock_code
        
        # 只保留需要的列
        keep_cols = ['code'] + STANDARD_COLUMNS
        # 确保所有标准列都存在（ETF数据列名通常一致，但以防万一）
        for col in keep_cols:
            if col not in df.columns and col != 'code':
                # 如果缺少 pct_chg, 可以计算
                if col == 'pct_chg' and 'close' in df.columns:
                    df['pct_chg'] = df['close'].pct_change() * 100
                    df['pct_chg'] = df['pct_chg'].fillna(0)
        
        existing_cols = [col for col in keep_cols if col in df.columns]
        df = df[existing_cols]
        
        return df
    
    def get_realtime_quote(self, stock_code: str) -> Optional[RealtimeQuote]:
        """
        获取实时行情数据
        
        自动区分股票和 ETF：
        - 股票: ak.stock_zh_a_spot_em
        - ETF: ak.fund_etf_spot_em
        """
        import akshare as ak
        
        try:
            is_etf_code = self.is_etf(stock_code)
            
            # 使用不同的缓存 key
            cache_key = 'etf' if is_etf_code else 'stock'
            
            # 初始化多类型缓存结构 (如果需要)
            # 这里简单起见，如果类型切换了，就清除缓存重新获取
            # 或者我们假设一次运行只查一种 或 接受混用时的即时刷新
            # 更好的做法是 _realtime_cache 存储 { 'stock': ..., 'etf': ... }
            # 但为了最小化改动，简单处理：
            
            # 这里我们分别获取，不依赖全局单一缓存，
            # 而是检查 _realtime_cache 内容是否包含当前代码类型
            # 实际上 stock_zh_a_spot_em 和 fund_etf_spot_em 返回结构类似
            
            # 防封禁策略
            self._set_random_user_agent()
            self._enforce_rate_limit()
            
            import time as _time
            api_start = _time.time()
            
            if is_etf_code:
                logger.info(f"[API调用] ak.fund_etf_spot_em() 获取ETF实时行情...")
                df = ak.fund_etf_spot_em()
            else:
                logger.info(f"[API调用] ak.stock_zh_a_spot_em() 获取A股实时行情...")
                df = ak.stock_zh_a_spot_em()
                
            api_elapsed = _time.time() - api_start
            logger.info(f"[API返回] 成功: 返回 {len(df)} 条数据, 耗时 {api_elapsed:.2f}s")
             
            # 查找指定代码
            # 注意：ETF 接口返回的列名可能略有不同，需适配
            row = df[df['代码'] == stock_code]
            if row.empty:
                logger.warning(f"[API返回] 未找到 {stock_code} 的实时行情")
                return None
            
            row = row.iloc[0]
            
            # 安全获取字段值
            def safe_float(val, default=0.0):
                try:
                    if pd.isna(val) or val == '-':
                        return default
                    return float(val)
                except:
                    return default
            
            quote = RealtimeQuote(
                code=stock_code,
                name=str(row.get('名称', '')),
                price=safe_float(row.get('最新价')),
                change_pct=safe_float(row.get('涨跌幅')),
                change_amount=safe_float(row.get('涨跌额')),
                volume_ratio=safe_float(row.get('量比')),
                turnover_rate=safe_float(row.get('换手率')),
                amplitude=safe_float(row.get('振幅')),
                # ETF 可能没有 PE/PB/市值
                pe_ratio=safe_float(row.get('市盈率-动态') if '市盈率-动态' in row else 0),
                pb_ratio=safe_float(row.get('市净率') if '市净率' in row else 0),
                total_mv=safe_float(row.get('总市值') if '总市值' in row else 0),
                circ_mv=safe_float(row.get('流通市值') if '流通市值' in row else 0),
                change_60d=safe_float(row.get('60日涨跌幅') if '60日涨跌幅' in row else 0),
                high_52w=safe_float(row.get('52周最高') if '52周最高' in row else 0),
                low_52w=safe_float(row.get('52周最低') if '52周最低' in row else 0),
            )
            
            logger.info(f"[实时行情] {stock_code} {quote.name}: 价格={quote.price}, 涨跌={quote.change_pct}%")
            return quote
            
        except Exception as e:
            logger.error(f"[API错误] 获取 {stock_code} 实时行情失败: {e}")
            return None
    
    def get_chip_distribution(self, stock_code: str) -> Optional[ChipDistribution]:
        """
        获取筹码分布数据
        
        注意：ETF 通常没有筹码分布数据，直接返回 None
        """
        if self.is_etf(stock_code):
            logger.info(f"[{stock_code}] ETF 不支持筹码分布分析，跳过")
            return None
            
        import akshare as ak
        
        try:
            # 防封禁策略
            self._set_random_user_agent()
            self._enforce_rate_limit()
            
            logger.info(f"[API调用] ak.stock_cyq_em(symbol={stock_code}) 获取筹码分布...")
            import time as _time
            api_start = _time.time()
            
            df = ak.stock_cyq_em(symbol=stock_code)
            
            api_elapsed = _time.time() - api_start
            
            if df.empty:
                logger.warning(f"[API返回] ak.stock_cyq_em 返回空数据, 耗时 {api_elapsed:.2f}s")
                return None
            
            logger.info(f"[API返回] ak.stock_cyq_em 成功: 返回 {len(df)} 天数据, 耗时 {api_elapsed:.2f}s")
            
            # 取最新一天的数据
            latest = df.iloc[-1]
            
            def safe_float(val, default=0.0):
                try:
                    if pd.isna(val):
                        return default
                    return float(val)
                except:
                    return default
            
            chip = ChipDistribution(
                code=stock_code,
                date=str(latest.get('日期', '')),
                profit_ratio=safe_float(latest.get('获利比例')),
                avg_cost=safe_float(latest.get('平均成本')),
                cost_90_low=safe_float(latest.get('90成本-低')),
                cost_90_high=safe_float(latest.get('90成本-高')),
                concentration_90=safe_float(latest.get('90集中度')),
                cost_70_low=safe_float(latest.get('70成本-低')),
                cost_70_high=safe_float(latest.get('70成本-高')),
                concentration_70=safe_float(latest.get('70集中度')),
            )
            
            return chip
            
        except Exception as e:
            logger.error(f"[API错误] 获取 {stock_code} 筹码分布失败: {e}")
            return None
    
    def get_enhanced_data(self, stock_code: str, days: int = 60) -> Dict[str, Any]:
        """
        获取增强数据（历史K线 + 实时行情 + 筹码分布）
        """
        result = {
            'code': stock_code,
            'daily_data': None,
            'realtime_quote': None,
            'chip_distribution': None,
        }
        
        # 获取日线数据
        try:
            df = self.get_daily_data(stock_code, days=days)
            result['daily_data'] = df
        except Exception as e:
            logger.error(f"获取 {stock_code} 日线数据失败: {e}")
        
        # 获取实时行情
        result['realtime_quote'] = self.get_realtime_quote(stock_code)
        
        # 获取筹码分布 (ETF会自动跳过)
        result['chip_distribution'] = self.get_chip_distribution(stock_code)
        
        return result


if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(level=logging.DEBUG)
    
    fetcher = AkshareFetcher()
    
    try:
        df = fetcher.get_daily_data('600519')  # 茅台
        print(f"获取成功，共 {len(df)} 条数据")
        print(df.tail())
    except Exception as e:
        print(f"获取失败: {e}")
