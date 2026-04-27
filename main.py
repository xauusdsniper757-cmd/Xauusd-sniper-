import asyncio
import pandas as pd
import numpy as np
import os
import sqlite3
import requests
from datetime import datetime, timedelta
from collections import deque
from typing import Dict, List, Tuple, Optional
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes
import time
from io import BytesIO
import matplotlib.pyplot as plt

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOKEN = "8426852609:AAEMc_7MnqS09e0moRP1tc1DfmXwgZJwAHg"
TWELVE_KEY = "dfbecf637ca6480088d2b584eeaa2914"
DB_NAME =  "xauusd_sniper.db"



class NewsDirectionalFilter:
    def __init__(self, api_key, currencies=("USD",)):
        self.api_key = api_key
        self.currencies = currencies
        self.cache = []
        self.last_fetch = None

    def _clean_value(self, val):
        if val is None:
            return None
        s = str(val).replace('%', '').replace('K', '').replace('M', '').replace('B', '').strip()
        try:
            return float(s)
        except (ValueError, TypeError):
            return None

    def fetch(self):
        url = "https://api.twelvedata.com/economic_calendar"
        params = {
            "apikey": self.api_key,
            "country": ",".join(self.currencies)
        }

        try:
            r = requests.get(url, params=params, timeout=15)
            data = r.json()

            events = []
            now = datetime.utcnow()

            for item in data.get("values", []):
                if item.get("impact") == "High":
                    try:
                        dt = datetime.strptime(item["datetime"], "%Y-%m-%d %H:%M:%S")
                    except (ValueError, KeyError, TypeError):
                        continue

                    actual = self._clean_value(item.get("actual"))
                    forecast = self._clean_value(item.get("forecast"))

                    bias = None
                    if actual is not None and forecast is not None:
                        if actual > forecast:
                            bias = "BULL"
                        elif actual < forecast:
                            bias = "BEAR"

                    events.append({
                        "time": dt,
                        "event": item.get("event", ""),
                        "bias": bias
                    })

            self.cache = events
            self.last_fetch = now

        except Exception as e:
            logger.warning(f"News fetch failed: {e}")

    def get_bias(self, buffer_minutes=30):
        now = datetime.utcnow()

        if not self.last_fetch or (now - self.last_fetch > timedelta(minutes=15)):
            self.fetch()

        for event in self.cache:
            delta = abs((event["time"] - now).total_seconds()) / 60

            if delta <= buffer_minutes:
                return event["bias"], event

        return None, None


def fetch_data(interval="15min", outputsize=500, max_retries=3):
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": "XAU/USD",
        "interval": interval,
        "outputsize": outputsize,
        "apikey": TWELVE_KEY
    }
    
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=30)
            data = r.json()
            
            if "values" not in data:
                raise Exception(f"API Error: {data.get('message', 'Unknown')}")
            
            df = pd.DataFrame(data["values"])
            df["datetime"] = pd.to_datetime(df["datetime"])
            
            for col in ["open", "high", "low", "close"]:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            if "volume" in df.columns:
                df["volume"] = pd.to_numeric(df["volume"], errors='coerce').fillna(0)
            else:
                df["volume"] = 0
            
            df = df.dropna(subset=["open", "high", "low", "close"])
            df = df.iloc[::-1].reset_index(drop=True)
            return df
            
        except Exception as e:
            logger.warning(f"Fetch attempt {attempt+1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise Exception(f"Data fetch failed after {max_retries} attempts: {e}")


def calculate_ema(prices, period):
    return prices.ewm(span=period, adjust=False).mean()


def calculate_atr(high, low, close, period=14):
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()


def calculate_sma(prices, period):
    return prices.rolling(window=period).mean()


def add_indicators(df):
    df["ema50"] = calculate_ema(df["close"], 50)
    df["ema200"] = calculate_ema(df["close"], 200)
    df["ema20"] = calculate_ema(df["close"], 20)
    df["atr"] = calculate_atr(df["high"], df["low"], df["close"], 14)
    df["volume_sma20"] = calculate_sma(df["volume"], 20)
    df.dropna(inplace=True)
    return df


class LiquidityEngine:
    def __init__(self, lookback=20):
        self.lookback = lookback
    
    def detect_sweep(self, df):
        if len(df) < self.lookback + 2:
            return 0, None
        
        recent = df.iloc[-self.lookback-1:-1]
        recent_high = recent["high"].max()
        recent_low = recent["low"].min()
        
        curr = df.iloc[-1]
        curr_high = curr["high"]
        curr_low = curr["low"]
        curr_close = curr["close"]
        curr_open = curr["open"]
        
        if curr_high > recent_high and curr_close < recent_high and curr_close < curr_open:
            return -1, recent_high
        
        if curr_low < recent_low and curr_close > recent_low and curr_close > curr_open:
            return 1, recent_low
        
        return 0, None
    
    def detect_order_block(self, df, direction):
        if len(df) < 3:
            return None, None
        
        if direction == 1:
            for i in range(-3, -len(df), -1):
                c1 = df.iloc[i]
                if c1["close"] < c1["open"]:
                    return c1["low"], c1["high"]
        
        elif direction == -1:
            for i in range(-3, -len(df), -1):
                c1 = df.iloc[i]
                if c1["close"] > c1["open"]:
                    return c1["low"], c1["high"]
        
        return None, None
    
    def detect_fvg(self, df):
        if len(df) < 3:
            return 0, None, None
        
        c1 = df.iloc[-3]
        c3 = df.iloc[-1]
        
        if c3["low"] > c1["high"]:
            return 1, c1["high"], c3["low"]
        
        if c3["high"] < c1["low"]:
            return -1, c3["high"], c1["low"]
        
        return 0, None, None
    
    def analyze(self, df):
        sweep_dir, sweep_level = self.detect_sweep(df)
        
        result = {
            "valid": False,
            "direction": "HOLD",
            "sweep_level": None,
            "entry_zone": None,
            "entry_zone_high": None,
            "ob_low": None,
            "ob_high": None,
            "fvg_dir": 0,
            "fvg_bottom": None,
            "fvg_top": None,
            "setup_type": None,
            "wick_ratio": 0.0,
            "volume_ratio": 0.0
        }
        
        if sweep_dir == 0 or sweep_level is None:
            return result
        
        curr = df.iloc[-1]
        candle_range = curr["high"] - curr["low"]
        if candle_range > 0:
            if sweep_dir == -1:
                result["wick_ratio"] = (curr["high"] - curr["close"]) / candle_range
            else:
                result["wick_ratio"] = (curr["close"] - curr["low"]) / candle_range
        
        vol_sma = df["volume_sma20"].iloc[-1] if "volume_sma20" in df.columns and len(df) >= 20 else 1
        if vol_sma > 0:
            result["volume_ratio"] = curr["volume"] / vol_sma
        
        ob_low, ob_high = self.detect_order_block(df, sweep_dir)
        fvg_dir, fvg_bottom, fvg_top = self.detect_fvg(df)
        
        entry_zone = None
        entry_zone_high = None
        setup_type = None
        
        if ob_low is not None and ob_high is not None:
            entry_zone = ob_low
            entry_zone_high = ob_high
            setup_type = "OB"
        elif fvg_dir == sweep_dir and fvg_bottom is not None:
            entry_zone = fvg_bottom
            entry_zone_high = fvg_top
            setup_type = "FVG"
        else:
            atr = df["atr"].iloc[-1] if "atr" in df.columns else df["close"].iloc[-1] * 0.001
            if sweep_dir == 1:
                entry_zone = sweep_level + atr * 0.3
                entry_zone_high = sweep_level + atr * 0.8
            else:
                entry_zone = sweep_level - atr * 0.3
                entry_zone_high = sweep_level - atr * 0.8
            setup_type = "SWEEP"
        
        result.update({
            "valid": True,
            "direction": "BUY" if sweep_dir == 1 else "SELL",
            "sweep_level": sweep_level,
            "entry_zone": entry_zone,
            "entry_zone_high": entry_zone_high,
            "ob_low": ob_low,
            "ob_high": ob_high,
            "fvg_dir": fvg_dir,
            "fvg_bottom": fvg_bottom,
            "fvg_top": fvg_top,
            "setup_type": setup_type
        })
        
        return result


class StructureAnalyzer:
    def __init__(self, swing_lookback=5):
        self.swing_lookback = swing_lookback
    
    def find_swing_highs_lows(self, df):
        if len(df) < self.swing_lookback * 2 + 1:
            return [], []
        
        highs = df["high"].values
        lows = df["low"].values
        
        swing_highs = []
        swing_lows = []
        
        for i in range(self.swing_lookback, len(df) - self.swing_lookback):
            window_high = highs[i-self.swing_lookback:i+self.swing_lookback+1]
            window_low = lows[i-self.swing_lookback:i+self.swing_lookback+1]
            
            if highs[i] == max(window_high):
                swing_highs.append((i, highs[i]))
            if lows[i] == min(window_low):
                swing_lows.append((i, lows[i]))
        
        return swing_highs, swing_lows
    
    def detect_bos_choch(self, df):
        if len(df) < 20:
            return {"bos": 0, "choch": 0, "last_sh": None, "last_sl": None}
        
        swing_highs, swing_lows = self.find_swing_highs_lows(df)
        
        if len(swing_highs) < 2 or len(swing_lows) < 2:
            return {"bos": 0, "choch": 0, "last_sh": None, "last_sl": None}
        
        last_sh_idx, last_sh_price = swing_highs[-1]
        prev_sh_idx, prev_sh_price = swing_highs[-2]
        last_sl_idx, last_sl_price = swing_lows[-1]
        prev_sl_idx, prev_sl_price = swing_lows[-2]
        
        current_close = df["close"].iloc[-1]
        
        bos = 0
        choch = 0
        
        if current_close > last_sh_price and last_sh_idx > prev_sh_idx:
            bos = 1
        elif current_close < last_sl_price and last_sl_idx > prev_sl_idx:
            bos = -1
        
        if len(swing_highs) >= 3 and len(swing_lows) >= 3:
            if current_close > last_sh_price and last_sh_price < prev_sh_price:
                choch = 1
            elif current_close < last_sl_price and last_sl_price > prev_sl_price:
                choch = -1
        
        return {
            "bos": bos,
            "choch": choch,
            "last_sh": last_sh_price,
            "last_sl": last_sl_price,
            "swing_highs": swing_highs,
            "swing_lows": swing_lows
        }


class KillzoneFilter:
    def __init__(self):
        self.london_open = (8, 11)
        self.ny_open = (13, 16)
        self.london_close = (16, 17)
        self.asian = (0, 7)
    
    def get_session_weight(self, hour):
        if self.london_open[0] <= hour < self.london_open[1]:
            return 1.3, "London Open"
        elif self.ny_open[0] <= hour < self.ny_open[1]:
            return 1.3, "NY Open"
        elif self.london_close[0] <= hour < self.london_close[1]:
            return 0.9, "London Close"
        elif self.asian[0] <= hour < self.asian[1]:
            return 0.5, "Asian"
        else:
            return 1.0, "Standard"
    
    def is_tradeable(self, hour):
        weight, _ = self.get_session_weight(hour)
        return weight >= 0.9


class MultiTimeframeAnalyzer:
    def __init__(self):
        self.liq_1h = LiquidityEngine(lookback=20)
        self.liq_4h = LiquidityEngine(lookback=20)
        self.struct_1h = StructureAnalyzer(swing_lookback=5)
        self.struct_4h = StructureAnalyzer(swing_lookback=5)
    
    def analyze(self, df_15m, df_1h, df_4h):
        result = {
            "htf_aligned": False,
            "sweep_1h_dir": "HOLD",
            "sweep_4h_dir": "HOLD",
            "bos_1h": 0,
            "bos_4h": 0,
            "trend_4h": 0,
            "alignment_score": 0.0
        }
        
        if len(df_1h) >= 50:
            liq_1h = self.liq_1h.analyze(df_1h)
            struct_1h = self.struct_1h.detect_bos_choch(df_1h)
            result["sweep_1h_dir"] = liq_1h.get("direction", "HOLD")
            result["bos_1h"] = struct_1h["bos"]
        
        if len(df_4h) >= 50:
            liq_4h = self.liq_4h.analyze(df_4h)
            struct_4h = self.struct_4h.detect_bos_choch(df_4h)
            result["sweep_4h_dir"] = liq_4h.get("direction", "HOLD")
            result["bos_4h"] = struct_4h["bos"]
            
            ema50_4h = df_4h["ema50"].iloc[-1] if "ema50" in df_4h.columns else df_4h["close"].iloc[-1]
            ema200_4h = df_4h["ema200"].iloc[-1] if "ema200" in df_4h.columns else df_4h["close"].iloc[-1]
            result["trend_4h"] = 1 if ema50_4h > ema200_4h else (-1 if ema50_4h < ema200_4h else 0)
        
        return result
    
    def is_aligned(self, mtf_result, direction_15m):
        dir_map = {"BUY": 1, "SELL": -1, "HOLD": 0}
        dir_15m = dir_map.get(direction_15m, 0)
        dir_1h = dir_map.get(mtf_result["sweep_1h_dir"], 0)
        dir_4h = dir_map.get(mtf_result["sweep_4h_dir"], 0)
        trend_4h = mtf_result["trend_4h"]
        
        if dir_15m == 0:
            return False
        
        aligned_count = 0
        total_checks = 0
        
        if dir_1h != 0:
            total_checks += 1
            if dir_1h == dir_15m:
                aligned_count += 1
        
        if trend_4h != 0:
            total_checks += 1
            if trend_4h == dir_15m:
                aligned_count += 1
        
        if dir_4h != 0:
            total_checks += 1
            if dir_4h == dir_15m:
                aligned_count += 1
        
        if total_checks == 0:
            return True
        
        alignment_ratio = aligned_count / total_checks
        mtf_result["alignment_score"] = alignment_ratio
        
        return alignment_ratio >= 0.5


class ConfidenceModel:
    def __init__(self):
        self.killzone = KillzoneFilter()
    
    def calculate(self, df, liquidity, mtf_result=None, structure=None, news_bias=None):
        if not liquidity["valid"]:
            return 0.0
        
        confidence = 0.5
        
        price = df["close"].iloc[-1]
        ema50 = df["ema50"].iloc[-1] if "ema50" in df.columns else price
        ema200 = df["ema200"].iloc[-1] if "ema200" in df.columns else price
        
        trend_bullish = ema50 > ema200
        trend_bearish = ema50 < ema200
        
        if liquidity["direction"] == "BUY" and trend_bullish:
            confidence += 0.15
        elif liquidity["direction"] == "SELL" and trend_bearish:
            confidence += 0.15
        elif liquidity["direction"] == "BUY" and trend_bearish:
            confidence -= 0.1
        elif liquidity["direction"] == "SELL" and trend_bullish:
            confidence -= 0.1
        
        atr = df["atr"].iloc[-1] if "atr" in df.columns else 0
        atr_pct = atr / price if price > 0 else 0
        if atr_pct > 0.002:
            confidence += 0.08
        
        if liquidity["setup_type"] == "OB":
            confidence += 0.15
        elif liquidity["setup_type"] == "FVG":
            confidence += 0.1
        
        wick_ratio = liquidity.get("wick_ratio", 0)
        if wick_ratio > 0.6:
            confidence += 0.12
        elif wick_ratio < 0.3:
            confidence -= 0.08
        
        vol_ratio = liquidity.get("volume_ratio", 0)
        if vol_ratio > 2.0:
            confidence += 0.08
        elif vol_ratio < 0.8:
            confidence -= 0.05
        
        if mtf_result and mtf_result.get("alignment_score", 0) > 0:
            alignment = mtf_result["alignment_score"]
            if alignment >= 0.66:
                confidence += 0.1
            elif alignment >= 0.5:
                confidence += 0.05
            else:
                confidence -= 0.1
        
        if structure:
            bos = structure.get("bos", 0)
            choch = structure.get("choch", 0)
            dir_map = {"BUY": 1, "SELL": -1}
            signal_dir = dir_map.get(liquidity["direction"], 0)
            
            if bos == signal_dir:
                confidence += 0.1
            if choch == signal_dir:
                confidence += 0.08
        
        current_hour = pd.to_datetime(df["datetime"].iloc[-1]).hour if "datetime" in df.columns else datetime.now().hour
        session_weight, session_name = self.killzone.get_session_weight(current_hour)
        
        if session_weight < 0.9:
            confidence -= 0.15
        elif session_weight >= 1.2:
            confidence += 0.05
        
        # News directional adjustment
        if news_bias and news_bias != "NEUTRAL":
            signal_dir = 1 if liquidity["direction"] == "BUY" else -1
            news_dir = 1 if news_bias == "BULL" else -1
            if signal_dir == news_dir:
                confidence += 0.05
            else:
                confidence -= 0.15
        
        return float(np.clip(confidence, 0.0, 1.0))


class RiskManager:
    def __init__(self, rr_tp1=2.5, rr_tp2=4.0):
        self.rr_tp1 = rr_tp1
        self.rr_tp2 = rr_tp2
        self.max_risk_atr_mult = 1.5
    
    def calculate_levels(self, direction, entry, sweep_level, atr):
        if direction == "BUY":
            sl = sweep_level - (atr * 0.5)
            if sl >= entry:
                sl = entry - (atr * 0.8)
            
            risk = entry - sl
            
            if risk > atr * self.max_risk_atr_mult:
                return None
            
            tp1 = entry + (risk * self.rr_tp1)
            tp2 = entry + (risk * self.rr_tp2)
        
        else:
            sl = sweep_level + (atr * 0.5)
            if sl <= entry:
                sl = entry + (atr * 0.8)
            
            risk = sl - entry
            
            if risk > atr * self.max_risk_atr_mult:
                return None
            
            tp1 = entry - (risk * self.rr_tp1)
            tp2 = entry - (risk * self.rr_tp2)
        
        return {
            "sl": round(sl, 2),
            "tp1": round(tp1, 2),
            "tp2": round(tp2, 2),
            "risk": round(abs(risk), 2),
            "rr1": self.rr_tp1,
            "rr2": self.rr_tp2
        }


class SignalGenerator:
    def __init__(self):
        self.liquidity = LiquidityEngine(lookback=20)
        self.confidence = ConfidenceModel()
        self.risk = RiskManager(rr_tp1=2.5, rr_tp2=4.0)
        self.mtf = MultiTimeframeAnalyzer()
        self.structure = StructureAnalyzer(swing_lookback=5)
        self.killzone = KillzoneFilter()
        self.news_filter = NewsDirectionalFilter(api_key=TWELVE_KEY, currencies=("USD",))
        self.min_confidence = 0.75
    
    def generate(self, df_15m, df_1h=None, df_4h=None):
        if len(df_15m) < 50:
            return {"signal": "HOLD", "reason": "Insufficient data"}
        
        liq = self.liquidity.analyze(df_15m)
        
        if not liq["valid"]:
            return {"signal": "HOLD", "reason": "No liquidity sweep detected"}
        
        current_hour = pd.to_datetime(df_15m["datetime"].iloc[-1]).hour if "datetime" in df_15m.columns else datetime.now().hour
        if not self.killzone.is_tradeable(current_hour):
            return {
                "signal": "HOLD",
                "reason": f"Outside tradeable session (hour {current_hour})",
                "confidence": 0,
                "liquidity": liq
            }
        
        # Check high-impact news directional bias
        news_bias, news_event = self.news_filter.get_bias(buffer_minutes=30)
        
        mtf_result = None
        if df_1h is not None and len(df_1h) >= 50 and df_4h is not None and len(df_4h) >= 50:
            mtf_result = self.mtf.analyze(df_15m, df_1h, df_4h)
            if not self.mtf.is_aligned(mtf_result, liq["direction"]):
                return {
                    "signal": "HOLD",
                    "reason": f"HTF not aligned (score: {mtf_result.get('alignment_score', 0):.1%})",
                    "confidence": 0,
                    "liquidity": liq,
                    "mtf": mtf_result,
                    "news_bias": news_bias,
                    "news_event": news_event
                }
        
        struct_15m = self.structure.detect_bos_choch(df_15m)
        
        conf = self.confidence.calculate(df_15m, liq, mtf_result, struct_15m, news_bias)
        
        if conf < self.min_confidence:
            return {
                "signal": "HOLD",
                "reason": f"Confidence too low ({conf:.1%} < {self.min_confidence:.0%})",
                "confidence": conf,
                "liquidity": liq,
                "mtf": mtf_result,
                "structure": struct_15m,
                "news_bias": news_bias,
                "news_event": news_event
            }
        
        # News directional filter: block signals against high-impact news
        if news_bias and news_bias != "NEUTRAL":
            if (liq["direction"] == "BUY" and news_bias == "BEAR") or (liq["direction"] == "SELL" and news_bias == "BULL"):
                return {
                    "signal": "HOLD",
                    "reason": f"News conflict: {news_event['event']} ({news_bias})",
                    "confidence": conf,
                    "liquidity": liq,
                    "mtf": mtf_result,
                    "structure": struct_15m,
                    "news_bias": news_bias,
                    "news_event": news_event
                }
        
        entry = liq["entry_zone"]
        atr = df_15m["atr"].iloc[-1] if "atr" in df_15m.columns else df_15m["close"].iloc[-1] * 0.001
        
        levels = self.risk.calculate_levels(liq["direction"], entry, liq["sweep_level"], atr)
        
        if levels is None:
            return {
                "signal": "HOLD",
                "reason": "Risk too wide (>1.5x ATR)",
                "confidence": conf,
                "liquidity": liq,
                "news_bias": news_bias,
                "news_event": news_event
            }
        
        return {
            "signal": liq["direction"],
            "entry": round(entry, 2),
            "entry_zone_high": round(liq["entry_zone_high"], 2) if liq["entry_zone_high"] else None,
            "sl": levels["sl"],
            "tp1": levels["tp1"],
            "tp2": levels["tp2"],
            "confidence": round(conf, 2),
            "rr1": levels["rr1"],
            "rr2": levels["rr2"],
            "setup_type": liq["setup_type"],
            "sweep_level": liq["sweep_level"],
            "reason": f"{liq['setup_type']} setup after {liq['direction']} liquidity sweep",
            "mtf": mtf_result,
            "structure": struct_15m,
            "session": self.killzone.get_session_weight(current_hour)[1],
            "news_bias": news_bias,
            "news_event": news_event
        }


class BacktestEngine:
    def __init__(self, lookforward=10):
        self.lookforward = lookforward
        self.generator = SignalGenerator()
    
    def run(self, df_15m, df_1h=None, df_4h=None):
        wins = 0
        losses = 0
        total = 0
        trades = []
        
        min_required = 250
        if len(df_15m) < min_required + self.lookforward:
            return {"win_rate": 0, "total": 0, "trades": []}
        
        for i in range(min_required, len(df_15m) - self.lookforward):
            sub_15m = df_15m.iloc[:i].copy()
            sub_1h = df_1h.iloc[:max(1, i//4)].copy() if df_1h is not None and len(df_1h) >= i//4 else None
            sub_4h = df_4h.iloc[:max(1, i//16)].copy() if df_4h is not None and len(df_4h) >= i//16 else None
            
            signal = self.generator.generate(sub_15m, sub_1h, sub_4h)
            if signal["signal"] == "HOLD":
                continue
            
            entry = signal["entry"]
            sl = signal["sl"]
            tp1 = signal["tp1"]
            direction = signal["signal"]
            
            future = df_15m.iloc[i+1:i+1+self.lookforward]
            result = None
            exit_price = None
            
            for _, candle in future.iterrows():
                if direction == "BUY":
                    if candle["low"] <= sl:
                        result = "loss"
                        exit_price = sl
                        break
                    if candle["high"] >= tp1:
                        result = "win"
                        exit_price = tp1
                        break
                else:
                    if candle["high"] >= sl:
                        result = "loss"
                        exit_price = sl
                        break
                    if candle["low"] <= tp1:
                        result = "win"
                        exit_price = tp1
                        break
            
            if result is None:
                result = "loss"
                exit_price = future["close"].iloc[-1]
            
            if result == "win":
                wins += 1
            else:
                losses += 1
            total += 1
            
            trades.append({
                "index": i,
                "direction": direction,
                "entry": entry,
                "exit": exit_price,
                "result": result,
                "setup_type": signal["setup_type"],
                "confidence": signal["confidence"]
            })
        
        win_rate = (wins / total * 100) if total > 0 else 0
        
        return {
            "win_rate": round(win_rate, 2),
            "wins": wins,
            "losses": losses,
            "total": total,
            "trades": trades[-20:]
        }


def generate_chart(df, signal=None):
    fig, ax = plt.subplots(figsize=(12, 6))
    
    plot_df = df.tail(100).copy()
    x_vals = plot_df.index
    
    ax.plot(x_vals, plot_df['close'], label='Close', color='black', linewidth=1.2)
    ax.plot(x_vals, plot_df['ema50'], label='EMA50', color='blue', linewidth=1, alpha=0.7)
    ax.plot(x_vals, plot_df['ema200'], label='EMA200', color='red', linewidth=1, alpha=0.7)
    
    if signal and signal["signal"] != "HOLD":
        color = 'green' if signal["signal"] == "BUY" else 'red'
        
        ax.axhspan(signal["entry"], signal.get("entry_zone_high", signal["entry"]), 
                   alpha=0.2, color=color, label=f'Entry Zone ({signal["setup_type"]})')
        
        ax.axhline(y=signal["sl"], color='red', linestyle='--', alpha=0.7, label=f'SL: {signal["sl"]}')
        ax.axhline(y=signal["tp1"], color='green', linestyle='--', alpha=0.7, label=f'TP1: {signal["tp1"]}')
        ax.axhline(y=signal["tp2"], color='darkgreen', linestyle=':', alpha=0.7, label=f'TP2: {signal["tp2"]}')
        
        if signal.get("sweep_level"):
            ax.axhline(y=signal["sweep_level"], color='orange', linestyle='-.', alpha=0.5, label=f'Sweep: {signal["sweep_level"]}')
    
    ax.set_title('XAUUSD Sniper Chart')
    ax.set_xlabel('Candles')
    ax.set_ylabel('Price')
    ax.legend(loc='upper left', fontsize=8)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    buf = BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)
    return buf


def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS signals(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        direction TEXT,
        entry REAL,
        sl REAL,
        tp1 REAL,
        tp2 REAL,
        confidence REAL,
        setup_type TEXT,
        session TEXT,
        mtf_alignment REAL,
        timestamp TEXT,
        result TEXT
    )""")
    conn.commit()
    conn.close()


def save_signal(signal):
    if signal.get("signal") == "HOLD":
        return
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    mtf_align = signal.get("mtf", {}).get("alignment_score", 0) if signal.get("mtf") else 0
    c.execute("""INSERT INTO signals 
        (direction, entry, sl, tp1, tp2, confidence, setup_type, session, mtf_alignment, timestamp, result)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (signal["signal"], signal["entry"], signal["sl"], signal["tp1"], 
         signal["tp2"], signal["confidence"], signal.get("setup_type", ""), 
         signal.get("session", ""), mtf_align,
         datetime.now().isoformat(), None))
    conn.commit()
    conn.close()


generator = SignalGenerator()
backtester = BacktestEngine()


async def start(update, context):
    keyboard = [
        [InlineKeyboardButton("🎯 Get Signal", callback_data="signal")],
        [InlineKeyboardButton("💧 Liquidity", callback_data="liquidity")],
        [InlineKeyboardButton("📈 Chart", callback_data="chart")],
        [InlineKeyboardButton("📊 Backtest", callback_data="backtest")],
        [InlineKeyboardButton("📰 News", callback_data="news")],
    ]
    await update.message.reply_text(
        "🤖 *XAUUSD AI Sniper System v2.0*\n\n"
        "SMC-based liquidity sweep detection\n"
        "Multi-timeframe confirmation + News filter\n"
        "High RR (2.5–4R) sniper entries\n\n"
        "Select an option:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def button(update, context):
    query = update.callback_query
    await query.answer("Processing...")
    
    try:
        df_15m = fetch_data("15min", 500)
        df_15m = add_indicators(df_15m)
        
        df_1h = None
        df_4h = None
        try:
            df_1h = fetch_data("1h", 500)
            df_1h = add_indicators(df_1h)
            df_4h = fetch_data("4h", 500)
            df_4h = add_indicators(df_4h)
        except Exception as e:
            logger.warning(f"HTF fetch failed: {e}")
        
        if query.data == "signal":
            signal = generator.generate(df_15m, df_1h, df_4h)
            
            if signal["signal"] == "HOLD":
                text = (
                    f"⏸️ *HOLD*\n\n"
                    f"Reason: `{signal.get('reason', 'No setup')}`\n"
                    f"Confidence: `{signal.get('confidence', 0):.0%}`"
                )
                if signal.get("news_bias"):
                    text += f"\n📰 News: `{signal['news_event']['event']}` ({signal['news_bias']})"
                await query.edit_message_text(text, parse_mode='Markdown')
                return
            
            emoji = "🟢" if signal["signal"] == "BUY" else "🔴"
            
            text = (
                f"{emoji} *XAUUSD {signal['signal']}*\n\n"
                f"📍 Entry: `{signal['entry']}`"
            )
            if signal.get("entry_zone_high"):
                text += f" – `{signal['entry_zone_high']}`"
            
            text += (
                f"\n🛡️ SL: `{signal['sl']}`\n"
                f"🎯 TP1: `{signal['tp1']}` (R:R {signal['rr1']})\n"
                f"🎯 TP2: `{signal['tp2']}` (R:R {signal['rr2']})\n\n"
                f"🧠 Confidence: `{signal['confidence']:.0%}`\n"
                f"📐 Setup: `{signal['setup_type']}`\n"
                f"⏰ Session: `{signal.get('session', 'N/A')}`\n"
            )
            
            if signal.get("mtf"):
                mtf = signal["mtf"]
                text += (
                    f"\n*Multi-Timeframe:*\n"
                    f"• 1H Sweep: `{mtf.get('sweep_1h_dir', 'N/A')}`\n"
                    f"• 4H Trend: `{'Bull' if mtf.get('trend_4h') == 1 else ('Bear' if mtf.get('trend_4h') == -1 else 'Neutral')}`\n"
                    f"• Alignment: `{mtf.get('alignment_score', 0):.0%}`"
                )
            
            if signal.get("news_bias"):
                text += f"\n📰 News: `{signal['news_event']['event']}` ({signal['news_bias']})"
            
            text += f"\n\n💡 {signal['reason']}"
            
            save_signal(signal)
            await query.edit_message_text(text, parse_mode='Markdown')
        
        elif query.data == "liquidity":
            liq = LiquidityEngine().analyze(df_15m)
            struct = StructureAnalyzer().detect_bos_choch(df_15m)
            
            if not liq["valid"]:
                await query.edit_message_text(
                    "💧 *Liquidity Analysis*\n\n"
                    "No sweep detected. Market is consolidating.",
                    parse_mode='Markdown'
                )
                return
            
            dir_emoji = "🟢" if liq["direction"] == "BUY" else "🔴"
            
            text = (
                f"💧 *Liquidity Analysis*\n\n"
                f"{dir_emoji} Bias: `{liq['direction']}`\n"
                f"📍 Sweep Level: `{liq['sweep_level']}`\n"
                f"🎯 Entry Zone: `{liq['entry_zone']}`"
            )
            if liq.get("entry_zone_high"):
                text += f" – `{liq['entry_zone_high']}`"
            
            text += (
                f"\n📐 Setup Type: `{liq['setup_type']}`\n"
                f"🕯️ Wick Ratio: `{liq.get('wick_ratio', 0):.2f}`\n"
                f"📊 Volume Ratio: `{liq.get('volume_ratio', 0):.2f}x`\n\n"
                f"*Structure:*\n"
                f"• BOS: `{'Bull' if struct.get('bos') == 1 else ('Bear' if struct.get('bos') == -1 else 'None')}`\n"
                f"• CHoCH: `{'Bull' if struct.get('choch') == 1 else ('Bear' if struct.get('choch') == -1 else 'None')}`\n\n"
                f"*Details:*\n"
                f"• OB Range: `{liq['ob_low']}` – `{liq['ob_high']}`\n"
                f"• FVG: `{liq['fvg_dir']}` (`{liq['fvg_bottom']}` – `{liq['fvg_top']}`)"
            )
            
            await query.edit_message_text(text, parse_mode='Markdown')
        
        elif query.data == "chart":
            signal = generator.generate(df_15m, df_1h, df_4h)
            await query.edit_message_text("⏳ Generating chart...")
            chart_buf = generate_chart(df_15m, signal)
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=chart_buf,
                caption=f"📈 XAUUSD Chart | Signal: {signal.get('signal', 'HOLD')}",
                parse_mode='Markdown'
            )
        
        elif query.data == "backtest":
            await query.edit_message_text("⏳ Running backtest...")
            results = backtester.run(df_15m, df_1h, df_4h)
            
            text = (
                f"📊 *Backtest Results*\n\n"
                f"Win Rate: `{results['win_rate']}%`\n"
                f"Wins: `{results['wins']}`\n"
                f"Losses: `{results['losses']}`\n"
                f"Total Trades: `{results['total']}`\n\n"
                f"*Last setups:*\n"
            )
            
            for t in results['trades'][-5:]:
                emoji = "✅" if t['result'] == 'win' else "❌"
                text += f"{emoji} {t['direction']} {t['setup_type']} @ {t['entry']} (conf: {t['confidence']:.0%})\n"
            
            await query.edit_message_text(text, parse_mode='Markdown')
        
        elif query.data == "news":
            news_bias, news_event = generator.news_filter.get_bias(buffer_minutes=30)
            if news_bias:
                text = (
                    f"📰 *High-Impact News*\n\n"
                    f"Event: `{news_event['event']}`\n"
                    f"Bias: `{news_bias}`\n"
                    f"Time: `{news_event['time'].strftime('%Y-%m-%d %H:%M')} UTC`"
                )
            else:
                text = (
                    "📰 *News Filter*\n\n"
                    "No high-impact USD events within 30 minutes."
                )
            await query.edit_message_text(text, parse_mode='Markdown')
    
    except Exception as e:
        logger.error(f"Button error: {e}")
        await query.edit_message_text(f"❌ Error: {str(e)}")


def main():
    init_db()
    
    application = ApplicationBuilder().token(TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button))
    
    logger.info("XAUUSD Sniper System v2.0 starting...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

