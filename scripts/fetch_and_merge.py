# scripts/fetch_and_merge.py
import os, sys, re, time, io
from datetime import datetime, timedelta
import requests, pandas as pd, numpy as np

SRC_FILE = "scripts/sources.txt"
OUT_FACTORS = "data/factors.csv"
OUT_HK20 = "data/hk20.csv"

# Weights（宏觀均衡 / 等權）
W_MACRO = {"HSI":28, "HSTECH":22, "BTC":15, "USDCNH":20, "VHSI":15}
W_EQUAL = {"HSI":20, "HSTECH":20, "BTC":20, "USDCNH":20, "VHSI":20}
WINDOW = 252  # 分位數窗口 ~1y


for p in {os.path.dirname(OUT_FACTORS), os.path.dirname(OUT_HK20)}:
    if os.path.exists(p) and not os.path.isdir(p):
        raise RuntimeError(f"'{p}' exists but is a file – please delete or rename it.")
    os.makedirs(p, exist_ok=True)

def fetch_csv(url, tries=3, timeout=20):
    last_err = None
    for k in range(tries):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 200 and "text" in r.headers.get("Content-Type",""):
                return r.text
            last_err = f"HTTP {r.status_code} content-type {r.headers.get('Content-Type')}"
        except Exception as e:
            last_err = str(e)
        time.sleep(2*(k+1))
    raise RuntimeError(f"fetch fail: {last_err} url={url}")

def parse_date_any(s):
    if pd.isna(s): return None
    x = str(s).strip()
    # 例如 2021年8月11日
    m = re.match(r"^\s*(\d{4})年(\d{1,2})月(\d{1,2})日\s*$", x)
    if m:
        y, mo, d = map(int, m.groups())
        return f"{y:04d}-{mo:02d}-{d:02d}"
    # 其他格式交給 pandas
    try:
        dt = pd.to_datetime(x, errors="raise", utc=False)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None

def pct_norm(series, window=WINDOW, reverse=False):
    """近 window 天位置分位數 → [-1,1]；USDCNH/VHSI 用 reverse=True 反向"""
    out = []
    vals = series.values.astype(float)
    n = len(series)
    for i in range(n):
        start = max(0, i - window + 1)
        w = vals[start:i+1]
        w = w[~np.isnan(w)]
        if len(w) < min(120, window//2) or np.isnan(vals[i]):
            out.append(np.nan); continue
        rank = (w <= vals[i]).sum() / len(w)
        s = 2*rank - 1
        if reverse: s = -s
        out.append(s)
    return pd.Series(out, index=series.index, dtype=float)

def norm_weights(d):
    items = [(k, float(v)) for k,v in d.items() if v is not None and float(v) != 0.0]
    s = sum(v for _,v in items)
    if s == 0: return {k: 1/len(items) for k,_ in items}
    return {k: v/s for k,v in items}

def fused(df_norm, weights):
    w = norm_weights(weights)
    tmp = pd.DataFrame(index=df_norm.index)
    for fac in ["HSI","HSTECH","USDCNH","VHSI","BTC"]:
        col = f"{fac}_norm"
        if col in df_norm.columns and fac in w:
            tmp[col] = df_norm[col] * w[fac]
    return tmp.sum(axis=1, skipna=True) if not tmp.empty else pd.Series(np.nan, index=df_norm.index)

def main():
    if not os.path.exists(SRC_FILE):
        print(f"missing {SRC_FILE}", file=sys.stderr); sys.exit(1)

    lines = []
    with open(SRC_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"): continue
            if "," not in line: continue
            k, url = line.split(",", 1)
            lines.append((k.strip().upper(), url.strip()))

    factors_df = None
    stocks = {}

    for code, url in lines:
        csv_text = fetch_csv(url)
        df = pd.read_csv(io.StringIO(csv_text))
        # 正規化日期
        df.columns = [str(c).strip() for c in df.columns]
        if "Date" not in df.columns:
            df.rename(columns={df.columns[0]: "Date"}, inplace=True)
        df["Date"] = df["Date"].map(parse_date_any)
        df = df.dropna(subset=["Date"]).copy()
        df["Date"] = pd.to_datetime(df["Date"])

        if code == "FACTORS":
            # 期望：Date, HSI, HSTECH, USDCNH, VHSI, BTC
            for c in ["HSI","HSTECH","USDCNH","VHSI","BTC"]:
                if c not in df.columns: df[c] = np.nan
            factors_df = df[["Date","HSI","HSTECH","USDCNH","VHSI","BTC"]].copy()
        else:
            # 個股：Date, Close
            close_col = "Close" if "Close" in df.columns else df.columns[1]
            s = df[["Date", close_col]].rename(columns={close_col: code})
            stocks[code] = s

    # 合併 20 檔
    if stocks:
        hk20 = None
        for code, s in stocks.items():
            hk20 = s if hk20 is None else hk20.merge(s, on="Date", how="outer")
        hk20 = hk20.sort_values("Date").reset_index(drop=True)
        os.makedirs(os.path.dirname(OUT_HK20), exist_ok=True)
        hk20.to_csv(OUT_HK20, index=False, float_format="%.6f")
        print(f"Wrote {OUT_HK20} ({len(hk20)} rows, {len(hk20.columns)-1} symbols)")
    else:
        print("No stock sources in sources.txt")

    # factors.csv：含 *_norm 與 Fused
    if factors_df is not None:
        df = factors_df.sort_values("Date").reset_index(drop=True)
        N = pd.DataFrame({"Date": df["Date"]})
        N["HSI_norm"]    = pct_norm(df["HSI"])
        N["HSTECH_norm"] = pct_norm(df["HSTECH"])
        N["USDCNH_norm"] = pct_norm(df["USDCNH"], reverse=True)
        N["VHSI_norm"]   = pct_norm(df["VHSI"], reverse=True)
        N["BTC_norm"]    = pct_norm(df["BTC"])

        F_macro = fused(N.set_index("Date"), W_MACRO).rename("Fused_macro")
        F_equal = fused(N.set_index("Date"), W_EQUAL).rename("Fused_equal")

        out = pd.DataFrame({
            "Date": df["Date"].dt.strftime("%Y-%m-%d"),
            "HSI": df["HSI"], "HSTECH": df["HSTECH"], "USDCNH": df["USDCNH"], "VHSI": df["VHSI"], "BTC": df["BTC"],
            "HSI_norm": N["HSI_norm"], "HSTECH_norm": N["HSTECH_norm"],
            "USDCNH_norm": N["USDCNH_norm"], "VHSI_norm": N["VHSI_norm"], "BTC_norm": N["BTC_norm"],
            "Fused_macro": F_macro.values, "Fused_equal": F_equal.values,
        })
        os.makedirs(os.path.dirname(OUT_FACTORS), exist_ok=True)
        out.to_csv(OUT_FACTORS, index=False, float_format="%.6f")
        print(f"Wrote {OUT_FACTORS} ({len(out)} rows)")
    else:
        print("No FACTORS source in sources.txt", file=sys.stderr)

if __name__ == "__main__":
    main()

