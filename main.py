#!/usr/bin/env python3
"""
每天对 4 个地点，批量调用 OpenWeatherMap Daily Aggregation API，
采集 2025-04-01 ~ 2025-09-30 全部天数的气温和降雨数据，追加到 weather_data.csv。
"""
import csv
import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

# ── 配置 ──
TOWNS_CSV = "townsNE.csv"
OUTPUT_CSV = "weather_data.csv"
PROGRESS_FILE = "progress.json"
BATCH_SIZE = 5          # 每天处理地点数
DATE_START = "2025-04-01"
DATE_END = "2025-09-30"
TZ_BEIJING = timezone(timedelta(hours=8))

API_KEY = os.environ.get("OPENWEATHER_API_KEY", "")
if not API_KEY:
    print("ERROR: OPENWEATHER_API_KEY 未设置")
    sys.exit(1)


def date_range(start_str, end_str):
    """生成 start~end 的日期字符串列表"""
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    dates = []
    d = start
    while d <= end:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return dates


DATES = date_range(DATE_START, DATE_END)
print(f"日期范围: {DATE_START} ~ {DATE_END} ({len(DATES)} 天)")


def load_towns():
    towns = []
    with open(TOWNS_CSV, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 3:
                continue
            tid = row[0].strip()
            name = row[1].strip()
            coord_str = row[2].strip().strip('"').replace("，", ",")
            parts = coord_str.split(",")
            if len(parts) != 2:
                continue
            towns.append((tid, name, parts[0].strip(), parts[1].strip()))
    return towns


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f).get("last_index", 0)
    return 0


def save_progress(index):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"last_index": index}, f)


def ensure_output_header():
    if not os.path.exists(OUTPUT_CSV) or os.path.getsize(OUTPUT_CSV) == 0:
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "id", "name", "lat", "lon", "date",
                "temp_min", "temp_max", "temp_afternoon",
                "temp_night", "temp_evening", "temp_morning",
                "precipitation_total"
            ])


def fetch_daily(lat, lon, date_str):
    url = (
        f"https://api.openweathermap.org/data/3.0/onecall/day_summary"
        f"?lat={lat}&lon={lon}&date={date_str}"
        f"&units=metric&appid={API_KEY}"
    )
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode() if e.fp else ""
        print(f"    HTTP {e.code}: {body[:120]}")
        return None
    except Exception as e:
        print(f"    异常: {e}")
        return None


def extract_row(tid, name, lat, lon, date_str, data):
    """从 API 响应提取一行数据"""
    temp = data.get("temperature", {})
    precip = data.get("precipitation", {})
    return [
        tid, name, lat, lon, date_str,
        temp.get("min", ""), temp.get("max", ""),
        temp.get("afternoon", ""), temp.get("night", ""),
        temp.get("evening", ""), temp.get("morning", ""),
        precip.get("total", ""),
    ]


def main():
    now_bj = datetime.now(TZ_BEIJING)
    print(f"运行时间: {now_bj.strftime('%Y-%m-%d %H:%M')} 北京时间")

    towns = load_towns()
    total = len(towns)
    print(f"总地点数: {total}")

    start = load_progress()
    if start >= total:
        print("✅ 所有地点已采集完毕！")
        return

    end = min(start + BATCH_SIZE, total)
    batch = towns[start:end]
    print(f"本次采集: 第 {start+1}~{end} / {total} 个地点（{len(batch)} 个）")
    print(f"每个地点采集 {len(DATES)} 天，共 {len(batch) * len(DATES)} 次 API 调用")

    ensure_output_header()

    all_rows = []
    for idx, (tid, name, lat, lon) in enumerate(batch):
        print(f"\n[{idx+1}/{len(batch)}] {tid} {name} ({lat}, {lon})")
        success = 0
        fail = 0
        for i, date_str in enumerate(DATES):
            data = fetch_daily(lat, lon, date_str)
            if data:
                all_rows.append(extract_row(tid, name, lat, lon, date_str, data))
                success += 1
            else:
                # 记录空行，标记失败
                all_rows.append([tid, name, lat, lon, date_str] + [""] * 7)
                fail += 1

            # 每 10 天打印一次进度
            if (i + 1) % 50 == 0 or (i + 1) == len(DATES):
                print(f"  {date_str}  ({i+1}/{len(DATES)})  OK:{success}  FAIL:{fail}")

            # 请求间隔，避免触发限流
            time.sleep(0.15)

    # 一次性写入
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(all_rows)

    next_index = end if end < total else 0
    save_progress(next_index)
    print(f"\n完成。写入 {len(all_rows)} 条记录。")
    if next_index == 0:
        print("🎉 全部 219 个地点采集完毕，已循环回起点。")
    else:
        remaining = total - next_index
        days_left = (remaining + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"剩余 {remaining} 个地点，约还需 {days_left} 天。")


if __name__ == "__main__":
    main()
