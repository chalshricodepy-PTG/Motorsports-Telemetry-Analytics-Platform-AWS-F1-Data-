import os
import json
import time
import traceback
from dataclasses import dataclass
from typing import Dict, List, Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import fastf1


@dataclass(frozen=True)
class Config:
    bucket: str
    season: int
    events: List[str]
    sessions: List[str]
    drivers_mode: str
    curated_prefix: str
    raw_prefix: str
    fastf1_cache_dir: str
    max_retries: int
    retry_backoff_seconds: int


def load_config() -> Config:
    bucket = os.environ["BUCKET"]
    season = int(os.environ.get("SEASON", "2024"))
    events = [e.strip() for e in os.environ.get("EVENTS", "Bahrain,Monaco,Great Britain").split(",") if e.strip()]
    sessions = [s.strip() for s in os.environ.get("SESSIONS", "Q,R").split(",") if s.strip()]
    drivers_mode = os.environ.get("DRIVERS_MODE", "top5_finishers")
    curated_prefix = os.environ.get("CURATED_PREFIX", "curated").rstrip("/")
    raw_prefix = os.environ.get("RAW_PREFIX", "raw").rstrip("/")
    fastf1_cache_dir = os.environ.get("FASTF1_CACHE_DIR", "/tmp/fastf1_cache")
    max_retries = int(os.environ.get("MAX_RETRIES", "3"))
    retry_backoff_seconds = int(os.environ.get("RETRY_BACKOFF_SECONDS", "10"))
    return Config(
        bucket=bucket,
        season=season,
        events=events,
        sessions=sessions,
        drivers_mode=drivers_mode,
        curated_prefix=curated_prefix,
        raw_prefix=raw_prefix,
        fastf1_cache_dir=fastf1_cache_dir,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
    )


def s3_client():
    return boto3.client("s3")


def put_json_to_s3(bucket: str, key: str, payload: Dict) -> None:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
    s3_client().put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def upload_file_to_s3(local_path: str, bucket: str, key: str) -> None:
    s3_client().upload_file(local_path, bucket, key)


def df_to_parquet_local(df: pd.DataFrame, local_path: str) -> None:
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, local_path, compression="snappy")


def write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    os.makedirs("/tmp/out", exist_ok=True)
    local_path = f"/tmp/out/{int(time.time() * 1000)}.parquet"
    df_to_parquet_local(df, local_path)
    upload_file_to_s3(local_path, bucket, key)


def with_retries(fn, *, max_retries: int, backoff_seconds: int, context: str):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except Exception as e:
            last_err = e
            print(f"[WARN] Attempt {attempt}/{max_retries} failed for {context}: {e}")
            if attempt < max_retries:
                sleep_s = backoff_seconds * attempt
                print(f"[INFO] Sleeping {sleep_s}s before retry...")
                time.sleep(sleep_s)
    raise last_err


def ergast_get_json(path: str) -> Dict:
    url = f"http://ergast.com/api/f1/{path}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def get_race_results_top_finishers(season: int, round_number: int, top_n: int = 5) -> List[str]:
    data = ergast_get_json(f"{season}/{round_number}/results.json")
    races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
    if not races:
        return []
    results = races[0].get("Results", [])
    out = []
    for row in results[:top_n]:
        drv = row.get("Driver", {})
        code = drv.get("code")
        driver_id = drv.get("driverId")
        out.append(code or driver_id)
    return out


def init_fastf1_cache(cache_dir: str) -> None:
    os.makedirs(cache_dir, exist_ok=True)
    fastf1.Cache.enable_cache(cache_dir)


def fastf1_load_session(season: int, event: str, session_code: str):
    ses = fastf1.get_session(season, event, session_code)
    ses.load(telemetry=True, weather=True, messages=False)
    return ses


def safe_slug(name: str) -> str:
    return (
        name.strip()
        .lower()
        .replace(" ", "-")
        .replace("/", "-")
        .replace("’", "")
        .replace("'", "")
    )


def curated_keys(cfg: Config, event: str, session_code: str) -> Dict[str, str]:
    event_slug = safe_slug(event)
    base = cfg.curated_prefix
    return {
        "laps": f"{base}/laps/season={cfg.season}/event={event_slug}/session={session_code}/laps.parquet",
        "weather": f"{base}/weather/season={cfg.season}/event={event_slug}/session={session_code}/weather.parquet",
        "meta": f"{base}/meta/season={cfg.season}/event={event_slug}/session={session_code}/session_meta.json",
        "telemetry_base": f"{base}/telemetry/season={cfg.season}/event={event_slug}/session={session_code}",
    }


def raw_keys(cfg: Config, event: str, session_code: str) -> Dict[str, str]:
    event_slug = safe_slug(event)
    base = cfg.raw_prefix
    return {
        "laps": f"{base}/fastf1/laps/season={cfg.season}/event={event_slug}/session={session_code}/laps.parquet",
        "weather": f"{base}/fastf1/weather/season={cfg.season}/event={event_slug}/session={session_code}/weather.parquet",
        "meta": f"{base}/fastf1/meta/season={cfg.season}/event={event_slug}/session={session_code}/session_meta.json",
        "telemetry_base": f"{base}/fastf1/telemetry/season={cfg.season}/event={event_slug}/session={session_code}",
        "ergast_results": f"{base}/ergast/results/season={cfg.season}/event={event_slug}/session={session_code}/top_finishers.json",
    }


def normalize_laps_df(laps: pd.DataFrame, season: int, event: str, session_code: str) -> pd.DataFrame:
    df = laps.copy()
    df["season"] = season
    df["event"] = safe_slug(event)
    df["session"] = session_code

    for col in ["LapTime", "Sector1Time", "Sector2Time", "Sector3Time"]:
        if col in df.columns and pd.api.types.is_timedelta64_dtype(df[col]):
            df[col + "_seconds"] = df[col].dt.total_seconds()

    wanted = [
        "season", "event", "session",
        "Driver", "Team", "LapNumber", "Stint",
        "Compound", "TyreLife",
        "IsPersonalBest", "IsAccurate",
        "TrackStatus", "PitInTime", "PitOutTime",
        "LapTime_seconds", "Sector1Time_seconds", "Sector2Time_seconds", "Sector3Time_seconds"
    ]
    keep_cols = [c for c in wanted if c in df.columns]

    out = df[keep_cols].rename(columns={
        "Driver": "driver",
        "Team": "team",
        "LapNumber": "lap_number",
        "Stint": "stint",
        "Compound": "compound",
        "TyreLife": "tyre_life",
        "IsPersonalBest": "is_personal_best",
        "IsAccurate": "is_accurate",
        "TrackStatus": "track_status",
        "PitInTime": "pit_in_time",
        "PitOutTime": "pit_out_time",
        "LapTime_seconds": "lap_time_seconds",
        "Sector1Time_seconds": "sector1_seconds",
        "Sector2Time_seconds": "sector2_seconds",
        "Sector3Time_seconds": "sector3_seconds",
    })

    for c in ["lap_number", "stint", "tyre_life"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int64")

    for c in ["lap_time_seconds", "sector1_seconds", "sector2_seconds", "sector3_seconds"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").astype("float64")

    for c in ["is_personal_best", "is_accurate"]:
        if c in out.columns:
            out[c] = out[c].astype("boolean")

    for c in ["driver", "team", "compound", "track_status", "pit_in_time", "pit_out_time", "event", "session"]:
        if c in out.columns:
            out[c] = out[c].astype("string")

    return out


def normalize_weather_df(weather: pd.DataFrame, season: int, event: str, session_code: str) -> pd.DataFrame:
    df = weather.copy()
    df["season"] = season
    df["event"] = safe_slug(event)
    df["session"] = session_code

    keep = [c for c in ["season", "event", "session", "Time", "AirTemp", "TrackTemp", "Humidity", "Pressure", "WindSpeed", "WindDirection", "Rainfall"] if c in df.columns]
    out = df[keep].rename(columns={
        "Time": "time",
        "AirTemp": "air_temp_c",
        "TrackTemp": "track_temp_c",
        "Humidity": "humidity_pct",
        "Pressure": "pressure_hpa",
        "WindSpeed": "wind_speed_ms",
        "WindDirection": "wind_direction_deg",
        "Rainfall": "rainfall",
    })
    return out


def normalize_telemetry_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    float_cols = ["X", "Y", "Z", "Speed", "Throttle", "Brake", "DRS"]
    int_cols = ["nGear", "RPM"]

    for c in float_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").astype("float64")

    for c in int_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int64")

    return out


def build_driver_telemetry(ses, driver_code: str) -> Optional[pd.DataFrame]:
    try:
        laps = ses.laps.pick_driver(driver_code)
        if laps is None or len(laps) == 0:
            return None

        frames = []
        for _, lap in laps.iterlaps():
            try:
                tel = lap.get_telemetry()
                if tel is None or len(tel) == 0:
                    continue

                df = tel.copy()
                df = normalize_telemetry_df(df)

                df["driver"] = driver_code
                df["lap_number"] = int(lap["LapNumber"]) if "LapNumber" in lap and pd.notna(lap["LapNumber"]) else None

                if "Time" in df.columns and pd.api.types.is_timedelta64_dtype(df["Time"]):
                    df["ts_ms"] = (df["Time"].dt.total_seconds() * 1000).astype("int64")

                keep = [c for c in ["driver", "lap_number", "ts_ms", "X", "Y", "Z", "Speed", "Throttle", "Brake", "nGear", "RPM", "DRS"] if c in df.columns]
                df = df[keep].rename(columns={
                    "X": "x",
                    "Y": "y",
                    "Z": "z",
                    "Speed": "speed_kmh",
                    "Throttle": "throttle",
                    "Brake": "brake",
                    "nGear": "gear",
                    "RPM": "rpm",
                    "DRS": "drs",
                })

                if "lap_number" in df.columns:
                    df["lap_number"] = pd.to_numeric(df["lap_number"], errors="coerce").astype("Int64")

                frames.append(df)
            except Exception:
                continue

        if not frames:
            return None
        return pd.concat(frames, ignore_index=True)
    except Exception:
        return None


def main():
    cfg = load_config()
    print(f"[INFO] Config: {cfg}")

    init_fastf1_cache(cfg.fastf1_cache_dir)

    for event in cfg.events:
        for session_code in cfg.sessions:
            print(f"[INFO] Loading session: season={cfg.season} event={event} session={session_code}")

            keys_raw = raw_keys(cfg, event, session_code)
            keys_cur = curated_keys(cfg, event, session_code)

            ses = with_retries(
                lambda: fastf1_load_session(cfg.season, event, session_code),
                max_retries=cfg.max_retries,
                backoff_seconds=cfg.retry_backoff_seconds,
                context=f"fastf1_load_session({cfg.season},{event},{session_code})"
            )

            meta = {
                "season": cfg.season,
                "event": event,
                "event_slug": safe_slug(event),
                "session": session_code,
                "session_name": getattr(ses, "name", None),
                "date": str(getattr(ses, "date", None)),
                "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }

            put_json_to_s3(cfg.bucket, keys_raw["meta"], meta)
            put_json_to_s3(cfg.bucket, keys_cur["meta"], meta)

            laps_df = ses.laps.copy()
            weather_df = ses.weather_data.copy() if getattr(ses, "weather_data", None) is not None else pd.DataFrame()

            write_parquet_to_s3(laps_df, cfg.bucket, keys_raw["laps"])
            if len(weather_df) > 0:
                write_parquet_to_s3(weather_df, cfg.bucket, keys_raw["weather"])

            curated_laps = normalize_laps_df(laps_df, cfg.season, event, session_code)
            write_parquet_to_s3(curated_laps, cfg.bucket, keys_cur["laps"])

            if len(weather_df) > 0:
                curated_weather = normalize_weather_df(weather_df, cfg.season, event, session_code)
                write_parquet_to_s3(curated_weather, cfg.bucket, keys_cur["weather"])

            drivers: List[str] = []
            if cfg.drivers_mode == "all":
                if "Driver" in laps_df.columns:
                    drivers = sorted(list(set(laps_df["Driver"].dropna().astype(str).tolist())))
            else:
                round_number = None
                try:
                    ev = getattr(ses, "event", None)
                    round_number = int(getattr(ev, "RoundNumber", None)) if ev is not None else None
                except Exception:
                    round_number = None

                if session_code == "R" and round_number is not None:
                    try:
                        top5 = get_race_results_top_finishers(cfg.season, round_number, top_n=5)
                        put_json_to_s3(cfg.bucket, keys_raw["ergast_results"], {"season": cfg.season, "round": round_number, "top5": top5})
                        drivers = [d for d in top5 if d]
                    except Exception as e:
                        print(f"[WARN] Ergast top5 failed: {e}")
                        if "Driver" in laps_df.columns:
                            drivers = sorted(list(set(laps_df["Driver"].dropna().astype(str).tolist())))
                else:
                    if "Driver" in laps_df.columns:
                        drivers = sorted(list(set(laps_df["Driver"].dropna().astype(str).tolist())))

                if len(drivers) > 5:
                    drivers = drivers[:5]

            print(f"[INFO] Drivers: {drivers}")

            for d in drivers:
                tel_df = build_driver_telemetry(ses, d)
                if tel_df is None or len(tel_df) == 0:
                    print(f"[WARN] No telemetry for {d}")
                    continue

                tel_df["season"] = cfg.season
                tel_df["event"] = safe_slug(event)
                tel_df["session"] = session_code

                raw_key = f"{keys_raw['telemetry_base']}/driver={d}/telemetry.parquet"
                cur_key = f"{keys_cur['telemetry_base']}/driver={d}/telemetry.parquet"

                write_parquet_to_s3(tel_df, cfg.bucket, raw_key)
                write_parquet_to_s3(tel_df, cfg.bucket, cur_key)

            print(f"[INFO] Completed event={event} session={session_code}")

    print("[INFO] Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[ERROR] Fatal:")
        print(str(e))
        print(traceback.format_exc())
        raise
