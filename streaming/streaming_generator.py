"""
EuroCrimePulse — scalable relational JSONL generator for Spark Structured Streaming.

Design:
- 3 independent sources: police, court, corrections.
- Immutable JSONL files with unique batch/file names.
- Full-width UUID-backed business IDs (no truncation).
- Canonical 9-digit National IDs with reversible presentation-only mutations.
- No unbounded person registry or Faker uniqueness cache.
- Optional deterministic test mode via --seed and --generated-at.
"""

import argparse
import json
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker

NUM_CRIMES = 100_000
NULL_INJECTION_RATE = 0.07
BAD_ID_FORMAT_RATE = 0.18
LOGICAL_ERROR_RATE = 0.06

EU_COUNTRIES_CITIES = {
    "Germany": [("Berlin", 52.5200, 13.4050), ("Munich", 48.1351, 11.5820), ("Hamburg", 53.5511, 9.9937)],
    "France": [("Paris", 48.8566, 2.3522), ("Lyon", 45.7640, 4.8357), ("Marseille", 43.2965, 5.3698)],
    "Spain": [("Madrid", 40.4168, -3.7038), ("Barcelona", 41.3874, 2.1686)],
    "Italy": [("Rome", 41.9028, 12.4964), ("Milan", 45.4642, 9.1900)],
    "Netherlands": [("Amsterdam", 52.3676, 4.9041), ("Rotterdam", 51.9244, 4.4777)],
    "Poland": [("Warsaw", 52.2297, 21.0122), ("Krakow", 50.0647, 19.9450)],
    "Belgium": [("Brussels", 50.8503, 4.3517)],
    "Ireland": [("Dublin", 53.3498, -6.2603)],
}

CRIME_TYPES = [
    "Theft", "Burglary", "Assault", "Fraud", "Drug Possession", "Robbery",
    "Vandalism", "Cybercrime", "DUI", "Homicide", "Money Laundering",
    "Bribery", "Smuggling",
]

RELEASE_REASONS = [
    "Served Full Term", "Released on Bail", "Early Parole",
    "Pardoned", "Sentence Commuted", "Escaped (Recaptured)",
]

VERDICT_OUTCOMES = [
    "Guilty - Custodial", "Guilty - Fine Only", "Guilty - Probation",
    "Not Guilty", "Case Dismissed",
]

COURT_LEVELS = ["District Court", "Regional Court", "Appellate Court", "Supreme Court"]
PRISON_NAMES = [
    "Stammheim Correctional Facility", "Fleury-Merogis Prison", "Fresnes Prison",
    "Bellevue State Penitentiary", "Ardennes Reform Institute", "Nordhavn Detention Center",
    "San Vittore Prison", "Vught Correctional Institute",
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def maybe_null(value, rng):
    return None if rng.random() < NULL_INJECTION_RATE else value


def format_date_source1(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def format_date_source2(dt, rng):
    return dt.strftime("%m/%d/%Y") if rng.random() < 0.5 else dt.strftime("%d-%b-%Y")


def format_date_source3(dt, rng):
    return dt.strftime("%d.%m.%Y") if rng.random() < 0.5 else dt.strftime("%Y-%m-%d")


def mutate_national_id(national_id, rng):
    """
    Presentation-only mutations. Every supported mutation is reversible
    without guessing or losing digits.
    """
    if rng.random() >= BAD_ID_FORMAT_RATE:
        return national_id

    variants = [
        f"ID-{national_id[:5]}-{national_id[5:]}",
        f"{national_id[:3]}-{national_id[3:6]}-{national_id[6:]}",
        f"{national_id[:4]} {national_id[4:]}",
        f"nid{national_id}",
    ]
    return rng.choice(variants)


def new_business_id(prefix, rng):
    # Full-width 128-bit ID, deterministic when a seed is supplied.
    return f"{prefix}-{rng.getrandbits(128):032X}"


def random_past_datetime(rng, days_back_min=1, days_back_max=1800, now=None):
    now = now or utc_now().replace(tzinfo=None)
    return now - timedelta(
        days=rng.randint(days_back_min, days_back_max),
        seconds=rng.randint(0, 86400),
    )


def compute_age(dob, event_date):
    return event_date.year - dob.year - (
        (event_date.month, event_date.day) < (dob.month, dob.day)
    )


class PersonFactory:
    """
    Does not retain every generated person.
    The canonical National ID is generated as a fixed-width 9-digit value.
    """

    def __init__(self, rng, faker, start=1):
        self.rng = rng
        self.fake = faker
        self.counter = start

    def create(self):
        if self.counter > 999_999_999:
            raise RuntimeError("9-digit National ID space exhausted.")
        national_id = f"{self.counter:09d}"
        self.counter += 1
        gender = self.rng.choice(["Male", "Female"])
        dob = self.fake.date_of_birth(minimum_age=16, maximum_age=85)
        return {
            "national_id": national_id,
            "full_name": self.fake.name_male() if gender == "Male" else self.fake.name_female(),
            "date_of_birth": dob,
            "gender": gender,
            "nationality": self.fake.country(),
        }


def get_officer(pool, counter, rng, faker):
    if not pool or rng.random() < 0.30:
        counter[0] += 1
        pool.append({
            "officer_id": f"OFC-{counter[0]:08d}",
            "officer_name": faker.name(),
            "badge_number": faker.bothify(text="BD-####??"),
        })
    return rng.choice(pool)


def get_judge(pool, counter, rng, faker):
    if not pool or rng.random() < 0.20:
        counter[0] += 1
        pool.append({
            "judge_id": f"JDG-{counter[0]:08d}",
            "judge_name": faker.name(),
            "judge_gender": rng.choice(["Male", "Female"]),
            "years_of_experience": rng.randint(2, 35),
            "court_name": f"{faker.city()} {rng.choice(COURT_LEVELS)}",
        })
    return rng.choice(pool)


def generate_crime_record(rng, faker, victim_factory, officer_pool, officer_counter, generated_now):
    crime_id = new_business_id("CR", rng)
    country = rng.choice(list(EU_COUNTRIES_CITIES))
    city, lat, lon = rng.choice(EU_COUNTRIES_CITIES[country])
    crime_dt = random_past_datetime(rng, 1, 1800, generated_now)
    crime_type = rng.choice(CRIME_TYPES)
    victim = victim_factory.create()
    officer = get_officer(officer_pool, officer_counter, rng, faker)

    actual_age = compute_age(victim["date_of_birth"], crime_dt.date())
    reported_age = actual_age
    dq_flag = None
    if rng.random() < LOGICAL_ERROR_RATE:
        reported_age = max(0, actual_age + rng.choice([-10, -5, 5, 10, 15]))
        dq_flag = "age_dob_mismatch"

    return {
        "source_system": "police_dept_api",
        "crime_id": crime_id,
        "crime_type": crime_type,
        "crime_date": format_date_source1(crime_dt),
        "location": {
            "country": country,
            "city": maybe_null(city, rng),
            "latitude": round(lat + rng.uniform(-0.05, 0.05), 6),
            "longitude": round(lon + rng.uniform(-0.05, 0.05), 6),
        },
        "arresting_officer": {
            "officer_id": officer["officer_id"],
            "officer_name": maybe_null(officer["officer_name"], rng),
            "badge_number": officer["badge_number"],
        },
        "victim": {
            "national_id": victim["national_id"],
            "full_name": maybe_null(victim["full_name"], rng),
            "date_of_birth": victim["date_of_birth"].isoformat(),
            "age": reported_age,
            "gender": victim["gender"],
            "nationality": maybe_null(victim["nationality"], rng),
        },
        "narrative": maybe_null(faker.sentence(nb_words=12), rng),
        "_data_quality_flag": dq_flag,
        "_ingested_at": generated_now.replace(tzinfo=timezone.utc).isoformat(),
    }, crime_id, crime_dt, crime_type


def generate_court_case(crime_id, crime_dt, crime_type, rng, faker, defendant_factory, judge_pool, judge_counter, generated_now):
    case_id = new_business_id("CASE", rng)
    verdict_dt = crime_dt + timedelta(days=rng.randint(15, 400))
    defendant = defendant_factory.create()
    judge = get_judge(judge_pool, judge_counter, rng, faker)
    verdict = rng.choice(VERDICT_OUTCOMES)

    sentence_type = None
    sentence_duration = None
    sentence_unit = None
    bail_amount = None
    fine_amount = None

    if verdict == "Guilty - Custodial":
        sentence_type = "Imprisonment"
        sentence_unit = rng.choice(["months", "years"])
        sentence_duration = rng.randint(1, 11) if sentence_unit == "months" else rng.randint(1, 20)
        bail_amount = round(rng.uniform(500, 50000), 2)
    elif verdict == "Guilty - Fine Only":
        sentence_type = "Fine"
        fine_amount = round(rng.uniform(100, 25000), 2)
    elif verdict == "Guilty - Probation":
        sentence_type = "Probation"
        sentence_unit = "months"
        sentence_duration = rng.randint(3, 24)
        fine_amount = round(rng.uniform(0, 5000), 2)

    if rng.random() < LOGICAL_ERROR_RATE:
        if bail_amount is not None:
            bail_amount = -abs(bail_amount)
        elif fine_amount is not None:
            fine_amount = -abs(fine_amount)

    age_at_verdict = compute_age(defendant["date_of_birth"], verdict_dt.date())

    record = {
        "source_system": "court_registry",
        "case_id": case_id,
        "linked_crime_id": crime_id,
        "crime_type": crime_type,
        "case_status": rng.choice(["Closed", "Closed", "Closed", "Under Appeal"]),
        "verdict": verdict,
        "verdict_date": format_date_source2(verdict_dt, rng),
        "judge": judge,
        "bail_amount": bail_amount,
        "fine_amount": fine_amount,
        "sentence_type": sentence_type,
        "sentence_duration": sentence_duration,
        "sentence_unit": sentence_unit,
        "defendant": {
            "national_id": mutate_national_id(defendant["national_id"], rng),
            "full_name": maybe_null(defendant["full_name"], rng),
            "date_of_birth": defendant["date_of_birth"].strftime("%d/%m/%Y"),
            "age": maybe_null(age_at_verdict, rng),
            "gender": defendant["gender"],
            "nationality": defendant["nationality"],
        },
        "_ingested_at": generated_now.replace(tzinfo=timezone.utc).isoformat(),
    }

    return (
        record, case_id, defendant, verdict_dt,
        verdict == "Guilty - Custodial",
        sentence_duration, sentence_unit
    )


def generate_corrections_record(case_id, defendant, verdict_dt, sentence_duration, sentence_unit, rng, generated_now):
    imprisonment_start = verdict_dt + timedelta(days=rng.randint(0, 14))
    unit_days = 365 if sentence_unit == "years" else 30
    planned_days = sentence_duration * unit_days

    release_reason = rng.choice(RELEASE_REASONS)
    actual_days = planned_days if release_reason == "Served Full Term" else int(planned_days * rng.uniform(0.2, 0.9))
    release_dt = imprisonment_start + timedelta(days=max(actual_days, 1))

    if rng.random() < LOGICAL_ERROR_RATE:
        release_dt = imprisonment_start - timedelta(days=rng.randint(1, 30))

    return {
        "source_system": "corrections_system",
        "record_id": new_business_id("COR", rng),
        "linked_case_id": case_id,
        "defendant": {
            "national_id": mutate_national_id(defendant["national_id"], rng),
            "full_name": maybe_null(defendant["full_name"], rng),
        },
        "prison_name": maybe_null(rng.choice(PRISON_NAMES), rng),
        "imprisonment_start_date": format_date_source3(imprisonment_start, rng),
        "release_date": format_date_source3(release_dt, rng),
        "release_reason": release_reason,
        "_ingested_at": generated_now.replace(tzinfo=timezone.utc).isoformat(),
    }


def json_default(obj):
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not JSON serializable")


def generate_to_jsonl(num_crimes: int, outdir: str, seed=None, generated_at=None):
    if num_crimes <= 0:
        raise ValueError("num_crimes must be > 0")

    rng = random.Random(seed)
    Faker.seed(seed)
    faker = Faker()
    generated_now = (
        datetime.fromisoformat(generated_at.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)
        if generated_at else utc_now().replace(tzinfo=None)
    )

    # Distinct source populations.
    victim_factory = PersonFactory(rng, faker, start=1)
    defendant_factory = PersonFactory(rng, faker, start=1)

    officer_pool = []
    judge_pool = []
    officer_counter = [0]
    judge_counter = [0]

    batch_id = f"{generated_now.strftime('%Y%m%dT%H%M%SZ')}_{rng.getrandbits(64):016X}"
    batch_root = Path(outdir) / f"batch_{batch_id}"

    police_path = batch_root / "police" / f"police_{batch_id}.jsonl"
    court_path = batch_root / "court" / f"court_{batch_id}.jsonl"
    corrections_path = batch_root / "corrections" / f"corrections_{batch_id}.jsonl"

    police_path.parent.mkdir(parents=True, exist_ok=True)
    court_path.parent.mkdir(parents=True, exist_ok=True)
    corrections_path.parent.mkdir(parents=True, exist_ok=True)

    counts = {"police": 0, "court": 0, "corrections": 0}

    with police_path.open("w", encoding="utf-8") as police_file, \
         court_path.open("w", encoding="utf-8") as court_file, \
         corrections_path.open("w", encoding="utf-8") as corrections_file:

        for _ in range(num_crimes):
            crime, crime_id, crime_dt, crime_type = generate_crime_record(
                rng, faker, victim_factory, officer_pool, officer_counter, generated_now
            )
            police_file.write(json.dumps(crime, ensure_ascii=False, default=json_default) + "\n")
            counts["police"] += 1

            (
                court, case_id, defendant, verdict_dt,
                goes_to_prison, sentence_duration, sentence_unit
            ) = generate_court_case(
                crime_id, crime_dt, crime_type, rng, faker,
                defendant_factory, judge_pool, judge_counter, generated_now
            )
            court_file.write(json.dumps(court, ensure_ascii=False, default=json_default) + "\n")
            counts["court"] += 1

            if goes_to_prison:
                corrections = generate_corrections_record(
                    case_id, defendant, verdict_dt, sentence_duration,
                    sentence_unit, rng, generated_now
                )
                corrections_file.write(json.dumps(corrections, ensure_ascii=False, default=json_default) + "\n")
                counts["corrections"] += 1

    return {
        **counts,
        "batch_id": batch_id,
        "batch_root": str(batch_root),
        "police_path": str(police_path),
        "court_path": str(court_path),
        "corrections_path": str(corrections_path),
    }

def continuous_loop(batch_size: int, outdir: str, interval: int, seed=None):
    """Generate new linked batches continuously until interrupted."""
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    if interval < 0:
        raise ValueError("interval must be >= 0")

    while running:
        try:
            result = generate_to_jsonl(
                batch_size,
                outdir,
                seed=seed,
            )
            print(
                f"[generator] batch={result['batch_id']} | "
                f"police={result['police']} | "
                f"court={result['court']} | "
                f"corrections={result['corrections']}",
                flush=True,
            )
        except Exception as exc:
            print(
                f"[generator] ERROR generating batch: {exc}",
                file=sys.stderr,
                flush=True,
            )

        for _ in range(interval):
            if not running:
                break
            time.sleep(1)


def parse_args():
    p = argparse.ArgumentParser(
        description="EuroCrimePulse relational JSONL generator"
    )
    p.add_argument(
        "--num-crimes",
        type=int,
        default=NUM_CRIMES,
        help="Number of crimes in one batch.",
    )
    p.add_argument(
        "--outdir",
        default="./landing",
        help="Output landing directory.",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional deterministic RNG seed.",
    )
    p.add_argument(
        "--generated-at",
        default=None,
        help="ISO-8601 UTC time for deterministic tests.",
    )
    p.add_argument(
        "--continuous",
        action="store_true",
        help="Continuously generate batches.",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=20,
        help="Crimes per batch in continuous mode.",
    )
    p.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Seconds between continuous batches.",
    )
    return p.parse_args()


def handle_sig(signum, frame):
    global running
    running = False


running = True


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    args = parse_args()
    Path(args.outdir).mkdir(parents=True, exist_ok=True)

    if args.continuous:
        print(
            f"Starting continuous generator: "
            f"batch_size={args.batch_size}, "
            f"interval={args.interval}s, "
            f"outdir={args.outdir}",
            flush=True,
        )
        continuous_loop(
            args.batch_size,
            args.outdir,
            args.interval,
            args.seed,
        )
        print("Generator stopped", flush=True)
    else:
        result = generate_to_jsonl(
            args.num_crimes,
            args.outdir,
            seed=args.seed,
            generated_at=args.generated_at,
        )
        print(
            f"batch={result['batch_id']} | "
            f"police={result['police']} | "
            f"court={result['court']} | "
            f"corrections={result['corrections']}"
        )
        print(f"batch_root={result['batch_root']}")
