"""Generate realistic HL7 v2.5.1 ADT messages for ED & ICU dashboard testing.

Usage:
  python generate_sample_data.py                          # generate locally only
  python generate_sample_data.py --upload                 # generate and upload to landing volume
  python generate_sample_data.py --upload --catalog users --schema ankur_nayyar --volume landing
"""
import argparse
import random
import os
import subprocess
import shutil
from datetime import datetime, timedelta

random.seed(42)

FACILITIES = ["HOSPITAL_A", "HOSPITAL_B"]
ED_LOCATIONS = ["ED^E01^A", "ED^E02^B", "ED^E03^A", "ED^TRIAGE^A", "ER^E04^B", "ER^E05^A"]
ICU_LOCATIONS = ["ICU^101^A", "ICU^102^B", "ICU^103^A", "MICU^201^A", "SICU^301^B", "CCU^401^A"]
MED_LOCATIONS = ["MED^401^A", "MED^402^B", "SURG^501^A", "OBS^601^B"]

LAST_NAMES = ["DOE", "SMITH", "JOHNSON", "WILLIAMS", "BROWN", "JONES", "GARCIA", "MARTINEZ",
              "ANDERSON", "TAYLOR", "THOMAS", "HERNANDEZ", "MOORE", "MARTIN", "JACKSON",
              "THOMPSON", "WHITE", "LOPEZ", "LEE", "GONZALEZ", "HARRIS", "CLARK", "LEWIS",
              "ROBINSON", "WALKER", "PEREZ", "HALL", "YOUNG", "ALLEN", "KING", "WRIGHT",
              "SCOTT", "TORRES", "NGUYEN", "HILL", "FLORES", "GREEN", "ADAMS", "NELSON", "BAKER"]
FIRST_NAMES = ["JOHN", "JANE", "MICHAEL", "SARAH", "ROBERT", "EMILY", "DAVID", "JESSICA",
               "JAMES", "ASHLEY", "WILLIAM", "JENNIFER", "RICHARD", "AMANDA", "JOSEPH", "STEPHANIE",
               "THOMAS", "NICOLE", "CHRISTOPHER", "ELIZABETH", "DANIEL", "MARIA", "MATTHEW", "LISA",
               "ANTHONY", "NANCY", "MARK", "KAREN", "CHARLES", "BETTY", "STEVEN", "MARGARET",
               "PAUL", "SANDRA", "ANDREW", "DOROTHY", "JOSHUA", "KIMBERLY", "KENNETH", "DONNA"]

DIAGNOSES_ED = [
    ("R07.9", "CHEST PAIN UNSPECIFIED"),
    ("S52.501A", "FRACTURE RADIUS"),
    ("J06.9", "UPPER RESPIRATORY INFECTION"),
    ("R10.9", "ABDOMINAL PAIN UNSPECIFIED"),
    ("S09.90XA", "HEAD INJURY"),
    ("T78.2XXA", "ANAPHYLAXIS"),
    ("I21.9", "ACUTE MYOCARDIAL INFARCTION"),
    ("R55", "SYNCOPE"),
    ("K35.80", "ACUTE APPENDICITIS"),
    ("J18.9", "PNEUMONIA"),
]
DIAGNOSES_ICU = [
    ("J96.01", "ACUTE RESPIRATORY FAILURE"),
    ("I21.3", "STEMI"),
    ("I50.21", "ACUTE SYSTOLIC HEART FAILURE"),
    ("A41.9", "SEPSIS UNSPECIFIED"),
    ("S06.5X0A", "TRAUMATIC SUBDURAL HEMORRHAGE"),
    ("G93.1", "ANOXIC BRAIN INJURY"),
    ("J80", "ARDS"),
    ("K72.00", "HEPATIC FAILURE ACUTE"),
]

PROVIDERS = [f"{1000+i}^{ln}^{fn}^^^DR^MD" for i, (ln, fn) in enumerate(zip(LAST_NAMES[:15], FIRST_NAMES[:15]))]
ALLERGIES = [
    ("70618", "PENICILLIN", "DA", "DRUG ALLERGY", "MO", "MODERATE", "RASH"),
    ("91935009", "PEANUT", "FA", "FOOD ALLERGY", "SV", "SEVERE", "ANAPHYLAXIS"),
    ("2670", "CODEINE", "DA", "DRUG ALLERGY", "MI", "MILD", "NAUSEA"),
    ("7980", "SULFONAMIDE", "DA", "DRUG ALLERGY", "MO", "MODERATE", "HIVES"),
]

VITALS = [
    ("8867-4", "HEART RATE", "/min", (60, 120)),
    ("8480-6", "SYSTOLIC BP", "mm[Hg]", (90, 180)),
    ("8462-4", "DIASTOLIC BP", "mm[Hg]", (50, 110)),
    ("8310-5", "BODY TEMPERATURE", "[degF]", (970, 1040)),  # x10
    ("9279-1", "RESPIRATORY RATE", "/min", (10, 30)),
    ("59408-5", "SPO2", "%", (88, 100)),
]


def fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S")


def make_pid(mrn: str, last: str, first: str, dob: str, sex: str, facility: str) -> str:
    city = random.choice(["ANYTOWN", "RIVERSIDE", "CORONA", "FONTANA", "ONTARIO", "POMONA"])
    return (f"PID|1||{mrn}^^^{facility}^MR||{last}^{first}^{random.choice('ABCDEFGHIJKLM')}||"
            f"{dob}|{sex}|||{random.randint(100,999)} {random.choice(['MAIN','OAK','ELM','PINE','MAPLE'])} "
            f"ST^^{city}^CA^{random.randint(90000,92999)}")


def make_obx(set_id: int, ts: datetime) -> str:
    code, name, unit, (lo, hi) = random.choice(VITALS)
    val = random.randint(lo, hi)
    if code == "8310-5":
        val = val / 10.0
    return f"OBX|{set_id}|NM|{code}^{name}^LN||{val}|{unit}|||N|||F|||{fmt_ts(ts)}"


def make_al1(set_id: int) -> str:
    a = random.choice(ALLERGIES)
    return f"AL1|{set_id}|{a[2]}^{a[3]}^HL70127|{a[0]}^{a[1]}^RxNorm|{a[4]}^{a[5]}^HL70128|{a[6]}|20200101"


def generate_adt_a01(msg_id: int, ts: datetime, mrn: str, last: str, first: str,
                     dob: str, sex: str, facility: str, location: str, provider: str,
                     dx_list: list) -> str:
    lines = [
        f"MSH|^~\\&|EPIC|{facility}|DATABRICKS|ANALYTICS|{fmt_ts(ts)}||ADT^A01^ADT_A01|MSG{msg_id:06d}|P|2.5.1|||AL|NE",
        f"EVN|A01|{fmt_ts(ts)}",
        make_pid(mrn, last, first, dob, sex, facility),
        f"PV1|1|{'I' if 'ICU' in location or 'MICU' in location or 'SICU' in location or 'CCU' in location else 'E'}|"
        f"{location}^{facility}||||{provider}|||MED||||||||V{msg_id:04d}||||||||||||||||||||{fmt_ts(ts - timedelta(minutes=random.randint(10, 60)))}",
    ]
    for i in range(random.randint(3, 5)):
        lines.append(make_obx(i + 1, ts - timedelta(minutes=random.randint(5, 30))))
    if random.random() < 0.4:
        lines.append(make_al1(1))
    for i, (code, desc) in enumerate(random.sample(dx_list, k=min(2, len(dx_list)))):
        lines.append(f"DG1|{i+1}|ICD10|{code}^{desc}^ICD10||{fmt_ts(ts)}|{'A' if i == 0 else 'W'}^{'ADMITTING' if i == 0 else 'WORKING'}^HL70052")
    if random.random() < 0.3:
        lines.append(f"IN1|1|BCBS123^BLUE CROSS BLUE SHIELD|123456^BCBS OF CALIFORNIA|BLUE CROSS BLUE SHIELD||||||||||||01^PATIENT^HL70063")
    return "\r".join(lines)


def generate_adt_a03(msg_id: int, ts: datetime, mrn: str, last: str, first: str,
                     dob: str, sex: str, facility: str, location: str, provider: str,
                     admit_ts: datetime) -> str:
    lines = [
        f"MSH|^~\\&|EPIC|{facility}|DATABRICKS|ANALYTICS|{fmt_ts(ts)}||ADT^A03^ADT_A03|MSG{msg_id:06d}|P|2.5.1|||AL|NE",
        f"EVN|A03|{fmt_ts(ts)}",
        make_pid(mrn, last, first, dob, sex, facility),
        f"PV1|1|{'I' if 'ICU' in location or 'MICU' in location or 'SICU' in location or 'CCU' in location else 'E'}|"
        f"{location}^{facility}||||{provider}|||MED||||||||V{msg_id:04d}||||||||||||||||||||{fmt_ts(admit_ts)}|{fmt_ts(ts)}",
    ]
    return "\r".join(lines)


def generate_adt_a08(msg_id: int, ts: datetime, mrn: str, last: str, first: str,
                     dob: str, sex: str, facility: str, location: str, provider: str) -> str:
    lines = [
        f"MSH|^~\\&|EPIC|{facility}|DATABRICKS|ANALYTICS|{fmt_ts(ts)}||ADT^A08^ADT_A08|MSG{msg_id:06d}|P|2.5.1|||AL|NE",
        f"EVN|A08|{fmt_ts(ts)}",
        make_pid(mrn, last, first, dob, sex, facility),
        f"PV1|1|I|{location}^{facility}||||{provider}|||MED||||||||V{msg_id:04d}||||||||||||||||||||{fmt_ts(ts - timedelta(hours=random.randint(1, 8)))}",
    ]
    for i in range(random.randint(2, 4)):
        lines.append(make_obx(i + 1, ts))
    return "\r".join(lines)


def generate_oru(msg_id: int, ts: datetime, mrn: str, last: str, first: str,
                 dob: str, sex: str, facility: str) -> str:
    lines = [
        f"MSH|^~\\&|LAB|{facility}|EMR|{facility}|{fmt_ts(ts)}||ORU^R01^ORU_R01|MSG{msg_id:06d}|P|2.5.1|||AL|NE",
        make_pid(mrn, last, first, dob, sex, facility),
        f"OBR|1|ORD{msg_id:04d}|FIL{msg_id:04d}|80053^COMPREHENSIVE METABOLIC PANEL|||{fmt_ts(ts - timedelta(hours=1))}",
    ]
    lab_tests = [
        ("2345-7", "GLUCOSE", "mg/dL", 70, 140),
        ("2160-0", "CREATININE", "mg/dL", 5, 20),  # x10
        ("3094-0", "BUN", "mg/dL", 7, 35),
        ("2951-2", "SODIUM", "mEq/L", 130, 150),
        ("2823-3", "POTASSIUM", "mEq/L", 30, 55),  # x10
    ]
    for i, (code, name, unit, lo, hi) in enumerate(lab_tests):
        val = random.randint(lo, hi)
        if code in ("2160-0", "2823-3"):
            val = val / 10.0
        lines.append(f"OBX|{i+1}|NM|{code}^{name}^LN||{val}|{unit}|||N|||F|||{fmt_ts(ts)}")
    return "\r".join(lines)


def main():
    start_date = datetime(2024, 1, 10, 0, 0, 0)
    num_days = 14
    msg_id = 1
    all_messages = []

    patients = []
    for i in range(80):
        patients.append({
            "mrn": f"MRN{300000 + i}",
            "last": random.choice(LAST_NAMES),
            "first": random.choice(FIRST_NAMES),
            "dob": f"{random.randint(1940, 2005)}{random.randint(1,12):02d}{random.randint(1,28):02d}",
            "sex": random.choice(["M", "F"]),
        })

    for day_offset in range(num_days):
        current_day = start_date + timedelta(days=day_offset)
        is_weekend = current_day.weekday() >= 5
        ed_daily = random.randint(40, 70) if not is_weekend else random.randint(55, 90)
        icu_daily = random.randint(8, 18)

        for hour in range(24):
            ts_base = current_day + timedelta(hours=hour)

            hour_weight = 1.0
            if 10 <= hour <= 14:
                hour_weight = 2.0
            elif 18 <= hour <= 22:
                hour_weight = 1.6
            elif 6 <= hour <= 9:
                hour_weight = 1.3
            elif 2 <= hour <= 5:
                hour_weight = 0.3

            ed_arrivals = max(1, round(ed_daily / 24 * hour_weight * random.uniform(0.6, 1.4)))
            for _ in range(ed_arrivals):
                pt = random.choice(patients)
                facility = random.choice(FACILITIES)
                loc = random.choice(ED_LOCATIONS)
                provider = random.choice(PROVIDERS)
                ts = ts_base + timedelta(minutes=random.randint(0, 59))
                msg = generate_adt_a01(msg_id, ts, pt["mrn"], pt["last"], pt["first"],
                                       pt["dob"], pt["sex"], facility, loc, provider, DIAGNOSES_ED)
                all_messages.append((ts, msg))
                msg_id += 1

                if random.random() < 0.7:
                    dc_ts = ts + timedelta(hours=random.randint(2, 8), minutes=random.randint(0, 59))
                    msg = generate_adt_a03(msg_id, dc_ts, pt["mrn"], pt["last"], pt["first"],
                                           pt["dob"], pt["sex"], facility, loc, provider, ts)
                    all_messages.append((dc_ts, msg))
                    msg_id += 1

                if random.random() < 0.5:
                    lab_ts = ts + timedelta(hours=random.randint(1, 3))
                    msg = generate_oru(msg_id, lab_ts, pt["mrn"], pt["last"], pt["first"],
                                       pt["dob"], pt["sex"], facility)
                    all_messages.append((lab_ts, msg))
                    msg_id += 1

            icu_arrivals = max(0, round(icu_daily / 24 * hour_weight * random.uniform(0.3, 1.4)))
            for _ in range(icu_arrivals):
                pt = random.choice(patients)
                facility = random.choice(FACILITIES)
                loc = random.choice(ICU_LOCATIONS)
                provider = random.choice(PROVIDERS)
                ts = ts_base + timedelta(minutes=random.randint(0, 59))
                msg = generate_adt_a01(msg_id, ts, pt["mrn"], pt["last"], pt["first"],
                                       pt["dob"], pt["sex"], facility, loc, provider, DIAGNOSES_ICU)
                all_messages.append((ts, msg))
                msg_id += 1

                if random.random() < 0.4:
                    dc_ts = ts + timedelta(hours=random.randint(24, 120), minutes=random.randint(0, 59))
                    msg = generate_adt_a03(msg_id, dc_ts, pt["mrn"], pt["last"], pt["first"],
                                           pt["dob"], pt["sex"], facility, loc, provider, ts)
                    all_messages.append((dc_ts, msg))
                    msg_id += 1

                if random.random() < 0.6:
                    upd_ts = ts + timedelta(hours=random.randint(2, 12))
                    msg = generate_adt_a08(msg_id, upd_ts, pt["mrn"], pt["last"], pt["first"],
                                           pt["dob"], pt["sex"], facility, loc, provider)
                    all_messages.append((upd_ts, msg))
                    msg_id += 1

                if random.random() < 0.7:
                    lab_ts = ts + timedelta(hours=random.randint(1, 6))
                    msg = generate_oru(msg_id, lab_ts, pt["mrn"], pt["last"], pt["first"],
                                       pt["dob"], pt["sex"], facility)
                    all_messages.append((lab_ts, msg))
                    msg_id += 1

    all_messages.sort(key=lambda x: x[0])

    out_dir = os.path.join(os.path.dirname(__file__), "generated")
    os.makedirs(out_dir, exist_ok=True)

    batch_size = 50
    file_num = 1
    for i in range(0, len(all_messages), batch_size):
        batch = all_messages[i:i + batch_size]
        filename = os.path.join(out_dir, f"hl7_batch_{file_num:04d}.hl7")
        with open(filename, "w") as f:
            for _, msg in batch:
                f.write(msg + "\n\n")
        file_num += 1

    print(f"Generated {msg_id - 1} messages in {file_num - 1} files under {out_dir}")
    return out_dir, file_num - 1


def upload_to_volume(local_dir: str, catalog: str, schema: str, volume: str):
    volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
    cli = shutil.which("databricks") or "/usr/local/bin/databricks"

    print(f"\nUploading files from {local_dir} to {volume_path} ...")
    result = subprocess.run(
        [cli, "fs", "cp", "--recursive", "--overwrite", local_dir, f"dbfs:{volume_path}"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"Upload failed: {result.stderr}")
        raise SystemExit(1)

    print(f"Upload complete. Files available at {volume_path}")

    result = subprocess.run(
        [cli, "fs", "ls", f"dbfs:{volume_path}", "--output", "text"],
        capture_output=True, text=True
    )
    file_count = len([l for l in result.stdout.strip().split("\n") if l.strip() and l.strip().endswith(".hl7")])
    print(f"Verified: {file_count} .hl7 files in {volume_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate and optionally upload HL7 sample data")
    parser.add_argument("--upload", action="store_true", help="Upload generated files to Databricks landing volume")
    parser.add_argument("--catalog", default="users", help="Unity Catalog name (default: users)")
    parser.add_argument("--schema", default="ankur_nayyar", help="Schema name (default: ankur_nayyar)")
    parser.add_argument("--volume", default="landing", help="Volume name (default: landing)")
    args = parser.parse_args()

    out_dir, num_files = main()

    if args.upload:
        upload_to_volume(out_dir, args.catalog, args.schema, args.volume)
