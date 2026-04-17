import psycopg2
import random
from faker import Faker
from datetime import timedelta

fake = Faker()

conn = psycopg2.connect(
    host="localhost",
    database="thesis_db",
    user="postgres",
    password="postgres123",
    port="5440"
)

cur = conn.cursor()

genders = ["Male", "Female"]

diagnosis_config = {
    "Diabetes": {
        "department": "Endocrinology",
        "medications": ["Metformin", "Insulin"],
        "lab_tests": ["Glucose", "Cholesterol"],
    },
    "Hypertension": {
        "department": "Cardiology",
        "medications": ["Amlodipine", "Atorvastatin"],
        "lab_tests": ["Cholesterol", "CRP"],
    },
    "Infection": {
        "department": "General Medicine",
        "medications": ["Paracetamol", "Ibuprofen"],
        "lab_tests": ["CRP", "Hemoglobin"],
    },
    "General Checkup": {
        "department": "General Medicine",
        "medications": ["Paracetamol"],
        "lab_tests": ["Glucose", "Hemoglobin"],
    }
}


def generate_height_weight(age, gender):
    if gender == "Male":
        height_cm = random.randint(165, 190)
    else:
        height_cm = random.randint(150, 178)

    base_bmi = random.uniform(18.5, 32.0)

    if age > 50:
        base_bmi += random.uniform(1.0, 3.0)

    height_m = height_cm / 100
    weight_kg = round(base_bmi * (height_m ** 2), 2)

    return height_cm, weight_kg


def calculate_bmi(height_cm, weight_kg):
    height_m = height_cm / 100
    return round(weight_kg / (height_m ** 2), 2)


def choose_diagnosis(age, bmi):
    r = random.random()

    if bmi >= 30:
        if r < 0.45:
            return "Diabetes"
        elif r < 0.80:
            return "Hypertension"
        elif r < 0.90:
            return "Infection"
        else:
            return "General Checkup"

    elif bmi >= 25:
        if r < 0.35:
            return "Hypertension"
        elif r < 0.65:
            return "Diabetes"
        elif r < 0.80:
            return "Infection"
        else:
            return "General Checkup"

    else:
        if age > 55:
            if r < 0.30:
                return "Hypertension"
            elif r < 0.50:
                return "Diabetes"
            elif r < 0.70:
                return "Infection"
            else:
                return "General Checkup"
        else:
            if r < 0.15:
                return "Diabetes"
            elif r < 0.30:
                return "Hypertension"
            elif r < 0.55:
                return "Infection"
            else:
                return "General Checkup"


def generate_baseline(diagnosis, bmi):
    if diagnosis == "Diabetes":
        return {
            "glucose": random.randint(165, 240),
            "bp": random.randint(125, 150) if bmi >= 25 else random.randint(120, 145),
            "temperature": round(random.uniform(36.4, 37.4), 1)
        }
    elif diagnosis == "Hypertension":
        return {
            "glucose": random.randint(90, 125),
            "bp": random.randint(150, 190),
            "temperature": round(random.uniform(36.3, 37.2), 1)
        }
    elif diagnosis == "Infection":
        return {
            "glucose": random.randint(90, 130),
            "bp": random.randint(110, 140),
            "temperature": round(random.uniform(37.8, 39.4), 1)
        }
    else:
        return {
            "glucose": random.randint(85, 115),
            "bp": random.randint(110, 130),
            "temperature": round(random.uniform(36.2, 37.0), 1)
        }


def generate_heart_rate(diagnosis, temperature):
    if diagnosis == "Infection":
        return random.randint(90, 120)
    elif diagnosis == "Hypertension":
        return random.randint(75, 100)
    elif diagnosis == "Diabetes":
        return random.randint(70, 95)
    else:
        return random.randint(78, 98) if temperature > 37.0 else random.randint(60, 85)


def generate_lab_result(test_name, diagnosis, glucose):
    if test_name == "Glucose":
        if diagnosis == "Diabetes":
            return round(random.uniform(glucose - 10, glucose + 15), 2)
        return round(random.uniform(85, 120), 2)

    if test_name == "Cholesterol":
        if diagnosis == "Hypertension":
            return round(random.uniform(190, 280), 2)
        elif diagnosis == "Diabetes":
            return round(random.uniform(170, 250), 2)
        return round(random.uniform(140, 210), 2)

    if test_name == "Hemoglobin":
        if diagnosis == "Infection":
            return round(random.uniform(11.0, 14.0), 2)
        return round(random.uniform(12.5, 16.5), 2)

    if test_name == "CRP":
        if diagnosis == "Infection":
            return round(random.uniform(20, 100), 2)
        elif diagnosis == "Hypertension":
            return round(random.uniform(5, 20), 2)
        return round(random.uniform(0.5, 10), 2)

    return round(random.uniform(50, 150), 2)


def choose_dosage(medication):
    dosage_map = {
        "Metformin": "500mg",
        "Insulin": "10 units",
        "Amlodipine": "5mg",
        "Atorvastatin": "20mg",
        "Paracetamol": "500mg",
        "Ibuprofen": "400mg"
    }
    return dosage_map.get(medication, "500mg")


try:
    for i in range(20000):
        name = fake.name()
        age = random.randint(18, 85)
        gender = random.choice(genders)

        height_cm, weight_kg = generate_height_weight(age, gender)
        bmi = calculate_bmi(height_cm, weight_kg)

        cur.execute("""
            INSERT INTO patients (name, age, gender, height_cm, weight_kg)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING patient_id
        """, (name, age, gender, height_cm, weight_kg))

        patient_id = cur.fetchone()[0]

        diagnosis = choose_diagnosis(age, bmi)
        department = diagnosis_config[diagnosis]["department"]
        baseline = generate_baseline(diagnosis, bmi)

        visits = random.randint(2, 5)
        visit_date = fake.date_time_between(start_date='-2y', end_date='-6mo')

        for visit in range(visits):
            if visit > 0:
                visit_date += timedelta(days=random.randint(15, 45))

            if diagnosis == "Diabetes":
                glucose = max(90, baseline["glucose"] - visit * random.randint(6, 12) + random.randint(-5, 5))
                bp = max(110, baseline["bp"] + random.randint(-4, 4))
                temperature = round(max(36.0, baseline["temperature"] + random.uniform(-0.2, 0.3)), 1)

            elif diagnosis == "Hypertension":
                bp = max(115, baseline["bp"] - visit * random.randint(4, 8) + random.randint(-3, 3))
                glucose = max(80, baseline["glucose"] + random.randint(-8, 8))
                temperature = round(max(36.0, baseline["temperature"] + random.uniform(-0.2, 0.2)), 1)

            elif diagnosis == "Infection":
                temperature = round(max(36.5, baseline["temperature"] - visit * random.uniform(0.3, 0.8) + random.uniform(-0.1, 0.2)), 1)
                glucose = max(80, baseline["glucose"] + random.randint(-8, 8))
                bp = max(100, baseline["bp"] + random.randint(-6, 6))

            else:
                glucose = max(80, baseline["glucose"] + random.randint(-6, 6))
                bp = max(100, baseline["bp"] + random.randint(-5, 5))
                temperature = round(max(36.0, baseline["temperature"] + random.uniform(-0.2, 0.2)), 1)

            heart_rate = generate_heart_rate(diagnosis, temperature)

            cur.execute("""
                INSERT INTO patient_visits
                (
                    patient_id, visit_number, visit_date,
                    blood_pressure, glucose_level, heart_rate, temperature,
                    diagnosis, department
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING visit_id
            """, (
                patient_id,
                visit + 1,
                visit_date,
                bp,
                glucose,
                heart_rate,
                temperature,
                diagnosis,
                department
            ))

            visit_id = cur.fetchone()[0]

            for test in diagnosis_config[diagnosis]["lab_tests"]:
                test_result = generate_lab_result(test, diagnosis, glucose)

                cur.execute("""
                    INSERT INTO lab_results
                    (visit_id, test_name, test_result)
                    VALUES (%s, %s, %s)
                """, (
                    visit_id,
                    test,
                    test_result
                ))

            medication = random.choice(diagnosis_config[diagnosis]["medications"])
            dosage = choose_dosage(medication)
            duration_days = random.randint(5, 30)

            cur.execute("""
                INSERT INTO treatments
                (visit_id, medication, dosage, duration_days)
                VALUES (%s, %s, %s, %s)
            """, (
                visit_id,
                medication,
                dosage,
                duration_days
            ))

        if i % 500 == 0 and i != 0:
            conn.commit()
            print(i, "patients generated")

    conn.commit()
    print("Dataset generated successfully")

except Exception as e:
    conn.rollback()
    print("Error:", e)

finally:
    cur.close()
    conn.close()