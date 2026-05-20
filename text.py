import sqlite3
import json
import requests
import time
import os
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import threading

# --- CONFIGURATION & SETTINGS ---
DIST_DB_PATH = "DistrictAndBlockData.db"
SCHOOL_DB_PATH = "GetSchoolData.db"
STUDENT_DB_PATH = "GetStudentDetails.db"

# Thread Safety ke liye database lock
db_lock = threading.Lock()

def init_student_db():
    """Output database and table schema initialization"""
    conn = sqlite3.connect(STUDENT_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS student_records (
            student_unique_id TEXT PRIMARY KEY,
            student_name TEXT,
            father_name TEXT,
            mother_name TEXT,
            gender TEXT,
            date_of_birth TEXT,
            mobile_number TEXT,
            class_id TEXT,
            section TEXT,
            udise_code TEXT,
            school_name TEXT,
            school_id TEXT,
            district_name TEXT,
            block_name TEXT,
            cluster_name TEXT,
            academic_year TEXT,
            created_by TEXT,
            created_time TEXT,
            updated_by TEXT,
            updated_time TEXT,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def fetch_and_save_worker(udise_code, academic_year, class_id, pbar):
    """Yeh worker background me parallel requests throw karega"""
    url = 'https://jgurujiapi.jharkhand.gov.in/api/login/GetStudentDetails'
    headers = {
        'user-agent': 'Dart/3.7 (dart:io)',
        'apikey': 'J12SHA98IZ82938KPP',
        'host': 'jgurujiapi.jharkhand.gov.in',
    }
    params = {
        '': '',
        'AcademicYear': str(academic_year),
        'eVVStudentId': '',
        'UdiseCode': str(udise_code),
        'ClassId': str(class_id),
    }

    try:
        # High speed timeout layout
        response = requests.get(url, params=params, headers=headers, timeout=12)
        
        if response.status_code == 200:
            raw_parsed = None
            try:
                raw_data = response.json()
                raw_parsed = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
            except ValueError:
                try:
                    raw_parsed = json.loads(response.text)
                    if isinstance(raw_parsed, str):
                        raw_parsed = json.loads(raw_parsed)
                except ValueError:
                    return

            if isinstance(raw_parsed, dict):
                students = [raw_parsed]
            elif isinstance(raw_parsed, list):
                students = raw_parsed
            else:
                return

            if students:
                # Thread lock se safely database write handle karna
                with db_lock:
                    db_conn = sqlite3.connect(STUDENT_DB_PATH)
                    cursor = db_conn.cursor()
                    
                    for std in students:
                        if isinstance(std, str) or not std:
                            continue
                        
                        unique_id = std.get('StudentUniqueId')
                        if not unique_id or unique_id == 'N/A':
                            continue

                        gender_raw = std.get('GENDER')
                        gender_str = "Male 👦" if gender_raw == 1 else "Female 👧" if gender_raw == 2 else "N/A"

                        cursor.execute("""
                            INSERT OR REPLACE INTO student_records (
                                student_unique_id, student_name, father_name, mother_name, gender,
                                date_of_birth, mobile_number, class_id, section, udise_code,
                                school_name, school_id, district_name, block_name, cluster_name,
                                academic_year, created_by, created_time, updated_by, updated_time
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            str(unique_id), std.get('StudentName', 'N/A'), std.get('FatherName', 'N/A'),
                            std.get('MotherName', 'N/A'), gender_str, std.get('Date_of_Birth', 'N/A'),
                            std.get('Mobile_Number', 'N/A'), str(class_id), std.get('section', 'N/A'),
                            str(udise_code), std.get('Schoolname', 'N/A'), std.get('SchoolId', 'N/A'),
                            std.get('District_Name', 'N/A'), std.get('Block_Name', 'N/A'), std.get('Cluster_Name', 'N/A'),
                            str(academic_year), std.get('Created_By', 'N/A'), std.get('Created_Time', 'N/A'),
                            std.get('Updated_By', 'N/A'), std.get('Updated_Time', 'N/A')
                        ))
                    db_conn.commit()
                    db_conn.close()
    except Exception:
        pass
    finally:
        # Request chahe fail ho ya pass, progress bar ko update karein
        pbar.update(1)

def precalculate_total_requests():
    """Poore database ka loop chalakar pehle hi sahi total request count nikalna"""
    print("📊 Calculating total requests across the state... Please wait...")
    
    school_conn = sqlite3.connect(SCHOOL_DB_PATH)
    school_cursor = school_conn.cursor()
    
    school_cursor.execute("SELECT class_frm, class_to FROM schools")
    all_ranges = school_cursor.fetchall()
    school_conn.close()
    
    total_requests = 0
    for class_frm, class_to in all_ranges:
        try:
            start_class = int(float(class_frm)) if class_frm is not None else 1
            end_class = int(float(class_to)) if class_to is not None else 12
        except (ValueError, TypeError):
            start_class, end_class = 1, 12
            
        for class_id in range(start_class, end_class + 1):
            if class_id == start_class:
                total_requests += 4  # Base class ke liye 4 saal
            else:
                total_requests += 1  # Baaki classes ke liye 1 saal
                
    return total_requests

def start_statewide_automation():
    if not os.path.exists(DIST_DB_PATH) or not os.path.exists(SCHOOL_DB_PATH):
        print("❌ Databases missing!")
        return

    init_student_db()
    
    # 1. Total Requests kitni hongi pehle hi calculate karo
    total_expected_tasks = precalculate_total_requests()
    print(f"📈 Total Planned API Requests to fire: {total_expected_tasks}\n")
    
    dist_conn = sqlite3.connect(DIST_DB_PATH)
    dist_cursor = dist_conn.cursor()
    school_conn = sqlite3.connect(SCHOOL_DB_PATH)
    school_cursor = school_conn.cursor()

    dist_cursor.execute("SELECT district_id, district_name FROM districts ORDER BY district_id ASC")
    districts = dist_cursor.fetchall()

    # 2. Live Progress Bar Initialize karein terminal par
    pbar = tqdm(total=total_expected_tasks, desc="🚀 JGuruji State Progress", unit="req")

    # 10 schools ek sath process karne ke liye (Averaging ~4-5 requests per school, max_workers 40-50 safe hai)
    with ThreadPoolExecutor(max_workers=40) as executor:
        for dist_id, dist_name in districts:
            dist_cursor.execute("SELECT block_id, block_name FROM blocks WHERE district_id = ? ORDER BY block_id ASC", (dist_id,))
            blocks = dist_cursor.fetchall()

            for blk_id, blk_name in blocks:
                school_cursor.execute("""
                    SELECT school_code, class_frm, class_to 
                    FROM schools 
                    WHERE lms_district_id = ? AND lms_block_id = ?
                    ORDER BY school_code ASC
                """, (dist_id, blk_id))
                schools = school_cursor.fetchall()

                for school_code, class_frm, class_to in schools:
                    if not school_code:
                        continue

                    try:
                        start_class = int(float(class_frm)) if class_frm is not None else 1
                        end_class = int(float(class_to)) if class_to is not None else 12
                    except (ValueError, TypeError):
                        start_class, end_class = 1, 12

                    # Tasks ko background workers me throw karein
                    for class_id in range(start_class, end_class + 1):
                        if class_id == start_class:
                            target_years = ["2023-24", "2024-25", "2025-26", "2026-27"]
                        else:
                            target_years = ["2023-24"]

                        for yr in target_years:
                            executor.submit(fetch_and_save_worker, school_code, yr, class_id, pbar)
                            
    pbar.close()
    dist_conn.close()
    school_conn.close()
    print("\n🎉 Poora state level selection data task successfully complete ho gaya hai!")

if __name__ == "__main__":
    start_statewide_automation()