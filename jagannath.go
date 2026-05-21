package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// --- CONFIGURATION ---
const (
	DistDBPath     = "DistrictAndBlockData.db"
	SchoolDBPath   = "GetSchoolData.db"
	StudentDBPath  = "GetStudentDetails.db"
	LogFilePath    = "logs.dat"
	FailedFilePath = "failed_tasks.txt"
	ProxyURL       = "socks5://127.0.0.1:10080"
	MaxWorkers     = 50  
	BatchSize      = 500 
)

type Task struct {
	SchoolCode   string
	AcademicYear string
	ClassID      int
}

// Gov API ka RAW JSON format bina kisi change ke
type StudentAPIResponse struct {
	AcademicYear    string      `json:"AcademicYear"`
	SchoolId        string      `json:"SchoolId"`
	Schoolname      interface{} `json:"Schoolname"`
	DistrictId      interface{} `json:"DistrictId"`
	DistrictName    interface{} `json:"District_Name"`
	BlockId         interface{} `json:"BlockId"`
	BlockName       interface{} `json:"Block_Name"`
	ClusterId       interface{} `json:"ClusterId"`
	ClusterName     interface{} `json:"Cluster_Name"`
	StudentUniqueId string      `json:"StudentUniqueId"`
	StudentName     interface{} `json:"StudentName"`
	FatherName      interface{} `json:"FatherName"`
	MotherName      interface{} `json:"MotherName"`
	Gender          interface{} `json:"GENDER"`
	StudyingClass   interface{} `json:"StudyingClass"`
	Section         interface{} `json:"Section"`
	DateOfBirth     interface{} `json:"Date_of_Birth"`
	MobileNumber    interface{} `json:"Mobile_Number"`
	CreatedBy       interface{} `json:"Created_By"`
	CreatedTime     interface{} `json:"Created_Time"`
	UpdatedBy       interface{} `json:"Updated_By"`
	UpdatedTime     interface{} `json:"Updated_Time"`
}

// Database Columns matching raw data structure
type StudentRecord struct {
	AcademicYear    string
	SchoolID        string
	SchoolName      string
	DistrictID      string
	DistrictName    string
	BlockID         string
	BlockName       string
	ClusterID       string
	ClusterName     string
	UniqueID        string
	StudentName     string
	FatherName      string
	MotherName      string
	Gender          string
	ClassID         string
	Section         string
	DateOfBirth     string
	MobileNumber    string
	UdiseCode       string // Mapping track ke liye
	CreatedBy       string
	CreatedTime     string
	UpdatedBy       string
	UpdatedTime     string
}

var logFile *os.File
var failedFile *os.File
var failedMu sync.Mutex

func logMessage(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	fullLine := fmt.Sprintf("[%s] %s\n", timestamp, msg)

	fmt.Print(fullLine)
	if logFile != nil {
		_, _ = logFile.WriteString(fullLine)
	}
}

func logFailure(task Task, reason string) {
	failedMu.Lock()
	defer failedMu.Unlock()
	if failedFile != nil {
		line := fmt.Sprintf("UDISE: %s | Year: %s | Class: %d | Reason: %s\n", 
			task.SchoolCode, task.AcademicYear, task.ClassID, reason)
		_, _ = failedFile.WriteString(line)
	}
}

func getString(val interface{}) string {
	if val == nil {
		return ""
	}
	if str, ok := val.(string); ok {
		return str
	}
	return fmt.Sprintf("%v", val)
}

func initStudentDB() *sql.DB {
	db, err := sql.Open("sqlite3", StudentDBPath)
	if err != nil {
		logMessage("❌ Student DB open error: %v", err)
		os.Exit(1)
	}

	// Naya schema including all DistrictId, BlockId, ClusterId fields as text/raw
	_, err = db.Exec(`
		PRAGMA journal_mode=WAL;
		PRAGMA synchronous=NORMAL;
		PRAGMA cache_size=-64000; 
		CREATE TABLE IF NOT EXISTS student_records (
			student_unique_id TEXT PRIMARY KEY,
			academic_year TEXT,
			school_id TEXT,
			school_name TEXT,
			district_id TEXT,
			district_name TEXT,
			block_id TEXT,
			block_name TEXT,
			cluster_id TEXT,
			cluster_name TEXT,
			student_name TEXT, 
			father_name TEXT, 
			mother_name TEXT, 
			gender TEXT,
			class_id TEXT, 
			section TEXT, 
			udise_code TEXT,
			date_of_birth TEXT, 
			mobile_number TEXT, 
			created_by TEXT, 
			created_time TEXT, 
			updated_by TEXT, 
			updated_time TEXT,
			fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		logMessage("❌ DB Initialization Error: %v", err)
		os.Exit(1)
	}
	return db
}

func main() {
	var err error
	logFile, err = os.OpenFile(LogFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("❌ Failed to open log file %s: %v\n", LogFilePath, err)
		os.Exit(1)
	}
	defer logFile.Close()

	failedFile, err = os.OpenFile(FailedFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		logMessage("❌ Failed to create failure tracking file: %v", err)
		os.Exit(1)
	}
	defer failedFile.Close()

	logMessage("🔍 Step 1: Checking source databases...")
	if _, err := os.Stat(DistDBPath); os.IsNotExist(err) {
		logMessage("❌ Source DB missing: %s", DistDBPath)
		return
	}
	if _, err := os.Stat(SchoolDBPath); os.IsNotExist(err) {
		logMessage("❌ Source DB missing: %s", SchoolDBPath)
		return
	}
	logMessage("✅ Source databases found.")

	logMessage("🔍 Step 2: Initializing Student Database...")
	studentDB := initStudentDB()
	defer studentDB.Close()
	logMessage("✅ Student DB initialized successfully.")

	logMessage("🔍 Step 3: Opening School Database to fetch data...")
	schoolDB, err := sql.Open("sqlite3", SchoolDBPath)
	if err != nil {
		logMessage("❌ School DB Error: %v", err)
		return
	}
	defer schoolDB.Close()

	logMessage("🔍 Step 4: Running SQL Query on schools table...")
	rows, err := schoolDB.Query("SELECT school_code, class_frm, class_to FROM schools WHERE school_code IS NOT NULL AND school_code != ''")
	if err != nil {
		logMessage("❌ Query Error: %v", err)
		return
	}
	defer rows.Close()

	logMessage("🔍 Step 5: Processing rows and creating task queue...")
	var tasks []Task
	schoolCount := 0
	for rows.Next() {
		schoolCount++
		var schoolCode string
		var classFrm, classTo sql.NullString
		if err := rows.Scan(&schoolCode, &classFrm, &classTo); err != nil {
			continue
		}

		startClass, endClass := 1, 12
		if classFrm.Valid {
			if val, err := strconv.ParseFloat(classFrm.String, 64); err == nil {
				startClass = int(val)
			}
		}
		if classTo.Valid {
			if val, err := strconv.ParseFloat(classTo.String, 64); err == nil {
				endClass = int(val)
			}
		}

		for cid := startClass; cid <= endClass; cid++ {
			targetYears := []string{"2023-24"}
			if cid == startClass {
				targetYears = []string{"2023-24", "2024-25", "2025-26", "2026-27"}
			}
			for _, yr := range targetYears {
				tasks = append(tasks, Task{SchoolCode: schoolCode, AcademicYear: yr, ClassID: cid})
			}
		}
	}

	logMessage("📊 Total Schools scanned from DB: %d", schoolCount)
	totalTasks := len(tasks)
	if totalTasks == 0 {
		logMessage("❌ OOPS! Task Queue khali hai. schools table check karein.")
		return
	}
	logMessage("🚀 Tasks Prepared! Total API Requests to hit: %d", totalTasks)

	taskChan := make(chan Task, totalTasks)
	resultChan := make(chan []StudentRecord, MaxWorkers)

	proxyURL, _ := url.Parse(ProxyURL)
	transport := &http.Transport{Proxy: http.ProxyURL(proxyURL)}
	client := &http.Client{Transport: transport, Timeout: 12 * time.Second}

	logMessage("👷 Launching %d Parallel Workers...", MaxWorkers)
	var wg sync.WaitGroup
	for i := 0; i < MaxWorkers; i++ {
		wg.Add(1)
		go worker(taskChan, resultChan, client, &wg)
	}

	logMessage("📥 Pushing tasks into queue channel...")
	for _, task := range tasks {
		taskChan <- task
	}
	close(taskChan)
	logMessage("✅ All tasks pushed. Workers are processing data...")

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var buffer []StudentRecord
	processedCount := 0
	totalSaved := 0
	startTime := time.Now()

	logMessage("⏳ Waiting for API responses and saving chunks to DB...")
	for records := range resultChan {
		processedCount++
		if len(records) > 0 {
			buffer = append(buffer, records...)
		}

		if len(buffer) >= BatchSize || processedCount == totalTasks {
			if len(buffer) > 0 {
				saved := saveBatch(studentDB, buffer)
				totalSaved += saved
				buffer = nil
			}

			elapsed := time.Since(startTime).Seconds()
			speed := float64(processedCount) / elapsed
			logMessage("⚡ Progress: %d/%d APIs Done | Saved Students: %d | Speed: %.2f req/sec",
				processedCount, totalTasks, totalSaved, speed)
		}
	}

	logMessage("🎉 Mission Accomplished! Check '%s' if any schools failed.", FailedFilePath)
	logMessage("🎉 Total %d students saved in local DB.", totalSaved)
}

func worker(tasks <-chan Task, results chan<- []StudentRecord, client *http.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	apiURL := "https://jgurujiapi.jharkhand.gov.in/api/login/GetStudentDetails"

	for task := range tasks {
		req, err := http.NewRequest("GET", apiURL, nil)
		if err != nil {
			logFailure(task, fmt.Sprintf("Request Creation Failed: %v", err))
			results <- nil
			continue
		}

		req.Header.Set("user-agent", "Dart/3.7 (dart:io)")
		req.Header.Set("apikey", "J12SHA98IZ82938KPP")
		req.Header.Set("host", "jgurujiapi.jharkhand.gov.in")

		q := req.URL.Query()
		q.Add("", "")
		q.Add("AcademicYear", task.AcademicYear)
		q.Add("eVVStudentId", "")
		q.Add("UdiseCode", task.SchoolCode)
		q.Add("ClassId", strconv.Itoa(task.ClassID))
		req.URL.RawQuery = q.Encode()

		resp, err := client.Do(req)
		if err != nil {
			logFailure(task, fmt.Sprintf("Network/Proxy Error: %v", err))
			results <- nil
			continue
		}

		if resp.StatusCode != 200 {
			logFailure(task, fmt.Sprintf("HTTP Status Error: %d", resp.StatusCode))
			resp.Body.Close()
			results <- nil
			continue
		}

		bodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			logFailure(task, fmt.Sprintf("Body Read Error: %v", err))
			results <- nil
			continue
		}

		var rawData interface{}
		if err := json.Unmarshal(bodyBytes, &rawData); err != nil {
			logFailure(task, fmt.Sprintf("JSON Master Parse Error: %v", err))
			results <- nil
			continue
		}

		if strData, ok := rawData.(string); ok {
			if err := json.Unmarshal([]byte(strData), &rawData); err != nil {
				logFailure(task, fmt.Sprintf("JSON String Unmarshal Error: %v", err))
				results <- nil
				continue
			}
		}

		var apiStudents []StudentAPIResponse
		switch v := rawData.(type) {
		case []interface{}:
			tempBytes, _ := json.Marshal(v)
			json.Unmarshal(tempBytes, &apiStudents)
		case map[string]interface{}:
			tempBytes, _ := json.Marshal(v)
			var singleObj StudentAPIResponse
			if err := json.Unmarshal(tempBytes, &singleObj); err == nil {
				apiStudents = append(apiStudents, singleObj)
			}
		}

		if len(apiStudents) == 0 {
			results <- nil
			continue
		}

		var batchRecords []StudentRecord
		for _, std := range apiStudents {
			if std.StudentUniqueId == "" || std.StudentUniqueId == "N/A" {
				continue
			}

			// PURE DATA INJECTION: Response se raw values as-it-is uthai hain bina kisi logical change ke
			record := StudentRecord{
				UniqueID:        std.StudentUniqueId,
				AcademicYear:    std.AcademicYear,
				SchoolID:        std.SchoolId,
				SchoolName:      getString(std.Schoolname),
				DistrictID:      getString(std.DistrictId),
				DistrictName:    getString(std.DistrictName),
				BlockID:         getString(std.BlockId),
				BlockName:       getString(std.BlockName),
				ClusterID:       getString(std.ClusterId),
				ClusterName:     getString(std.ClusterName),
				StudentName:     getString(std.StudentName),
				FatherName:      getString(std.FatherName),
				MotherName:      getString(std.MotherName),
				Gender:          getString(std.Gender), // Ab 1 ya 2 jo response me hoga, wahi safe rahega
				ClassID:         getString(std.StudyingClass), // Direct raw key values
				Section:         getString(std.Section),
				DateOfBirth:     getString(std.DateOfBirth),
				MobileNumber:    getString(std.MobileNumber),
				UdiseCode:       task.SchoolCode,
				CreatedBy:       getString(std.CreatedBy),
				CreatedTime:     getString(std.CreatedTime),
				UpdatedBy:       getString(std.UpdatedBy),
				UpdatedTime:     getString(std.UpdatedTime),
			}
			batchRecords = append(batchRecords, record)
		}
		results <- batchRecords
	}
}

func saveBatch(db *sql.DB, records []StudentRecord) int {
	tx, err := db.Begin()
	if err != nil {
		return 0
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO student_records (
			student_unique_id, academic_year, school_id, school_name, district_id,
			district_name, block_id, block_name, cluster_id, cluster_name,
			student_name, father_name, mother_name, gender, class_id,
			section, udise_code, date_of_birth, mobile_number, created_by,
			created_time, updated_by, updated_time
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return 0
	}
	defer stmt.Close()

	successCount := 0
	for _, r := range records {
		_, err := stmt.Exec(
			r.UniqueID, r.AcademicYear, r.SchoolID, r.SchoolName, r.DistrictID,
			r.DistrictName, r.BlockID, r.BlockName, r.ClusterID, r.ClusterName,
			r.StudentName, r.FatherName, r.MotherName, r.Gender, r.ClassID,
			r.Section, r.UdiseCode, r.DateOfBirth, r.MobileNumber, r.CreatedBy,
			r.CreatedTime, r.UpdatedBy, r.UpdatedTime,
		)
		if err == nil {
			successCount++
		}
	}
	tx.Commit()
	return successCount
}