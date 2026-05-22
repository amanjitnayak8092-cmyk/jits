package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// --- CONFIGURATION ---
const (
	DistDBPath     = "DistrictAndBlockData.db"
	SchoolDBPath   = "GetSchoolData.db"
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
	Section         interface{} `json:"section"`
	DateOfBirth     interface{} `json:"Date_of_Birth"`
	MobileNumber    interface{} `json:"Mobile_Number"`
	CreatedBy       interface{} `json:"Created_By"`
	CreatedTime     interface{} `json:"Created_Time"`
	UpdatedBy       interface{} `json:"Updated_By"`
	UpdatedTime     interface{} `json:"Updated_Time"`
}

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
	UdiseCode       string
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

// PRIMARY KEY HATADI HAI - AB DATA DUPICATE BHI HOGA TOH DB REJECT NAHI KAREGA, DUMP KAR DEGA
func initYearDB(year string) *sql.DB {
	dbPath := fmt.Sprintf("GetStudentDetails_%s.db", year)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		logMessage("❌ Student DB open error for %s: %v", year, err)
		os.Exit(1)
	}

	_, err = db.Exec(`
		PRAGMA journal_mode=WAL;
		PRAGMA synchronous=NORMAL;
		PRAGMA cache_size=-64000; 
		CREATE TABLE IF NOT EXISTS student_records (
			student_unique_id TEXT, -- No PRIMARY KEY constraint anymore
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
		logMessage("❌ DB Initialization Error for %s: %v", year, err)
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

	logMessage("🔍 Step 2: Initializing Separate Year-wise Databases...")
	targetYearsList := []string{"2023-24", "2024-25", "2025-26", "2026-27"}
	dbConnections := make(map[string]*sql.DB)
	
	for _, yr := range targetYearsList {
		dbConnections[yr] = initYearDB(yr)
		defer dbConnections[yr].Close()
	}
	logMessage("✅ All 4 Year Databases initialized safely.")

	// --- TEST MODE / BULK MODE CHOICE ---
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("\n=============================================")
	fmt.Println("Select Mode:")
	fmt.Println("1 -> Full Bulk Mode (Run using School Database)")
	fmt.Println("2 -> Test Mode / Custom UDISE Input")
	fmt.Print("Enter option (1 or 2): ")
	
	modeInput, _ := reader.ReadString('\n')
	modeInput = strings.TrimSpace(modeInput)

	var tasks []Task

	if modeInput == "2" {
		// --- INTERACTIVE TEST MODE ---
		fmt.Print("Enter how many schools you want to test: ")
		countStr, _ := reader.ReadString('\n')
		countStr = strings.TrimSpace(countStr)
		schoolCount, _ := strconv.Atoi(countStr)
		if schoolCount <= 0 {
			schoolCount = 1
		}

		for i := 1; i <= schoolCount; i++ {
			fmt.Printf("Enter UDISE Code for School %d/%d: ", i, schoolCount)
			udise, _ := reader.ReadString('\n')
			udise = strings.TrimSpace(udise)
			if udise == "" {
				continue
			}

			// Custom schools ke liye saari classes (1-12) aur targets distribute kar dete hain
			for cid := 1; cid <= 12; cid++ {
				targetYears := []string{"2023-24"}
				if cid == 1 {
					targetYears = []string{"2023-24", "2024-25", "2025-26", "2026-27"}
				}
				for _, yr := range targetYears {
					tasks = append(tasks, Task{SchoolCode: udise, AcademicYear: yr, ClassID: cid})
				}
			}
		}
	} else {
		// --- STANDARD BULK MODE ---
		if _, err := os.Stat(SchoolDBPath); os.IsNotExist(err) {
			logMessage("❌ Source DB missing for Bulk Mode: %s", SchoolDBPath)
			return
		}
		schoolDB, err := sql.Open("sqlite3", SchoolDBPath)
		if err != nil {
			logMessage("❌ School DB Error: %v", err)
			return
		}
		defer schoolDB.Close()

		rows, err := schoolDB.Query("SELECT school_code, class_frm, class_to FROM schools WHERE school_code IS NOT NULL AND school_code != ''")
		if err != nil {
			logMessage("❌ Query Error: %v", err)
			return
		}
		defer rows.Close()

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
	}

	totalTasks := len(tasks)
	if totalTasks == 0 {
		logMessage("❌ Task Queue khali hai. Application stopping.")
		return
	}
	logMessage("🚀 Processing Started! Total Network Requests to make: %d", totalTasks)

	taskChan := make(chan Task, totalTasks)
	resultChan := make(chan []StudentRecord, MaxWorkers)

	proxyURL, _ := url.Parse(ProxyURL)
	transport := &http.Transport{Proxy: http.ProxyURL(proxyURL)}
	client := &http.Client{Transport: transport, Timeout: 12 * time.Second}

	var wg sync.WaitGroup
	for i := 0; i < MaxWorkers; i++ {
		wg.Add(1)
		go worker(taskChan, resultChan, client, &wg)
	}

	for _, task := range tasks {
		taskChan <- task
	}
	close(taskChan)

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var buffer []StudentRecord
	processedCount := 0
	totalSaved := 0
	startTime := time.Now()

	for records := range resultChan {
		processedCount++
		if len(records) > 0 {
			buffer = append(buffer, records...)
		}

		if len(buffer) >= BatchSize || processedCount == totalTasks {
			if len(buffer) > 0 {
				saved := saveBatchByYear(dbConnections, buffer)
				totalSaved += saved
				buffer = nil
			}

			elapsed := time.Since(startTime).Seconds()
			speed := float64(processedCount) / elapsed
			logMessage("⚡ Progress: %d/%d Done | Total Records Processed Across DBs: %d | Speed: %.2f req/sec",
				processedCount, totalTasks, totalSaved, speed)
		}
	}

	logMessage("🎉 Execution Complete! Check the year-wise DB files for data.")
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

		q := req.URL.Query()
		q.Add("AcademicYear", task.AcademicYear)
		q.Add("UdiseCode", task.SchoolCode)
		q.Add("ClassId", strconv.Itoa(task.ClassID))
		req.URL.RawQuery = q.Encode()

		resp, err := client.Do(req)
		if err != nil {
			logFailure(task, fmt.Sprintf("Network Error: %v", err))
			results <- nil
			continue
		}

		if resp.StatusCode != 200 {
			logFailure(task, fmt.Sprintf("HTTP Status: %d", resp.StatusCode))
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
			results <- nil
			continue
		}

		if strData, ok := rawData.(string); ok {
			if err := json.Unmarshal([]byte(strData), &rawData); err != nil {
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
			// Yahan strict validation skip kiya, agar uniqueID nahi bhi ho toh response filter out na ho
			uniqueID := std.StudentUniqueId
			if uniqueID == "" {
				uniqueID = "UNKNOWN"
			}

			record := StudentRecord{
				UniqueID:        uniqueID,
				AcademicYear:    task.AcademicYear, 
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
				Gender:          getString(std.Gender), 
				ClassID:         getString(std.StudyingClass), 
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

// BINA KISI CONDITION KE SEEDHE DB ME DATA COORDiNATION AUR INSERT
func saveBatchByYear(connections map[string]*sql.DB, records []StudentRecord) int {
	grouped := make(map[string][]StudentRecord)
	for _, r := range records {
		grouped[r.AcademicYear] = append(grouped[r.AcademicYear], r)
	}

	totalSuccess := 0

	for yr, yrRecords := range grouped {
		db, exists := connections[yr]
		if !exists {
			continue 
		}

		tx, err := db.Begin()
		if err != nil {
			continue
		}

		// INSERT OR REPLACE ki jagah plain INSERT lagaya hai taaki constraints check karne me CPU cycles bachein
		stmt, err := tx.Prepare(`
			INSERT INTO student_records (
				student_unique_id, academic_year, school_id, school_name, district_id,
				district_name, block_id, block_name, cluster_id, cluster_name,
				student_name, father_name, mother_name, gender, class_id,
				section, udise_code, date_of_birth, mobile_number, created_by,
				created_time, updated_by, updated_time
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			_ = tx.Rollback()
			continue
		}

		for _, r := range yrRecords {
			_, err := stmt.Exec(
				r.UniqueID, r.AcademicYear, r.SchoolID, r.SchoolName, r.DistrictID,
				r.DistrictName, r.BlockID, r.BlockName, r.ClusterID, r.ClusterName,
				r.StudentName, r.FatherName, r.MotherName, r.Gender, r.ClassID,
				r.Section, r.UdiseCode, r.DateOfBirth, r.MobileNumber, r.CreatedBy,
				r.CreatedTime, r.UpdatedBy, r.UpdatedTime,
			)
			if err == nil {
				totalSuccess++
			}
		}
		_ = stmt.Close()
		_ = tx.Commit()
	}

	return totalSuccess
}
