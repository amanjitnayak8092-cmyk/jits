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

	_ "modernc.org/sqlite"
)

// --- CONFIGURATION ---
const (
	DistDBPath     = "DistrictAndBlockData.db"
	SchoolDBPath   = "GetSchoolData.db"
	LogFilePath    = "logs.dat"
	FailedFilePath = "failed_tasks.txt"
	ProxyURL       = "socks5://127.0.0.1:10075"
	MaxWorkers     = 15 // तुम्हारी डिमांड पर Max Requests/Workers 15 कर दिया
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

// PRIMARY KEY हटा दी है - अब जो आएगा, सीधा सेव होगा!
func initYearDB(year string) *sql.DB {
	dbPath := fmt.Sprintf("GetStudentDetails_%s.db", year)
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		logMessage("❌ Student DB open error for %s: %v", year, err)
		os.Exit(1)
	}

	_, err = db.Exec(`
		PRAGMA journal_mode=WAL;
		PRAGMA synchronous=NORMAL;
		PRAGMA cache_size=-64000; 
		CREATE TABLE IF NOT EXISTS student_records (
			student_unique_id TEXT,
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

	logMessage("🔍 Step 1: Initializing Separate Year-wise Databases...")
	targetYearsList := []string{"2023-24", "2024-25", "2025-26", "2026-27"}
	dbConnections := make(map[string]*sql.DB)
	
	for _, yr := range targetYearsList {
		dbConnections[yr] = initYearDB(yr)
		defer dbConnections[yr].Close()
	}
	logMessage("✅ All 4 Year Databases initialized successfully (No Primary Key Constraint).")

	// --- TESTING MODE LOGIC ---
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("\n=============================================")
	fmt.Println("Select Mode:")
	fmt.Println("1. Normal Mode (Full DB Scraping)")
	fmt.Println("2. Test Mode (Specific UDISE Codes)")
	fmt.Print("Enter choice (1 or 2): ")
	
	choiceInput, _ := reader.ReadString('\n')
	choiceInput = strings.TrimSpace(choiceInput)

	var tasks []Task
	targetYears := []string{"2023-24", "2024-25", "2025-26", "2026-27"}

	if choiceInput == "2" {
		fmt.Println("\n--- 🛠️ TEST MODE ACTIVATED ---")
		fmt.Print("How many schools do you want to test? ")
		countInput, _ := reader.ReadString('\n')
		countInput = strings.TrimSpace(countInput)
		schoolCount, _ := strconv.Atoi(countInput)

		for i := 1; i <= schoolCount; i++ {
			fmt.Printf("Enter UDISE Code for School %d: ", i)
			udiseInput, _ := reader.ReadString('\n')
			udiseInput = strings.TrimSpace(udiseInput)

			if udiseInput != "" {
				// हर स्कूल के लिए 1 से 12 क्लास और चारों साल का टास्क बनाना
				for classID := 1; classID <= 12; classID++ {
					for _, year := range targetYears {
						tasks = append(tasks, Task{
							SchoolCode:   udiseInput,
							AcademicYear: year,
							ClassID:      classID,
						})
					}
				}
			}
		}
		logMessage("📝 Test Mode: Generated %d tasks for testing.", len(tasks))
	} else {
		// --- NORMAL MODE ---
		logMessage("🚀 Normal Mode: Loading schools from database...")
		if _, err := os.Stat(SchoolDBPath); os.IsNotExist(err) {
			logMessage("❌ Source DB missing: %s", SchoolDBPath)
			return
		}

		schoolDB, err := sql.Open("sqlite", SchoolDBPath)
		if err != nil {
			logMessage("❌ School DB Error: %v", err)
			return
		}
		defer schoolDB.Close()

		rows, err := schoolDB.Query("SELECT school_code FROM schools WHERE school_code IS NOT NULL AND school_code != ''")
		if err != nil {
			logMessage("❌ Query Error: %v", err)
			return
		}
		defer rows.Close()

		// एक स्कूल का एक क्लास और एक साल रिपीट न हो, इसके लिए map Tracker
		taskTracker := make(map[string]bool)

		for rows.Next() {
			var schoolCode string
			if err := rows.Scan(&schoolCode); err != nil {
				continue
			}

			for classID := 1; classID <= 12; classID++ {
				for _, year := range targetYears {
					// Unique key formulation: "UDISE_YEAR_CLASS"
					uniqueKey := fmt.Sprintf("%s_%s_%d", schoolCode, year, classID)
					
					if !taskTracker[uniqueKey] {
						taskTracker[uniqueKey] = true
						tasks = append(tasks, Task{
							SchoolCode:   schoolCode,
							AcademicYear: year,
							ClassID:      classID,
						})
					}
				}
			}
		}
		logMessage("✅ Total unique tasks generated for scraping: %d", len(tasks))
	}

	if len(tasks) == 0 {
		logMessage("⚠️ No tasks to execute. Exiting.")
		return
	}

	// --- WORKER POOL EXECUTION (MAX 15 REQUESTS AT A TIME) ---
	logMessage("⚡ Starting scraping with Max Workers: %d", MaxWorkers)
	
	var wg sync.WaitGroup
	taskChan := make(chan Task, len(tasks))

	// Workers start करना
	for i := 0; i < MaxWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for task := range taskChan {
				// यहाँ तुम्हारा API कॉल करने और DB में डालने का लॉजिक आएगा
				// dbConnections[task.AcademicYear] का इस्तेमाल करके सही साल के DB में डेटा पुश होगा
				processScrapingTask(task, dbConnections[task.AcademicYear])
			}
		}(i)
	}

	// Channel में टास्क डालना
	for _, task := range tasks {
		taskChan <- task
	}
	close(taskChan)

	wg.Wait()
	logMessage("🏁 Scraping process completed successfully!")
}

// डेटाबेस में सीधे डेटा इंसर्ट करने का डमी फंक्शन (इसे अपने API Logic से रिप्लेस कर लेना)
func processScrapingTask(task Task, targetDB *sql.DB) {
	// 1. यहाँ अपना http.Get या Client Request मारो ProxyURL का इस्तेमाल करके।
	// 2. Response JSON को StudentAPIResponse स्ट्रक्चर में Unmarshal करो।
	// 3. बिना किसी झंझट के सीधे INSERT मारो:
	
	/* 💡 SQL Insertion Example (बिना Primary Key के सीधा सेव):
	_, err := targetDB.Exec(`INSERT INTO student_records (student_unique_id, academic_year, udise_code, class_id) VALUES (?, ?, ?, ?)`, 
		studentUID, task.AcademicYear, task.SchoolCode, task.ClassID)
	*/
}
