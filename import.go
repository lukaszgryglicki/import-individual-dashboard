package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var gDebugSQL bool

func fatalOnError(err error) {
	if err != nil {
		tm := time.Now()
		fmt.Printf("Error(time=%+v):\nError: '%s'\nStacktrace:\n%s\n", tm, err.Error(), string(debug.Stack()))
		fmt.Fprintf(os.Stderr, "Error(time=%+v):\nError: '%s'\nStacktrace:\n", tm, err.Error())
		panic("stacktrace")
	}
}

func fatalf(f string, a ...interface{}) {
	fatalOnError(fmt.Errorf(f, a...))
}
func queryOut(query string, args ...interface{}) {
	fmt.Printf("%s\n", query)
	if len(args) > 0 {
		s := ""
		for vi, vv := range args {
			switch v := vv.(type) {
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, complex64, complex128, string, bool, time.Time:
				s += fmt.Sprintf("%d:%+v ", vi+1, v)
			case *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64, *float32, *float64, *complex64, *complex128, *string, *bool, *time.Time:
				s += fmt.Sprintf("%d:%+v ", vi+1, v)
			case nil:
				s += fmt.Sprintf("%d:(null) ", vi+1)
			default:
				s += fmt.Sprintf("%d:%+v ", vi+1, reflect.ValueOf(vv).Elem())
			}
		}
		fmt.Printf("[%s]\n", s)
	}
}

func query(db *sql.DB, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := db.Query(query, args...)
	if err != nil || gDebugSQL {
		queryOut(query, args...)
	}
	return rows, err
}

func exec(db *sql.DB, skip, query string, args ...interface{}) (sql.Result, error) {
	res, err := db.Exec(query, args...)
	if err != nil || gDebugSQL {
		if skip == "" || !strings.Contains(err.Error(), skip) || gDebugSQL {
			queryOut(query, args...)
		}
	}
	return res, err
}

func getThreadsNum() int {
	st := os.Getenv("ST") != ""
	if st {
		return 1
	}
	nCPUs := 0
	if os.Getenv("NCPUS") != "" {
		n, err := strconv.Atoi(os.Getenv("NCPUS"))
		fatalOnError(err)
		if n > 0 {
			nCPUs = n
		}
	}
	if nCPUs > 0 {
		n := runtime.NumCPU()
		if nCPUs > n {
			nCPUs = n
		}
		runtime.GOMAXPROCS(nCPUs)
		return nCPUs
	}
	nCPUs = runtime.NumCPU()
	runtime.GOMAXPROCS(nCPUs)
	return nCPUs
}

func importCSVfiles(db *sql.DB, fileNames []string) (err error) {
	dbg := os.Getenv("DEBUG") != ""
	dry := os.Getenv("DRY") != ""
	gDebugSQL = os.Getenv("DEBUG_SQL") != ""
	identitiesFile := fileNames[0]
	affiliationsFile := fileNames[1]
	fmt.Printf("Importing (%s,%s)\n", identitiesFile, affiliationsFile)
	var fileIdentities *os.File
	fileIdentities, err = os.Open(identitiesFile)
	if err != nil {
		return
	}
	defer func() {
		_ = fileIdentities.Close()
	}()
	var fileAffiliations *os.File
	fileAffiliations, err = os.Open(affiliationsFile)
	if err != nil {
		return
	}
	defer func() {
		_ = fileAffiliations.Close()
	}()
	var identitiesLines [][]string
	identitiesLines, err = csv.NewReader(fileIdentities).ReadAll()
	if err != nil {
		return
	}
	// action identity_id identity_name identity_username identity_email identity_source user_sfid user_email
	hdr := []string{}
	for i, line := range identitiesLines {
		if i == 0 {
			for _, col := range line {
				hdr = append(hdr, col)
			}
			if dbg {
				fmt.Printf("%s\n", hdr)
			}
			continue
		}
		row := map[string]string{}
		for c, col := range line {
			row[hdr[c]] = col
		}
		if dbg {
			fmt.Printf("%v\n", row)
		}
	}
	var affiliationsLines [][]string
	affiliationsLines, err = csv.NewReader(fileAffiliations).ReadAll()
	if err != nil {
		return
	}
	// action identity_id user_sfid user_name user_email project_slug project_id project_name to_org_name to_start_date to_end_date from_org_name from_start_date from_end_date
	hdr = []string{}
	for i, line := range affiliationsLines {
		if i == 0 {
			for _, col := range line {
				hdr = append(hdr, col)
			}
			if dbg {
				fmt.Printf("%s\n", hdr)
			}
			continue
		}
		row := map[string]string{}
		for c, col := range line {
			row[hdr[c]] = col
		}
		if dbg {
			fmt.Printf("%v\n", row)
		}
	}
	if !dry {
		rows, err := query(db, "select 'hello'")
		fatalOnError(err)
		str := ""
		for rows.Next() {
			fatalOnError(rows.Scan(&str))
			fmt.Printf("%s\n", str)
		}
	}
	return
}

// getConnectString - get MariaDB SH (Sorting Hat) database DSN
// Either provide full DSN via SH_DSN='shuser:shpassword@tcp(shhost:shport)/shdb?charset=utf8&parseTime=true'
// Or use some SH_ variables, only SH_PASS is required
// Defaults are: "shuser:required_pwd@tcp(localhost:3306)/shdb?charset=utf8
// SH_DSN has higher priority; if set no SH_ varaibles are used
func getConnectString(prefix string) string {
	//dsn := "shuser:"+os.Getenv("PASS")+"@/shdb?charset=utf8")
	dsn := os.Getenv(prefix + "DSN")
	if dsn == "" {
		pass := os.Getenv(prefix + "PASS")
		user := os.Getenv(prefix + "USR")
		if user == "" {
			user = os.Getenv(prefix + "USER")
		}
		proto := os.Getenv(prefix + "PROTO")
		if proto == "" {
			proto = "tcp"
		}
		host := os.Getenv(prefix + "HOST")
		if host == "" {
			host = "localhost"
		}
		port := os.Getenv(prefix + "PORT")
		if port == "" {
			port = "3306"
		}
		db := os.Getenv(prefix + "DB")
		if db == "" {
			fatalf("please specify database via %sDB=...", prefix)
		}
		params := os.Getenv(prefix + "PARAMS")
		if params == "" {
			params = "?charset=utf8&parseTime=true"
		}
		if params == "-" {
			params = ""
		}
		dsn = fmt.Sprintf(
			"%s:%s@%s(%s:%s)/%s%s",
			user,
			pass,
			proto,
			host,
			port,
			db,
			params,
		)
	}
	return dsn
}

func main() {
	// Connect to MariaDB
	if len(os.Args) < 3 {
		fmt.Printf("Arguments required: user_identities_YYYYMMDDHHMI.csv user_affiliations_YYYYMMDDHHMI.csv\n")
		return
	}
	dtStart := time.Now()
	var db *sql.DB
	dsn := getConnectString("SH_")
	db, err := sql.Open("mysql", dsn)
	fatalOnError(err)
	defer func() { fatalOnError(db.Close()) }()
	err = importCSVfiles(db, os.Args[1:len(os.Args)])
	fatalOnError(err)
	dtEnd := time.Now()
	fmt.Printf("Time(%s): %v\n", os.Args[0], dtEnd.Sub(dtStart))
}