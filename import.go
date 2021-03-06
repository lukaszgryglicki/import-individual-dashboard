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
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	cDateTimeFormat = "%Y-%m-%dT%H:%i:%s.%fZ"
)

var (
	gDebugSQL           bool
	gMtx                *sync.Mutex
	gUpdatedIdentities  map[string]struct{}
	gUpdatedEnrollments map[string]struct{}
	gUpdatedUIdentities map[string]struct{}
	gUpdatedProfiles    map[string]struct{}
	gIDMtx              map[string]*sync.Mutex
	gUUIDMtx            map[string]*sync.Mutex
	gOrgMap             map[string]int
	gSlugMap            map[string]string
	gOrgMiss            map[string]struct{}
	gSlugMiss           map[string]struct{}
)

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

func exec(db *sql.Tx, skip, query string, args ...interface{}) (sql.Result, error) {
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

func updateIdentity(ch chan error, db *sql.DB, dbg, dry bool, row map[string]string) (err error) {
	// action identity_id identity_name identity_username identity_email identity_source user_sfid user_email
	if ch != nil {
		defer func() {
			ch <- err
		}()
	}
	if dbg {
		fmt.Printf("%v\n", row)
	}
	id, _ := row["identity_id"]
	if id == "" {
		err = fmt.Errorf("identity_id cannot be empty in %v", row)
		return
	}
	rows, err := query(db, "select uuid, trim(coalesce(name, '')), trim(coalesce(username, '')), trim(coalesce(email, '')), trim(source) from identities where id = ?", id)
	fatalOnError(err)
	uuid, name, username, email, source, found := "", "", "", "", "", false
	for rows.Next() {
		fatalOnError(rows.Scan(&uuid, &name, &username, &email, &source))
		found = true
		break
	}
	fatalOnError(rows.Err())
	fatalOnError(rows.Close())
	if !found {
		fmt.Printf("WARNING: cannot find identity with id=%s (row %v)\n", id, row)
		return
	}
	if dbg {
		fmt.Printf("Found: (%s,%s,%s,%s,%s) for id %s\n", uuid, name, username, email, source, id)
	}
	newName, _ := row["identity_name"]
	newUsername, _ := row["identity_username"]
	newEmail, _ := row["identity_email"]
	newSource, _ := row["identity_source"]
	newName = strings.TrimSpace(newName)
	newUsername = strings.TrimSpace(newUsername)
	newEmail = strings.TrimSpace(newEmail)
	if source != newSource {
		err = fmt.Errorf("identity_id %s/%s updating source is not supported, attempted %s -> %s in %v", id, uuid, source, newSource, row)
		return
	}
	if name == newName && username == newUsername && email == newEmail {
		if dbg {
			fmt.Printf("identity_id %s/%s (%s,%s,%s) nothing changed in %v\n", id, uuid, name, username, email, row)
		}
		return
	}
	// Concurrecncy check
	if gMtx != nil {
		// Lock working on identity ID
		gMtx.Lock()
		mtx, found := gIDMtx[id]
		gMtx.Unlock()
		if !found {
			mtx = &sync.Mutex{}
			gMtx.Lock()
			gIDMtx[id] = mtx
			gMtx.Unlock()
		} else if dbg {
			fmt.Printf("Duplicate id %s found in %v\n", id, row)
		}
		mtx.Lock()
		defer mtx.Unlock()
		// Lock working on uidentity/profile UUID
		gMtx.Lock()
		umtx, found := gUUIDMtx[uuid]
		gMtx.Unlock()
		if !found {
			umtx = &sync.Mutex{}
			gMtx.Lock()
			gUUIDMtx[uuid] = umtx
			gMtx.Unlock()
		} else if dbg {
			fmt.Printf("Duplicate uuid %s found in %v\n", uuid, row)
		}
		umtx.Lock()
		defer umtx.Unlock()
	}
	args := []interface{}{}
	query := "update identities set "
	msg := "identity_id " + id + "/" + uuid + " "
	if newName != name {
		query += "name = ?, "
		args = append(args, newName)
		msg += "name " + name + " -> " + newName + " "
	}
	if newUsername != username {
		query += "username = ?, "
		args = append(args, newUsername)
		msg += "username " + username + " -> " + newUsername + " "
	}
	if newEmail != email {
		query += "email = ?, "
		args = append(args, newEmail)
		msg += "email " + email + " -> " + newEmail + " "
	}
	query += "last_modified = now(), last_modified_by = ?, locked_by = ? where id = ?"
	userSFID, _ := row["user_sfid"]
	userEmail, _ := row["user_email"]
	userSFID = strings.TrimSpace(userSFID)
	userEmail = strings.TrimSpace(userEmail)
	who := "email:" + userEmail + ",sfid:" + userSFID
	msg += " by " + who
	args = append(args, who, "individual", id)
	if dry {
		fmt.Printf("%s\n", msg)
		if dbg {
			fmt.Printf("(%s,%v)\n", query, args)
		}
		return
	}
	// Actual updates
	var (
		affectedI int64
		affectedP int64
		affectedU int64
		tx        *sql.Tx
		res       sql.Result
	)
	tx, err = db.Begin()
	if err != nil {
		err = fmt.Errorf("error starting transaction %v for row %v", err, row)
		return
	}
	collision := false
	defer func() {
		if tx != nil {
			if !collision || dbg {
				fmt.Printf("rollback %s\n", msg)
			}
			_ = tx.Rollback()
		}
	}()
	// Update identities
	skip := "Error 1062"
	res, err = exec(tx, skip, query, args...)
	if err != nil {
		if strings.Contains(err.Error(), skip) {
			err = nil
			collision = true
			if dbg {
				fmt.Printf("%s: collision\n", msg)
			}
			return
		}
		err = fmt.Errorf("error updating identities %v for (%s,%v) for row %v", err, query, args, row)
		return
	}
	affectedI, err = res.RowsAffected()
	if err != nil {
		err = fmt.Errorf("error getting affected rows count %v for (%s,%v) for row %v", err, query, args, row)
		return
	}
	if affectedI <= 0 || dbg {
		fmt.Printf("%s: affected %d identities rows\n", msg, affectedI)
	}
	// Update uidentities
	res, err = exec(tx, "", "update uidentities set last_modified = now(), last_modified_by = ?, locked_by = ? where uuid = ?", who, "individual", uuid)
	if err != nil {
		err = fmt.Errorf("error updating uidentities %v for uuid %s for row %v", err, uuid, row)
		return
	}
	affectedU, err = res.RowsAffected()
	if err != nil {
		err = fmt.Errorf("error getting affected rows count %v for uuid %s for row %v", err, uuid, row)
		return
	}
	if affectedU <= 0 || dbg {
		fmt.Printf("%s: affected %d uidentities rows\n", msg, affectedU)
	}
	// Update profiles
	res, err = exec(tx, "", "update profiles set last_modified = now(), last_modified_by = ?, locked_by = ? where uuid = ?", who, "individual", uuid)
	if err != nil {
		err = fmt.Errorf("error updating profiles %v for uuid %s for row %v", err, uuid, row)
		return
	}
	affectedP, err = res.RowsAffected()
	if err != nil {
		err = fmt.Errorf("error getting affected rows count %v for uuid %s for row %v", err, uuid, row)
		return
	}
	if affectedP <= 0 || dbg {
		fmt.Printf("%s: affected %d profiles rows\n", msg, affectedU)
	}
	if affectedI <= 0 || affectedU <= 0 || affectedP <= 0 {
		fmt.Printf("WARNING: %s: didn't affect identities or uidentities or profiles: (%d,%d,%d)\n", msg, affectedI, affectedU, affectedP)
		return
	}
	err = tx.Commit()
	if err != nil {
		err = fmt.Errorf("error committing transaction %v for row %v", err, row)
		return
	}
	tx = nil
	if gMtx != nil {
		gMtx.Lock()
		if affectedI > 0 {
			gUpdatedIdentities[id] = struct{}{}
		}
		if affectedU > 0 {
			gUpdatedUIdentities[uuid] = struct{}{}
		}
		if affectedP > 0 {
			gUpdatedProfiles[uuid] = struct{}{}
		}
		gMtx.Unlock()
	}
	return
}

func orgNameToID(db *sql.DB, dbg bool, orgName string) (orgID int, err error) {
	var found bool
	if gMtx != nil {
		gMtx.Lock()
	}
	orgID, found = gOrgMap[orgName]
	if gMtx != nil {
		gMtx.Unlock()
	}
	if found {
		if dbg {
			fmt.Printf("org found in cache %s -> %d\n", orgName, orgID)
		}
		return
	}
	rows, e := query(db, "select id from organizations where name = ?", orgName)
	fatalOnError(e)
	for rows.Next() {
		fatalOnError(rows.Scan(&orgID))
		found = true
		break
	}
	fatalOnError(rows.Err())
	fatalOnError(rows.Close())
	if !found {
		err = fmt.Errorf("cannot find organization_id for %s", orgName)
		return
	}
	if gMtx != nil {
		gMtx.Lock()
	}
	gOrgMap[orgName] = orgID
	if gMtx != nil {
		gMtx.Unlock()
	}
	if dbg {
		fmt.Printf("org found in DB %s -> %d\n", orgName, orgID)
	}
	return
}

func sfdcSlugToDASlug(db *sql.DB, dbg bool, sfdcSlug string) (daSlug string, err error) {
	var found bool
	if gMtx != nil {
		gMtx.Lock()
	}
	daSlug, found = gSlugMap[sfdcSlug]
	if gMtx != nil {
		gMtx.Unlock()
	}
	if found {
		if dbg {
			fmt.Printf("slug found in cache %s -> %s\n", sfdcSlug, daSlug)
		}
		return
	}
	rows, e := query(db, "select da_name from slug_mapping where sf_name = ?", sfdcSlug)
	fatalOnError(e)
	for rows.Next() {
		fatalOnError(rows.Scan(&daSlug))
		found = true
		break
	}
	fatalOnError(rows.Err())
	fatalOnError(rows.Close())
	if !found {
		err = fmt.Errorf("cannot find DA slug for SFDC slug %s", sfdcSlug)
		return
	}
	if gMtx != nil {
		gMtx.Lock()
	}
	gSlugMap[sfdcSlug] = daSlug
	if gMtx != nil {
		gMtx.Unlock()
	}
	if dbg {
		fmt.Printf("slug found in DB %s -> %s\n", sfdcSlug, daSlug)
	}
	return
}

func timeParseAny(dtStr string) (time.Time, error) {
	formats := []string{
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02 15",
		"2006-01-02",
		"2006-01",
		"2006",
	}
	for _, format := range formats {
		t, e := time.Parse(format, dtStr)
		if e == nil {
			return t, nil
		}
	}
	err := fmt.Errorf("cannot parse datetime: '%s'", dtStr)
	return time.Now(), err
}

func toYMDDate(dt time.Time) string {
	return fmt.Sprintf("%04d-%02d-%02d", dt.Year(), dt.Month(), dt.Day())
}

func updateEnrollment(ch chan error, db *sql.DB, dbg, dry bool, row map[string]string) (err error) {
	// action identity_id user_sfid user_name user_email project_slug project_id project_name
	// to_org_name to_start_date to_end_date from_org_name from_start_date from_end_date
	if ch != nil {
		defer func() {
			ch <- err
		}()
	}
	if dbg {
		fmt.Printf("%v\n", row)
	}
	id, _ := row["identity_id"]
	if id == "" {
		err = fmt.Errorf("identity_id cannot be empty in %v", row)
		return
	}
	rows, err := query(db, "select uuid from identities where id = ?", id)
	fatalOnError(err)
	uuid, found := "", false
	for rows.Next() {
		fatalOnError(rows.Scan(&uuid))
		found = true
		break
	}
	fatalOnError(rows.Err())
	fatalOnError(rows.Close())
	if !found {
		fmt.Printf("WARNING: cannot find identity with id=%s (row %v)\n", id, row)
		return
	}
	if dbg {
		fmt.Printf("Found: uuid %s for id %s\n", uuid, id)
	}
	orgName, _ := row["from_org_name"]
	orgName = strings.TrimSpace(orgName)
	var (
		tStartDate    time.Time
		tEndDate      time.Time
		tNewStartDate time.Time
		tNewEndDate   time.Time
	)
	startDate, _ := row["from_start_date"]
	startDate = strings.TrimSpace(startDate)
	if startDate != "" {
		tStartDate, err = timeParseAny(startDate)
		if err != nil {
			err = fmt.Errorf("identity_id %s/%s cannot parse date %s %v", id, uuid, startDate, row)
			return
		}
	} else {
		tStartDate = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	startDate = toYMDDate(tStartDate)
	endDate, _ := row["from_end_date"]
	endDate = strings.TrimSpace(endDate)
	if endDate != "" {
		tEndDate, err = timeParseAny(endDate)
		if err != nil {
			err = fmt.Errorf("identity_id %s/%s cannot parse date %s %v", id, uuid, endDate, row)
			return
		}
	} else {
		tEndDate = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	endDate = toYMDDate(tEndDate)
	newOrgName, _ := row["to_org_name"]
	newOrgName = strings.TrimSpace(newOrgName)
	if newOrgName == "" {
		err = fmt.Errorf("identity_id %s/%s to_org_name cannot be empty in %v", id, uuid, row)
		return
	}
	newStartDate, _ := row["to_start_date"]
	newStartDate = strings.TrimSpace(newStartDate)
	if newStartDate != "" {
		tNewStartDate, err = timeParseAny(newStartDate)
		if err != nil {
			err = fmt.Errorf("identity_id %s/%s cannot parse date %s %v", id, uuid, newStartDate, row)
			return
		}
	} else {
		tNewStartDate = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	newStartDate = toYMDDate(tNewStartDate)
	newEndDate, _ := row["to_end_date"]
	newEndDate = strings.TrimSpace(newEndDate)
	if newEndDate != "" {
		tNewEndDate, err = timeParseAny(newEndDate)
		if err != nil {
			err = fmt.Errorf("identity_id %s/%s cannot parse date %s %v", id, uuid, newEndDate, row)
			return
		}
	} else {
		tNewEndDate = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	newEndDate = toYMDDate(tNewEndDate)
	var (
		orgID       int
		newOrgID    int
		projectSlug string
	)
	sfdcProjectSlug, _ := row["project_slug"]
	sfdcProjectSlug = strings.TrimSpace(sfdcProjectSlug)
	if sfdcProjectSlug != "" {
		projectSlug, err = sfdcSlugToDASlug(db, dbg, sfdcProjectSlug)
		if err != nil {
			// err = fmt.Errorf("identity_id %s/%s error %v in row %v", id, uuid, err, row)
			if dbg {
				fmt.Printf("WARNING: identity_id %s/%s error %v in row %v\n", id, uuid, err, row)
			}
			if gMtx != nil {
				gMtx.Lock()
			}
			_, rep := gSlugMiss[sfdcProjectSlug]
			if !rep {
				gSlugMiss[sfdcProjectSlug] = struct{}{}
				fmt.Printf("SFDC project slug not found in SH DB: %s\n", sfdcProjectSlug)
			}
			if gMtx != nil {
				gMtx.Unlock()
			}
			err = nil
			return
		}
	}
	if orgName != "" {
		orgID, err = orgNameToID(db, dbg, orgName)
		if err != nil {
			// err = fmt.Errorf("identity_id %s/%s error %v in row %v", id, uuid, err, row)
			if dbg {
				fmt.Printf("WARNING: identity_id %s/%s error %v in row %v\n", id, uuid, err, row)
			}
			if gMtx != nil {
				gMtx.Lock()
			}
			_, rep := gOrgMiss[orgName]
			if !rep {
				gOrgMiss[orgName] = struct{}{}
				fmt.Printf("Organization not found in SH DB: %s\n", orgName)
			}
			if gMtx != nil {
				gMtx.Unlock()
			}
			err = nil
			return
		}
	}
	newOrgID, err = orgNameToID(db, dbg, newOrgName)
	if err != nil {
		// err = fmt.Errorf("identity_id %s/%s error %v in row %v", id, uuid, err, row)
		if dbg {
			fmt.Printf("WARNING: identity_id %s/%s error %v in row %v\n", id, uuid, err, row)
		}
		if gMtx != nil {
			gMtx.Lock()
		}
		_, rep := gOrgMiss[newOrgName]
		if !rep {
			gOrgMiss[newOrgName] = struct{}{}
			fmt.Printf("Organization not found in SH DB: %s\n", newOrgName)
		}
		if gMtx != nil {
			gMtx.Unlock()
		}
		err = nil
		return
	}
	// action identity_id user_sfid user_name user_email project_slug project_id project_name
	// to_org_name to_start_date to_end_date from_org_name from_start_date from_end_date
	// fmt.Printf("(%s,%s,%s,%s) (%v,%v,%v,%v)\n", startDate, endDate, newStartDate, newEndDate, tStartDate, tEndDate, tNewStartDate, tNewEndDate)
	eid := 0
	if orgName != "" {
		// Update mode - we have
		args := []interface{}{uuid, projectSlug, orgID}
		q := "select id from enrollments where uuid = ? and trim(coalesce(project_slug, '')) = ? and organization_id = ?"
		if startDate != "" {
			q += " and start = str_to_date(?, ?)"
			args = append(args, startDate, cDateTimeFormat)
		}
		if endDate != "" {
			q += " and end = str_to_date(?, ?)"
			args = append(args, endDate, cDateTimeFormat)
		}
		found := 0
		rows, err = query(db, q, args...)
		fatalOnError(err)
		for rows.Next() {
			fatalOnError(rows.Scan(&eid))
			found++
			if found > 1 {
				break
			}
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
		if found == 0 {
			fmt.Printf("WARNING: cannot find identity with uuid=%s project_slug=%s organization=%s/%d start=%s end=%s (row %v)\n", uuid, projectSlug, orgName, orgID, startDate, endDate, row)
			return
		}
		if found > 1 {
			fmt.Printf("WARNING: found more than one identities with uuid=%s project_slug=%s organization=%s/%d start=%s end=%s (row %v)\n", uuid, projectSlug, orgName, orgID, startDate, endDate, row)
			return
		}
		if dbg {
			fmt.Printf("Found: (%d) for uuid=%s project_slug=%s organization=%s/%d start=%s end=%s\n", eid, uuid, projectSlug, orgName, orgID, startDate, endDate)
		}
	} else if dbg {
		fmt.Printf("identity %s/%s insert mode for row %v\n", id, uuid, row)
	}
	if orgID == newOrgID && startDate == newStartDate && endDate == newEndDate {
		if dbg {
			fmt.Printf("enrollment %d for identity_id %s/%s nothing changed in %v\n", eid, id, uuid, row)
		}
		return
	}
	// Concurrecncy check
	if gMtx != nil {
		// Lock working on identity ID
		gMtx.Lock()
		mtx, found := gIDMtx[id]
		gMtx.Unlock()
		if !found {
			mtx = &sync.Mutex{}
			gMtx.Lock()
			gIDMtx[id] = mtx
			gMtx.Unlock()
		} else if dbg {
			fmt.Printf("Duplicate id %s found in %v\n", id, row)
		}
		mtx.Lock()
		defer mtx.Unlock()
		// Lock working on uidentity/profile UUID
		gMtx.Lock()
		umtx, found := gUUIDMtx[uuid]
		gMtx.Unlock()
		if !found {
			umtx = &sync.Mutex{}
			gMtx.Lock()
			gUUIDMtx[uuid] = umtx
			gMtx.Unlock()
		} else if dbg {
			fmt.Printf("Duplicate uuid %s found in %v\n", uuid, row)
		}
		umtx.Lock()
		defer umtx.Unlock()
	}
	args := []interface{}{}
	query, msg, who := "", "", ""
	if eid > 0 {
		query = "update enrollments set "
		msg = fmt.Sprintf("enrollment %d identity_id %s/%s ", eid, id, uuid)
		if newOrgID != orgID {
			query += "organization_id = ?, "
			args = append(args, newOrgName)
			msg += fmt.Sprintf("org %s/%d -> %s/%d ", orgName, orgID, newOrgName, newOrgID)
		}
		if newStartDate != startDate {
			query += "start = str_to_date(?, ?), "
			args = append(args, newStartDate, cDateTimeFormat)
			msg += "start " + startDate + " -> " + newStartDate + " "
		}
		if newEndDate != endDate {
			query += "end = str_to_date(?, ?), "
			args = append(args, newEndDate, cDateTimeFormat)
			msg += "end " + endDate + " -> " + newEndDate + " "
		}
		query += "last_modified = now(), last_modified_by = ?, locked_by = ? where id = ?"
		userSFID, _ := row["user_sfid"]
		userName, _ := row["user_name"]
		userEmail, _ := row["user_email"]
		userSFID = strings.TrimSpace(userSFID)
		userName = strings.TrimSpace(userName)
		userEmail = strings.TrimSpace(userEmail)
		who = "email:" + userEmail + ",name:" + userName + ",sfid:" + userSFID
		msg += " by " + who
		args = append(args, who, "individual", id)
	} else {
		userSFID, _ := row["user_sfid"]
		userName, _ := row["user_name"]
		userEmail, _ := row["user_email"]
		userSFID = strings.TrimSpace(userSFID)
		userName = strings.TrimSpace(userName)
		userEmail = strings.TrimSpace(userEmail)
		who = "email:" + userEmail + ",name:" + userName + ",sfid:" + userSFID
		query = "insert into enrollments(uuid, organization_id, project_slug, start, end, last_modified_by, locked_by) "
		query += "values(?, ?, ?, str_to_date(?, ?), str_to_date(?, ?), ?, ?)"
		args = append(args, uuid, newOrgID, projectSlug, newStartDate, cDateTimeFormat, newEndDate, cDateTimeFormat, who, "individual")
		msg = fmt.Sprintf("new enrollment identity_id %s/%s %s/%d %s %s %s by %s", id, uuid, newOrgName, newOrgID, projectSlug, newStartDate, newEndDate, who)
	}
	if dry {
		fmt.Printf("%s\n", msg)
		if dbg {
			fmt.Printf("(%s,%v)\n", query, args)
		}
		return
	}
	// Actual updates
	var (
		affectedE int64
		affectedP int64
		affectedU int64
		tx        *sql.Tx
		res       sql.Result
	)
	tx, err = db.Begin()
	if err != nil {
		err = fmt.Errorf("error starting transaction %v for row %v", err, row)
		return
	}
	collision := false
	defer func() {
		if tx != nil {
			if !collision || dbg {
				fmt.Printf("rollback %s\n", msg)
			}
			_ = tx.Rollback()
		}
	}()
	// Update/Insert enrollments
	skip := "Error 1062"
	res, err = exec(tx, skip, query, args...)
	if err != nil {
		if strings.Contains(err.Error(), skip) {
			err = nil
			collision = true
			if dbg {
				fmt.Printf("%s: collision\n", msg)
			}
			return
		}
		err = fmt.Errorf("error updating/adding enrollments %v for (%s,%v) for row %v", err, query, args, row)
		return
	}
	affectedE, err = res.RowsAffected()
	if err != nil {
		err = fmt.Errorf("error getting affected rows count %v for (%s,%v) for row %v", err, query, args, row)
		return
	}
	if affectedE <= 0 || dbg {
		fmt.Printf("%s: affected %d enrollments rows\n", msg, affectedE)
	}
	// Update uidentities
	res, err = exec(tx, "", "update uidentities set last_modified = now(), last_modified_by = ?, locked_by = ? where uuid = ?", who, "individual", uuid)
	if err != nil {
		err = fmt.Errorf("error updating uidentities %v for uuid %s for row %v", err, uuid, row)
		return
	}
	affectedU, err = res.RowsAffected()
	if err != nil {
		err = fmt.Errorf("error getting affected rows count %v for uuid %s for row %v", err, uuid, row)
		return
	}
	if affectedU <= 0 || dbg {
		fmt.Printf("%s: affected %d uidentities rows\n", msg, affectedU)
	}
	// Update profiles
	res, err = exec(tx, "", "update profiles set last_modified = now(), last_modified_by = ?, locked_by = ? where uuid = ?", who, "individual", uuid)
	if err != nil {
		err = fmt.Errorf("error updating profiles %v for uuid %s for row %v", err, uuid, row)
		return
	}
	affectedP, err = res.RowsAffected()
	if err != nil {
		err = fmt.Errorf("error getting affected rows count %v for uuid %s for row %v", err, uuid, row)
		return
	}
	if affectedP <= 0 || dbg {
		fmt.Printf("%s: affected %d profiles rows\n", msg, affectedU)
	}
	if affectedE <= 0 || affectedU <= 0 || affectedP <= 0 {
		fmt.Printf("WARNING: %s: didn't affect enrollments or uidentities or profiles: (%d,%d,%d)\n", msg, affectedE, affectedU, affectedP)
		return
	}
	err = tx.Commit()
	if err != nil {
		err = fmt.Errorf("error committing transaction %v for row %v", err, row)
		return
	}
	tx = nil
	if gMtx != nil {
		gMtx.Lock()
		if affectedE > 0 {
			gUpdatedEnrollments[id] = struct{}{}
		}
		if affectedU > 0 {
			gUpdatedUIdentities[uuid] = struct{}{}
		}
		if affectedP > 0 {
			gUpdatedProfiles[uuid] = struct{}{}
		}
		gMtx.Unlock()
	}
	return
}

func importCSVfiles(db *sql.DB, fileNames []string) (err error) {
	gUpdatedEnrollments = make(map[string]struct{})
	gUpdatedIdentities = make(map[string]struct{})
	gUpdatedUIdentities = make(map[string]struct{})
	gUpdatedProfiles = make(map[string]struct{})
	gOrgMap = make(map[string]int)
	gSlugMap = make(map[string]string)
	gOrgMiss = make(map[string]struct{})
	gSlugMiss = make(map[string]struct{})
	gDebugSQL = os.Getenv("DEBUG_SQL") != ""
	dbg := os.Getenv("DEBUG") != ""
	dry := os.Getenv("DRY") != ""
	identitiesFile := fileNames[0]
	affiliationsFile := fileNames[1]
	fmt.Printf("Importing: %s, %s files\n", identitiesFile, affiliationsFile)
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
	thrN := getThreadsNum()
	if thrN > 1 {
		gMtx = &sync.Mutex{}
		gIDMtx = make(map[string]*sync.Mutex)
		gUUIDMtx = make(map[string]*sync.Mutex)
	}
	// Identities CSV data
	var identitiesLines [][]string
	identitiesLines, err = csv.NewReader(fileIdentities).ReadAll()
	if err != nil {
		return
	}

	// Enrollments/Affiliations CSV data
	var enrollmentsLines [][]string
	enrollmentsLines, err = csv.NewReader(fileAffiliations).ReadAll()
	if err != nil {
		return
	}

	// Identities
	hdr := []string{}
	ch := make(chan error)
	if thrN > 1 {
		nThreads := 0
		for i, line := range identitiesLines {
			if i == 0 {
				for _, col := range line {
					hdr = append(hdr, col)
				}
				if dbg {
					fmt.Printf("Identities header: %s\n", hdr)
				}
				continue
			}
			row := map[string]string{}
			for c, col := range line {
				row[hdr[c]] = col
			}
			go func() {
				_ = updateIdentity(ch, db, dbg, dry, row)
			}()
			nThreads++
			if nThreads == thrN {
				err = <-ch
				nThreads--
				if err != nil {
					return
				}
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
	} else {
		for i, line := range identitiesLines {
			if i == 0 {
				for _, col := range line {
					hdr = append(hdr, col)
				}
				if dbg {
					fmt.Printf("Identities header: %s\n", hdr)
				}
				continue
			}
			row := map[string]string{}
			for c, col := range line {
				row[hdr[c]] = col
			}
			err = updateIdentity(nil, db, dbg, dry, row)
			if err != nil {
				return
			}
		}
	}
	fmt.Printf("Updated %d identities, %d uidentities, %d profiles\n", len(gUpdatedIdentities), len(gUpdatedUIdentities), len(gUpdatedProfiles))

	// Enrollments/Affiliations
	if thrN > 1 {
		gIDMtx = make(map[string]*sync.Mutex)
		gUUIDMtx = make(map[string]*sync.Mutex)
	}
	hdr = []string{}
	ch = make(chan error)
	if thrN > 1 {
		nThreads := 0
		for i, line := range enrollmentsLines {
			if i == 0 {
				for _, col := range line {
					hdr = append(hdr, col)
				}
				if dbg {
					fmt.Printf("Enrollments header: %s\n", hdr)
				}
				continue
			}
			row := map[string]string{}
			for c, col := range line {
				row[hdr[c]] = col
			}
			go func() {
				_ = updateEnrollment(ch, db, dbg, dry, row)
			}()
			nThreads++
			if nThreads == thrN {
				err = <-ch
				nThreads--
				if err != nil {
					return
				}
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
	} else {
		for i, line := range enrollmentsLines {
			if i == 0 {
				for _, col := range line {
					hdr = append(hdr, col)
				}
				if dbg {
					fmt.Printf("Enrollments header: %s\n", hdr)
				}
				continue
			}
			row := map[string]string{}
			for c, col := range line {
				row[hdr[c]] = col
			}
			err = updateEnrollment(nil, db, dbg, dry, row)
			if err != nil {
				return
			}
		}
	}
	fmt.Printf("Updated %d enrollments, %d uidentities, %d profiles\n", len(gUpdatedEnrollments), len(gUpdatedUIdentities), len(gUpdatedProfiles))
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
