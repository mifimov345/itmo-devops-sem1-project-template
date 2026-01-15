package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

const (
	dbConnString = "host=localhost port=5432 user=validator password=val1dat0r dbname=project-sem-1 sslmode=disable"
)

var db *sql.DB

func main() {
	var err error

	db, err = sql.Open("postgres", dbConnString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	r := mux.NewRouter().StrictSlash(true)
	r.HandleFunc("/api/v0/prices", postPrices).Methods(http.MethodPost)
	r.HandleFunc("/api/v0/prices", getPrices).Methods(http.MethodGet)

	log.Println("Server started on :8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}

type csvRow struct {
	Name       string
	Category   string
	Price      float64
	CreateDate time.Time
}

func postPrices(w http.ResponseWriter, r *http.Request) {
	archiveType := r.URL.Query().Get("type")
	if archiveType == "" {
		archiveType = "zip"
	}

	data, err := readRequestData(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	csvData, err := extractCSV(data, archiveType)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rows, err := parseAndValidateCSV(csvData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "cannot start transaction", http.StatusInternalServerError)
		return
	}

	stmt, err := tx.Prepare(`
		INSERT INTO prices (name, category, price, create_date)
		VALUES ($1,$2,$3,$4)
	`)
	if err != nil {
		tx.Rollback()
		http.Error(w, "prepare failed", http.StatusInternalServerError)
		return
	}
	defer stmt.Close()

	for _, row := range rows {
		_, err = stmt.Exec(row.Name, row.Category, row.Price, row.CreateDate)
		if err != nil {
			tx.Rollback()
			http.Error(w, "insert failed", http.StatusInternalServerError)
			return
		}
	}

	if err = tx.Commit(); err != nil {
		http.Error(w, "commit failed", http.StatusInternalServerError)
		return
	}

	var totalCategories int
	var totalPrice float64

	err = db.QueryRow(`
		SELECT COUNT(DISTINCT category), COALESCE(SUM(price),0)
		FROM prices
	`).Scan(&totalCategories, &totalPrice)

	if err != nil {
		http.Error(w, "aggregation failed", http.StatusInternalServerError)
		return
	}

	resp := map[string]interface{}{
		"total_items":      len(rows),
		"total_categories": totalCategories,
		"total_price":      totalPrice,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func getPrices(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(`
		SELECT name, category, price, create_date
		FROM prices
	`)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	buf := &bytes.Buffer{}
	writer := csv.NewWriter(buf)
	writer.Write([]string{"name", "category", "price", "create_date"})

	for rows.Next() {
		var name, category string
		var price float64
		var date time.Time

		if err := rows.Scan(&name, &category, &price, &date); err != nil {
			http.Error(w, "row scan error", http.StatusInternalServerError)
			return
		}

		writer.Write([]string{
			name,
			category,
			strconv.FormatFloat(price, 'f', 2, 64),
			date.Format("2006-01-02"),
		})
	}

	if err := rows.Err(); err != nil {
		http.Error(w, "rows error", http.StatusInternalServerError)
		return
	}

	writer.Flush()

	zipBuf := &bytes.Buffer{}
	zw := zip.NewWriter(zipBuf)
	f, err := zw.Create("data.csv")
	if err != nil {
		http.Error(w, "zip error", http.StatusInternalServerError)
		return
	}

	if _, err := f.Write(buf.Bytes()); err != nil {
		http.Error(w, "zip write error", http.StatusInternalServerError)
		return
	}

	zw.Close()

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", `attachment; filename="data.zip"`)
	w.Write(zipBuf.Bytes())
}

func readRequestData(r *http.Request) ([]byte, error) {
	file, _, err := r.FormFile("file")
	if err == nil {
		defer file.Close()
		return io.ReadAll(file)
	}
	return io.ReadAll(r.Body)
}

func extractCSV(data []byte, archiveType string) ([]byte, error) {
	if archiveType == "tar" {
		tr := tar.NewReader(bytes.NewReader(data))
		for {
			h, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			if strings.HasSuffix(h.Name, ".csv") {
				return io.ReadAll(tr)
			}
		}
	} else {
		zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return nil, err
		}
		for _, f := range zr.File {
			if strings.HasSuffix(f.Name, ".csv") {
				rc, _ := f.Open()
				defer rc.Close()
				return io.ReadAll(rc)
			}
		}
	}
	return nil, errors.New("csv not found in archive")
}

func parseAndValidateCSV(data []byte) ([]csvRow, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) < 2 {
		return nil, errors.New("no data rows")
	}
	records = records[1:]

	var result []csvRow
	for _, r := range records {
		price, err := strconv.ParseFloat(r[3], 64)
		if err != nil {
			return nil, err
		}
		date, err := time.Parse("2006-01-02", r[4])
		if err != nil {
			return nil, err
		}
		result = append(result, csvRow{
			Name:       r[1],
			Category:   r[2],
			Price:      price,
			CreateDate: date,
		})
	}
	return result, nil
}
