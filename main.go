package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

var db *sql.DB

func main() {
	var err error

	db, err = sql.Open(
		"postgres",
		"host=db port=5432 user=validator password=val1dat0r dbname=project-sem-1 sslmode=disable",
	)
	if err != nil {
		log.Fatal(err)
	}

	r := mux.NewRouter().StrictSlash(true)
	r.HandleFunc("/api/v0/prices", postPrices).Methods(http.MethodPost)
	r.HandleFunc("/api/v0/prices", getPrices).Methods(http.MethodGet)

	log.Println("Server started on :8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}

func postPrices(w http.ResponseWriter, r *http.Request) {
	archiveType := r.URL.Query().Get("type")
	if archiveType == "" {
		archiveType = "zip"
	}

	var data []byte

	file, _, err := r.FormFile("file")
	if err == nil {
		defer file.Close()
		data, _ = io.ReadAll(file)
	} else {
		data, _ = io.ReadAll(r.Body)
	}

	var csvData []byte

	if archiveType == "tar" {
		tr := tar.NewReader(bytes.NewReader(data))
		for {
			h, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				http.Error(w, "bad tar", 400)
				return
			}
			if strings.HasSuffix(h.Name, "data.csv") {
				csvData, _ = io.ReadAll(tr)
				break
			}
		}
	} else {
		zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			http.Error(w, "bad zip", 400)
			return
		}
		for _, f := range zr.File {
			if strings.HasSuffix(f.Name, "data.csv") {
				rc, _ := f.Open()
				csvData, _ = io.ReadAll(rc)
				rc.Close()
				break
			}
		}
	}

	reader := csv.NewReader(bytes.NewReader(csvData))
	records, _ := reader.ReadAll()

	if len(records) > 0 {
		records = records[1:]
	}

	for _, r := range records {
		id, _ := strconv.Atoi(r[0])
		price, _ := strconv.Atoi(r[3])

		db.Exec(
			`INSERT INTO prices (id, name, category, price, create_date)
			 VALUES ($1,$2,$3,$4,$5)`,
			id, r[1], r[2], price, r[4],
		)
	}

	var totalItems, totalCategories, totalPrice int
	db.QueryRow(`
		SELECT COUNT(*),
		       COUNT(DISTINCT category),
		       COALESCE(SUM(price),0)
		FROM prices
	`).Scan(&totalItems, &totalCategories, &totalPrice)

	resp := map[string]int{
		"total_items":      totalItems,
		"total_categories": totalCategories,
		"total_price":      totalPrice,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func getPrices(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(`
		SELECT id, name, category, price, create_date
		FROM prices
	`)
	if err != nil {
		http.Error(w, "db error", 500)
		return
	}
	defer rows.Close()

	csvBuf := &bytes.Buffer{}
	cw := csv.NewWriter(csvBuf)
	cw.Write([]string{"id", "name", "category", "price", "create_date"})

	for rows.Next() {
		var id, price int
		var name, category, date string
		rows.Scan(&id, &name, &category, &price, &date)

		cw.Write([]string{
			strconv.Itoa(id),
			name,
			category,
			strconv.Itoa(price),
			date,
		})
	}
	cw.Flush()

	// ZIP
	zipBuf := &bytes.Buffer{}
	zw := zip.NewWriter(zipBuf)

	f, err := zw.Create("data.csv")
	if err != nil {
		http.Error(w, "zip error", 500)
		return
	}

	_, err = f.Write(csvBuf.Bytes())
	if err != nil {
		http.Error(w, "zip write error", 500)
		return
	}

	if err := zw.Close(); err != nil {
		http.Error(w, "zip close error", 500)
		return
	}

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", `attachment; filename="response.zip"`)
	w.Header().Set("Content-Length", strconv.Itoa(zipBuf.Len()))

	w.Write(zipBuf.Bytes())
}
