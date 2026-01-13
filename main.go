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

	r := mux.NewRouter()
	r.HandleFunc("/api/v0/prices", postPrices).Methods(http.MethodPost)
	r.HandleFunc("/api/v0/prices", getPrices).Methods(http.MethodGet)

	log.Println("HTTP server started on :8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}

func postPrices(w http.ResponseWriter, r *http.Request) {
	archiveType := r.URL.Query().Get("type")
	if archiveType == "" {
		archiveType = "zip"
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var csvData []byte

	if archiveType == "tar" {
		tr := tar.NewReader(bytes.NewReader(body))
		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if hdr.Name == "data.csv" {
				csvData, _ = io.ReadAll(tr)
				break
			}
		}
	} else {
		zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for _, f := range zr.File {
			if f.Name == "data.csv" {
				rc, _ := f.Open()
				csvData, _ = io.ReadAll(rc)
				rc.Close()
				break
			}
		}
	}

	reader := csv.NewReader(bytes.NewReader(csvData))
	records, _ := reader.ReadAll()

	for _, rec := range records {
		price, _ := strconv.Atoi(rec[4])
		db.Exec(
			"INSERT INTO prices VALUES ($1,$2,$3,$4,$5)",
			rec[0], rec[1], rec[2], rec[3], price,
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
	rows, err := db.Query("SELECT id, created_at, name, category, price FROM prices")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	buf := &bytes.Buffer{}
	cw := csv.NewWriter(buf)

	for rows.Next() {
		var id, name, category string
		var date string
		var price int
		rows.Scan(&id, &date, &name, &category, &price)
		cw.Write([]string{id, date, name, category, strconv.Itoa(price)})
	}
	cw.Flush()

	zipBuf := &bytes.Buffer{}
	zw := zip.NewWriter(zipBuf)
	f, _ := zw.Create("data.csv")
	f.Write(buf.Bytes())
	zw.Close()

	w.Header().Set("Content-Type", "application/zip")
	w.Write(zipBuf.Bytes())
}
