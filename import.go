package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
)

// ImportResult — common response for all import endpoints
type ImportResult struct {
	Imported int      `json:"imported"`
	Skipped  int      `json:"skipped"`
	Errors   []string `json:"errors"`
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func trimFields(row []string) []string {
	out := make([]string, len(row))
	for i, v := range row {
		out[i] = strings.TrimSpace(v)
	}
	return out
}

func parseFloatSafe(s string) float64 {
	s = strings.ReplaceAll(s, ",", "")
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

func parseIntSafe(s string) int {
	s = strings.ReplaceAll(s, ",", "")
	v, _ := strconv.Atoi(s)
	return v
}

// readCSVFromRequest reads CSV from multipart "file" OR raw CSV body
func readCSVFromRequest(c *fiber.Ctx) (headers []string, records [][]string, err error) {
	ct := string(c.Request().Header.ContentType())

	if strings.Contains(ct, "multipart/form-data") {
		fh, ferr := c.FormFile("file")
		if ferr != nil {
			return nil, nil, fmt.Errorf("ไม่พบ field 'file' ใน form-data: %v", ferr)
		}
		f, ferr := fh.Open()
		if ferr != nil {
			return nil, nil, ferr
		}
		defer f.Close()
		r := csv.NewReader(f)
		r.TrimLeadingSpace = true
		all, ferr := r.ReadAll()
		if ferr != nil || len(all) == 0 {
			return nil, nil, fmt.Errorf("ไม่สามารถอ่าน CSV: %v", ferr)
		}
		return all[0], all[1:], nil
	}

	// raw CSV body
	r := csv.NewReader(strings.NewReader(string(c.Body())))
	r.TrimLeadingSpace = true
	all, ferr := r.ReadAll()
	if ferr != nil || len(all) == 0 {
		return nil, nil, fmt.Errorf("ไม่สามารถอ่าน CSV: %v", ferr)
	}
	return all[0], all[1:], nil
}

// colIndex returns position of header name (case-insensitive), or -1
func colIndex(headers []string, names ...string) int {
	for _, name := range names {
		for i, h := range headers {
			if strings.EqualFold(strings.TrimSpace(h), name) {
				return i
			}
		}
	}
	return -1
}

func safeGet(row []string, idx int) string {
	if idx < 0 || idx >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[idx])
}

// ─── POST /api/import/dealers ─────────────────────────────────────────────────
// CSV columns (order flexible, matched by header name):
//   dealer_id, dealer_name, province, region, latitude, longitude, loan_type
// dealer_id is optional — auto-generated if blank
func handleImportDealers(c *fiber.Ctx) error {
	headers, records, err := readCSVFromRequest(c)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	// Also try JSON body
	if len(records) == 0 && strings.Contains(string(c.Request().Header.ContentType()), "application/json") {
		var rows []map[string]interface{}
		if err := json.Unmarshal(c.Body(), &rows); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "ไม่สามารถอ่านข้อมูล JSON: " + err.Error()})
		}
		return importDealersFromJSON(c, rows)
	}

	iID      := colIndex(headers, "dealer_id")
	iName    := colIndex(headers, "dealer_name", "name")
	iProv    := colIndex(headers, "province", "จังหวัด")
	iRegion  := colIndex(headers, "region", "ภาค", "ภูมิภาค")
	iLat     := colIndex(headers, "latitude", "lat")
	iLng     := colIndex(headers, "longitude", "lng", "lon")
	iType    := colIndex(headers, "loan_type", "ประเภท", "type")

	if iName < 0 {
		return c.Status(400).JSON(fiber.Map{"error": "ต้องมีคอลัมน์ dealer_name"})
	}

	res := ImportResult{Errors: []string{}}
	for lineNo, raw := range records {
		row := trimFields(raw)
		name := safeGet(row, iName)
		if name == "" {
			res.Skipped++
			continue
		}
		id := safeGet(row, iID)
		if id == "" {
			id = fmt.Sprintf("DX%d", time.Now().UnixNano())
			time.Sleep(time.Microsecond) // ensure unique
		}
		prov   := safeGet(row, iProv)
		region := safeGet(row, iRegion)
		lat    := parseFloatSafe(safeGet(row, iLat))
		lng    := parseFloatSafe(safeGet(row, iLng))
		ltype  := safeGet(row, iType)
		if ltype == "" {
			ltype = "เช่าซื้อ"
		}

		_, err := DB.Exec(`
			INSERT INTO dealers(dealer_id, dealer_name, province, region, latitude, longitude, loan_type)
			VALUES($1,$2,$3,$4,$5,$6,$7)
			ON CONFLICT(dealer_id) DO UPDATE
			  SET dealer_name=$2, province=$3, region=$4,
			      latitude=$5, longitude=$6, loan_type=$7
		`, id, name, prov, region, lat, lng, ltype)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("แถว %d (%s): %v", lineNo+2, name, err))
			res.Skipped++
		} else {
			res.Imported++
		}
	}

	hub.broadcast("refresh")
	return c.JSON(res)
}

func importDealersFromJSON(c *fiber.Ctx, rows []map[string]interface{}) error {
	res := ImportResult{Errors: []string{}}
	for i, m := range rows {
		name, _ := m["dealer_name"].(string)
		if name == "" {
			name, _ = m["name"].(string)
		}
		if name == "" {
			res.Skipped++
			continue
		}
		id, _ := m["dealer_id"].(string)
		if id == "" {
			id = fmt.Sprintf("DX%d", time.Now().UnixNano())
			time.Sleep(time.Microsecond)
		}
		prov, _   := m["province"].(string)
		region, _ := m["region"].(string)
		lat  := parseFloatSafe(fmt.Sprintf("%v", m["latitude"]))
		lng  := parseFloatSafe(fmt.Sprintf("%v", m["longitude"]))
		ltype, _ := m["loan_type"].(string)
		if ltype == "" {
			ltype = "เช่าซื้อ"
		}
		_, err := DB.Exec(`
			INSERT INTO dealers(dealer_id, dealer_name, province, region, latitude, longitude, loan_type)
			VALUES($1,$2,$3,$4,$5,$6,$7)
			ON CONFLICT(dealer_id) DO UPDATE
			  SET dealer_name=$2, province=$3, region=$4,
			      latitude=$5, longitude=$6, loan_type=$7
		`, id, name, prov, region, lat, lng, ltype)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("รายการ %d (%s): %v", i+1, name, err))
			res.Skipped++
		} else {
			res.Imported++
		}
	}
	hub.broadcast("refresh")
	return c.JSON(res)
}

// ─── POST /api/import/customers ───────────────────────────────────────────────
// CSV columns: customer_id (optional), full_name, id_card, phone, province
func handleImportCustomers(c *fiber.Ctx) error {
	headers, records, err := readCSVFromRequest(c)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	iID    := colIndex(headers, "customer_id")
	iName  := colIndex(headers, "full_name", "name", "ชื่อ")
	iCard  := colIndex(headers, "id_card", "เลขบัตร")
	iPhone := colIndex(headers, "phone", "โทรศัพท์", "tel")
	iProv  := colIndex(headers, "province", "จังหวัด")

	if iName < 0 {
		return c.Status(400).JSON(fiber.Map{"error": "ต้องมีคอลัมน์ full_name"})
	}

	res := ImportResult{Errors: []string{}}
	for lineNo, raw := range records {
		row  := trimFields(raw)
		name := safeGet(row, iName)
		if name == "" {
			res.Skipped++
			continue
		}
		id := safeGet(row, iID)
		if id == "" {
			// check if customer exists by name
			var existID string
			lookErr := DB.QueryRow(`SELECT customer_id FROM customers WHERE full_name=$1 LIMIT 1`, name).Scan(&existID)
			if lookErr == nil {
				// already exists → update
				id = existID
			} else {
				id = fmt.Sprintf("CX%d", time.Now().UnixNano())
				time.Sleep(time.Microsecond)
			}
		}
		card  := safeGet(row, iCard)
		phone := safeGet(row, iPhone)
		prov  := safeGet(row, iProv)

		_, err := DB.Exec(`
			INSERT INTO customers(customer_id, full_name, id_card, phone, province)
			VALUES($1,$2,NULLIF($3,''),NULLIF($4,''),NULLIF($5,''))
			ON CONFLICT(customer_id) DO UPDATE
			  SET full_name=$2,
			      id_card=COALESCE(NULLIF($3,''), customers.id_card),
			      phone=COALESCE(NULLIF($4,''), customers.phone),
			      province=COALESCE(NULLIF($5,''), customers.province)
		`, id, name, card, phone, prov)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("แถว %d (%s): %v", lineNo+2, name, err))
			res.Skipped++
		} else {
			res.Imported++
		}
	}
	return c.JSON(res)
}

// ─── POST /api/import/loans ───────────────────────────────────────────────────
// CSV columns:
//   loan_id (optional), dealer_id, customer_name (or customer_id),
//   principal_amount, outstanding_balance (optional=principal),
//   loan_type, dpd, status (optional=ACTIVE), contract_date (optional)
func handleImportLoans(c *fiber.Ctx) error {
	headers, records, err := readCSVFromRequest(c)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	iLoanID     := colIndex(headers, "loan_id")
	iDealerID   := colIndex(headers, "dealer_id")
	iCustName   := colIndex(headers, "customer_name", "full_name", "ชื่อลูกค้า")
	iCustID     := colIndex(headers, "customer_id")
	iPrincipal  := colIndex(headers, "principal_amount", "principal", "วงเงิน")
	iOutstand   := colIndex(headers, "outstanding_balance", "outstanding", "ค้างชำระ")
	iType       := colIndex(headers, "loan_type", "ประเภท", "type")
	iDPD        := colIndex(headers, "dpd")
	iStatus     := colIndex(headers, "status", "สถานะ")
	iContract   := colIndex(headers, "contract_date", "วันทำสัญญา")

	if iDealerID < 0 {
		return c.Status(400).JSON(fiber.Map{"error": "ต้องมีคอลัมน์ dealer_id"})
	}
	if iPrincipal < 0 {
		return c.Status(400).JSON(fiber.Map{"error": "ต้องมีคอลัมน์ principal_amount"})
	}

	res := ImportResult{Errors: []string{}}
	for lineNo, raw := range records {
		row      := trimFields(raw)
		dealerID := safeGet(row, iDealerID)
		if dealerID == "" {
			res.Skipped++
			continue
		}

		principal := parseFloatSafe(safeGet(row, iPrincipal))
		if principal <= 0 {
			res.Errors = append(res.Errors, fmt.Sprintf("แถว %d: principal_amount ต้องมากกว่า 0", lineNo+2))
			res.Skipped++
			continue
		}

		// Resolve customer
		custID := safeGet(row, iCustID)
		if custID == "" {
			custName := safeGet(row, iCustName)
			if custName == "" {
				res.Errors = append(res.Errors, fmt.Sprintf("แถว %d: ต้องมี customer_name หรือ customer_id", lineNo+2))
				res.Skipped++
				continue
			}
			lookErr := DB.QueryRow(`SELECT customer_id FROM customers WHERE full_name=$1 LIMIT 1`, custName).Scan(&custID)
			if lookErr == sql.ErrNoRows {
				custID = fmt.Sprintf("CX%d", time.Now().UnixNano())
				time.Sleep(time.Microsecond)
				if _, e := DB.Exec(`INSERT INTO customers(customer_id,full_name) VALUES($1,$2)`, custID, custName); e != nil {
					res.Errors = append(res.Errors, fmt.Sprintf("แถว %d: สร้าง customer ไม่ได้: %v", lineNo+2, e))
					res.Skipped++
					continue
				}
			} else if lookErr != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("แถว %d: %v", lineNo+2, lookErr))
				res.Skipped++
				continue
			}
		}

		outstanding := parseFloatSafe(safeGet(row, iOutstand))
		if outstanding <= 0 {
			outstanding = principal
		}
		ltype := safeGet(row, iType)
		if ltype == "" {
			ltype = "เช่าซื้อ"
		}
		dpd := parseIntSafe(safeGet(row, iDPD))

		status := safeGet(row, iStatus)
		if status == "" {
			status = "ACTIVE"
		}
		status = strings.ToUpper(status)

		// Validate dealer exists
		var dummy string
		if e := DB.QueryRow(`SELECT dealer_id FROM dealers WHERE dealer_id=$1`, dealerID).Scan(&dummy); e == sql.ErrNoRows {
			res.Errors = append(res.Errors, fmt.Sprintf("แถว %d: dealer_id '%s' ไม่พบในระบบ", lineNo+2, dealerID))
			res.Skipped++
			continue
		}

		loanID := safeGet(row, iLoanID)
		if loanID == "" {
			loanID = fmt.Sprintf("LX%d", time.Now().UnixNano())
			time.Sleep(time.Microsecond)
		}

		contractDate := safeGet(row, iContract)
		var contractArg interface{}
		if contractDate != "" {
			contractArg = contractDate
		}

		_, err := DB.Exec(`
			INSERT INTO loans(loan_id, dealer_id, customer_id, principal_amount,
				outstanding_balance, loan_type, status, dpd, contract_date)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,COALESCE($9::date, NOW()))
			ON CONFLICT(loan_id) DO UPDATE
			  SET outstanding_balance=$5, loan_type=$6, status=$7, dpd=$8
		`, loanID, dealerID, custID, principal, outstanding, ltype, status, dpd, contractArg)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("แถว %d (loan %s): %v", lineNo+2, loanID, err))
			res.Skipped++
		} else {
			res.Imported++
		}
	}

	hub.broadcast("refresh")
	return c.JSON(res)
}

// ─── GET /api/import/template/:type ──────────────────────────────────────────
// Download a template CSV for dealers / customers / loans
func handleImportTemplate(c *fiber.Ctx) error {
	ttype := c.Params("type")
	var filename string
	var headerRow []string
	var exampleRows [][]string

	switch ttype {
	case "dealers":
		filename = "template_dealers.csv"
		headerRow = []string{"dealer_id", "dealer_name", "province", "region", "latitude", "longitude", "loan_type"}
		exampleRows = [][]string{
			{"", "บริษัท สยาม มอเตอร์ จำกัด", "กรุงเทพมหานคร", "กรุงเทพฯ และปริมณฑล", "13.7563", "100.5018", "เช่าซื้อ"},
			{"", "เอ็มที ออโต้ เซลส์", "เชียงใหม่", "ภาคเหนือ", "18.7883", "98.9853", "เช่าซื้อ"},
			{"", "สุราษฎร์ คาร์ เซ็นเตอร์", "สุราษฎร์ธานี", "ภาคใต้", "9.1382", "99.3144", "Personal"},
		}
	case "customers":
		filename = "template_customers.csv"
		headerRow = []string{"customer_id", "full_name", "id_card", "phone", "province"}
		exampleRows = [][]string{
			{"", "นายสมชาย ใจดี", "1234567890123", "0812345678", "กรุงเทพมหานคร"},
			{"", "นางสาวสมหญิง รักดี", "9876543210987", "0898765432", "เชียงใหม่"},
			{"", "นายวิชัย มั่งมี", "5555555555555", "0861234567", "ขอนแก่น"},
		}
	case "loans":
		filename = "template_loans.csv"
		headerRow = []string{"loan_id", "dealer_id", "customer_name", "principal_amount", "outstanding_balance", "loan_type", "dpd", "status", "contract_date"}
		exampleRows = [][]string{
			{"", "D001", "นายสมชาย ใจดี", "500000", "480000", "เช่าซื้อ", "15", "ACTIVE", "2024-01-15"},
			{"", "D001", "นางสาวสมหญิง รักดี", "300000", "300000", "Personal", "95", "ACTIVE", "2023-06-01"},
			{"", "D002", "นายวิชัย มั่งมี", "750000", "700000", "เช่าซื้อ", "0", "ACTIVE", "2024-03-20"},
		}
	default:
		return c.Status(400).JSON(fiber.Map{"error": "type ต้องเป็น dealers, customers หรือ loans"})
	}

	c.Set("Content-Type", "text/csv; charset=utf-8")
	c.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))

	w := csv.NewWriter(c.Response().BodyWriter())
	w.Write(headerRow)
	for _, row := range exampleRows {
		w.Write(row)
	}
	w.Flush()
	return nil
}

// ─── GET /api/import/validate (dry-run preview) ───────────────────────────────
// POST /api/import/validate/:type — parses CSV and returns preview rows without saving
func handleImportValidate(c *fiber.Ctx) error {
	ttype := c.Params("type")
	headers, records, err := readCSVFromRequest(c)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	type PreviewRow struct {
		Row    int                    `json:"row"`
		Data   map[string]string      `json:"data"`
		Issues []string               `json:"issues,omitempty"`
	}

	var preview []PreviewRow
	limit := 20
	if len(records) < limit {
		limit = len(records)
	}

	for i, raw := range records[:limit] {
		row := trimFields(raw)
		d := map[string]string{}
		issues := []string{}
		for j, h := range headers {
			if j < len(row) {
				d[strings.TrimSpace(h)] = row[j]
			}
		}

		switch ttype {
		case "dealers":
			if d["dealer_name"] == "" {
				issues = append(issues, "dealer_name ว่าง")
			}
		case "customers":
			if d["full_name"] == "" {
				issues = append(issues, "full_name ว่าง")
			}
		case "loans":
			if d["dealer_id"] == "" {
				issues = append(issues, "dealer_id ว่าง")
			}
			if parseFloatSafe(d["principal_amount"]) <= 0 {
				issues = append(issues, "principal_amount ต้องมากกว่า 0")
			}
			if d["customer_name"] == "" && d["customer_id"] == "" {
				issues = append(issues, "ต้องมี customer_name หรือ customer_id")
			}
		}

		preview = append(preview, PreviewRow{Row: i + 2, Data: d, Issues: issues})
	}

	if preview == nil {
		preview = []PreviewRow{}
	}
	return c.JSON(fiber.Map{
		"headers":    headers,
		"preview":    preview,
		"totalRows":  len(records),
		"previewMax": limit,
	})
}
