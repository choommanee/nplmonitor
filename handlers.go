package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"math"
	"time"

	"github.com/gofiber/fiber/v2"
)

// helper: risk level string
func riskLevel(rate float64) string {
	if rate >= 0.10 {
		return "high"
	} else if rate >= 0.05 {
		return "mid"
	}
	return "low"
}

// GET /api/health
func handleHealth(c *fiber.Ctx) error {
	return c.JSON(HealthResponse{Status: "ok", Version: "1.0.0"})
}

// GET /api/summary
func handleSummary(c *fiber.Ctx) error {
	row := DB.QueryRow(`
		SELECT
			COALESCE(SUM(l.principal_amount), 0)                                                      AS total_port,
			COALESCE(SUM(CASE WHEN l.dpd > 90  THEN l.outstanding_balance ELSE 0 END), 0)            AS total_npl,
			COUNT(DISTINCT l.loan_id)                                                                  AS total_loans,
			COUNT(DISTINCT d.dealer_id)                                                                AS total_dealers,
			COUNT(CASE WHEN l.dpd BETWEEN 30 AND 59 THEN 1 END)                                       AS dpd30,
			COALESCE(SUM(CASE WHEN l.dpd BETWEEN 30 AND 59 THEN l.outstanding_balance ELSE 0 END), 0) AS dpd30_amt,
			COUNT(CASE WHEN l.dpd BETWEEN 60 AND 89 THEN 1 END)                                       AS dpd60,
			COALESCE(SUM(CASE WHEN l.dpd BETWEEN 60 AND 89 THEN l.outstanding_balance ELSE 0 END), 0) AS dpd60_amt
		FROM loans l
		JOIN dealers d ON d.dealer_id = l.dealer_id
		WHERE l.status <> 'CLOSED'
	`)
	var port, npl, dpd30Amt, dpd60Amt float64
	var totalLoans, totalDealers, dpd30, dpd60 int
	if err := row.Scan(&port, &npl, &totalLoans, &totalDealers,
		&dpd30, &dpd30Amt, &dpd60, &dpd60Amt); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	// count risk per dealer
	rows, err := DB.Query(`
		SELECT
			SUM(CASE WHEN l.dpd > 90 THEN l.outstanding_balance ELSE 0 END) / NULLIF(SUM(l.principal_amount),0) AS rate
		FROM loans l
		WHERE l.status <> 'CLOSED'
		GROUP BY l.dealer_id
	`)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	defer rows.Close()
	var high, mid, low int
	for rows.Next() {
		var rate sql.NullFloat64
		rows.Scan(&rate)
		r := rate.Float64
		switch riskLevel(r) {
		case "high":
			high++
		case "mid":
			mid++
		default:
			low++
		}
	}

	nplRate := 0.0
	if port > 0 {
		nplRate = math.Round(npl/port*10000) / 100
	}

	return c.JSON(SummaryResponse{
		TotalPort:    port,
		TotalNPL:     npl,
		NPLRate:      nplRate,
		HighRisk:     high,
		MidRisk:      mid,
		LowRisk:      low,
		TotalDealers: totalDealers,
		TotalLoans:   totalLoans,
		Dpd30:        dpd30,
		Dpd30Amt:     dpd30Amt,
		Dpd60:        dpd60,
		Dpd60Amt:     dpd60Amt,
	})
}

// GET /api/dealers?region=&risk=&province=
func handleDealers(c *fiber.Ctx) error {
	regionFilter   := c.Query("region", "")
	riskFilter     := c.Query("risk", "")
	provinceFilter := c.Query("province", "")

	query := `
		SELECT
			d.dealer_id,
			d.dealer_name,
			d.province,
			d.region,
			d.latitude,
			d.longitude,
			COALESCE(SUM(l.principal_amount), 0)                                            AS port,
			COALESCE(SUM(CASE WHEN l.dpd > 90 THEN l.outstanding_balance ELSE 0 END), 0)   AS npl_amt,
			COUNT(l.loan_id)                                                                 AS loans,
			d.loan_type
		FROM dealers d
		LEFT JOIN loans l ON l.dealer_id = d.dealer_id AND l.status <> 'CLOSED'
		WHERE ($1 = '' OR d.region = $1)
		  AND ($2 = '' OR d.province = $2)
		GROUP BY d.dealer_id, d.dealer_name, d.province, d.region,
		         d.latitude, d.longitude, d.loan_type
		ORDER BY npl_amt DESC
	`
	rows, err := DB.Query(query, regionFilter, provinceFilter)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	defer rows.Close()

	var result []DealerRow
	for rows.Next() {
		var d DealerRow
		rows.Scan(&d.ID, &d.Name, &d.Province, &d.Region,
			&d.Lat, &d.Lng, &d.Port, &d.NplAmt, &d.Loans, &d.Type)
		if d.Port > 0 {
			d.NplRate = math.Round(d.NplAmt/d.Port*10000) / 100
		}
		d.RiskLevel = riskLevel(d.NplAmt / maxF(d.Port, 1))
		if riskFilter == "" || d.RiskLevel == riskFilter {
			result = append(result, d)
		}
	}
	if result == nil {
		result = []DealerRow{}
	}
	return c.JSON(result)
}

// GET /api/dealers/:id
func handleDealerDetail(c *fiber.Ctx) error {
	id := c.Params("id")
	row := DB.QueryRow(`
		SELECT
			d.dealer_id, d.dealer_name, d.province, d.region,
			d.latitude, d.longitude, d.loan_type,
			COALESCE(SUM(l.principal_amount), 0)                                          AS port,
			COALESCE(SUM(CASE WHEN l.dpd > 90 THEN l.outstanding_balance ELSE 0 END), 0) AS npl_amt,
			COUNT(l.loan_id)                                                               AS loans,
			COUNT(CASE WHEN l.dpd > 90 THEN 1 END)                                        AS loans_90,
			COALESCE(AVG(CASE WHEN l.dpd > 0 THEN l.dpd END), 0)                          AS avg_dpd,
			COALESCE(AVG(l.principal_amount), 0)                                           AS avg_loan,
			COUNT(CASE WHEN l.status = 'PENDING' THEN 1 END)                               AS pending
		FROM dealers d
		LEFT JOIN loans l ON l.dealer_id = d.dealer_id AND l.status <> 'CLOSED'
		WHERE d.dealer_id = $1
		GROUP BY d.dealer_id, d.dealer_name, d.province,
		         d.region, d.latitude, d.longitude, d.loan_type
	`, id)

	var det DealerDetail
	err := row.Scan(
		&det.ID, &det.Name, &det.Province, &det.Region,
		&det.Lat, &det.Lng, &det.Type,
		&det.Port, &det.NplAmt, &det.Loans,
		&det.Loans90, &det.AvgDPD, &det.AvgLoan, &det.Pending,
	)
	if err == sql.ErrNoRows {
		return c.Status(404).JSON(fiber.Map{"error": "dealer not found"})
	}
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	if det.Port > 0 {
		det.NplRate = math.Round(det.NplAmt/det.Port*10000) / 100
	}
	det.RiskLevel = riskLevel(det.NplAmt / maxF(det.Port, 1))
	return c.JSON(det)
}

// GET /api/regions
func handleRegions(c *fiber.Ctx) error {
	rows, err := DB.Query(`
		SELECT
			d.region,
			COALESCE(SUM(l.principal_amount), 0)                                          AS port,
			COALESCE(SUM(CASE WHEN l.dpd > 90 THEN l.outstanding_balance ELSE 0 END), 0) AS npl_amt,
			COUNT(DISTINCT d.dealer_id)                                                    AS dealers
		FROM dealers d
		LEFT JOIN loans l ON l.dealer_id = d.dealer_id AND l.status <> 'CLOSED'
		GROUP BY d.region
		ORDER BY npl_amt DESC
	`)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	defer rows.Close()

	var result []RegionRow
	for rows.Next() {
		var r RegionRow
		rows.Scan(&r.Region, &r.Port, &r.NplAmt, &r.Dealers)
		if r.Port > 0 {
			r.NplRate = math.Round(r.NplAmt/r.Port*10000) / 100
		}
		result = append(result, r)
	}
	if result == nil {
		result = []RegionRow{}
	}
	return c.JSON(result)
}

// GET /api/provinces?region=
func handleProvinces(c *fiber.Ctx) error {
	regionFilter := c.Query("region", "")
	rows, err := DB.Query(`
		SELECT
			d.province,
			d.region,
			COALESCE(SUM(l.principal_amount), 0)                                          AS port,
			COALESCE(SUM(CASE WHEN l.dpd > 90 THEN l.outstanding_balance ELSE 0 END), 0) AS npl_amt,
			COUNT(DISTINCT d.dealer_id)                                                    AS dealers
		FROM dealers d
		LEFT JOIN loans l ON l.dealer_id = d.dealer_id AND l.status <> 'CLOSED'
		WHERE ($1 = '' OR d.region = $1)
		GROUP BY d.province, d.region
		ORDER BY npl_amt DESC
		LIMIT 15
	`, regionFilter)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	defer rows.Close()

	var result []ProvinceRow
	for rows.Next() {
		var p ProvinceRow
		rows.Scan(&p.Province, &p.Region, &p.Port, &p.NplAmt, &p.Dealers)
		if p.Port > 0 {
			p.NplRate = math.Round(p.NplAmt/p.Port*10000) / 100
		}
		result = append(result, p)
	}
	if result == nil {
		result = []ProvinceRow{}
	}
	return c.JSON(result)
}

// GET /api/top-npls?limit=10
func handleTopNPLs(c *fiber.Ctx) error {
	limit := c.QueryInt("limit", 10)
	rows, err := DB.Query(`
		SELECT
			ROW_NUMBER() OVER (ORDER BY l.outstanding_balance DESC) AS rank,
			cu.customer_id,
			cu.full_name,
			d.dealer_name,
			d.province,
			d.region,
			l.outstanding_balance                                   AS npl_amt,
			l.principal_amount                                      AS port,
			l.dpd,
			COUNT(l.loan_id) OVER (PARTITION BY cu.customer_id)    AS total_loans,
			l.loan_type
		FROM loans l
		JOIN customers cu ON cu.customer_id = l.customer_id
		JOIN dealers   d  ON d.dealer_id    = l.dealer_id
		WHERE l.dpd > 90 AND l.status <> 'CLOSED'
		ORDER BY l.outstanding_balance DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	defer rows.Close()

	var result []TopNPLRow
	for rows.Next() {
		var t TopNPLRow
		rows.Scan(&t.Rank, &t.CustomerID, &t.Name, &t.DealerName,
			&t.Province, &t.Region, &t.NplAmt, &t.Port,
			&t.DPD, &t.Loans, &t.LoanType)
		if t.Port > 0 {
			t.NplRate = math.Round(t.NplAmt/t.Port*10000) / 100
		}
		result = append(result, t)
	}
	if result == nil {
		result = []TopNPLRow{}
	}
	return c.JSON(result)
}

// GET /api/trend?months=12
func handleTrend(c *fiber.Ctx) error {
	months := c.QueryInt("months", 12)
	rows, err := DB.Query(`
		SELECT
			TO_CHAR(month_date, 'YYYY-MM')                AS month,
			COALESCE(SUM(port_snapshot), 0)               AS port,
			COALESCE(SUM(npl_snapshot), 0)                AS npl_amt
		FROM npl_monthly_snapshot
		WHERE month_date >= NOW() - ($1 || ' months')::INTERVAL
		GROUP BY month_date
		ORDER BY month_date ASC
	`, months)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	defer rows.Close()

	var result []TrendRow
	for rows.Next() {
		var t TrendRow
		rows.Scan(&t.Month, &t.Port, &t.NplAmt)
		if t.Port > 0 {
			t.NplRate = math.Round(t.NplAmt/t.Port*10000) / 100
		}
		result = append(result, t)
	}
	if result == nil {
		result = []TrendRow{}
	}
	return c.JSON(result)
}

func maxF(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// GET /api/loans?dealer_id=
func handleGetLoans(c *fiber.Ctx) error {
	dealerID := c.Query("dealer_id", "")
	rows, err := DB.Query(`
		SELECT
			l.loan_id, l.dealer_id, l.customer_id,
			COALESCE(cu.full_name, l.customer_id)  AS customer_name,
			l.principal_amount, l.outstanding_balance,
			l.loan_type, l.status, l.dpd,
			COALESCE(l.contract_date::text, '')    AS contract_date
		FROM loans l
		LEFT JOIN customers cu ON cu.customer_id = l.customer_id
		WHERE ($1 = '' OR l.dealer_id = $1)
		ORDER BY l.dpd DESC, l.outstanding_balance DESC
	`, dealerID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	defer rows.Close()

	var result []LoanRow
	for rows.Next() {
		var l LoanRow
		rows.Scan(&l.ID, &l.DealerID, &l.CustomerID, &l.CustomerName,
			&l.Principal, &l.Outstanding, &l.LoanType, &l.Status, &l.DPD, &l.ContractDate)
		result = append(result, l)
	}
	if result == nil {
		result = []LoanRow{}
	}
	return c.JSON(result)
}

// PUT /api/loans/:id  — partial update via COALESCE (nil = keep existing)
func handleUpdateLoan(c *fiber.Ctx) error {
	id := c.Params("id")
	var body struct {
		DPD         *int     `json:"dpd"`
		Outstanding *float64 `json:"outstanding"`
		Status      *string  `json:"status"`
	}
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid body"})
	}
	_, err := DB.Exec(`
		UPDATE loans SET
			dpd                 = COALESCE($1, dpd),
			outstanding_balance = COALESCE($2, outstanding_balance),
			status              = COALESCE($3, status)
		WHERE loan_id = $4`,
		body.DPD, body.Outstanding, body.Status, id)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	hub.broadcast("refresh")
	return c.JSON(fiber.Map{"ok": true})
}

// GET /api/collection-priority?limit=30
func handleCollectionPriority(c *fiber.Ctx) error {
	limit := c.QueryInt("limit", 30)
	rows, err := DB.Query(`
		SELECT
			ROW_NUMBER() OVER (ORDER BY (CAST(l.dpd AS FLOAT8) * l.outstanding_balance) DESC) AS priority,
			l.loan_id,
			COALESCE(cu.full_name, l.customer_id) AS customer_name,
			d.dealer_name,
			d.province,
			d.region,
			l.dpd,
			l.outstanding_balance,
			l.principal_amount,
			l.loan_type,
			ROUND((CAST(l.dpd AS FLOAT8) * l.outstanding_balance / 1000000.0)::numeric, 2) AS priority_score,
			CASE
				WHEN l.dpd <= 29  THEN 'MONITOR'
				WHEN l.dpd <= 89  THEN 'CALL'
				WHEN l.dpd <= 120 THEN 'FIELD'
				WHEN l.dpd <= 180 THEN 'LEGAL'
				ELSE 'REPOSSESS'
			END AS action,
			COALESCE(
				TO_CHAR(
					(SELECT created_at FROM collection_logs WHERE loan_id = l.loan_id ORDER BY created_at DESC LIMIT 1)
					AT TIME ZONE 'Asia/Bangkok', 'DD/MM/YY'
				), ''
			) AS last_contact,
			COALESCE(
				(SELECT result FROM collection_logs WHERE loan_id = l.loan_id ORDER BY created_at DESC LIMIT 1),
				''
			) AS last_result
		FROM loans l
		JOIN customers cu ON cu.customer_id = l.customer_id
		JOIN dealers   d  ON d.dealer_id    = l.dealer_id
		WHERE l.status <> 'CLOSED' AND l.dpd > 0
		ORDER BY priority_score DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	defer rows.Close()

	var result []CollectionItem
	for rows.Next() {
		var item CollectionItem
		rows.Scan(&item.Priority, &item.LoanID, &item.CustomerName,
			&item.DealerName, &item.Province, &item.Region,
			&item.DPD, &item.Outstanding, &item.Principal,
			&item.LoanType, &item.PriorityScore, &item.Action,
			&item.LastContact, &item.LastResult)
		result = append(result, item)
	}
	if result == nil {
		result = []CollectionItem{}
	}
	return c.JSON(result)
}

// POST /api/loans
func handleCreateLoan(c *fiber.Ctx) error {
	var body struct {
		DealerID     string  `json:"dealerId"`
		CustomerName string  `json:"customerName"`
		Principal    float64 `json:"principal"`
		Outstanding  float64 `json:"outstanding"`
		LoanType     string  `json:"loanType"`
		DPD          int     `json:"dpd"`
	}
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid body"})
	}
	if body.DealerID == "" || body.CustomerName == "" || body.Principal <= 0 {
		return c.Status(400).JSON(fiber.Map{"error": "dealerId, customerName, principal required"})
	}
	if body.Outstanding <= 0 {
		body.Outstanding = body.Principal
	}
	if body.LoanType == "" {
		body.LoanType = "เช่าซื้อ"
	}

	// Find or create customer
	var custID string
	err := DB.QueryRow(`SELECT customer_id FROM customers WHERE full_name = $1 LIMIT 1`,
		body.CustomerName).Scan(&custID)
	if err == sql.ErrNoRows {
		custID = fmt.Sprintf("CX%d", time.Now().UnixMilli())
		if _, err = DB.Exec(
			`INSERT INTO customers(customer_id,full_name) VALUES($1,$2)`,
			custID, body.CustomerName); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
	} else if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	loanID := fmt.Sprintf("LX%d", time.Now().UnixMilli())
	if _, err = DB.Exec(`
		INSERT INTO loans(loan_id,dealer_id,customer_id,principal_amount,
			outstanding_balance,loan_type,status,dpd,contract_date)
		VALUES($1,$2,$3,$4,$5,$6,'ACTIVE',$7,NOW())`,
		loanID, body.DealerID, custID,
		body.Principal, body.Outstanding, body.LoanType, body.DPD); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	hub.broadcast("refresh")
	return c.Status(201).JSON(fiber.Map{"ok": true, "loanId": loanID})
}

// GET /api/loans/:id/logs
func handleGetLoanLogs(c *fiber.Ctx) error {
	id := c.Params("id")
	rows, err := DB.Query(`
		SELECT id, loan_id, action_type, result,
		       COALESCE(promise_date::text,''), COALESCE(promise_amt,0),
		       COALESCE(notes,''), created_by,
		       TO_CHAR(created_at AT TIME ZONE 'Asia/Bangkok', 'DD/MM/YY HH24:MI') AS ts
		FROM collection_logs
		WHERE loan_id = $1
		ORDER BY created_at DESC
		LIMIT 20
	`, id)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	defer rows.Close()
	var result []CollectionLog
	for rows.Next() {
		var l CollectionLog
		rows.Scan(&l.ID, &l.LoanID, &l.ActionType, &l.Result,
			&l.PromiseDate, &l.PromiseAmt, &l.Notes, &l.CreatedBy, &l.CreatedAt)
		result = append(result, l)
	}
	if result == nil {
		result = []CollectionLog{}
	}
	return c.JSON(result)
}

// POST /api/loans/:id/logs
func handleCreateLoanLog(c *fiber.Ctx) error {
	loanID := c.Params("id")
	var body struct {
		ActionType  string  `json:"actionType"`
		Result      string  `json:"result"`
		PromiseDate string  `json:"promiseDate"`
		PromiseAmt  float64 `json:"promiseAmt"`
		Notes       string  `json:"notes"`
		CreatedBy   string  `json:"createdBy"`
	}
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid body"})
	}
	if body.ActionType == "" || body.Result == "" {
		return c.Status(400).JSON(fiber.Map{"error": "actionType and result required"})
	}
	if body.CreatedBy == "" {
		body.CreatedBy = "ทีมงาน"
	}

	var promiseDate interface{} = nil
	if body.PromiseDate != "" {
		promiseDate = body.PromiseDate
	}
	var promiseAmt interface{} = nil
	if body.PromiseAmt > 0 {
		promiseAmt = body.PromiseAmt
	}

	_, err := DB.Exec(`
		INSERT INTO collection_logs(loan_id, action_type, result, promise_date, promise_amt, notes, created_by)
		VALUES($1,$2,$3,$4,$5,$6,$7)`,
		loanID, body.ActionType, body.Result, promiseDate, promiseAmt, body.Notes, body.CreatedBy)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	hub.broadcast("activity")
	return c.Status(201).JSON(fiber.Map{"ok": true})
}

// GET /api/activity-feed?limit=20
func handleActivityFeed(c *fiber.Ctx) error {
	limit := c.QueryInt("limit", 15)
	rows, err := DB.Query(`
		SELECT cl.id, cl.loan_id,
		       COALESCE(cu.full_name, cl.loan_id) AS customer_name,
		       d.dealer_name,
		       cl.action_type, cl.result,
		       COALESCE(cl.notes,''), cl.created_by,
		       TO_CHAR(cl.created_at AT TIME ZONE 'Asia/Bangkok', 'DD/MM/YY HH24:MI') AS ts,
		       l.dpd, l.outstanding_balance
		FROM collection_logs cl
		JOIN loans l       ON l.loan_id     = cl.loan_id
		JOIN customers cu  ON cu.customer_id = l.customer_id
		JOIN dealers d     ON d.dealer_id   = l.dealer_id
		ORDER BY cl.created_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	defer rows.Close()
	var result []ActivityFeedItem
	for rows.Next() {
		var a ActivityFeedItem
		rows.Scan(&a.ID, &a.LoanID, &a.CustomerName, &a.DealerName,
			&a.ActionType, &a.Result, &a.Notes, &a.CreatedBy,
			&a.CreatedAt, &a.DPD, &a.Outstanding)
		result = append(result, a)
	}
	if result == nil {
		result = []ActivityFeedItem{}
	}
	return c.JSON(result)
}

// GET /api/targets
func handleGetTargets(c *fiber.Ctx) error {
	rows, err := DB.Query(`
		SELECT target_key, target_rate,
		       TO_CHAR(updated_at AT TIME ZONE 'Asia/Bangkok', 'DD/MM/YY HH24:MI')
		FROM npl_targets ORDER BY target_key
	`)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	defer rows.Close()
	var result []NplTarget
	for rows.Next() {
		var t NplTarget
		rows.Scan(&t.Key, &t.TargetRate, &t.UpdatedAt)
		result = append(result, t)
	}
	if result == nil {
		result = []NplTarget{}
	}
	return c.JSON(result)
}

// PUT /api/targets/:key
func handleSetTarget(c *fiber.Ctx) error {
	key := c.Params("key")
	var body struct {
		TargetRate float64 `json:"targetRate"`
	}
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid body"})
	}
	if body.TargetRate <= 0 || body.TargetRate > 100 {
		return c.Status(400).JSON(fiber.Map{"error": "targetRate must be between 0.01 and 100"})
	}
	_, err := DB.Exec(`
		INSERT INTO npl_targets(target_key, target_rate, updated_at)
		VALUES($1, $2, NOW())
		ON CONFLICT(target_key) DO UPDATE
		SET target_rate = EXCLUDED.target_rate, updated_at = NOW()
	`, key, body.TargetRate)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	hub.broadcast("refresh")
	return c.JSON(fiber.Map{"ok": true})
}

// GET /api/export/collection
func handleExportCollection(c *fiber.Ctx) error {
	rows, err := DB.Query(`
		SELECT
			ROW_NUMBER() OVER (ORDER BY (CAST(l.dpd AS FLOAT8) * l.outstanding_balance) DESC),
			l.loan_id,
			COALESCE(cu.full_name, l.customer_id),
			d.dealer_name, d.province, d.region,
			l.dpd, l.outstanding_balance, l.principal_amount, l.loan_type,
			ROUND((CAST(l.dpd AS FLOAT8) * l.outstanding_balance / 1000000.0)::numeric, 2),
			CASE
				WHEN l.dpd <= 29  THEN 'MONITOR'
				WHEN l.dpd <= 89  THEN 'CALL'
				WHEN l.dpd <= 120 THEN 'FIELD'
				WHEN l.dpd <= 180 THEN 'LEGAL'
				ELSE 'REPOSSESS'
			END,
			COALESCE(
				(SELECT result FROM collection_logs WHERE loan_id = l.loan_id ORDER BY created_at DESC LIMIT 1), ''
			)
		FROM loans l
		JOIN customers cu ON cu.customer_id = l.customer_id
		JOIN dealers   d  ON d.dealer_id    = l.dealer_id
		WHERE l.status <> 'CLOSED' AND l.dpd > 0
		ORDER BY (CAST(l.dpd AS FLOAT8) * l.outstanding_balance) DESC
		LIMIT 500
	`)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	defer rows.Close()

	c.Set("Content-Type", "text/csv; charset=utf-8")
	c.Set("Content-Disposition", "attachment; filename=collection_priority.csv")

	w := csv.NewWriter(c.Response().BodyWriter())
	w.Write([]string{"ลำดับ", "Loan ID", "ชื่อลูกค้า", "ดีลเลอร์", "จังหวัด", "ภูมิภาค", "DPD", "ยอดค้างชำระ", "เงินต้น", "ประเภท", "Priority Score", "Action", "ผลล่าสุด"})

	for rows.Next() {
		var priority int
		var loanID, name, dealer, province, region, loanType, action, lastResult string
		var dpd int
		var outstanding, principal, score float64
		rows.Scan(&priority, &loanID, &name, &dealer, &province, &region,
			&dpd, &outstanding, &principal, &loanType, &score, &action, &lastResult)
		w.Write([]string{
			fmt.Sprintf("%d", priority), loanID, name, dealer, province, region,
			fmt.Sprintf("%d", dpd),
			fmt.Sprintf("%.2f", outstanding),
			fmt.Sprintf("%.2f", principal),
			loanType,
			fmt.Sprintf("%.2f", score),
			action, lastResult,
		})
	}
	w.Flush()
	return nil
}

// GET /api/export/dealers
func handleExportDealers(c *fiber.Ctx) error {
	rows, err := DB.Query(`
		SELECT
			d.dealer_id, d.dealer_name, d.province, d.region, d.loan_type,
			COALESCE(SUM(l.principal_amount), 0),
			COALESCE(SUM(CASE WHEN l.dpd > 90 THEN l.outstanding_balance ELSE 0 END), 0),
			COUNT(l.loan_id),
			COUNT(CASE WHEN l.dpd > 90 THEN 1 END),
			COALESCE(AVG(CASE WHEN l.dpd > 0 THEN l.dpd END), 0)
		FROM dealers d
		LEFT JOIN loans l ON l.dealer_id = d.dealer_id AND l.status <> 'CLOSED'
		GROUP BY d.dealer_id, d.dealer_name, d.province, d.region, d.loan_type
		ORDER BY SUM(CASE WHEN l.dpd > 90 THEN l.outstanding_balance ELSE 0 END) DESC NULLS LAST
	`)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	defer rows.Close()

	c.Set("Content-Type", "text/csv; charset=utf-8")
	c.Set("Content-Disposition", "attachment; filename=dealer_npl_summary.csv")

	w := csv.NewWriter(c.Response().BodyWriter())
	w.Write([]string{"Dealer ID", "ชื่อดีลเลอร์", "จังหวัด", "ภูมิภาค", "ประเภทสินเชื่อ", "Port รวม", "NPL รวม", "สัญญาทั้งหมด", "สัญญา NPL", "Avg DPD", "NPL Rate %"})

	for rows.Next() {
		var id, name, prov, region, loanType string
		var port, npl, avgDPD float64
		var loans, nplLoans int
		rows.Scan(&id, &name, &prov, &region, &loanType, &port, &npl, &loans, &nplLoans, &avgDPD)
		nplRate := 0.0
		if port > 0 {
			nplRate = math.Round(npl/port*10000) / 100
		}
		w.Write([]string{
			id, name, prov, region, loanType,
			fmt.Sprintf("%.2f", port),
			fmt.Sprintf("%.2f", npl),
			fmt.Sprintf("%d", loans),
			fmt.Sprintf("%d", nplLoans),
			fmt.Sprintf("%.1f", avgDPD),
			fmt.Sprintf("%.2f", nplRate),
		})
	}
	w.Flush()
	return nil
}
