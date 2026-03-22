package main

import (
	"database/sql"
	"log"
)

// ── Schema Migration ──────────────────────────────────────────

func migrate(db *sql.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS dealers (
		dealer_id   TEXT PRIMARY KEY,
		dealer_name TEXT NOT NULL,
		province    TEXT NOT NULL,
		region      TEXT NOT NULL,
		latitude    DOUBLE PRECISION NOT NULL,
		longitude   DOUBLE PRECISION NOT NULL,
		loan_type   TEXT NOT NULL DEFAULT 'เช่าซื้อ',
		created_at  TIMESTAMP DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS customers (
		customer_id TEXT PRIMARY KEY,
		full_name   TEXT NOT NULL,
		id_card     TEXT,
		phone       TEXT,
		province    TEXT,
		created_at  TIMESTAMP DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS loans (
		loan_id            TEXT PRIMARY KEY,
		dealer_id          TEXT REFERENCES dealers(dealer_id),
		customer_id        TEXT REFERENCES customers(customer_id),
		principal_amount   NUMERIC(15,2) NOT NULL,
		outstanding_balance NUMERIC(15,2) NOT NULL,
		loan_type          TEXT NOT NULL,
		status             TEXT NOT NULL DEFAULT 'ACTIVE',  -- ACTIVE / CLOSED / PENDING
		dpd                INT  NOT NULL DEFAULT 0,         -- Days Past Due
		contract_date      DATE,
		last_payment_date  DATE,
		created_at         TIMESTAMP DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS npl_monthly_snapshot (
		snapshot_id    SERIAL PRIMARY KEY,
		dealer_id      TEXT REFERENCES dealers(dealer_id),
		month_date     DATE NOT NULL,
		port_snapshot  NUMERIC(15,2) NOT NULL,
		npl_snapshot   NUMERIC(15,2) NOT NULL,
		created_at     TIMESTAMP DEFAULT NOW(),
		UNIQUE(dealer_id, month_date)
	);

	CREATE INDEX IF NOT EXISTS idx_loans_dealer   ON loans(dealer_id);
	CREATE INDEX IF NOT EXISTS idx_loans_customer ON loans(customer_id);
	CREATE INDEX IF NOT EXISTS idx_loans_dpd      ON loans(dpd);
	CREATE INDEX IF NOT EXISTS idx_loans_status   ON loans(status);

	CREATE TABLE IF NOT EXISTS collection_logs (
	    id           SERIAL PRIMARY KEY,
	    loan_id      TEXT REFERENCES loans(loan_id),
	    action_type  TEXT NOT NULL,   -- CALL, FIELD, LEGAL, SMS, OTHER
	    result       TEXT NOT NULL,   -- PROMISE_TO_PAY, NO_ANSWER, REFUSED, PAID, UNREACHABLE
	    promise_date DATE,
	    promise_amt  NUMERIC(15,2),
	    notes        TEXT,
	    created_by   TEXT NOT NULL DEFAULT 'ทีมงาน',
	    created_at   TIMESTAMP DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_logs_loan ON collection_logs(loan_id);
	CREATE INDEX IF NOT EXISTS idx_logs_created ON collection_logs(created_at DESC);

	CREATE TABLE IF NOT EXISTS npl_targets (
	    target_key  TEXT PRIMARY KEY,
	    target_rate NUMERIC(5,2) NOT NULL DEFAULT 5.00,
	    updated_at  TIMESTAMP DEFAULT NOW()
	);
	`
	_, err := db.Exec(schema)
	if err != nil {
		return err
	}
	log.Println("✅ Schema migrated")
	return nil
}

// ── Seed Mock Data ─────────────────────────────────────────────

func seed(db *sql.DB) error {
	// ตรวจว่ามีข้อมูลแล้วหรือยัง
	var cnt int
	db.QueryRow("SELECT COUNT(*) FROM dealers").Scan(&cnt)
	if cnt > 0 {
		log.Printf("⏭  Seed skipped (dealers: %d)", cnt)
		return nil
	}

	log.Println("🌱 Seeding mock data...")

	// ── Dealers ───────────────────────────────────────────────
	dealers := []struct {
		id, name, province, region, loanType string
		lat, lng                             float64
	}{
		{"D001","ออโต้สยาม มอเตอร์",     "กรุงเทพฯ",       "กลาง",  "เช่าซื้อ", 13.7563,100.5018},
		{"D002","ธนชัย ยานยนต์",          "กรุงเทพฯ",       "กลาง",  "Personal", 13.8021,100.5514},
		{"D003","เจริญทรัพย์ ออโต้",      "กรุงเทพฯ",       "กลาง",  "เช่าซื้อ", 13.7234,100.6102},
		{"D004","สุขสวัสดิ์ ไฟแนนซ์",    "สมุทรปราการ",    "กลาง",  "Personal", 13.5990,100.6101},
		{"D005","นนทบุรี ออโต้เฮาส์",    "นนทบุรี",        "กลาง",  "Nano",     13.8621,100.5010},
		{"D006","ปทุม ยนตการ",            "ปทุมธานี",       "กลาง",  "เช่าซื้อ", 14.0208,100.5255},
		{"D007","รุ่งเรือง มอเตอร์",      "อยุธยา",         "กลาง",  "Personal", 14.3532,100.5601},
		{"D008","ชลบุรี สปีด ออโต้",     "ชลบุรี",         "ตะวันออก","เช่าซื้อ",13.3611,100.9847},
		{"D009","พัทยา พรีเมียม คาร์",   "ชลบุรี",         "ตะวันออก","Personal",12.9280,100.8778},
		{"D010","ระยอง ยนตการ",           "ระยอง",          "ตะวันออก","Nano",    12.6814,101.2816},
		{"D011","อีสเทิร์น ออโต้ลีส",    "ระยอง",          "ตะวันออก","เช่าซื้อ",12.7200,101.1500},
		{"D012","ขอนแก่น ยนตการ",         "ขอนแก่น",        "อีสาน", "เช่าซื้อ", 16.4419,102.8360},
		{"D013","สหมิตร มอเตอร์",         "ขอนแก่น",        "อีสาน", "Personal", 16.5000,102.7500},
		{"D014","อุดร ออโต้ เซ็นเตอร์",  "อุดรธานี",       "อีสาน", "เช่าซื้อ", 17.4139,102.7872},
		{"D015","โคราช เฟิร์สคาร์",      "นครราชสีมา",     "อีสาน", "เช่าซื้อ", 14.9799,102.0978},
		{"D016","โคราช ออโต้ ลีส",       "นครราชสีมา",     "อีสาน", "Personal", 14.9300,102.1400},
		{"D017","อุบล ยนตการ",            "อุบลราชธานี",    "อีสาน", "Nano",     15.2287,104.8593},
		{"D018","ร้อยเอ็ด ออโต้",         "ร้อยเอ็ด",       "อีสาน", "เช่าซื้อ", 16.0538,103.6520},
		{"D019","สกล ยนตรกิจ",            "สกลนคร",         "อีสาน", "Personal", 17.1664,104.1486},
		{"D020","บุรีรัมย์ มอเตอร์",     "บุรีรัมย์",      "อีสาน", "เช่าซื้อ", 14.9930,103.1029},
		{"D021","ศรีสะเกษ ออโต้",         "ศรีสะเกษ",       "อีสาน", "Nano",     15.1186,104.3220},
		{"D022","เชียงใหม่ ออโต้ วิลล์", "เชียงใหม่",      "เหนือ", "เช่าซื้อ", 18.7883,98.9853},
		{"D023","ล้านนา มอเตอร์",         "เชียงใหม่",      "เหนือ", "Personal", 18.8000,99.0200},
		{"D024","เชียงราย ยนตการ",        "เชียงราย",       "เหนือ", "เช่าซื้อ", 19.9105,99.8406},
		{"D025","ลำปาง ออโต้เฮาส์",      "ลำปาง",          "เหนือ", "Nano",     18.2888,99.4928},
		{"D026","พิษณุโลก มอเตอร์",      "พิษณุโลก",       "เหนือ", "เช่าซื้อ", 16.8211,100.2659},
		{"D027","สุโขทัย ยนตการ",         "สุโขทัย",        "เหนือ", "Personal", 17.0069,99.8265},
		{"D028","นครสวรรค์ ออโต้",        "นครสวรรค์",      "กลาง",  "เช่าซื้อ", 15.7030,100.1372},
		{"D029","กาญจน์ มอเตอร์",         "กาญจนบุรี",      "ตะวันตก","Nano",    14.0023,99.5328},
		{"D030","สุพรรณ ยนตการ",           "สุพรรณบุรี",     "กลาง",  "เช่าซื้อ", 14.4735,100.1182},
		{"D031","สุราษฎร์ ออโต้",          "สุราษฎร์ธานี",  "ใต้",   "เช่าซื้อ", 9.1382,99.3214},
		{"D032","ภูเก็ต พรีเมียม ออโต้",  "ภูเก็ต",         "ใต้",   "Personal", 7.8804,98.3923},
		{"D033","หาดใหญ่ ยนตการ",          "สงขลา",          "ใต้",   "เช่าซื้อ", 7.0086,100.4747},
		{"D034","สงขลา มอเตอร์",           "สงขลา",          "ใต้",   "Nano",     7.1897,100.5952},
		{"D035","นครศรี ออโต้ ลีส",       "นครศรีธรรมราช", "ใต้",   "เช่าซื้อ", 8.4304,99.9631},
		{"D036","ชุมพร ยนตรกิจ",           "ชุมพร",          "ใต้",   "Personal", 10.4930,99.1800},
	}

	for _, d := range dealers {
		_, err := db.Exec(`
			INSERT INTO dealers(dealer_id,dealer_name,province,region,latitude,longitude,loan_type)
			VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`,
			d.id, d.name, d.province, d.region, d.lat, d.lng, d.loanType)
		if err != nil {
			return err
		}
	}

	// ── Customers ─────────────────────────────────────────────
	customers := []struct{ id, name, province string }{
		{"C001","นายสมชาย วงศ์เจริญ",   "นครราชสีมา"},
		{"C002","นายอนันต์ ทองดี",       "กรุงเทพฯ"},
		{"C003","นางสาวพิมพ์ใจ รักไทย", "สงขลา"},
		{"C004","นายธนวัฒน์ สุขศรี",    "กรุงเทพฯ"},
		{"C005","นายวิเชียร แก้วมณี",    "ขอนแก่น"},
		{"C006","นางสมหมาย ชัยมงคล",    "ชลบุรี"},
		{"C007","นายสุรชัย เพ็ชรดี",    "นครราชสีมา"},
		{"C008","นางรัตนา สิทธิพร",     "พิษณุโลก"},
		{"C009","นายประเสริฐ ดีมาก",    "กรุงเทพฯ"},
		{"C010","นางสาวมาลี สุวรรณ",    "เชียงใหม่"},
		{"C011","นายบุญมี หาญกล้า",     "อุดรธานี"},
		{"C012","นางจิราภรณ์ ใจดี",     "สุราษฎร์ธานี"},
		{"C013","นายนิรันดร์ รอดภัย",   "ขอนแก่น"},
		{"C014","นายชาญชัย มั่นคง",     "กรุงเทพฯ"},
		{"C015","นางสาวสุภาพร เที่ยงธรรม","ภูเก็ต"},
		{"C016","นายวีระ ศรีสมบูรณ์",   "นครราชสีมา"},
		{"C017","นายปรีชา แสงทอง",      "สมุทรปราการ"},
		{"C018","นางอัมพร คงมั่น",       "ชลบุรี"},
		{"C019","นายกิตติพงษ์ พรมดี",  "อุบลราชธานี"},
		{"C020","นางสาวนิภา ดาวเด่น",   "สงขลา"},
	}
	for _, cu := range customers {
		_, err := db.Exec(`
			INSERT INTO customers(customer_id,full_name,province)
			VALUES($1,$2,$3) ON CONFLICT DO NOTHING`,
			cu.id, cu.name, cu.province)
		if err != nil {
			return err
		}
	}

	// ── Loans ─────────────────────────────────────────────────
	// แต่ละ dealer มีหลาย loan, DPD > 90 = NPL
	loans := []struct {
		id, dealerID, custID, loanType, status string
		principal, outstanding                 float64
		dpd                                    int
	}{
		// High NPL dealers
		{"L001","D015","C001","เช่าซื้อ","ACTIVE", 3200000,2850000,180},
		{"L002","D001","C002","เช่าซื้อ","ACTIVE", 2400000,2100000,150},
		{"L003","D033","C003","Personal", "ACTIVE", 2100000,1950000,120},
		{"L004","D003","C004","เช่าซื้อ","ACTIVE", 2000000,1800000,95},
		{"L005","D012","C005","เช่าซื้อ","ACTIVE", 1900000,1650000,88},
		{"L006","D009","C006","Personal", "ACTIVE", 1800000,1500000,75},
		{"L007","D016","C007","เช่าซื้อ","ACTIVE", 1600000,1200000,68},
		{"L008","D026","C008","เช่าซื้อ","ACTIVE", 1400000,980000, 62},
		// Normal loans (DPD=0)
		{"L009","D001","C009","เช่าซื้อ","ACTIVE", 1500000,1450000,0},
		{"L010","D022","C010","เช่าซื้อ","ACTIVE", 1200000,1100000,0},
		{"L011","D014","C011","เช่าซื้อ","ACTIVE", 1800000,1750000,0},
		{"L012","D031","C012","เช่าซื้อ","ACTIVE", 1100000,1050000,0},
		{"L013","D012","C013","Personal", "ACTIVE", 900000, 870000, 30},
		{"L014","D003","C014","เช่าซื้อ","ACTIVE", 2200000,2150000,0},
		{"L015","D032","C015","Personal", "ACTIVE", 1600000,1550000,0},
		{"L016","D015","C016","เช่าซื้อ","ACTIVE", 2500000,2400000,45},
		{"L017","D004","C017","Personal", "ACTIVE", 1800000,1700000,0},
		{"L018","D008","C018","เช่าซื้อ","ACTIVE", 2800000,2750000,0},
		{"L019","D017","C019","Nano",     "ACTIVE", 500000, 480000, 0},
		{"L020","D034","C020","Nano",     "ACTIVE", 600000, 580000, 110},
		// Larger portfolio loans
		{"L021","D001","C001","เช่าซื้อ","ACTIVE", 1800000,600000, 0},
		{"L022","D003","C002","เช่าซื้อ","ACTIVE", 2100000,1900000,0},
		{"L023","D008","C003","เช่าซื้อ","ACTIVE", 3100000,3000000,0},
		{"L024","D012","C004","เช่าซื้อ","ACTIVE", 2600000,2500000,0},
		{"L025","D022","C005","เช่าซื้อ","ACTIVE", 2200000,2100000,0},
		{"L026","D033","C006","เช่าซื้อ","ACTIVE", 1900000,1850000,0},
		{"L027","D015","C007","เช่าซื้อ","ACTIVE", 3800000,3700000,0},
		{"L028","D032","C008","Personal", "ACTIVE", 2400000,2350000,0},
		{"L029","D009","C009","Personal", "ACTIVE", 1600000,800000, 0},
		{"L030","D016","C010","Personal", "ACTIVE", 1400000,1380000,0},
		// Closed loans
		{"L031","D001","C011","เช่าซื้อ","CLOSED",1200000,0,       0},
		{"L032","D005","C012","Nano",     "CLOSED",400000, 0,       0},
		{"L033","D006","C013","เช่าซื้อ","CLOSED",800000, 0,       0},
	}
	for _, l := range loans {
		_, err := db.Exec(`
			INSERT INTO loans(loan_id,dealer_id,customer_id,principal_amount,
				outstanding_balance,loan_type,status,dpd,contract_date)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,NOW()-INTERVAL '12 months') ON CONFLICT DO NOTHING`,
			l.id, l.dealerID, l.custID, l.principal, l.outstanding,
			l.loanType, l.status, l.dpd)
		if err != nil {
			return err
		}
	}

	// ── Monthly NPL Snapshot (12 months) ────────────────────
	snapshotSQL := `
	INSERT INTO npl_monthly_snapshot(dealer_id, month_date, port_snapshot, npl_snapshot)
	SELECT
		dealer_id,
		generate_series(
			DATE_TRUNC('month', NOW()) - INTERVAL '11 months',
			DATE_TRUNC('month', NOW()),
			INTERVAL '1 month'
		)::date AS month_date,
		SUM(principal_amount) * (0.85 + RANDOM() * 0.3)           AS port_snapshot,
		SUM(CASE WHEN dpd > 90 THEN outstanding_balance ELSE 0 END)
			* (0.5 + RANDOM() * 1.0)                              AS npl_snapshot
	FROM loans
	WHERE status <> 'CLOSED'
	GROUP BY dealer_id
	ON CONFLICT DO NOTHING
	`
	if _, err := db.Exec(snapshotSQL); err != nil {
		log.Printf("snapshot seed warning: %v", err)
	}

	log.Printf("✅ Seeded: %d dealers, %d customers, %d loans",
		len(dealers), len(customers), len(loans))

	// ── Default NPL Targets ────────────────────────────────────
	if _, errT := db.Exec(`
		INSERT INTO npl_targets(target_key, target_rate) VALUES
		    ('overall', 5.00), ('กลาง', 6.00), ('เหนือ', 5.00),
		    ('ใต้', 5.00), ('ตะวันออก', 5.00), ('ตะวันออกเฉียงเหนือ', 5.00), ('ตะวันตก', 5.00)
		ON CONFLICT(target_key) DO NOTHING
	`); errT != nil {
		log.Printf("npl_targets seed warning: %v", errT)
	}

	return nil
}
