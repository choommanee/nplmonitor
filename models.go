package main

// HealthResponse — GET /api/health
type HealthResponse struct {
	Status  string `json:"status"`
	Version string `json:"version"`
}

// SummaryResponse — GET /api/summary
type SummaryResponse struct {
	TotalPort    float64 `json:"totalPort"`
	TotalNPL     float64 `json:"totalNpl"`
	NPLRate      float64 `json:"nplRate"`
	HighRisk     int     `json:"highRisk"`
	MidRisk      int     `json:"midRisk"`
	LowRisk      int     `json:"lowRisk"`
	TotalDealers int     `json:"totalDealers"`
	TotalLoans   int     `json:"totalLoans"`
	// Pre-NPL Early Warning buckets
	Dpd30    int     `json:"dpd30"`    // count DPD 30–59
	Dpd30Amt float64 `json:"dpd30Amt"` // outstanding at risk
	Dpd60    int     `json:"dpd60"`    // count DPD 60–89
	Dpd60Amt float64 `json:"dpd60Amt"` // outstanding at risk
}

// CollectionItem — GET /api/collection-priority
type CollectionItem struct {
	Priority      int     `json:"priority"`
	LoanID        string  `json:"loanId"`
	CustomerName  string  `json:"customerName"`
	DealerName    string  `json:"dealerName"`
	Province      string  `json:"province"`
	Region        string  `json:"region"`
	DPD           int     `json:"dpd"`
	Outstanding   float64 `json:"outstanding"`
	Principal     float64 `json:"principal"`
	LoanType      string  `json:"loanType"`
	PriorityScore float64 `json:"priorityScore"`
	Action        string  `json:"action"` // MONITOR/CALL/FIELD/LEGAL/REPOSSESS
}

// DealerRow — GET /api/dealers
type DealerRow struct {
	ID        string  `json:"id"`
	Name      string  `json:"name"`
	Province  string  `json:"province"`
	Region    string  `json:"region"`
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	Port      float64 `json:"port"`
	NplAmt    float64 `json:"nplAmt"`
	NplRate   float64 `json:"nplRate"`
	Loans     int     `json:"loans"`
	Type      string  `json:"loanType"`
	RiskLevel string  `json:"riskLevel"`
}

// DealerDetail — GET /api/dealers/:id
type DealerDetail struct {
	ID        string  `json:"id"`
	Name      string  `json:"name"`
	Province  string  `json:"province"`
	Region    string  `json:"region"`
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	Type      string  `json:"loanType"`
	Port      float64 `json:"port"`
	NplAmt    float64 `json:"nplAmt"`
	NplRate   float64 `json:"nplRate"`
	Loans     int     `json:"loans"`
	Loans90   int     `json:"loans90"`
	AvgDPD    float64 `json:"avgDpd"`
	AvgLoan   float64 `json:"avgLoan"`
	Pending   int     `json:"pending"`
	RiskLevel string  `json:"riskLevel"`
}

// RegionRow — GET /api/regions
type RegionRow struct {
	Region  string  `json:"region"`
	Port    float64 `json:"port"`
	NplAmt  float64 `json:"nplAmt"`
	NplRate float64 `json:"nplRate"`
	Dealers int     `json:"dealers"`
}

// ProvinceRow — GET /api/provinces
type ProvinceRow struct {
	Province string  `json:"province"`
	Region   string  `json:"region"`
	Port     float64 `json:"port"`
	NplAmt   float64 `json:"nplAmt"`
	NplRate  float64 `json:"nplRate"`
	Dealers  int     `json:"dealers"`
}

// TopNPLRow — GET /api/top-npls
type TopNPLRow struct {
	Rank       int     `json:"rank"`
	CustomerID string  `json:"customerId"`
	Name       string  `json:"name"`
	DealerName string  `json:"dealerName"`
	Province   string  `json:"province"`
	Region     string  `json:"region"`
	NplAmt     float64 `json:"nplAmt"`
	Port       float64 `json:"port"`
	DPD        int     `json:"dpd"`
	Loans      int     `json:"loans"`
	LoanType   string  `json:"loanType"`
	NplRate    float64 `json:"nplRate"`
}

// TrendRow — GET /api/trend
type TrendRow struct {
	Month   string  `json:"month"`
	Port    float64 `json:"port"`
	NplAmt  float64 `json:"nplAmt"`
	NplRate float64 `json:"nplRate"`
}

// LoanRow — GET /api/loans
type LoanRow struct {
	ID           string  `json:"id"`
	DealerID     string  `json:"dealerId"`
	CustomerID   string  `json:"customerId"`
	CustomerName string  `json:"customerName"`
	Principal    float64 `json:"principal"`
	Outstanding  float64 `json:"outstanding"`
	LoanType     string  `json:"loanType"`
	Status       string  `json:"status"`
	DPD          int     `json:"dpd"`
	ContractDate string  `json:"contractDate"`
}
