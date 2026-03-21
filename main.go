package main

import (
	"database/sql"
	"log"
	"os"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

var DB *sql.DB

func main() {
	// โหลด .env (สำหรับ local dev)
	_ = godotenv.Load()

	// เชื่อมต่อ PostgreSQL
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	var err error
	DB, err = sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("cannot open db: %v", err)
	}
	if err = DB.Ping(); err != nil {
		log.Fatalf("cannot connect db: %v", err)
	}
	log.Println("✅ Database connected")

	// สร้าง table + seed mock data อัตโนมัติ
	if err = migrate(DB); err != nil {
		log.Fatalf("migrate failed: %v", err)
	}
	if err = seed(DB); err != nil {
		log.Fatalf("seed failed: %v", err)
	}

	// Fiber app
	app := fiber.New(fiber.Config{AppName: "SGF+ NPL API v1.0"})

	// Middleware
	app.Use(logger.New())
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowHeaders: "Origin, Content-Type, Accept",
	}))

	// Routes
	api := app.Group("/api")
	api.Get("/health",       handleHealth)
	api.Get("/summary",      handleSummary)
	api.Get("/dealers",      handleDealers)
	api.Get("/dealers/:id",  handleDealerDetail)
	api.Get("/regions",      handleRegions)
	api.Get("/provinces",    handleProvinces)
	api.Get("/top-npls",     handleTopNPLs)
	api.Get("/trend",        handleTrend)
	api.Get("/events",               handleSSE)
	api.Get("/loans",                handleGetLoans)
	api.Put("/loans/:id",            handleUpdateLoan)
	api.Post("/loans",               handleCreateLoan)
	api.Get("/collection-priority",  handleCollectionPriority)

	// Serve frontend (index.html) from current directory
	app.Static("/", ".", fiber.Static{Index: "index.html"})

	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	log.Printf("🚀 Server running on :%s", port)
	log.Fatal(app.Listen(":" + port))
}
