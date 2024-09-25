package config

import (
	"fmt"
	"strings"
	"time"
)

// Config holds all configuration for the application.
type Config struct {
	App    AppConfig
	HTTP   HTTPConfig
	GRPC   GRPCConfig
	Logger LoggerConfig
	DB     DatabaseConfig
	Kafka  KafkaConfig
}

// AppConfig holds application-level configuration.
type AppConfig struct {
	Name        string
	Version     string
	Environment string
	// Timezone decides where a "business day" starts, which is when
	// order numbers reset to 1.
	Timezone string
}

// HTTPConfig holds the health/metrics HTTP listener.
type HTTPConfig struct {
	Port string
}

// GRPCConfig holds gRPC server configuration.
type GRPCConfig struct {
	Port            string
	ShutdownTimeout time.Duration
}

// LoggerConfig holds logger settings.
type LoggerConfig struct {
	Level    string
	Format   string
	Output   string
	FilePath string
	Service  string
}

// DatabaseConfig holds PostgreSQL connection settings.
type DatabaseConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string
	SSLMode  string
	// AllowInsecure permits DB_SSL_MODE=disable in production. Only for a
	// database on the same private network (e.g. the compose stack).
	AllowInsecure   bool
	MaxConns        int
	MinConns        int
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
	ConnectTimeout  time.Duration
	AutoMigrate     bool
}

// KafkaConfig holds Kafka settings. Empty Brokers disables publishing.
type KafkaConfig struct {
	Brokers     []string
	OrdersTopic string
}

// Load reads configuration from environment variables and validates it.
func Load() (*Config, error) {
	env := getEnv("APP_ENV", "development")
	loadDotenv(env)

	cfg := &Config{
		App: AppConfig{
			Name:        getEnv("APP_NAME", "kfc-order"),
			Version:     getEnv("APP_VERSION", "1.0.0"),
			Environment: env,
			Timezone:    getEnv("APP_TIMEZONE", "Europe/Moscow"),
		},
		HTTP: HTTPConfig{
			Port: getEnv("HTTP_PORT", "8003"),
		},
		GRPC: GRPCConfig{
			Port:            getEnv("GRPC_PORT", "9103"),
			ShutdownTimeout: getEnvDuration("GRPC_SHUTDOWN_TIMEOUT", 10*time.Second),
		},
		Logger: LoggerConfig{
			Level:    getEnv("LOG_LEVEL", defaultLogLevel(env)),
			Format:   getEnv("LOG_FORMAT", defaultLogFormat(env)),
			Output:   getEnv("LOG_OUTPUT", "stdout"),
			FilePath: getEnv("LOG_FILE_PATH", "logs/app.log"),
			Service:  getEnv("LOG_SERVICE", "kfc-order"),
		},
		DB: DatabaseConfig{
			Host:            getEnv("DB_HOST", "localhost"),
			Port:            getEnv("DB_PORT", "5432"),
			User:            getEnv("DB_USER", "postgres"),
			Password:        getEnv("DB_PASSWORD", "postgres"),
			Name:            getEnv("DB_NAME", "kfc_orders"),
			SSLMode:         getEnv("DB_SSL_MODE", "disable"),
			AllowInsecure:   getEnvBool("DB_ALLOW_INSECURE", false),
			MaxConns:        getEnvInt("DB_MAX_CONNS", 20),
			MinConns:        getEnvInt("DB_MIN_CONNS", 5),
			MaxConnLifetime: getEnvDuration("DB_MAX_CONN_LIFETIME", time.Hour),
			MaxConnIdleTime: getEnvDuration("DB_MAX_CONN_IDLE_TIME", 30*time.Minute),
			ConnectTimeout:  getEnvDuration("DB_CONNECT_TIMEOUT", 10*time.Second),
			AutoMigrate:     getEnvBool("DB_AUTO_MIGRATE", true),
		},
		Kafka: KafkaConfig{
			Brokers:     getEnvSlice("KAFKA_BROKERS", []string{}),
			OrdersTopic: getEnv("KAFKA_ORDERS_TOPIC", "orders.events"),
		},
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}
	return cfg, nil
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	var errs []string

	switch c.App.Environment {
	case "development", "staging", "production":
	default:
		errs = append(errs, fmt.Sprintf("invalid APP_ENV: %q", c.App.Environment))
	}
	if _, err := time.LoadLocation(c.App.Timezone); err != nil {
		errs = append(errs, fmt.Sprintf("invalid APP_TIMEZONE: %q", c.App.Timezone))
	}
	if c.DB.MaxConns < c.DB.MinConns {
		errs = append(errs, "DB_MAX_CONNS must be greater than or equal to DB_MIN_CONNS")
	}
	if c.IsProduction() && c.DB.SSLMode == "disable" && !c.DB.AllowInsecure {
		errs = append(errs, "DB_SSL_MODE should not be 'disable' in production (or set DB_ALLOW_INSECURE=true for a database on a private network)")
	}

	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

// IsProduction returns true if environment is production.
func (c *Config) IsProduction() bool {
	return c.App.Environment == "production"
}

// Location returns the business timezone. Validate guarantees it loads.
func (c *Config) Location() *time.Location {
	loc, _ := time.LoadLocation(c.App.Timezone)
	return loc
}

// DSN returns the PostgreSQL connection string.
func (c *Config) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s connect_timeout=%d",
		c.DB.Host, c.DB.Port, c.DB.User, c.DB.Password,
		c.DB.Name, c.DB.SSLMode, int(c.DB.ConnectTimeout.Seconds()),
	)
}

func defaultLogLevel(env string) string {
	if env == "production" {
		return "info"
	}
	return "debug"
}

func defaultLogFormat(env string) string {
	if env == "production" {
		return "json"
	}
	return "text"
}
