package service

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ramisoul84/kfc-order/pkg/metrics"
)

var (
	ordersCreated = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "orders_created_total",
		Help: "Orders created, by channel (kiosk, web, pos).",
	}, []string{"channel"})

	statusChanges = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "order_status_changes_total",
		Help: "Order status transitions, by new status.",
	}, []string{"status"})
)

func init() {
	metrics.Registry.MustRegister(ordersCreated, statusChanges)
}
