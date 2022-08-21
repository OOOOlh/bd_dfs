package hdfs

import (
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

// 创建监控项,并且用标签的形式区分
var (
	GaugeVecApiDuration = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: "apiDuration",
		Help: "api耗时单位ms",
	}, []string{"method"})
	GaugeVecApiMethod = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "apiCount",
		Help: "各种网络请求次数",
	}, []string{"method"})
	GaugeVecApiError = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "apiErrorCount",
		Help: "请求api错误的次数type: api/ws",
	}, []string{"type"})
)

// 初始化Prometheus模型
func init() {
	prometheus.MustRegister(GaugeVecApiDuration, GaugeVecApiMethod, GaugeVecApiError)
}

func MwPrometheusHttp(c *gin.Context) {
	start := time.Now().UnixNano()
	method := c.Request.Method
	GaugeVecApiMethod.WithLabelValues(method).Inc()

	c.Next()
	// after request
	end := time.Now().UnixNano()
	d := end - start
	GaugeVecApiDuration.WithLabelValues(method).Observe(float64(d))
}

