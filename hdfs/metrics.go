package hdfs

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
)

// 创建监控项,并且用标签的形式区分
//作业要求：

//基本性能测试：
//元数据面单个请求的端到端延时
//元数据节点的各个请求的最高负载（QPS，每秒请求数，服务器在每秒内所能处理多少请求）

//基本指标监控：
//各接口请求总量、请求成功率、网络流量、存储数据分布（存储在哪些节点上）、
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

