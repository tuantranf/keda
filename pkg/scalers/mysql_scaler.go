package scalers

import (
	"context"
	"fmt"
	"strconv"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	v2beta1 "k8s.io/api/autoscaling/v2beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/external_metrics"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	recordCountMetricName    = "MysqlRecordCount"
	defaultMysqlHost         = "dbserver.default.svc.cluster.local"
	defaultMysqlPort         = "3306"
	defaultMysqlUser         = "root"
	defaultMysqlPassword     = ""
)

type mysqlScaler struct {
	metadata *mysqlMetadata
}

type mysqlMetadata struct {

	targetRecordCount int
	host          	  string
	port 			  string
	user          	  string
	password          string
	database          string
	query	          string
}

var mysqlLog = logf.Log.WithName("mysql_scaler")

// NewMysqlScaler creates a new mysqlScaler
func NewMysqlScaler(resolvedEnv, metadata, authParams map[string]string) (Scaler, error) {
	meta, err := parseMysqlMetadata(metadata, resolvedEnv, authParams)
	if err != nil {
		return nil, fmt.Errorf("error parsing mysql metadata: %s", err)
	}

	return &mysqlScaler{
		metadata: meta,
	}, nil
}

func parseMysqlMetadata(metadata, resolvedEnv, authParams map[string]string) (*mysqlMetadata, error) {
	meta := mysqlMetadata{}

	if val, ok := metadata["host"]; ok && val != "" {
		meta.port = val
	} else {
		meta.port = defaultMysqlHost
	}

	if val, ok := metadata["port"]; ok && val != "" {
		meta.port = val
	} else {
		meta.port =  defaultMysqlPort
	}

	if val, ok := metadata["user"]; ok && val != "" {
		meta.user = val
	} else {
		meta.user =  defaultMysqlUser
	}

	meta.password = defaultMysqlPassword
	if val, ok := authParams["password"]; ok {
		meta.password = val
	} else if val, ok := metadata["password"]; ok && val != "" {
		meta.password = val
	} else {
		meta.password = defaultMysqlPassword
	}

	if database, ok := metadata["database"]; ok {
		meta.database = database
	} else {
		return nil, fmt.Errorf("no database given")
	}

	if query, ok := metadata["query"]; ok {
		meta.query = query
	} else {
		return nil, fmt.Errorf("no query given")
	}

	if val, ok := metadata["count"]; ok {
		count, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("Count parsing error %s", err.Error())
		}
		meta.targetRecordCount = count
	} else {
		return nil, fmt.Errorf("no count given")
	}

	return &meta, nil
}

// IsActive checks if there is any count of query
func (s *mysqlScaler) IsActive(ctx context.Context) (bool, error) {
	length, err := getRecordCount(
		ctx, s.metadata.host, s.metadata.port, s.metadata.user, s.metadata.password, s.metadata.database, s.metadata.query)

	if err != nil {
		mysqlLog.Error(err, "error")
		return false, err
	}

	return length > 0, nil
}

func (s *mysqlScaler) Close() error {
	return nil
}

// GetMetricSpecForScaling returns the metric spec for the HPA
func (s *mysqlScaler) GetMetricSpecForScaling() []v2beta1.MetricSpec {
	targetRecordCountQty := resource.NewQuantity(int64(s.metadata.targetRecordCount), resource.DecimalSI)
	externalMetric := &v2beta1.ExternalMetricSource{MetricName: recordCountMetricName, TargetAverageValue: targetRecordCountQty}
	metricSpec := v2beta1.MetricSpec{External: externalMetric, Type: externalMetricType}
	return []v2beta1.MetricSpec{metricSpec}
}

// GetMetrics connects to Mysql and get count of query
func (s *mysqlScaler) GetMetrics(ctx context.Context, metricName string, metricSelector labels.Selector) ([]external_metrics.ExternalMetricValue, error) {
	count, err := getRecordCount(ctx, s.metadata.host, s.metadata.port, s.metadata.user, s.metadata.password, s.metadata.database, s.metadata.query)

	if err != nil {
		mysqlLog.Error(err, "error getting record count")
		return []external_metrics.ExternalMetricValue{}, err
	}

	metric := external_metrics.ExternalMetricValue{
		MetricName: metricName,
		Value:      *resource.NewQuantity(count, resource.DecimalSI),
		Timestamp:  metav1.Now(),
	}

	return append([]external_metrics.ExternalMetricValue{}, metric), nil
}

func getRecordCount(ctx context.Context, host string, port string, user string, password string, database string, query string) (int64, error) {
	sqlUrl := fmt.Sprintf("%s:%s@%s:%s/%s?parseTime=true", user, password, host, port, database)
	db, err := sql.Open("mysql", sqlUrl)
	if err != nil {
		mysqlLog.Error(err, "error")
		return -1, err
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		mysqlLog.Error(err, "error")
		return -1, err
	}

	rows, err := db.Query(query)
	defer rows.Close()

	if err != nil {
		mysqlLog.Error(err, "error")
		return -1, err
	}

	var count int64
	for rows.Next() {
		if err := rows.Scan(&count); err != nil {
			mysqlLog.Error(err, "error")
			return -1, err
		}
		mysqlLog.Info("Total rows found = %d\n", count)
	}
	if err := rows.Err(); err != nil {
		mysqlLog.Error(err, "error")
		return -1, err
	}

	return count, nil
}