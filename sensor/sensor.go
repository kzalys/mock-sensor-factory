package sensor

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/kzalys/sensor-control-service/consts"
	"github.com/kzalys/sensor-control-service/types"
	"log"
	"math/rand"
	"net/http"
	"time"
)

type Config struct {
	SensorGroup   string              `json:"sensorGroup"`
	SensorAddress string              `json:"sensorAddress"`
	PushInterval  int64               `json:"pushInterval"`
	Port          int                 `json:"port"`
	Measurements  map[string][]string `json:"measurements"`
}

type MockSensor struct {
	influxWriteApi api.WriteAPI
	server         *http.Server
	config         Config
	stop           chan bool
}

func NewMockSensor(influxWriteApi api.WriteAPI, config Config) *MockSensor {
	gServer := gin.Default()
	router := gServer.Group("/")

	hServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", config.Port),
		Handler: gServer,
	}

	if config.SensorAddress == "" {
		config.SensorAddress = fmt.Sprintf("127.0.0.1:%d", config.Port)
	}

	sensor := &MockSensor{
		influxWriteApi: influxWriteApi,
		config:         config,
		server:         hServer,
	}

	router.GET("status", sensor.handleGetStatus)
	router.PATCH("status", sensor.handlePatchStatus)

	return sensor
}

func (s *MockSensor) Run() {
	log.Printf("[%s] Starting sensor\n", s.config.SensorGroup)
	s.stop = make(chan bool)
	go func() {
		if err := s.server.ListenAndServe(); err != nil {
			log.Fatalf("[%s] Could not start sensor: %s\n", s.config.SensorGroup, err.Error())
		}
	}()

	log.Printf("[%s] Publishing sensor config\n", s.config.SensorGroup)
	s.influxWriteApi.WritePoint(newSensorConfigPoint(s.makeSensorStatus(), time.Now()))

	timer := time.NewTimer(time.Duration(s.config.PushInterval) * time.Millisecond)
	for {
		select {
		case <-s.stop:
			s.shutdown()
			timer.Stop()
			log.Printf("[%s] Stopped sensor\n", s.config.SensorGroup)
			return
		case <-timer.C:
			log.Printf("[%s] Push interval passed\n", s.config.SensorGroup)
			for measurement, fields := range s.config.Measurements {
				readings := map[string]interface{}{}
				for _, field := range fields {
					readings[field] = rand.Uint64()
				}

				s.influxWriteApi.WritePoint(influxdb2.NewPoint(measurement, map[string]string{
					"sensor_group":   s.config.SensorGroup,
					"sensor_address": s.config.SensorAddress,
				}, readings, time.Now()))
				s.emitCreatedDatapoints(1)
			}
			s.influxWriteApi.Flush()

			timer.Reset(time.Duration(s.config.PushInterval) * time.Millisecond)
		case err := <-s.influxWriteApi.Errors():
			log.Fatalf("[%s] Could not flush metrics buffer %s\n", s.config.SensorGroup, err.Error())
		}
	}
}

func (s *MockSensor) Stop() {
	log.Printf("[%s] Stopping sensor\n", s.config.SensorGroup)
	close(s.stop)
}

func (s *MockSensor) shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.server.Shutdown(ctx); err != nil {
		log.Fatalf("[%s] Could not shutdown sensor server: %s\n", s.config.SensorGroup, err.Error())
	}
}

func (s *MockSensor) emitCreatedDatapoints(createdDatapointsCount int64) {
	s.influxWriteApi.WritePoint(influxdb2.NewPoint("sensor_status", map[string]string{
		"sensor_group": s.config.SensorGroup,
	}, map[string]interface{}{
		"created_datapoints": createdDatapointsCount,
	}, time.Now()))
}

func (s *MockSensor) handleGetStatus(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, s.makeSensorStatus())
}

func (s *MockSensor) handlePatchStatus(ctx *gin.Context) {
	var sensor types.SensorStatus
	err := ctx.Bind(&sensor)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, err.Error())
		return
	}

	if sensor.PushInterval != 0 {
		s.config.PushInterval = sensor.PushInterval
	}

	if sensor.SensorGroup != "" {
		s.config.SensorGroup = sensor.SensorGroup
	}

	log.Printf("[%s] Publishing sensor config\n", s.config.SensorGroup)
	s.influxWriteApi.WritePoint(newSensorConfigPoint(s.makeSensorStatus(), time.Now()))

	ctx.Status(http.StatusNoContent)
}

func (s *MockSensor) makeSensorStatus() types.SensorStatus {
	return types.SensorStatus{
		SensorGroup:   s.config.SensorGroup,
		SensorAddress: s.config.SensorAddress,
		PushInterval:  s.config.PushInterval,
		InfluxHost:    "localhost",
		InfluxPort:    "8086",
		InfluxOrg:     "mock",
		InfluxBucket:  "mock",
	}
}

func newSensorConfigPoint(sensor types.SensorStatus, time time.Time) *write.Point {
	return influxdb2.NewPoint(consts.SENSOR_CONFIG_METRIC_NAME, map[string]string{
		"sensor_group": sensor.SensorGroup,
	}, map[string]interface{}{
		"push_interval":  sensor.PushInterval,
		"influx_host":    sensor.InfluxHost,
		"influx_port":    sensor.InfluxPort,
		"influx_org":     sensor.InfluxOrg,
		"influx_bucket":  sensor.InfluxBucket,
		"sensor_address": sensor.SensorAddress,
	}, time)
}
