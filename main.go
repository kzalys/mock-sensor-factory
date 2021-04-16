package main

import (
	"encoding/json"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/kzalys/mock-sensor-factory/sensor"
	"io/ioutil"
	"os"
	"os/signal"
)

const sensorDefinitions = "sensors.json"
const influxHost = "http://localhost:8001"
const influxToken = "l2u3KdkqeCdLagKTNkt_DSgiy0p3yhiDljkuNAmuAVHmIpUlWVvaea03zKw6SnghTeld80o4wEtrD_ytq1O8dw=="
const influxOrg = "iot"
const influxBucket = "iot"

func main() {
	file, _ := ioutil.ReadFile(sensorDefinitions)
	var sensorConfigs []sensor.Config
	err := json.Unmarshal(file, &sensorConfigs)
	if err != nil {
		panic(err)
	}

	if len(sensorConfigs) == 0 {
		return
	}

	writeApi := influxdb2.NewClient(influxHost, influxToken).WriteAPI(influxOrg, influxBucket)

	var sensors []*sensor.MockSensor
	for _, config := range sensorConfigs {
		sensors = append(sensors, sensor.NewMockSensor(writeApi, config))
	}

	for _, sensor := range sensors {
		go sensor.Run()
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
}
