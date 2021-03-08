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
const influxHost = "http://localhost:8086"
const influxToken = "5PxwxHZrfsvCt1m7I2xD4YO7Zz5v_RCj0MZToFOw8t5fcv-CNVVj3DNPGfAWehyaaU__1TtUW0nFhoDeKodaqg=="
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
