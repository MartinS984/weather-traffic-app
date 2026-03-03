const { Kafka } = require('kafkajs');
const express = require('express');
const client = require('prom-client');

const app = express();
const port = 3001;

// 1. Prometheus Metrics Setup
const register = new client.Registry();

const tempGauge = new client.Gauge({
  name: 'weather_temperature_celsius',
  help: 'Current temperature in Celsius',
  labelNames: ['sensor_id']
});

const humidityGauge = new client.Gauge({
  name: 'weather_humidity_percent',
  help: 'Current humidity percentage',
  labelNames: ['sensor_id']
});

const trafficGauge = new client.Gauge({
  name: 'traffic_cars_per_minute',
  help: 'Number of cars per minute',
  labelNames: ['sensor_id', 'status']
});

register.registerMetric(tempGauge);
register.registerMetric(humidityGauge);
register.registerMetric(trafficGauge);

// 2. Kafka Consumer Setup
const kafka = new Kafka({
  clientId: 'weather-traffic-consumer',
  brokers: ['weather-broker-kafka-bootstrap.kafka.svc.cluster.local:9092']
});

const consumer = kafka.consumer({ groupId: 'telemetry-metrics-group' });

const run = async () => {
  try {
    await consumer.connect();
    console.log('✅ Consumer connected to Strimzi Kafka Cluster');
    
    // Subscribe to the topic. We only want new messages (fromBeginning: false)
    await consumer.subscribe({ topic: 'iot-weather-data', fromBeginning: false });
    
    // Process messages as they arrive
    await consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const payload = JSON.parse(message.value.toString());
          console.log(`📥 Processed: ${payload.sensor_id} at ${payload.timestamp}`);
          
          // Route data to the correct Prometheus gauge
          if (payload.type === 'weather') {
            tempGauge.labels(payload.sensor_id).set(payload.temperature_c);
            humidityGauge.labels(payload.sensor_id).set(payload.humidity);
          } else if (payload.type === 'traffic') {
            trafficGauge.labels(payload.sensor_id, payload.status).set(payload.cars_per_minute);
          }
        } catch (err) {
          console.error('❌ Error parsing message:', err);
        }
      },
    });
  } catch (err) {
    console.error('💥 Fatal error connecting to Kafka:', err);
    process.exit(1);
  }
};

run();

// 3. Expose the HTTP Endpoint for Prometheus
app.get('/metrics', async (req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

app.listen(port, () => {
  console.log(`📊 Metrics server listening on port ${port}`);
});