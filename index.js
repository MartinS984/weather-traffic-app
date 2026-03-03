const { Kafka } = require('kafkajs');

// We use the internal Kubernetes DNS name created by the Strimzi Operator
const kafka = new Kafka({
  clientId: 'weather-traffic-producer',
  brokers: ['weather-broker-kafka-bootstrap.kafka.svc.cluster.local:9092']
});

const producer = kafka.producer();

const generateTelemetry = () => {
  const isWeather = Math.random() > 0.5;
  const timestamp = new Date().toISOString();

  if (isWeather) {
    return {
      sensor_id: `skopje-temp-${Math.floor(Math.random() * 5) + 1}`,
      temperature_c: parseFloat((Math.random() * 15 + 5).toFixed(1)), // 5.0 to 20.0 C
      humidity: Math.floor(Math.random() * 40 + 40), // 40% to 80%
      type: 'weather',
      timestamp: timestamp
    };
  } else {
    return {
      sensor_id: `skopje-traffic-${Math.floor(Math.random() * 5) + 1}`,
      cars_per_minute: Math.floor(Math.random() * 60 + 10),
      status: Math.random() > 0.8 ? 'congested' : 'clear',
      type: 'traffic',
      timestamp: timestamp
    };
  }
};

const run = async () => {
  try {
    await producer.connect();
    console.log('✅ Connected to Strimzi Kafka Cluster');

    setInterval(async () => {
      const payload = generateTelemetry();
      try {
        await producer.send({
          topic: 'iot-weather-data',
          messages: [{ value: JSON.stringify(payload) }],
        });
        console.log(`📤 Sent telemetry: ${JSON.stringify(payload)}`);
      } catch (err) {
        console.error('❌ Error sending message', err);
      }
    }, 3000); // Emits data every 3 seconds

  } catch (err) {
    console.error('💥 Fatal error connecting to Kafka:', err);
    process.exit(1);
  }
};

run();