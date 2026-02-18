# acit-3855-lab

### SSH into VM

From your laptop:
```
ssh <user>@<VM IP>
```

### Start Docker Services (Kafka + Zookeeper + MySQL)

Go to project root:

```
cd ~/acit-3855-lab
```

Start containers:
```
docker compose up -d
```

Verify:
```
docker compose ps
```

You must see:
```
kafka → Up

zookeeper → Up

db → Up
```

If anything says Exited → stop demo and fix.

### Verify MySQL is reachable

```
docker exec -it acit-3855-lab-db-1 mysql -u jas -p traffic
```


Then:
```
SHOW TABLES;
```

Exit.


### Start Storage Service

Open a new terminal tab (keep Docker running).
```
cd ~/acit-3855-lab/Storage
source venv/bin/activate
python app.py
```

You MUST see logs like:
```
Kafka consumer thread started
Storage Kafka consumer starting...
```

If you don’t see those → consumer thread not running.

Leave this terminal running.

### Start Receiver Service

Open another terminal tab:
```
cd ~/acit-3855-lab/Receiver
source venv/bin/activate
python app.py
```

Receiver must start without errors.

Leave it running.

SYSTEM IS NOW FULLY LIVE

    Docker: running
    Kafka: running
    MySQL: running
    Storage: consuming
    Receiver: accepting


### Test Receiver → Kafka → Storage

From your laptop or VM:
```
curl -i -X POST "http://136.114.154.173:8080/events/speeding" \
  -H "Content-Type: application/json" \
  -d '{
    "sender_id": "d290f1ee-6c54-4b01-90e6-d701748f0851",
    "location_id": "SZ-042",
    "sent_timestamp": "2026-02-18T00:00:00Z",
    "violations": [
      {
        "recorded_timestamp": "2026-02-18T00:00:05Z",
        "speed_kmh": 120.0,
        "speed_limit_kmh": 90.0,
        "direction": "NORTHBOUND"
      }
    ]
  }'
```

You should see:
```
HTTP/1.1 201 Created
```


### Prove Kafka Received It

```
docker exec -it acit-3855-lab-kafka-1 bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic events
```

You’ll see the JSON message.

Exit.


### Prove MySQL Stored It

```
docker exec -it acit-3855-lab-db-1 mysql -u jas -p traffic
```

Then:
```
SELECT COUNT(*) FROM speeding_violation;
```

Number should increase.

That proves:

`Receiver → Kafka → Storage → MySQL`

