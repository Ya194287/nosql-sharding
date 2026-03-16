import time
import random
from datetime import datetime, timezone
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import argparse
import csv
import statistics

# Подключение к mongos
client = MongoClient("mongodb://127.0.0.1:27017/")
db = client["spacex_db"]
telemetry_range = db["telemetry"]
telemetry_hashed = db["telemetry_hashed"]

def generate_telemetry_doc():
    return {
        "mission_id": f"mission-{random.randint(1000, 9999)}-{random.choice(['A', 'B', 'C'])}",
        "timestamp": datetime.now(timezone.utc),
        "altitude": round(random.uniform(0, 200000), 2),
        "velocity": round(random.uniform(0, 28000), 2),
        "temperature": {
            "engine": random.randint(800, 3200),
            "tank": random.randint(-200, 100)
        },
        "sensor_status": random.choice(["nominal", "warning", "critical"])
    }

def run_write_test(collection, num_docs=10000, batch_size=1000):
    start_time = time.time()
    inserted = 0
    for i in range(0, num_docs, batch_size):
        batch = [generate_telemetry_doc() for _ in range(min(batch_size, num_docs - i))]
        try:
            result = collection.insert_many(batch, ordered=False)
            inserted += len(result.inserted_ids)
        except BulkWriteError as bwe:
            inserted += len(bwe.details['writeErrors'])
        except Exception as e:
            print(f"Write error: {e}")
        if i % 20000 == 0 and i > 0:
            elapsed = time.time() - start_time
            print(f"Inserted {inserted} docs | {inserted / elapsed:.2f} docs/sec")
    total_time = time.time() - start_time
    throughput = inserted / total_time if total_time > 0 else 0
    return throughput, total_time, inserted

def run_read_test(collection, num_reads=10000):
    # Берём случайные mission_id из существующих документов
    pipeline = [{"$sample": {"size": num_reads}}]
    start_time = time.time()
    count = 0
    for doc in collection.aggregate(pipeline):
        # Просто считаем, что нашли
        count += 1
    total_time = time.time() - start_time
    throughput = count / total_time if total_time > 0 else 0
    print(f"Read {count} documents in {total_time:.2f} sec → {throughput:.2f} ops/sec")
    return throughput, total_time, count

def run_mixed_test(collection, num_ops=20000, write_ratio=0.5):
    num_writes = int(num_ops * write_ratio)
    num_reads = num_ops - num_writes
    start_time = time.time()
    inserted = 0
    read_count = 0
    # Пишем
    for i in range(0, num_writes, 1000):
        batch_size = min(1000, num_writes - i)
        batch = [generate_telemetry_doc() for _ in range(batch_size)]
        try:
            result = collection.insert_many(batch, ordered=False)
            inserted += len(result.inserted_ids)
        except:
            pass
    # Читаем
    pipeline = [{"$sample": {"size": num_reads}}]
    for doc in collection.aggregate(pipeline):
        read_count += 1
    total_time = time.time() - start_time
    throughput = (inserted + read_count) / total_time if total_time > 0 else 0
    print(f"Mixed: {inserted} writes + {read_count} reads → {throughput:.2f} ops/sec")
    return throughput, total_time, inserted, read_count

def run_test(collection_name, mode, num_ops, repeats, output_file=None):
    collection = telemetry_range if collection_name == "range" else telemetry_hashed
    results = []
    print(f"\n=== {mode.upper()} тест на коллекции {collection_name} ({num_ops} операций, {repeats} повторов) ===")

    for rep in range(1, repeats + 1):
        print(f"\nПовтор {rep}/{repeats}")
        if mode == "write":
            tp, t, cnt = run_write_test(collection, num_ops)
            results.append(tp)
        elif mode == "read":
            tp, t, cnt = run_read_test(collection, num_ops)
            results.append(tp)
        elif mode == "mixed":
            tp, t, w, r = run_mixed_test(collection, num_ops)
            results.append(tp)
        else:
            raise ValueError("Unknown mode")

    avg = statistics.mean(results) if results else 0
    print(f"\nСредний throughput: {avg:.2f} ops/sec")

    # Сохраняем в CSV
    if output_file:
        with open(output_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([collection_name, mode, num_ops, repeats, f"{avg:.2f}", f"{min(results):.2f}", f"{max(results):.2f}"])

    return avg

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Нагрузочное тестирование шардированных коллекций SpaceX")
    parser.add_argument("--docs", type=int, default=20000, help="Количество операций (write/read/mixed)")
    parser.add_argument("--collection", choices=["range", "hashed", "both"], default="both")
    parser.add_argument("--mode", choices=["write", "read", "mixed"], default="write", help="Тип нагрузки")
    parser.add_argument("--repeats", type=int, default=3, help="Количество повторов теста")
    parser.add_argument("--output", type=str, default=None, help="Сохранить результаты в CSV")
    args = parser.parse_args()

    if args.output:
        with open(args.output, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Collection", "Mode", "Ops", "Repeats", "Avg Throughput", "Min", "Max"])

    collections = ["range", "hashed"] if args.collection == "both" else [args.collection]

    for col in collections:
        run_test(col, args.mode, args.docs, args.repeats, args.output)

    print("\nТестирование завершено.")
