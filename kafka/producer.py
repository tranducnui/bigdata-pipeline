from kafka import KafkaProducer
from faker import Faker
import json
import random
import time
import uuid
from datetime import datetime

# ─── Config ──────────────────────────────────────────────────────────────────
KAFKA_BROKER   = 'kafka:29092'
TOPIC          = 'sales_topic'
TOTAL_RECORDS  = 1_000_000   # tổng số records muốn sinh
BATCH_SIZE     = 500          # gửi theo batch để nhanh hơn
DELAY_PER_BATCH = 0.01        # delay giữa các batch (giây)

# ─── Faker setup ─────────────────────────────────────────────────────────────
fake = Faker(['vi_VN', 'en_US'])  # mix tiếng Việt + tiếng Anh
Faker.seed(42)
random.seed(42)

# ─── Data templates (giống thật hơn) ─────────────────────────────────────────
TRANSACTION_TYPES = [
    "transfer",       # chuyển khoản
    "payment",        # thanh toán
    "withdrawal",     # rút tiền
    "deposit",        # nạp tiền
    "purchase",       # mua hàng
]

CATEGORIES = [
    "Food & Beverage",
    "Shopping",
    "Transportation",
    "Entertainment",
    "Healthcare",
    "Education",
    "Utilities",
    "Travel",
]

BANKS = [
    "Vietcombank", "Techcombank", "BIDV",
    "VPBank", "MBBank", "ACB", "VIB", "TPBank"
]

STATUSES = ["success", "success", "success", "failed", "pending"]
# success nhiều hơn để realistic

# ─── Pre-generate customer pool (10,000 customers) ──────────────────────────
print("Generating customer pool...")
CUSTOMER_POOL = [
    {
        "customer_id": f"CUS-{str(i).zfill(6)}",
        "customer_name": fake.name(),
        "bank": random.choice(BANKS),
        "account": fake.bban(),
        "city": random.choice([
            "Hà Nội", "TP.HCM", "Đà Nẵng",
            "Hải Phòng", "Cần Thơ", "Huế"
        ])
    }
    for i in range(10_000)
]
print(f"Generated {len(CUSTOMER_POOL)} customers")


def generate_transaction() -> dict:
    customer   = random.choice(CUSTOMER_POOL)
    tx_type    = random.choice(TRANSACTION_TYPES)

    # Amount theo từng loại giao dịch (realistic)
    if tx_type == "transfer":
        amount = round(random.uniform(100_000, 50_000_000), 0)
    elif tx_type == "withdrawal":
        amount = round(random.choice([500_000, 1_000_000, 2_000_000, 5_000_000]), 0)
    elif tx_type == "deposit":
        amount = round(random.uniform(1_000_000, 100_000_000), 0)
    elif tx_type == "payment":
        amount = round(random.uniform(50_000, 5_000_000), 0)
    else:  # purchase
        amount = round(random.uniform(10_000, 2_000_000), 0)

    return {
        "transaction_id"  : str(uuid.uuid4()),
        "customer_id"     : customer["customer_id"],
        "customer_name"   : customer["customer_name"],
        "bank"            : customer["bank"],
        "account"         : customer["account"],
        "city"            : customer["city"],
        "transaction_type": tx_type,
        "category"        : random.choice(CATEGORIES),
        "amount"          : amount,
        "currency"        : "VND",
        "status"          : random.choice(STATUSES),
        "timestamp"       : datetime.now().isoformat(),
        "device"          : random.choice(["mobile", "web", "atm", "pos"]),
    }


# ─── Producer ────────────────────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
    batch_size=65536,        # 64KB batch
    linger_ms=10,            # đợi 10ms để gom batch
    compression_type='gzip', # nén để gửi nhanh hơn
)

print(f"\nProducer starting — target: {TOTAL_RECORDS:,} records")
print(f"Batch size: {BATCH_SIZE} | Topic: {TOPIC}\n")

sent       = 0
start_time = time.time()

while sent < TOTAL_RECORDS:
    # Gửi theo batch
    batch = [generate_transaction() for _ in range(BATCH_SIZE)]
    for tx in batch:
        producer.send(TOPIC, value=tx)

    sent += BATCH_SIZE

    # Progress log mỗi 10,000 records
    if sent % 10_000 == 0:
        elapsed  = time.time() - start_time
        rate     = sent / elapsed
        eta      = (TOTAL_RECORDS - sent) / rate
        print(
            f"Sent: {sent:>10,} / {TOTAL_RECORDS:,} "
            f"| Rate: {rate:,.0f} rec/s "
            f"| ETA: {eta:.0f}s"
        )

    time.sleep(DELAY_PER_BATCH)

producer.flush()
elapsed = time.time() - start_time
print(f"\n✅ Done! Sent {sent:,} records in {elapsed:.1f}s")
print(f"   Average rate: {sent/elapsed:,.0f} records/second")