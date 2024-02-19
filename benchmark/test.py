import redis
import time
from multiprocessing import Pool
import random
# 连接到 Redis 或兼容 Redis 的存储引擎
r = redis.Redis(host='localhost', port=6379, password="123456", db=0)

# 测试的并发客户端数
num_clients = 50
# 每个客户端执行的操作数
num_requests_per_client = 2000

def benchmark(client_id):
    for _ in range(num_requests_per_client):
        # 使用随机生成的 key 和 value 进行 SET 操作
        key = f"key:{client_id}:{random.uniform(1, 100000)}"
        value = f"value:{random.uniform(1, 100000)}"
        r.set(key,value)
    return f"Client {client_id} completed"

if __name__ == "__main__":
    start_time = time.time()

    with Pool(num_clients) as p:
        results = p.map(benchmark, range(num_clients))

    end_time = time.time()
    total_time = end_time - start_time
    print("\n".join(results))
    print(f"Total time for {num_clients * num_requests_per_client} requests: {total_time} seconds")
    print(f"Overall querys per second (QPS): {num_clients * num_requests_per_client / total_time}")

