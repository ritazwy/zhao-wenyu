import csv
import threading
from collections import defaultdict

class MapReduceThread(threading.Thread):
    def __init__(self, data, intermediate_results):
        super(MapReduceThread, self).__init__()
        self.data = data
        self.intermediate_results = intermediate_results

    def run(self):
        # Map阶段
        passenger_counts = defaultdict(int)
        for row in self.data:
            passenger_id = row[0]  # 乘客ID
            passenger_counts[passenger_id] += 1

        # 将结果保存到中间结果列表
        self.intermediate_results.append(passenger_counts)

def analyze_flight_counts(file_path, num_threads):
    # 读取CSV文件
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # 跳过表头
        data = list(reader)  # 读取所有数据

    # 计算每个线程处理的数据块大小
    block_size = len(data) // num_threads

    # 创建线程列表和中间结果列表
    threads = []
    intermediate_results = []

    # 分配数据块给每个线程进行处理
    for i in range(num_threads):
        start_idx = i * block_size
        end_idx = start_idx + block_size if i < num_threads - 1 else len(data)
        thread_data = data[start_idx:end_idx]

        # 创建线程，并执行MapReduce过程
        thread_result = []
        thread = MapReduceThread(thread_data, thread_result)
        thread.start()

        threads.append(thread)
        intermediate_results.append(thread_result)

    # 等待所有线程执行完成
    for thread in threads:
        thread.join()

    # 合并中间结果
    final_result = defaultdict(int)
    for result in intermediate_results:
        for passenger_counts in result:
            for passenger_id, count in passenger_counts.items():
                final_result[passenger_id] += count

    # 根据飞行次数排序，找到飞行次数最多的乘客
    max_flights = max(final_result.items(), key=lambda x: x[1])
    return max_flights

# 测试
csv_file = 'D:/研究生/大数据与云计算/coursework/AComp_Passenger_data_no_error.csv'
num_threads = 4
max_flights = analyze_flight_counts(csv_file, num_threads)
print("乘客ID：", max_flights[0])
print("飞行次数：", max_flights[1])

