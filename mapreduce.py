import csv
import threading
from collections import defaultdict

#define the class MapReduceThread which is performing the mapreduce operation within a thread
class MapReduceThread(threading.Thread):
    def __init__(self, data, results):
        super(MapReduceThread, self).__init__()
        self.data = data
        self.results = results

    # execution logic of the thread
    def run(self):
        passenger_counts = defaultdict(int)
        for row in self.data:
            passenger_id = row[0]  
            passenger_counts[passenger_id] += 1

        self.results.append(passenger_counts)

def flight_counts(file_path, num_threads):
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader) 
        data = list(reader) 

    # calculate
    block_size = len(data) // num_threads

    # create list
    threads = []    #thread object
    results = []    #thread result

    # divide the dataset and allocate to each thread
    for i in range(num_threads):
        start_idx = i * block_size
        end_idx = start_idx + block_size if i < num_threads - 1 else len(data)
        thread_data = data[start_idx:end_idx]

        # create thread,excute the mapreduce
        thread_result = []
        thread = MapReduceThread(thread_data, thread_result)
        thread.start()

        threads.append(thread)
        results.append(thread_result)

    # time delay and wait all threads
    for thread in threads:
        thread.join()

    # count
    final_result = defaultdict(int)
    for result in results:
        for passenger_counts in result:
            for passenger_id, count in passenger_counts.items():
                final_result[passenger_id] += count

    # sort and seek the most number
    max_flights = max(final_result.items(), key=lambda x: x[1])
    return max_flights

# main
csv_file = 'D:/研究生/大数据与云计算/coursework/AComp_Passenger_data_no_error.csv'
num_threads = 4
max_flights = flight_counts(csv_file, num_threads)
print("ID：", max_flights[0])
print("times：", max_flights[1])

