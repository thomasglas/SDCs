import subprocess
import re
import csv
import logging

# setup logger
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='results.log',
                    filemode='w')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

result_file_path = "results.csv"

block_sizes = [
    1000,
    2000,
    4000,
    8000,
    16000,
    32000,
    64000,
    128000,
    256000,
    512000
]

def extract_runtime(text):
    seconds = re.findall("Time measured: ([0-9]+.[0-9]+) seconds", text)
    return [float(x) for x in seconds]

def run_query(workload, block_size, n):
    run_times = []
    for i in range(0,n):
        result = subprocess.run(f"./sdcs {workload} {block_size}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode!=0:
            logging.error(f"return code {result.returncode}")
            break
        if result.stderr:
            logging.warning(result.stderr.decode('utf-8'))
            break
        run_time = extract_runtime(result.stdout.decode('utf-8'))
        run_times.append(run_time)
    return run_times

def get_median(run_times):
    if len(run_times) == 0:
        return "timeout"
    run_times.sort()
    if len(run_times)%2==0:
        median = (run_times[len(run_times)//2-1] + run_times[len(run_times)//2])/2
    else:
        median = run_times[len(run_times)//2]
    return round(median, 3)

def write_results(row):
     with open(result_file_path,'a') as f:
        csv_writer = csv.writer(f, delimiter=",")
        csv_writer.writerow(row)


if __name__=='__main__':
    with open(result_file_path,'w') as f:
        csv_writer = csv.writer(f, delimiter=",")
        csv_writer.writerow(["w1'","w1''","w1'''","w2'","w2''","w2'''","w3'","w3''","w3'''","w4'","w4''","w4'''"])

    # tpch queries
    for block_size in block_sizes:
        run_times = []
        for workload in range(1,5):

            # warmup
            logging.info(f"warming up workload {workload} with block size {block_size}")
            logging.info(run_query(workload, block_size, 2))

            # benchmark
            logging.info(f"benchmarking workload {workload} with block size {block_size}")
            results = run_query(workload, block_size, 10)
            logging.info(results)
            for i in range(0,3):
                median = get_median([x[i] for x in results])
                run_times.append(median)

        write_results(run_times)

    logging.info("finished")
