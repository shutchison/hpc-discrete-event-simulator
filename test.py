from des import DES 
import pickle

simulator = DES()

machines_csv = "machines.csv"
jobs_csv = "jobs.csv"

for algo in ["fcfs", "sjf-oracle", "sjf", "bfbp"]:
    print("="*40)
    print(algo)
    simulator.reset(machines_csv, jobs_csv)

    if algo == "sjf-oracle":
        simulator.run_simulation("sjf", True)
    else:
        simulator.run_simulation(algo)

    print(simulator.summary_statistics())
    #print(simulator.completed_jobs)

    simulator.write_completed_jobs_output_csv(f"completed_{algo}.csv")

    print(f"{round(simulator.get_avg_job_queue_time(),2):,}")