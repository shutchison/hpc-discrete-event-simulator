from cluster import Cluster
from machine import Machine
from job import Job
import queue
import logging
from datetime import datetime

infinite_loop_detector_max = 1000000

class DES():
    def __init__(self):
        self.cluster = Cluster()
        self.global_clock = 0
        self.clock_offset = 0

        self.future_jobs = queue.PriorityQueue()  # ordered based on submit time
        self.job_queue = []
        self.running_jobs = queue.PriorityQueue() # ordered based on end time
        self.completed_jobs = []
        self.unrunable_jobs = []
        
        logging.basicConfig(filename="simulation.log",
                            filemode="w",
                            
                            level=logging.INFO)
        self.logger = logging.getLogger()

        self.logger.info("Times are simulation times, not real wall clock time.")

    def log(self, message: str = ""):
        self.logger.info(f"{datetime.fromtimestamp(self.global_clock)} - {message}")
        
    def reset(self, 
              machines_csv : str, 
              jobs_csv : str):
        """
        Loads the machine from the machines_csv file and the jobs from the jobs_csv file.
        Removes the first job from future jobs and puts it in the jobs_queue, and updates the global clock
        """
        self.load_cluster(machines_csv)
        self.load_jobs(jobs_csv)
        self.global_clock, job = self.future_jobs.get()
        self.clock_offset = self.global_clock
        self.job_queue.append(job)
        self.unrunable_jobs = []

        self.log("Reset called")
        

    def load_cluster(self, 
                     csv_file_name : str):
        self.machines_file = csv_file_name
        self.cluster.machines = []
        self.cluster.load_machines(csv_file_name)

        self.log(f"{len(self.cluster.machines)} machines loaded from {csv_file_name}")

    def load_jobs(self, 
                  csv_file_name : str):
        self.jobs_file = csv_file_name
        self.future_jobs = queue.PriorityQueue()  # ordered based on submit time
        self.job_queue = []
        self.running_jobs = queue.PriorityQueue() # ordered based on end time
        self.completed_jobs = []
        
        f = open(csv_file_name)
        lines = f.readlines()
        f.close()
        lines = list(map(str.strip, lines))
        # print(lines)
        headers = lines[0]
        for line in lines[1:]:
            elements = line.split(",")
            j = Job(elements[0], *map(int, elements[1:]))
            # wrap this in a tuple, so they are ordered by their sumbission time.
            self.future_jobs.put( (j.submission_time, j) )
        # initialize global clock to be the submission time of the first job
        self.global_clock = self.future_jobs.queue[0][0]

        self.log(f"{len(self.future_jobs.queue)} jobs loaded from {csv_file_name}")

    def run_simulation(self, scheduling_algorithm : str, oracle_mode:bool = False):
        if scheduling_algorithm == "fcfs":
            self.first_come_first_serve()
        elif scheduling_algorithm == "sjf":
            self.shortest_job_first(oracle_mode)
        elif scheduling_algorithm == "bfbp":
            self.best_fit_bin_packing()
        else:
            raise ValueError("f{scheduling_algorithm} is not currently implemented.")

    def shortest_job_first(self, oracle_mode:bool = False):
        # Shortest Job First scheduling algorithm.  Choose the shortest schedulable job
        # from the job_queue and schedule it.
        # If oracle_mode is set to True, the job's actual duration will be used
        # instead of the requested duration.  Typically, a scheduler would not have access
        # to this information and would instead rely upon the requested duration.
        counter = 0

        while not self.simulation_is_complete():
            counter += 1
            if counter >= infinite_loop_detector_max:
                print("has the loop gone infinite?")
                break

            # move all jobs submitted at the current time into the job_queue
            # until all jobs that have been submitted are queued
            self.queue_submitted_jobs()
            self.stop_ending_jobs()
            if len(self.job_queue) == 0 and not self.future_jobs.empty():
                self.advance_to_next_submitted()

            # run the shortest job on the first machine that has enough resources to run it
            if not len(self.job_queue) == 0:
                shortest_index = self.get_shortest_schedulable_job_index(oracle_mode)
                
                # No jobs in the queue are scheudlable, so we need to put more jobs in the job_queue
                if shortest_index is None:
                    
                    next_submit = 1e100
                    next_end = 1e100
                    if not self.future_jobs.empty():
                        next_submit = self.future_jobs.queue[0][0]
                    elif not self.running_jobs.empty():
                        next_end = self.running_jobs.queue[0][0]

                    if next_submit == 1e100 and next_end == 1e100:
                        print("Nothing to submit and nothing to end and no runnable jobs in the queue")
                        break
                    if next_submit <= next_end:
                        self.advance_to_next_submitted()
                        continue
                    elif next_end < next_submit:
                        self.advance_to_next_ending()
                        continue
                    else:
                        # can't do anything else.  All jobs in the queue are unrunnable
                        break

                shortest_job = self.job_queue.pop(shortest_index)
                for machine_index, machine in enumerate(self.cluster.machines):
                    if machine.can_schedule(shortest_job):
                        self.run_job(shortest_job, machine_index)
                        break
                continue

            # nothing left in the queue that can start, so stop running jobs and advance time
            if self.running_jobs.empty():
                print("No running jobs to end?")
                break
            next_ending_time, next_ending_job = self.running_jobs.queue[0]
            self.global_clock = next_ending_time
            self.queue_submitted_jobs()
            self.stop_ending_jobs()
        while len(self.job_queue) != 0:
            self.unrunable_jobs.append(self.job_queue.pop(0))
    
    def get_shortest_schedulable_job_index(self, oracle_mode : bool = False):
        # returns the index of the job with the shortest requested duration which is schedulable
        # if oracle mode is set to True, will return the index of the job with the shortest actual duration
        # requires external verification that the job queue is not empty.

        if oracle_mode:
            times_list = [(index, job.actual_duration) for index, job in enumerate(self.job_queue)]
        else:
            times_list = [(index, job.req_duration) for index, job in enumerate(self.job_queue)]

        found_one = False
        for job_index, time in sorted(times_list, key=lambda x: x[1]):
            if self.this_job_can_be_scheduled(self.job_queue[job_index]):
                found_one == True
                return job_index

        if not found_one:
            return None

    def best_fit_bin_packing(self):
        # locates the job in the job_queue that will fill up a machine to the fullest
        # and starts it on that machine.
        counter = 0

        while not self.simulation_is_complete():
            counter += 1
            if counter >= infinite_loop_detector_max:
                print("has the loop gone infinite?")
                break

            # move all jobs submitted at the current time into the job_queue
            # until all jobs that have been submitted are queued
            self.queue_submitted_jobs()
            self.stop_ending_jobs()
            if len(self.job_queue) == 0 and not self.future_jobs.empty():
                self.advance_to_next_submitted()
                continue

            # Find the job that will fill up any machine the most and run it on that machine.
            if not len(self.job_queue) == 0:
                if not self.some_job_can_be_scheduled():
                    if self.running_jobs.empty():
                        self.advance_to_next_submitted()
                        continue
                    next_ending_time, next_ending_job = self.running_jobs.queue[0]
                    self.global_clock = next_ending_time
                    self.queue_submitted_jobs()
                    self.stop_ending_jobs()
                    continue
                job_index, machine_index = self.get_bfbp_job_machine_index()
                job_to_run = self.job_queue.pop(job_index)
                self.run_job(job_to_run, machine_index)
                continue
            # nothing left in the queue that can start, so stop running jobs and advance time
            if self.running_jobs.empty():
                print("No running jobs to end?")
                break
            
            # if we've made it this far, we need to end a job
            self.advance_to_next_ending()

        while len(self.job_queue) != 0:
            self.unrunable_jobs.append(self.job_queue.pop(0))

    def advance_to_next_submitted(self):
        # advances the global_clock to the next submitted job, 
        # stop any running jobs that may have occured before that point.
        # if the next job is not submittable anywhere, move it to unkillable and try again.
        if not self.future_jobs.empty():
            next_submitted_time, next_submitted_job = self.future_jobs.queue[0]
            if not self.any_empty_machine_could_run_this(next_submitted_job):
                next_submitted_time, next_submitted_job = self.future_jobs.get(0)
                self.unrunable_jobs.append(next_submitted_job)
            else:
                self.global_clock = next_submitted_time
                self.queue_submitted_jobs()
                self.stop_ending_jobs()
        else:
            print("future_jobs is empty, and we cannot advance time to next submitted job.")

    def advance_to_next_ending(self):
        # advances the global_clock to the next ending job, 
        # submit any jobs that may have occured before that point.
        if not self.running_jobs.empty():
            next_ending_time, next_ending_job = self.running_jobs.queue[0]
            self.global_clock = next_ending_time
            self.queue_submitted_jobs()
            self.stop_ending_jobs()
        else:
            print("running_jobs is empty, and we cannot advance time to next ending job.")

    def get_bfbp_job_machine_index(self):
        # returns the (job_index, machine_index) tuple for the job in the job queue
        # and the machine on which to run it which will result in the highest
        # percente utilization if the job were to be started on that machine.

        best_machine_index = None
        best_job_index = None
        lowest_remaining_resources = 10

        for job_index in self.get_schedulable_jobs_indexes():
            current_job = self.job_queue[job_index]

            for machine_index, machine in enumerate(self.cluster.machines):
                if machine.can_schedule(current_job):
                    mem_margin = 0.0
                    cpu_margin = 0.0
                    gpu_margin = 0.0

                    # count how many attributes the node has to normalize the final margin
                    n_attributes = 0

                    if current_job.req_mem > 0:
                        mem_margin = (machine.avail_mem - current_job.req_mem)/machine.total_mem
                        n_attributes += 1

                    if current_job.req_cpus > 0:
                        cpu_margin = (machine.avail_cpus - current_job.req_cpus)/machine.total_cpus
                        n_attributes += 1

                    if current_job.req_gpus > 0:
                        gpu_margin = (machine.avail_gpus - current_job.req_gpus)/machine.total_gpus
                        n_attributes += 1

                    remaining_resources_avg = (mem_margin + cpu_margin + gpu_margin)/n_attributes
                    if remaining_resources_avg < lowest_remaining_resources :
                        lowest_remaining_resources = remaining_resources_avg
                        best_job_index = job_index
                        best_machine_index = machine_index

        return (best_job_index, best_machine_index)

    def first_come_first_serve(self):
        # First come first serve scheduling algorthim
        # schedule the next submitted job.  If we can't, wait until resources
        # are freed up so it can be run.
        counter = 0

        while not self.simulation_is_complete():
            counter += 1
            if counter >= infinite_loop_detector_max:
                print("has the loop gone infinite?")
                break

            # move all jobs submitted at the current time into the job_queue
            # until all jobs that have been submitted are queued
            self.queue_submitted_jobs()
            self.stop_ending_jobs()
            if len(self.job_queue) == 0 and not self.future_jobs.empty():
                next_submitted_time, next_submitted_job = self.future_jobs.queue[0]
                self.global_clock = next_submitted_time
                self.queue_submitted_jobs()
                self.stop_ending_jobs()
                continue

            # ensure the first job is runnable anywhere at all or kill it.
            if len(self.job_queue) > 0:
                first_job = self.job_queue[0]
                if not self.any_empty_machine_could_run_this(first_job):
                    first_job = self.job_queue.pop(0)
                    self.unrunable_jobs.append(first_job)
                    continue

            # we need to start the oldest submitted job if we can
            if self.first_job_can_be_scheduled():
                first_job = self.job_queue.pop(0)
                for machine_index, machine in enumerate(self.cluster.machines):
                    if machine.can_schedule(first_job):
                        self.run_job(first_job, machine_index)
                        break
            # advance time and free up some resources, stopping any jobs
            # that need to be stopped
            else:
                if self.running_jobs.empty():
                    print("No running jobs to end?")
                    break
                next_ending_time, next_ending_job = self.running_jobs.queue[0]
                self.global_clock = next_ending_time
                self.queue_submitted_jobs()
                self.stop_ending_jobs()
                
    def any_empty_machine_could_run_this(self, job : Job) -> bool:
        # returns true if the job is schdulable on any machines in the cluster
        # or false if no machine at all can run this job even when they are all empty.
        # used to detect when a job can be removed since it will never be runable
        for machine in self.cluster.machines:
            if machine.total_mem >= job.req_mem and machine.total_cpus >= job.req_cpus and machine.total_gpus >= machine.total_gpus >= job.req_gpus:
                return True

        return False

    def queue_submitted_jobs(self):
        # will move all jobs submitted at or before the current global clock from the 
        # future_jobs nto the job queue.
        # Does not adjust the global_clock.
        while True:
            if not self.future_jobs.empty():
                next_submitted_time, next_submitted_job = self.future_jobs.queue[0]
                if next_submitted_time <= self.global_clock:
                    next_submitted_time, next_submitted_job = self.future_jobs.get(0)
                    self.job_queue.append(next_submitted_job)

                    self.log(f"{next_submitted_job.job_name} submitted to job queue")
                    continue
                else:
                    break
            else:
                break
    
    def stop_ending_jobs(self):
        # will stop all jobs ending at of before the current global clock and
        # move them to the completed jobs, freeing up the resources they've used
        # Does not adjust the global clock
        while True:
            if not self.running_jobs.empty():
                next_ending_time, next_ending_job = self.running_jobs.queue[0]
                if next_ending_time <= self.global_clock:
                    next_ending_time, next_ending_job = self.running_jobs.get(0)
                    self.stop_job(next_ending_job)
                    self.completed_jobs.append(next_ending_job)
                    continue
                else:
                    break
            else:
                break

    def run_job(self, 
                job : Job,
                machine_index : int):
        job.start_time = self.global_clock
        # added the min to simulate that a job will be killed if its 
        # requested duration has been reached
        # or its actual duration, whichever is sooner.
        job.end_time = job.start_time + min(job.actual_duration, job.req_duration)
        self.cluster.machines[machine_index].start_job(job)
        self.running_jobs.put( (job.end_time, job) )
        job.ran_on = self.cluster.machines[machine_index].node_name

        self.log(f"{job.job_name} started running on {self.cluster.machines[machine_index].node_name}")

    def stop_job(self, job_to_stop: Job):
        for machine in self.cluster.machines:
            for running_job in machine.running_jobs:
                if job_to_stop == running_job:
                    machine.stop_job(job_to_stop.job_name)
                    self.log(f"{job_to_stop.job_name} stopped running on {machine.node_name}")
                    return
                
    def get_avg_job_queue_time(self):
        # returns the avg job queue time of all jobs which have been completed
        # otherwise, returns 1e100 if no jobs have been completed.
        num_completed = len(self.completed_jobs)
        queue_times = [(job.start_time - job.submission_time) for job in self.completed_jobs]
        if num_completed == 0:
            return 1e100
        else:
            avg_queue_time = sum(queue_times)/num_completed
            return avg_queue_time
        
    def first_job_can_be_scheduled(self):
        # returns true if the first job in the queue can be scheduled, false otherwise
        if len(self.job_queue) == 0:
            return False
        
        first_job = self.job_queue[0]
        for machine in self.cluster.machines:
            if machine.can_schedule(first_job):
                return True
        return False
    
    def get_schedulable_jobs_indexes(self):
        indexes_of_runnable_jobs = []
        for job_index, job in enumerate(self.job_queue):
            if self.this_job_can_be_scheduled(job):
                indexes_of_runnable_jobs.append(job_index)
        return indexes_of_runnable_jobs


    def this_job_can_be_scheduled(self, job:Job):
        # returns true if the inputted job can be scheduled somewhere.
        # otherwise, false.
        for machine in self.cluster.machines:
            if machine.can_schedule(job):
                return True
        return False
    
    def some_job_can_be_scheduled(self):
        # returns True if there is at least one job which can be scheduled with 
        # the current available resources in the cluster, False otherwise.
        for job in self.job_queue:
            for machine in self.cluster.machines:
                if machine.can_schedule(job):
                    return True
        return False
    
    def simulation_is_complete(self):
        # returns True if there is nothing left to do.
        # False otherwise
        sim_complete = False
        if self.future_jobs.empty() and self.running_jobs.empty() and not self.some_job_can_be_scheduled():
            #print("No future jobs, no running jobs, and no more jobs I can schedule!  Nothing left to do.")
            sim_complete = True
            
        if self.global_clock == 1e100:
            print("Global clock has gone to infinity.  Something went wrong or we're done?")
            sim_complete = True
        
        if sim_complete:
            self.log("Detected simulation is finished")

        return sim_complete
    
    def fast_forward(self):
        # fast_forward will advance time until at least one job can be scheduled or ended,
        # and then it will queue and/or end those jobs.
        # It will do no scheduling itself.

        # returns truncated to step so it will return
        # False when the state can schedule more jobs or True if there 
        # is nothing else for the simulation to do.
        
        # Gets the minimum time value between the next job submission and/or job end (both could happen at once)
        # Updates the simulation state depending on which action(s) are taken
        # Then sees if we can schedule more jobs
        # If not, repeats the process until we can
        # This should create a system state such that upon return, a job can be scheduled.
        
        #print("global clock: {:,}".format(self.global_clock))

        # If there are no more tasks for the simulation, return.
        if self.simulation_is_complete():
            return True

        # If there are still schedulable jobs, don't update the system state.
        if self.some_job_can_be_scheduled():
            return False
        
        # Move time to the next event, update the system state, check for schedulable jobs, possibly repeat
        advancing_time = True
        while advancing_time:

            next_job_submit_time = 1e100
            next_job_end_time = 1e100
            if not self.future_jobs.empty(): 
                next_job_submit_time = self.future_jobs.queue[0][0] 
            if not self.running_jobs.empty(): 
                next_job_end_time = self.running_jobs.queue[0][0]

            if self.future_jobs.empty() and self.running_jobs.empty():
                if self.some_job_can_be_scheduled():
                    return False
                else:
                    # Equivalent state to self.simulation_is_complete()
                    return True
            else:
                self.global_clock = min([next_job_submit_time, next_job_end_time])
                # print("Updating global clock to {:,}".format(self.global_clock))

                if self.global_clock == next_job_submit_time: 
                    self.tick_queue_jobs()
                
                if self.global_clock == next_job_end_time: 
                    self.tick_end_jobs()
                
                if self.some_job_can_be_scheduled():
                    advancing_time = False
            
                if self.simulation_is_complete():
                    return True

        return False

    def tick_queue_jobs(self):
        # iterate through self.future_jobs to find all jobs with job.submission_time == self.global_clock
        # move these jobs to self.job_queue ordered based on job.submision_time
        while not self.future_jobs.empty():
            submit_time = self.future_jobs.queue[0][0]
            if submit_time > self.global_clock:
                break
            elif submit_time <= self.global_clock:
                new_time, new_job = self.future_jobs.get()
                #print("{} submitted at time {:,}".format(new_job.job_name, self.global_clock))
                self.job_queue.append(new_job)

    def tick_end_jobs(self):
        # iterate through self.running jobs and remove all jobs from machines whose job.end_time == self.global_clock
        # append these jobs to self.completed_jobs
        while not self.running_jobs.empty():
            end_time = self.running_jobs.queue[0][0]
            if end_time > self.global_clock:
                break
            elif end_time <= self.global_clock:
                old_time, ending_job = self.running_jobs.get()
                found_job = False
                for machine in self.cluster.machines:
                    for running_job in machine.running_jobs:
                        if ending_job.job_name == running_job.job_name:
                            #print("job {} ending at time {}".format(ending_job.job_name, self.global_clock))
                            found_job = True
                            machine.stop_job(ending_job.job_name)
                            self.completed_jobs.append(ending_job)
                            #self.machines_log_status()
                            #self.log_training_data_csv(job, self.machines, m.node_name, "Stop")
                            break
                    if found_job:
                        break
                if not found_job:
                    print("Error! Failed to find and end {}".format(ending_job.job_name))   

    def summary_statistics(self):
        s = "="*40 + "\n"
        s += "global logical clock = {:,}\n".format(self.global_clock - self.clock_offset)
        #s += "num future jobs = {}, {}\n".format(len(self.future_jobs.queue), self.future_jobs.queue)
        #s += "num queued jobs = {}, {}\n".format(len(self.job_queue), self.job_queue)
        #s += "num running jobs = {}, {}\n".format(len(self.running_jobs.queue), self.running_jobs.queue)
        s += "num future jobs = {}\n".format(len(self.future_jobs.queue))
        s += "num queued jobs = {}\n".format(len(self.job_queue))
        s += "num running jobs = {}\n".format(len(self.running_jobs.queue))
        s += "num completed jobs = {}\n".format(len(self.completed_jobs))
        s += "num unrunable jobs = {}\n".format(len(self.unrunable_jobs))
        s += "="*40
        
        return s    
    
    def write_completed_jobs_output_csv(self, filename:str):
        f = open(filename, "w")
        one_job = self.completed_jobs[0]
        f.write(",".join(one_job.__dict__.keys()) + "\n")
        for job in self.completed_jobs:
            job_dict = job.__dict__
            f.write(",".join(list(map(str, list(job_dict.values())))))
            f.write("\n")
        print(f"output written to {filename}")
        f.close()

    def __repr__(self):
        s = "DES("
        for key, value in self.__dict__.items():
            if type(value) == queue.PriorityQueue:
                s += str(value.queue)
            else:
                s += str(key) + "=" + repr(value) + ",\n"
        return s[:-2] + ")"
    
    def __str__(self):
        s =  f"Cluster size         = {len(self.cluster.machines)}\n"
        s += f"Current global_clock = {self.global_clock}\n"
        s += f"Num future jobs      = {len(self.future_jobs.queue)}\n"
        s += f"Num jobs queued      = {len(self.job_queue)}\n"
        s += f"Num running jobs     = {len(self.running_jobs.queue)}\n"
        s += f"Num completed jobs   = {len(self.completed_jobs)}"

        return s
    