from typing import List
import time
from config import API_CONFIGS
import threading
from logger import get_logger
import threading
import queue
import time

import threading
import queue
import time
from datetime import datetime, timedelta
from typing import List, Callable, Dict
from logger import get_logger


class Scheduler:
    def __init__(self, num_workers: int, interval: float):
        self.logger = get_logger()
        self.tasks = queue.Queue()
        self.num_workers = num_workers
        self.workers = [
            threading.Thread(target=self.worker) for _ in range(num_workers)
        ]
        self._stop_event = threading.Event()
        self.interval = interval
        self.next_run_time: Dict[Callable, datetime] = {}
        self.lock = threading.Lock()

    def start(self):
        self.logger.info("Starting scheduler")
        for worker in self.workers:
            worker.start()

    def stop(self):
        self.logger.info("Stopping scheduler")
        self._stop_event.set()
        self.tasks.put(None)  # Trigger exit for all workers
        for worker in self.workers:
            worker.join()

    def add_task(self, task: Callable):
        with self.lock:
            if task in self.next_run_time and self.next_run_time[task] > datetime.now():
                self.logger.info(
                    f"Skipping task {task}, it's scheduled to run at {self.next_run_time[task]}"
                )
                return
            self.next_run_time[task] = datetime.now() + timedelta(seconds=self.interval)
        self.tasks.put(task)
        self.logger.info(f"Added task {task} to queue")

    def worker(self):
        while not self._stop_event.is_set():
            task = self.tasks.get()
            if task is None:
                break
            try:
                task()
                self.tasks.task_done()
            except Exception as e:
                self.logger.error(f"Error executing task: {e}")

    def schedule_tasks(self, scrapers: List[Callable]):
        while not self._stop_event.is_set():
            for scraper in scrapers:
                self.add_task(scraper.scrape_api_config)
            time.sleep(
                self.interval
            )  # This is just to throttle the loop, not task specific
