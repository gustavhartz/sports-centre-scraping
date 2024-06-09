import datetime
import time
from config import API_CONFIGS
from logger import setup_logging, get_logger
import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
import uuid
from scheduler import Scheduler

DAYS_TO_EXTRACT = 14
NUM_WORKERS = 2
EXTRACTION_INTERVAL = 20 * 60  # 20 minutes


class APIScraper:
    def __init__(
        self, api_config, base_url, headers, output_file, request_interval_seconds=0.5
    ):
        self.api_config = api_config
        self.base_url = base_url
        self.headers = headers
        self.output_file = output_file
        self.logger = get_logger()
        self.request_interval_seconds = request_interval_seconds
        self.last_request_time = time.time() - self.request_interval_seconds
        # set uuid
        self.uuid = uuid.uuid4()

    def rate_limit(self):
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.request_interval_seconds:
            time.sleep(self.request_interval_seconds - time_since_last_request)

    def _fetch_data(self, params, url=None):
        if url is None:
            url = self.base_url
        self.logger.info(f"Fetching data from {self.base_url} with params: {params}")
        self.rate_limit()
        response = requests.get(url, headers=self.headers, params=params)
        self.last_request_time = time.time()
        if response.status_code != 200:
            self.logger.error(f"Failed to retrieve data: {response.status_code}")
            self.logger.error(response.text)
            return None
        return response

    def process_data(self, data, static_attributes={}):
        """
        Return type
        court_id: str
        sport: str
        date: str
        slot_type: str
        Duration: float
        extraction_time: str
        """
        raise NotImplementedError

    def save_data(self, results):
        self.logger.info(f"Saving data to {self.output_file}")
        try:
            existing_data = pd.read_csv(self.output_file)
        except FileNotFoundError:
            existing_data = None
        if existing_data is None:
            pd.DataFrame(results).to_csv(self.output_file, index=False)
        else:
            joint = pd.concat((existing_data, pd.DataFrame(results)))
            # drop duplicates ignoring the index and extraction_time
            joint = joint.drop_duplicates(
                subset=joint.columns.difference(["extraction_time"])
            )
            joint.to_csv(self.output_file, index=False)
        self.logger.info(f"Data saved to {self.output_file}")

    def scrape_api_config(self):
        raise NotImplementedError


# Usage
class PlaytomicScraper(APIScraper):

    def process_data(self, data, static_attributes={}):
        self.logger.info(f"Processing data for Playtomic")
        results = []
        for resource in data:
            for slot in resource["slots"]:
                resource_id = resource["resource_id"]
                start_time = slot["start_time"]
                duration = slot["duration"]
                price = slot["price"]
                results.append(
                    {
                        "court_id": resource_id,
                        "sport": "PADEL",
                        "date": start_time,
                        "slot_type": "PADEL",
                        "duration": duration,
                        "price": price,
                        "extraction_time": datetime.datetime.now().isoformat(),
                        **static_attributes,
                    }
                )
        return results

    def scrape_api_config(self):
        name = self.api_config["name"]
        tenant_id = self.api_config["tenant_id"]
        sport_id = self.api_config["sport_id"]
        data = []
        # Calculate the current timestamp
        today = datetime.datetime.now()
        today = today.replace(hour=0, minute=0, second=0, microsecond=0)
        for day_offset in range(DAYS_TO_EXTRACT):
            local_start_min = today + datetime.timedelta(days=day_offset)
            local_start_max = today + datetime.timedelta(days=day_offset)

            params = {
                "tenant_id": tenant_id,
                "sport_id": sport_id,
                "local_start_min": local_start_min.strftime("%Y-%m-%dT00:00:00"),
                "local_start_max": local_start_max.strftime("%Y-%m-%dT23:59:59"),
                "user_id": "me",
            }
            response = self._fetch_data(params)
            if response:
                data += response.json()

        results = self.process_data(data, {"name": name, "api": "Playtomic"})
        self.save_data(results)


class GotCourtsScraper(APIScraper):
    def process_data(self, data, static_attributes={}):
        self.logger.info(f"Processing data for GotCourts")
        extraction_time = datetime.datetime.now().isoformat()
        results = []
        for day in data:
            for court in day["data"]["courts"]:
                courtId = court["courtId"]
                sport = court["sport"]
                surfaceType = court["surfaceType"]
                courtType = court["courtType"]
                name = court["name"]
                for slot in court["slots"]:
                    startTime = datetime.datetime.fromtimestamp(
                        int(slot["startDateTime"])
                    )
                    endTime = datetime.datetime.fromtimestamp(int(slot["endDateTime"]))
                    slotType = slot["slotType"]
                    slotContext = slot["slotContext"]
                    results.append(
                        {
                            "courtId": courtId,
                            "sport": sport,
                            "date": startTime.strftime("%Y-%m-%d"),
                            "slotType": slotType,
                            "slotContext": slotContext,
                            "court": name,
                            "extraction_time": extraction_time,
                            "slot": f"{startTime.strftime('%Y-%m-%d')} - {endTime.strftime('%Y-%m-%d')}",
                            "surfaceType": surfaceType,
                            "courtType": courtType,
                            "length": (endTime - startTime).seconds / 3600,
                            **static_attributes,
                        }
                    )
        return results

    def scrape_api_config(self):
        club_id = self.api_config["club_id"]
        name = self.api_config["name"]
        data = []
        # Calculate the current timestamp
        today = datetime.datetime.now()
        for day_offset in range(DAYS_TO_EXTRACT):
            date = today + datetime.timedelta(days=day_offset)
            date = date.replace(hour=0, minute=0, second=0, microsecond=0)
            timestamp = int(date.timestamp())
            response = self._fetch_data(
                {"day": f"{timestamp}"}, url=f"{self.base_url}{club_id}"
            )
            if response:
                data.append(response.json())
        results = self.process_data(data, {"name": name, "api": "GotCourts"})
        self.save_data(results)


class MatchiScraper(APIScraper):
    def scrape_api_config(self):
        facility_id = self.api_config["facility_id"]
        sport = self.api_config["sport"]
        name = self.api_config["name"]
        results = []
        extraction_time = datetime.datetime.now().isoformat()
        for i in range(DAYS_TO_EXTRACT):
            date = datetime.datetime.now() + datetime.timedelta(days=i)
            date_to_check = date.strftime("%Y-%m-%d")
            response = self._fetch_data(
                {
                    "facilityId": facility_id,
                    "date": date_to_check,
                    "sport": sport,
                }
            )
            if response:
                results += self.process_data(
                    response.content,
                    {
                        "api": "Matchi",
                        "name": name,
                        "date": date_to_check,
                        "extraction_time": extraction_time,
                    },
                )
        self.save_data(results)

    def process_data(self, data, static_attributes={}):
        self.logger.info(f"Processing data for Matchi")
        results = []
        soup = BeautifulSoup(data, "html.parser")
        # find table-bordered daily class
        table = soup.find("table", {"class": "table-bordered daily"})
        # find all table cells
        rows = table.find_all("tr")
        # Skip first row
        rows = rows[2:]
        # Loop through rows and print cells
        for row in rows:
            # If no table, skip
            if not row.find("table"):
                continue
            availability = row.find("table").find_all("td")
            for time in availability:
                raw = time.get("title")
                if raw == "Time passed":
                    continue
                status, court, slot = time.get("title").split("<br>")
                results.append(
                    {
                        "status": status,
                        "court": court.strip(),
                        "slot": slot.strip(),
                        **static_attributes,
                    }
                )
        return results


def test():
    GotCourtsScraper(
        api_config=API_CONFIGS["GotCourts"][0],
        base_url="https://app-api.gotcourts.com/v1/prod/slots/club/",
        headers={},
        output_file="data/GotCourts.csv",
    ).scrape_api_config()
    PlaytomicScraper(
        api_config=API_CONFIGS["Playtomic"][0],
        base_url="https://playtomic.io/api/v1/availability",
        headers={},
        output_file="data/Playtomic.csv",
    ).scrape_api_config()
    MatchiScraper(
        api_config=API_CONFIGS["Matchi"][0],
        base_url="https://www.matchi.se/book/schedule",
        headers={},
        output_file="data/Matchi.csv",
    ).scrape_api_config()


def main():
    logger = setup_logging("logs")
    logger.info("Starting web scraper program")
    playtomic_config_scrapers = [
        PlaytomicScraper(
            api_config=config,
            base_url="https://playtomic.io/api/v1/availability",
            headers={},
            output_file=f"data/Playtomic_{config['name']}.csv",
            request_interval_seconds=2,
        )
        for config in API_CONFIGS["Playtomic"]
    ]
    gotcourts_config_scrapers = [
        GotCourtsScraper(
            api_config=config,
            base_url="https://app-api.gotcourts.com/v1/prod/slots/club/",
            headers={},
            output_file=f"data/GotCourts_{config['name']}.csv",
            request_interval_seconds=2,
        )
        for config in API_CONFIGS["GotCourts"]
    ]
    matchi_config_scraper = [
        MatchiScraper(
            api_config=config,
            base_url="https://www.matchi.se/book/schedule",
            headers={},
            output_file=f"data/Matchi_{config['name']}.csv",
            request_interval_seconds=2,
        )
        for config in API_CONFIGS["Matchi"]
    ]
    scheduler = Scheduler(
        num_workers=NUM_WORKERS,
        interval=EXTRACTION_INTERVAL,
    )
    scheduler.start()
    scheduler.schedule_tasks(
        matchi_config_scraper + gotcourts_config_scrapers + playtomic_config_scrapers,
    )


if __name__ == "__main__":
    main()
