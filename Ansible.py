import json
import time
import logging
import requests
from dataclasses import dataclass, field
from typing import Optional
from pydantic import BaseModel, Field


# ============================================================
# LOGGING CONFIG
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)


# ============================================================
# RunConfig → external Pydantic model
# ============================================================
class RunConfig(BaseModel):
    with_wait: bool = True
    timeout: int = Field(default=900, description="Timeout in seconds")
    polling_interval: int = Field(default=5, description="Seconds between status checks")


# ============================================================
# Ansible class
# ============================================================
class Ansible:

    # --------------------------------------------------------
    # Internal dataclass for job launch parameters
    # --------------------------------------------------------
    @dataclass
    class DataLaunchJobTemplate:
        name: str
        extra_vars: dict
        inventory: Optional[str] = None
        run: RunConfig = field(default_factory=RunConfig)   # <-- FIX HERE

    # --------------------------------------------------------
    # Init
    # --------------------------------------------------------
    def __init__(self, ansible_url: str, ansible_token: str) -> None:
        self.url = ansible_url.rstrip("/")
        self.token = ansible_token

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        })

    # --------------------------------------------------------
    # HTTP helpers
    # --------------------------------------------------------
    def _session_post(self, endpoint: str, payload: dict) -> int:
        url = f"{self.url}/api/v2/{endpoint}"
        logger.debug(f"POST {url} payload={payload}")

        resp = self.session.post(url, json=payload)
        if resp.status_code not in {200, 201}:
            logger.error(f"POST {url} returned {resp.status_code}: {resp.text}")
            raise RuntimeError(f"POST error: {resp.text}")

        return resp.json().get("id")

    def _session_get(self, endpoint: str) -> dict:
        url = f"{self.url}/api/v2/{endpoint}"
        logger.debug(f"GET {url}")

        resp = self.session.get(url)
        if resp.status_code != 200:
            logger.error(f"GET {url} returned {resp.status_code}: {resp.text}")
            raise RuntimeError(f"GET error: {resp.text}")

        return resp.json()

    # --------------------------------------------------------
    # Inventory / template helpers
    # --------------------------------------------------------
    def get_job_template_by_name(self, name: str) -> Optional[int]:
        result = self._session_get(f"job_templates/?name={name}")
        results = result.get("results", [])
        return results[0]["id"] if results else None

    def get_inventory_by_name(self, name: str) -> Optional[int]:
        result = self._session_get(f"inventories/?name={name}")
        results = result.get("results", [])
        return results[0]["id"] if results else None

    def get_jobs_status(self, job_id: int) -> str:
        result = self._session_get(f"jobs/{job_id}/")
        return result.get("status", "unknown")

    # --------------------------------------------------------
    # Wait for job completion
    # --------------------------------------------------------
    def wait_status(self, job_id: int, *, timeout: int, interval: int) -> str:
        logger.info(f"Waiting for job {job_id} (timeout={timeout}s, interval={interval}s)")
        start = time.time()

        while True:
            status = self.get_jobs_status(job_id)
            logger.debug(f"Job {job_id} → status: {status}")

            if status in ("successful", "failed", "error", "canceled"):
                logger.info(f"Job {job_id} finished with status: {status}")
                return status

            if (time.time() - start) >= timeout:
                logger.error(f"Timeout after {timeout}s for job {job_id}")
                raise TimeoutError(f"Timeout after {timeout}s for job {job_id}")

            time.sleep(interval)

    # --------------------------------------------------------
    # Launch job template
    # --------------------------------------------------------
    def launch_ansible_job_template(
        self,
        data: DataLaunchJobTemplate,
    ) -> dict:

        logger.info(f"Launching job template '{data.name}'")

        payload = {"extra_vars": json.dumps(data.extra_vars)}

        # Inventory resolution
        if data.inventory:
            inventory_id = self.get_inventory_by_name(data.inventory)
            if inventory_id is None:
                raise ValueError(f"Inventory '{data.inventory}' not found")

            logger.info(f"Inventory '{data.inventory}' resolved to ID = {inventory_id}")
            payload["inventory"] = inventory_id

        # Template resolution
        job_template_id = self.get_job_template_by_name(data.name)
        if job_template_id is None:
            logger.error(f"Job template '{data.name}' not found")
            raise ValueError(f"Job template '{data.name}' not found")

        endpoint = f"job_templates/{job_template_id}/launch/"
        job_id = self._session_post(endpoint, payload)

        logger.info(f"Job launched → job_id = {job_id}")

        # Optional waiting
        if data.run.with_wait:
            status = self.wait_status(
                job_id,
                timeout=data.run.timeout,
                interval=data.run.polling_interval
            )
            return {"job_id": job_id, "status": status}

        return {"job_id": job_id}
