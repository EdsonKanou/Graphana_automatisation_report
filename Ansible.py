import json
import time
import logging
from typing import Optional

import requests
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
# Pydantic Models
# ============================================================
class RunConfig(BaseModel):
    with_wait: bool = True
    timeout: int = Field(default=900, description="Timeout en secondes")
    polling_interval: int = Field(default=5, description="Temps entre 2 checks du job")


class DataLaunchJobTemplate(BaseModel):
    name: str
    extra_vars: dict
    inventory: Optional[str] = None
    run: RunConfig = RunConfig()


# ============================================================
# Main Ansible Client Class
# ============================================================
class Ansible:

    def __init__(self, ansible_url: str, ansible_token: str) -> None:
        self.url = ansible_url.rstrip("/")
        self.token = ansible_token
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        })

    # --------------------------------------------------------
    # Internal helpers
    # --------------------------------------------------------
    def _session_post(self, endpoint: str, payload: dict) -> int:
        url = f"{self.url}/api/v2/{endpoint}"
        logger.debug(f"POST {url} payload={payload}")

        resp = self.session.post(url, json=payload)
        if resp.status_code not in {200, 201}:
            logger.error(f"POST {url} → {resp.status_code} : {resp.text}")
            raise RuntimeError(f"POST error: {resp.text}")

        data = resp.json()
        job_id = data.get("id")
        logger.info(f"POST réussi → job_id={job_id}")
        return job_id

    def _session_get(self, endpoint: str) -> dict:
        url = f"{self.url}/api/v2/{endpoint}"
        logger.debug(f"GET {url}")

        resp = self.session.get(url)
        if resp.status_code != 200:
            logger.error(f"GET {url} → {resp.status_code} : {resp.text}")
            raise RuntimeError(f"GET error: {resp.text}")

        return resp.json()

    # --------------------------------------------------------
    # API lookup helpers
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
        logger.info(f"Attente du job {job_id} (timeout={timeout}s, interval={interval}s)")
        start = time.time()

        while True:
            status = self.get_jobs_status(job_id)
            logger.debug(f"Job {job_id} → statut : {status}")

            if status in ("successful", "failed", "error", "canceled"):
                logger.info(f"Job {job_id} terminé avec statut : {status}")
                return status

            if (time.time() - start) >= timeout:
                logger.error(f"Timeout après {timeout}s pour le job {job_id}")
                raise TimeoutError(f"Timeout après {timeout}s pour le job {job_id}")

            time.sleep(interval)

    # --------------------------------------------------------
    # Launch job template
    # --------------------------------------------------------
    def launch_ansible_job_template(
        self,
        data: DataLaunchJobTemplate,
    ) -> dict:

        logger.info(f"Lancement du job template '{data.name}'")

        # -------------------------------
        # Préparation du payload
        # -------------------------------
        payload = {
            "extra_vars": json.dumps(data.extra_vars)
        }

        # Inventory optionnel
        if data.inventory:
            inventory_id = self.get_inventory_by_name(data.inventory)
            logger.info(f"Inventory '{data.inventory}' → ID = {inventory_id}")

            if inventory_id is None:
                raise ValueError(f"Inventory '{data.inventory}' introuvable")

            payload["inventory"] = inventory_id

        # -------------------------------
        # Résolution du job template
        # -------------------------------
        job_template_id = self.get_job_template_by_name(data.name)

        if job_template_id is None:
            logger.error(f"Job template '{data.name}' introuvable")
            raise ValueError(f"Job template '{data.name}' introuvable")

        logger.info(f"Job template '{data.name}' → ID = {job_template_id}")

        # -------------------------------
        # Lancement du job
        # -------------------------------
        endpoint = f"job_templates/{job_template_id}/launch/"
        job_id = self._session_post(endpoint, payload)

        logger.info(f"Job lancé → job_id = {job_id}")

        # -------------------------------
        # Attente optionnelle
        # -------------------------------
        if data.run.with_wait:
            logger.info("with_wait=True → attente du statut final")
            status = self.wait_status(
                job_id,
                timeout=data.run.timeout,
                interval=data.run.polling_interval
            )
            return {"job_id": job_id, "status": status}

        logger.info("with_wait=False → retour immédiat")
        return {"job_id": job_id}
