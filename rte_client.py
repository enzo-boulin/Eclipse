"""
Client Python minimal pour RTE Speed Data API (OAuth2 client_credentials)
Basé sur "RTE Speed Data API User Guide Version 2.0" (token endpoint & usage).
https://data.rte-france.org/documents/20182/22648/EN_GuideOauth2_v5.1.pdf/54d3d183-f20f-4290-9417-bcae122b9e46
"""

import base64
import json
import time
from typing import Any, Dict

import pandas as pd
import requests

import config
from config import PrevisionType


class RTEAuthError(Exception):
    pass


class RTEClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_endpoint: str = config.TOKEN_ENDPOINT,
        api_base: str = config.RTE_BASE_URL,
        token_cache_file: str | None = None,
        timeout: int = 10,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = api_base + token_endpoint.rstrip("/") + "/"
        self.api_base = api_base.rstrip("/") + "/"
        self.timeout = timeout

        # token state
        self._access_token: str | None = None
        self._token_expiry: float = 0.0
        self._token_cache_file = token_cache_file

        if token_cache_file:
            self._load_token_from_file()

    # ---------- token helpers ----------
    def _basic_auth_header(self) -> str:
        pair = f"{self.client_id}:{self.client_secret}"
        b64 = base64.b64encode(pair.encode("utf-8")).decode("utf-8")
        # NOTE: case sensitive "Basic"
        return f"Basic {b64}"

    def _save_token_to_file(self) -> None:
        if not self._token_cache_file:
            return
        payload = {
            "access_token": self._access_token,
            "expiry": self._token_expiry,
        }
        with open(self._token_cache_file, "w") as f:
            json.dump(payload, f)

    def _load_token_from_file(self) -> None:
        try:
            with open(self._token_cache_file, "r") as f:
                payload = json.load(f)
            self._access_token = payload.get("access_token")
            self._token_expiry = float(payload.get("expiry", 0))
        except Exception:
            # ignore, we'll fetch a new token
            self._access_token = None
            self._token_expiry = 0.0

    def _is_token_valid(self) -> bool:
        return self._access_token is not None and time.time() + 10 < self._token_expiry
        # +10s margin to avoid edge cases

    def get_access_token(self, force_refresh: bool = False) -> str:
        """
        Returns a valid access token. Fetches a new one if necessary.
        """
        if not force_refresh and self._is_token_valid():
            return self._access_token

        # Request a token using client_credentials flow
        headers = {
            "Authorization": self._basic_auth_header(),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        # According to the doc, grant_type and scope are not given. The Authorization header is sufficient.
        data = {}
        try:
            resp = requests.post(
                self.token_url, headers=headers, data=data, timeout=self.timeout
            )
        except requests.RequestException as e:
            raise RTEAuthError(f"Erreur réseau lors de la requête de token: {e}")

        if resp.status_code != 200:
            # Try to decode JSON error if present
            try:
                err = resp.json()
            except Exception:
                err = resp.text
            raise RTEAuthError(f"Token endpoint error {resp.status_code}: {err}")

        try:
            body = resp.json()
        except Exception as e:
            raise RTEAuthError(f"Impossible de parser la réponse token JSON: {e}")

        access_token = body.get("access_token")
        token_type = body.get("token_type")
        expires_in = body.get("expires_in")

        if not access_token or not token_type:
            raise RTEAuthError(f"Réponse token invalide: {body}")

        # compute expiry
        expires = time.time() + int(expires_in) if expires_in else time.time() + 3600

        self._access_token = access_token
        self._token_expiry = expires
        self._save_token_to_file()
        return self._access_token

    # ---------- API request ----------
    def request(
        self,
        endpoint: str,
        *,
        method: str = "POST",
        headers: Dict[str, str] | None = None,
        params: Dict[str, str] | None = None,
        data: Any = None,
        force_token_refresh_on_401: bool = True,
    ) -> requests.Response:
        """
        Call a endpoint (relative to api_base).
        """

        url = self.api_base + endpoint.lstrip("/")

        # get token
        token = self.get_access_token()
        # default headers for OAuth protected resource
        req_headers = {"Authorization": f"Bearer {token}"}
        if headers:
            req_headers.update(headers)

        method = method.upper()
        try:
            resp = requests.request(
                method,
                url,
                headers=req_headers,
                params=params,
                data=data,
                timeout=self.timeout,
            )
        except requests.RequestException as e:
            raise RuntimeError(f"Erreur lors de l'appel API: {e}")

        # handle common token issues
        if resp.status_code in (401, 403) and force_token_refresh_on_401:
            # maybe token expired or invalid -> try refresh once
            # avoid infinite loop by disabling refresh on next call
            try:
                self.get_access_token(force_refresh=True)
            except RTEAuthError:
                # token refresh failed; propagate original error
                pass
            # retry once
            token = self._access_token
            if token:
                req_headers["Authorization"] = f"Bearer {token}"
                resp = requests.request(
                    method,
                    url,
                    headers=req_headers,
                    params=params,
                    data=data,
                    timeout=self.timeout,
                )

        return resp

    # ---------- API methods ----------
    def get_france_power_exchanges(self) -> pd.DataFrame:
        resp = self.request(config.WHOLESALE_MARKET_ENDPOINT, method="GET")
        resp.raise_for_status()
        data = resp.json().get("france_power_exchanges", [])

        frames = []
        for entry in data:
            values = entry.get("values", [])
            if not values:
                continue
            df = pd.DataFrame(values)
            frames.append(df)

        if not frames:
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True)
        df["start_date"] = pd.to_datetime(df["start_date"])
        df["end_date"] = pd.to_datetime(df["end_date"])
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        return df

    def get_short_term_consumption(
        self,
        types: PrevisionType | list[PrevisionType] | None = None,
        start_date: pd.Timestamp | None = None,
        end_date: pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        """
        Renvoie la consommation réalisée ou prévue en France.
        """

        params = {}
        if types:
            if isinstance(types, list):
                params["type"] = ",".join(types)
            else:
                params["type"] = types
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        resp = self.request(config.CONSUMPTION_ENDPOINT, method="GET", params=params)
        resp.raise_for_status()
        data = resp.json().get("short_term", [])

        if not data:
            return pd.DataFrame()

        frames = []
        for entry in data:
            values = entry.get("values", [])
            if not values:
                continue
            df = pd.DataFrame(values)
            df["type"] = entry.get("type")
            frames.append(df)

        if not frames:
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True)

        for col in [
            "start_date",
            "end_date",
        ]:
            if col in df:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        if "value" in df:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")

        df = df.sort_values(["type", "start_date"]).reset_index(drop=True)
        return df


# -------------------- Exemple d'utilisation --------------------

if __name__ == "__main__":
    # Remplacez par vos identifiants

    client = RTEClient(
        client_id=config.RTE_CONSUMPTION_CLIENT_ID,
        client_secret=config.RTE_CONSUMPTION_CLIENT_SECRET,
        token_cache_file="rte_consumption_token_cache.json",
    )

    # Exemple 1 : récupération automatique (les prévisions les plus à jour)
    df = client.get_short_term_consumption()
    print(df.head())

    # Exemple 2 : récupération d’une période précise
    df = client.get_short_term_consumption(
        types=["REALISED", "ID"],
        start_date="2025-10-01T00:00:00+02:00",
        end_date="2025-10-02T00:00:00+02:00",
    )
    print(df.head())

    client = RTEClient(
        client_id=config.RTE_WHOLESALE_MARKET_CLIENT_ID,
        client_secret=config.RTE_WHOLESALE_MARKET_CLIENT_SECRET,
    )

    df = client.get_france_power_exchanges()
    print(df.head())

    # # 🔹 URL correcte d'après le document
    # endpoint = "open_api/wholesale_market/v3/france_power_exchanges"

    # # 🔸 Pour tester en environnement sandbox, tu peux utiliser :
    # # endpoint = "open_api/wholesale_market/v3/sandbox/france_power_exchanges"

    # # 🔹 Appel REST GET
    # response = client.request(endpoint, method="GET")

    # print("Status code:", response.status_code)
    # print("Headers:", response.headers)

    # # --- Extraction et normalisation des valeurs ---
    # # Format JSON : {"france_power_exchanges": [ { start_date, end_date, updated_date, values:[...] } ]}
    # data = response.json()
    # entries = data.get("france_power_exchanges", [])
    # if not entries:
    #     raise ValueError("Pas de données dans la réponse RTE.")

    # # Chaque bloc 'values' contient une liste de pas de temps (15 min ou 1h)
    # frames = []
    # for entry in entries:
    #     values = entry.get("values", [])
    #     if not values:
    #         continue
    #     df = pd.DataFrame(values)
    #     df["start_date_day"] = entry.get("start_date")
    #     df["end_date_day"] = entry.get("end_date")
    #     df["updated_date"] = entry.get("updated_date")
    #     frames.append(df)

    # # --- Fusion finale ---
    # df = pd.concat(frames, ignore_index=True)

    # # --- Nettoyage / typage ---
    # df["start_date"] = pd.to_datetime(df["start_date"])
    # df["end_date"] = pd.to_datetime(df["end_date"])
    # df["updated_date"] = pd.to_datetime(df["updated_date"])

    # df["value"] = pd.to_numeric(df["value"], errors="coerce")
    # df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # # --- Affichage ---
    # print(df.head())
    # print(df.describe())

    # # --- Exemple d’utilisation ---
    # # prix moyens par heure
    # df["hour"] = df["start_date"].dt.hour
    # avg_price = df.groupby("hour")["price"].mean()
    # print("\nPrix moyen par heure :\n", avg_price)

    # # Si tout va bien, tu obtiens un 200 + un JSON avec prix et volumes
    # try:
    #     print("Body (JSON):", response.json())
    # except Exception:
    #     print("Body (texte):", response.text[:1000])

    # # Exemple: récupérer un token
    # try:
    #     token = client.get_access_token()
    #     print("Token obtenu (début):", token[:40] + "..." if token else None)
    # except RTEAuthError as e:
    #     print("Erreur d'authent:", e)
    #     raise SystemExit(1)

    # # Exemple : appel SOAP (adapté à l'exemple du document)
    # sample_soap_envelope = """<?xml version="1.0" encoding="UTF-8"?>
    # <soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope">
    #   <soap:Body>
    #     <!-- Insérez ici votre requête SOAP spécifique (getDonneesPhysiquesAction par ex.) -->
    #   </soap:Body>
    # </soap:Envelope>"""

    # resp = client.soap_request(
    #     "privateapi/sandbox/getDonneesPhysiques/V1",
    #     soap_xml=sample_soap_envelope,
    #     action="getDonneesPhysiquesAction",
    # )
    # print("Status:", resp.status_code)
    # print("Headers:", resp.headers)
    # print("Body (tronc.):", resp.text[:1000])
