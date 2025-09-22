import aiohttp
import async_timeout
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import os
from dotenv import load_dotenv

load_dotenv()
ESCAVADOR_API_KEY = os.getenv("ESCAVADOR_API_KEY")
ESCAVADOR_API_BASE = "https://api.escavador.com/api/v2/processos/numero_cnj"

if not ESCAVADOR_API_KEY:
    raise RuntimeError("ESCAVADOR_API_KEY não configurada!")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(Exception))
async def consultar_numero(session: aiohttp.ClientSession, numero: str) -> dict:
    headers = {
        "Authorization": f"Bearer {ESCAVADOR_API_KEY}",
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json"
    }
    url = f"{ESCAVADOR_API_BASE}/{numero}"
    async with async_timeout.timeout(15):
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                raise Exception(f"Erro HTTP {resp.status} para {numero}")
            return await resp.json()
