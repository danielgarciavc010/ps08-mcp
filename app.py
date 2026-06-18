"""
MCP Server – kyoe-consultas
Transporte: SSE (HTTP)

Arranque:
    python server.py
    → escucha en http://0.0.0.0:8000/sse

Herramientas:
    - consultar_comisarias
    - consultar_cita_dnie
    - alta_cita_dnie
    - anular_cita_dnie
    - modificar_cita_dnie
    - enviar_sms
    - crear_codigo_peticion
    - buscar_codigo_localidad   (NUEVA: nombre -> codigos INE)
"""

import os
import csv
import httpx
import string
import secrets
import unicodedata
from fastmcp import FastMCP

# ──────────────────────────────────────────────
# Configuración
# ──────────────────────────────────────────────
BASE_URL = "http://rag.kyoe.es"
TIMEOUT  = 15.0
HOST     = "0.0.0.0"
PORT     = 8000

# CSV de codigos INE, ubicado junto a este server.py (ruta relativa)
CSV_CODIGOS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "codigos_ine.csv")

mcp = FastMCP(
    name="kyoe-consultas",
    instructions=(
        "Herramientas para consultar comisarías disponibles, consultar, dar de "
        "alta, anular y modificar citas de DNI/NIE/pasaporte a través de los "
        "servicios de rag.kyoe.es, para enviar SMS y para traducir nombres de "
        "provincia/localidad a sus códigos INE."
    ),
)

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def _error(code: str, message: str) -> dict:
    return {"ok": False, "error": {"code": code, "message": message}}


async def _request(method: str, endpoint: str, base_url: str = BASE_URL, **kwargs):
    """Llama a base_url+endpoint y devuelve (raw, error).
    Si hay error, raw es None y error es el dict _error(...)."""
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            response = await client.request(method, f"{base_url}{endpoint}", **kwargs)
            response.raise_for_status()
    except httpx.TimeoutException:
        return None, _error("TIMEOUT", "El servicio no respondió a tiempo.")
    except httpx.HTTPStatusError as e:
        return None, _error("HTTP_ERROR", f"El servicio devolvió HTTP {e.response.status_code}.")
    except httpx.RequestError as e:
        return None, _error("CONNECTION_ERROR", f"No se pudo conectar al servicio: {e}.")

    try:
        return response.json(), None
    except Exception:
        return response.text, None


def _normalizar(texto: str) -> str:
    """Pasa a minusculas, quita acentos y espacios sobrantes para comparar
    nombres de forma flexible (el ciudadano no escribe con tildes perfectas)."""
    if texto is None:
        return ""
    t = texto.strip().lower()
    t = "".join(
        c for c in unicodedata.normalize("NFD", t)
        if unicodedata.category(c) != "Mn"
    )
    return " ".join(t.split())


# Carga del CSV de codigos INE.
# Nota: el MCP se levanta en cada peticion, por lo que esto se ejecuta
# al importar el modulo. El fichero viaja en el repo (ruta relativa).
def _cargar_codigos():
    filas = []
    try:
        with open(CSV_CODIGOS, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                filas.append({
                    "id_provincia": r["id_provincia"],
                    "provincia":    r["provincia"],
                    "id_localidad": r["id_localidad"],
                    "localidad":    r["localidad"],
                    # campos normalizados para busqueda
                    "_provincia_norm": _normalizar(r["provincia"]),
                    "_localidad_norm": _normalizar(r["localidad"]),
                })
    except FileNotFoundError:
        pass
    return filas


_CODIGOS = _cargar_codigos()


# ──────────────────────────────────────────────
# Tools
# ──────────────────────────────────────────────
@mcp.tool()
def buscar_codigo_localidad(localidad: str, provincia: str = "") -> dict:
    """
    Traduce el nombre de una localidad (y opcionalmente su provincia) a los
    códigos INE id_provincia e id_localidad, necesarios para consultar_comisarias.

    La búsqueda ignora mayúsculas y acentos. Si se indica la provincia, se usa
    para desambiguar localidades con el mismo nombre en distintas provincias.

    Args:
        localidad: Nombre de la localidad tal como lo dice el ciudadano (ej. 'Merida').
        provincia: Nombre de la provincia (opcional, ayuda a desambiguar).

    Returns:
        Encontrado unico:
            {"ok": true, "data": {"id_provincia": "06", "provincia": "Badajoz",
                                   "id_localidad": "06083", "localidad": "Mérida"}}
        No encontrado:
            {"ok": false, "error": {"code": "NOT_FOUND",
                                    "message": "No se ha encontrado la localidad '...'."}}
        Varias coincidencias:
            {"ok": false, "error": {"code": "MULTIPLE",
                                    "message": "Hay varias localidades que coinciden.",
                                    "candidatos": [ {...}, {...} ]}}
    """
    if not _CODIGOS:
        return _error("DATA_ERROR", "No se ha podido cargar la tabla de códigos INE.")

    loc_norm = _normalizar(localidad)
    if not loc_norm:
        return _error("INVALID_PARAM", "Debe indicar el nombre de la localidad.")

    prov_norm = _normalizar(provincia)

    # 1) coincidencia exacta de localidad (y provincia si se dio)
    exactos = [
        r for r in _CODIGOS
        if r["_localidad_norm"] == loc_norm
        and (not prov_norm or r["_provincia_norm"] == prov_norm)
    ]

    # 2) si no hay exactos, buscar localidad que contenga el texto
    if not exactos:
        exactos = [
            r for r in _CODIGOS
            if loc_norm in r["_localidad_norm"]
            and (not prov_norm or r["_provincia_norm"] == prov_norm)
        ]

    def _limpio(r):
        return {
            "id_provincia": r["id_provincia"],
            "provincia":    r["provincia"],
            "id_localidad": r["id_localidad"],
            "localidad":    r["localidad"],
        }

    if len(exactos) == 1:
        return {"ok": True, "data": _limpio(exactos[0])}

    if len(exactos) == 0:
        return _error("NOT_FOUND", f"No se ha encontrado la localidad '{localidad}'.")

    # varias coincidencias -> devolver candidatos para que el agente pregunte
    candidatos = [_limpio(r) for r in exactos[:15]]
    return {
        "ok": False,
        "error": {
            "code": "MULTIPLE",
            "message": "Hay varias localidades que coinciden. Pide al ciudadano que concrete.",
            "candidatos": candidatos,
        },
    }


@mcp.tool()
async def consultar_comisarias(
    codigo_peticion: str,
    id_provincia: int,
    id_localidad: int,
) -> dict:
    """
    Devuelve las comisarías disponibles para tramitar DNI/NIE/pasaporte
    en una provincia y localidad concretas.

    Args:
        codigo_peticion: Identificador de la petición (ej. 'ABC123').
        id_provincia:    Código numérico de provincia  (ej. 28 → Madrid).
        id_localidad:    Código numérico de localidad  (ej. 28079 → Madrid capital).

    Returns:
        {
            "ok": true,
            "data": {
                "provincia": 28,
                "localidad": 28079,
                "comisarias": [ { ...campos del servicio... } ]
            }
        }
    """
    params = {
        "codigoPeticion": codigo_peticion,
        "idProvincia":    id_provincia,
        "idLocalidad":    id_localidad,
    }

    raw, err = await _request("GET", "/ConsultarComisarias", params=params)
    if err:
        return err

    return {
        "ok": True,
        "data": {
            "provincia":  id_provincia,
            "localidad":  id_localidad,
            "comisarias": raw,
        },
    }


@mcp.tool()
async def consultar_cita_dnie(
    codigo_peticion: str,
    tipo_titular: str,
    id_titular: str,
) -> dict:
    """
    Consulta la cita de DNI/NIE/pasaporte asociada a un titular.

    Args:
        codigo_peticion: Identificador de la petición (ej. 'ABC123456').
        tipo_titular:    Tipo de documento: 'D' (DNI), 'N' (NIE), 'P' (pasaporte).
        id_titular:      Número de documento (ej. '12345678Z').

    Returns:
        {
            "ok": true,
            "data": {
                "tipo_titular": "D",
                "id_titular": "12345678Z",
                "cita": { ...campos del servicio... }
            }
        }
    """
    tipo_titular = tipo_titular.upper()
    if tipo_titular not in {"D", "N", "P"}:
        return _error("INVALID_PARAM", "tipo_titular debe ser 'D', 'N' o 'P'.")

    params = {
        "codigoPeticion": codigo_peticion,
        "tipotitular":    tipo_titular,
        "Idtitular":      id_titular,
    }

    raw, err = await _request("GET", "/ConsultarCitaDnie", params=params)
    if err:
        return err

    return {
        "ok": True,
        "data": {
            "tipo_titular": tipo_titular,
            "id_titular":   id_titular,
            "cita":         raw,
        },
    }


def _build_alta_body(codigo_peticion, tipo_titular, id_titular,
                      id_accion="", id_tramite="", id_comisaria="", id_movil=""):
    body = {
        "codigoPeticion": codigo_peticion,
        "tipotitular":    tipo_titular,
        "Idtitular":      id_titular,
    }
    opcionales = {
        "idAccion":    id_accion,
        "idTramite":   id_tramite,
        "idComisaria": id_comisaria,
        "idMovil":     id_movil,
    }
    body.update({k: v for k, v in opcionales.items() if v})
    return body


@mcp.tool()
async def alta_cita_dnie(
    codigo_peticion: str,
    tipo_titular: str,
    id_titular: str,
    id_accion: str = "",
    id_tramite: str = "",
    id_comisaria: str = "",
    id_movil: str = "",
) -> dict:
    """
    Da de alta una cita de DNIe/pasaporte para un titular.

    Args:
        codigo_peticion: Identificador de la petición (ej. 'ABC123456').
        tipo_titular:    Tipo de documento: 'X' (NIE) o 'D' (DNI).
        id_titular:      Número de documento (ej. '12345678Z').
        id_accion:       Código de acción (opcional).
        id_tramite:      Código de trámite (opcional).
        id_comisaria:    Código de la comisaría elegida (opcional).
        id_movil:        Código de unidad móvil (opcional).

    Returns:
        {
            "ok": true,
            "data": { ...campos del servicio (cita asignada)... }
        }
    """
    tipo_titular = tipo_titular.upper()
    if tipo_titular not in {"X", "D"}:
        return _error("INVALID_PARAM", "tipo_titular debe ser 'X' o 'D'.")

    body = _build_alta_body(codigo_peticion, tipo_titular, id_titular,
                             id_accion, id_tramite, id_comisaria, id_movil)

    raw, err = await _request("POST", "/AltaCitaDnie", json=body)
    if err:
        return err

    return {"ok": True, "data": raw}


@mcp.tool()
async def anular_cita_dnie(
    codigo_peticion: str,
    tipo_titular: str,
    id_titular: str,
) -> dict:
    """
    Anula la cita de DNIe/pasaporte asociada a un titular.

    Args:
        codigo_peticion: Identificador de la petición (ej. 'ABC123456').
        tipo_titular:    Tipo de documento: 'X' (NIE) o 'D' (DNI).
        id_titular:      Número de documento (ej. '12345678Z').

    Returns:
        {
            "ok": true,
            "data": { ...campos del servicio (cita anulada)... }
        }
    """
    tipo_titular = tipo_titular.upper()
    if tipo_titular not in {"X", "D"}:
        return _error("INVALID_PARAM", "tipo_titular debe ser 'X' o 'D'.")

    body = {
        "codigoPeticion": codigo_peticion,
        "tipotitular":    tipo_titular,
        "Idtitular":      id_titular,
    }

    raw, err = await _request("PUT", "/AnularCitaDnie", json=body)
    if err:
        return err

    return {"ok": True, "data": raw}


@mcp.tool()
async def modificar_cita_dnie(
    codigo_peticion: str,
    tipo_titular: str,
    id_titular: str,
    id_accion: str = "",
    id_tramite: str = "",
    id_comisaria: str = "",
    id_movil: str = "",
) -> dict:
    """
    Modifica la cita de un titular: anula la cita existente y da de alta una
    nueva con los datos indicados.

    Args:
        codigo_peticion: Identificador de la petición (ej. 'ABC123456').
        tipo_titular:    Tipo de documento: 'X' (NIE) o 'D' (DNI).
        id_titular:      Número de documento (ej. '12345678Z').
        id_accion:       Código de acción para la nueva cita (opcional).
        id_tramite:      Código de trámite para la nueva cita (opcional).
        id_comisaria:    Código de la comisaría para la nueva cita (opcional).
        id_movil:        Código de unidad móvil para la nueva cita (opcional).

    Returns:
        {
            "ok": true,
            "data": { ...respuesta AltaCitaDnie de la nueva cita... }
        }
    """
    tipo_titular = tipo_titular.upper()
    if tipo_titular not in {"X", "D"}:
        return _error("INVALID_PARAM", "tipo_titular debe ser 'X' o 'D'.")

    body_anular = {
        "codigoPeticion": codigo_peticion,
        "tipotitular":    tipo_titular,
        "Idtitular":      id_titular,
    }
    raw_anular, err = await _request("PUT", "/AnularCitaDnie", json=body_anular)
    if err:
        return err

    body_alta = _build_alta_body(codigo_peticion, tipo_titular, id_titular,
                                  id_accion, id_tramite, id_comisaria, id_movil)
    raw_alta, err = await _request("POST", "/AltaCitaDnie", json=body_alta)
    if err:
        return err

    return {"ok": True, "data": raw_alta}


@mcp.tool()
async def enviar_sms(destinatario: str, mensaje: str) -> dict:
    """
    Envía un SMS a un número de teléfono.

    Args:
        destinatario: Número de teléfono en formato E.164 (ej. '+34612345678').
        mensaje:      Texto del SMS a enviar.

    Returns:
        {
            "ok": true,
            "data": {
                "status": "success",
                "sid": "SMXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
            }
        }
    """
    body = {"to": destinatario, "message": mensaje}

    raw, err = await _request("POST", "/sms/send/", json=body)
    if err:
        return err

    return {"ok": True, "data": raw}


@mcp.tool()
def crear_codigo_peticion() -> dict:
    """
    Genera un código alfanumérico aleatorio de 20 caracteres (mayúsculas y
    dígitos), útil como codigoPeticion para las demás tools.

    Returns:
        {
            "ok": true,
            "data": { "codigo": "A1B2C3D4E5F6G7H8I9J0" }
        }
    """
    alfabeto = string.ascii_letters + string.digits
    codigo = "".join(secrets.choice(alfabeto) for _ in range(20))
    return {"ok": True, "data": {"codigo": codigo}}


# ──────────────────────────────────────────────
# Arranque
# ──────────────────────────────────────────────
if __name__ == "__main__":
    mcp.run(transport="sse", host=HOST, port=PORT)