"""
MCP Server - kyoe-consultas
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
    - consultar_slots_comisaria
"""

import os
import csv
import httpx
import string
import secrets
import unicodedata
from datetime import datetime
from fastmcp import FastMCP

# ──────────────────────────────────────────────
# Configuración
# ──────────────────────────────────────────────
BASE_URL = "http://rag.kyoe.es"
TIMEOUT  = 15.0
HOST     = "0.0.0.0"
PORT     = 8000

# Maximo de comisarias que se devuelven al agente. El servicio puede devolver
# decenas (p. ej. 35 en Madrid); enviarlas todas satura al modelo. Recortamos
# a unas pocas, ya limpias y numeradas, para que el agente no se atasque.
MAX_COMISARIAS = 5

# Maximo de huecos (slots) que se muestran al agente, ya numerados y limpios.
MAX_SLOTS = 8

# CSV de codigos INE, ubicado junto a este server.py (ruta relativa)
CSV_CODIGOS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "codigos_ine.csv")

mcp = FastMCP(
    name="kyoe-consultas",
    instructions=(
        "Herramientas para consultar comisarías disponibles, consultar, dar de "
        "alta, anular y modificar citas de DNI/NIE/pasaporte a través de los "
        "servicios de rag.kyoe.es, para enviar SMS, consultar slots disponibles, "
        "y para traducir nombres de provincia/localidad a sus códigos INE."
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
    id_provincia: str = "",
    id_localidad: str = "",
) -> dict:
    """
    Devuelve las comisarías disponibles para tramitar DNI/NIE/pasaporte.

    Se debe indicar EXACTAMENTE UNO de los dos: id_localidad (si el ciudadano
    pidió una localidad concreta) o id_provincia (si pidió toda una provincia).
    Nunca los dos a la vez.

    Args:
        codigo_peticion: Identificador de la petición (ej. 'ABC123').
        id_provincia:    Código INE de provincia (ej. '28' → Madrid). Solo si NO se da localidad.
        id_localidad:    Código INE de localidad (ej. '28079' → Madrid capital). Solo si NO se da provincia.

    Returns (lista ya recortada y limpia, como maximo MAX_COMISARIAS):
        {
            "ok": true,
            "data": {
                "provincia": "",
                "localidad": "28079",
                "total": 35,
                "mostradas": 5,
                "listado_texto": "1. MADRID-CENTRO - Calle Luna 17\n2. ...",
                "comisarias": [
                    {
                        "numero": 1,
                        "id_comisaria": "8693",
                        "nombre": "MADRID-CENTRO",
                        "direccion": "Calle Luna 17. MADRID"
                    }
                ]
            }
        }
    "listado_texto" es la lista ya numerada y escrita; el agente la muestra tal cual
    al usuario sin transformar nada. "comisarias" sirve para mapear el numero
    elegido por el usuario a su id_comisaria.
    """
    id_provincia = str(id_provincia).strip()
    id_localidad = str(id_localidad).strip()

    if id_provincia and id_localidad:
        return _error("INVALID_PARAM",
                      "Indica solo id_provincia o solo id_localidad, no ambos.")
    if not id_provincia and not id_localidad:
        return _error("INVALID_PARAM",
                      "Debes indicar id_provincia o id_localidad.")

    params = {"codigoPeticion": codigo_peticion}
    if id_localidad:
        params["idLocalidad"] = id_localidad
    else:
        params["idProvincia"] = id_provincia

    raw, err = await _request("GET", "/ConsultarComisarias", params=params)
    if err:
        return err

    tab = raw.get("tabComi") or [] if isinstance(raw, dict) else []

    comisarias = [
        {
            "numero":       i,
            "id_comisaria": c.get("comisariaCita", ""),
            "nombre":       c.get("desComisariaCita", ""),
            "direccion":    c.get("direccionCita", ""),
        }
        for i, c in enumerate(tab[:MAX_COMISARIAS], start=1)
    ]

    # Lista ya escrita y numerada, lista para mostrar al usuario tal cual.
    # Usamos guion ASCII normal (no raya larga) para que el modelo cuantizado
    # pueda copiarla literal sin atascarse con caracteres raros.
    listado_texto = "\n".join(
        f"{c['numero']}. {c['nombre']} - {c['direccion']}" for c in comisarias
    )

    return {
        "ok": True,
        "data": {
            "provincia":     id_provincia,
            "localidad":     id_localidad,
            "total":         len(tab),
            "mostradas":     len(comisarias),
            "comisarias":    comisarias,
            "listado_texto": listado_texto,
        },
    }


@mcp.tool()
async def consultar_cita_dnie(
    codigo_peticion: str,
    tipo_documento: str,
    numero_documento: str,
) -> dict:
    """
    Consulta la cita de DNI/NIE/pasaporte asociada a un titular.

    Args:
        codigo_peticion:  Identificador de la petición (ej. 'ABC123456').
        tipo_documento:   Tipo de documento: 'D' (DNI), 'N' (NIE).
        numero_documento: Número de documento (ej. '12345678Z').

    Returns:
        {
            "ok": true,
            "data": {
                "tipo_documento": "D",
                "numero_documento": "12345678Z",
                "cita": { ...campos del servicio... }
            }
        }
    """
    if not numero_documento or not numero_documento.strip():
        return _error(
            "FALTA_DATO",
            "Falta el numero de documento. Preguntaselo al ciudadano antes de llamar a esta herramienta.",
        )

    tipo_documento = tipo_documento.upper()
    if tipo_documento not in {"D", "N"}:
        return _error("INVALID_PARAM", "tipo_documento debe ser 'D' o 'N'.")

    params = {
        "codigoPeticion": codigo_peticion,
        "tipotitular":    tipo_documento,
        "Idtitular":      numero_documento,
    }

    raw, err = await _request("GET", "/ConsultarCitaDnie", params=params)
    if err:
        return err

    return {
        "ok": True,
        "data": {
            "tipo_documento":   tipo_documento,
            "numero_documento": numero_documento,
            "cita":             raw,
        },
    }


def _build_alta_body(codigo_peticion, tipo_documento, numero_documento,
                      id_comisaria, id_tramite="", fecha_cita="", hora_cita=""):
    body = {
        "codigoPeticion": codigo_peticion,
        "tipotitular":    tipo_documento,
        "Idtitular":      numero_documento,
        "idComisaria":    id_comisaria,
    }
    opcionales = {
        "idTramite": id_tramite,
        "fechaCita": fecha_cita,
        "horaCita":  hora_cita,
    }
    body.update({k: v for k, v in opcionales.items() if v})
    return body


@mcp.tool()
async def alta_cita_dnie(
    codigo_peticion: str,
    tipo_documento: str,
    numero_documento: str,
    id_comisaria: str,
    fechaCita: str,
    horaCita: str,
    id_tramite: str = "",
) -> dict:
    """
    Da de alta una cita de DNIe/pasaporte para un titular.

    Args:
        codigo_peticion:  Identificador de la petición (ej. 'ABC123456').
        tipo_documento:   Tipo de documento: 'X' (NIE) o 'D' (DNI).
        numero_documento: Número de documento (ej. '12345678Z').
        id_comisaria:     Código de la comisaría elegida (obligatorio).
        fechaCita:        Fecha de la cita (ej. '20240601').
        horaCita:         Hora de la cita (ej. '09:20').
        id_tramite:       Código de trámite (opcional): 'DNIE' (DNI o NIE), 'PASAPORTE' (pasaporte).

    Returns:
        {
            "ok": true,
            "data": { ...campos del servicio (cita asignada)... }
        }
    """
    tipo_documento = tipo_documento.upper()
    if tipo_documento not in {"X", "D"}:
        return _error("INVALID_PARAM", "tipo_documento debe ser 'X' o 'D'.")

    body = _build_alta_body(codigo_peticion, tipo_documento, numero_documento,
                             id_comisaria, id_tramite,
                             fecha_cita=fechaCita, hora_cita=horaCita)

    raw, err = await _request("POST", "/AltaCitaDnie", json=body)
    if err:
        return err

    return {"ok": True, "data": raw}


@mcp.tool()
async def anular_cita_dnie(
    codigo_peticion: str,
    tipo_documento: str,
    numero_documento: str,
) -> dict:
    """
    Anula la cita de DNIe/pasaporte asociada a un titular.

    Args:
        codigo_peticion:  Identificador de la petición (ej. 'ABC123456').
        tipo_documento:   Tipo de documento: 'X' (NIE) o 'D' (DNI).
        numero_documento: Número de documento (ej. '12345678Z').

    Returns:
        {
            "ok": true,
            "data": { ...campos del servicio (cita anulada)... }
        }
    """
    tipo_documento = tipo_documento.upper()
    if tipo_documento not in {"X", "D"}:
        return _error("INVALID_PARAM", "tipo_documento debe ser 'X' o 'D'.")

    body = {
        "codigoPeticion": codigo_peticion,
        "tipotitular":    tipo_documento,
        "Idtitular":      numero_documento,
    }

    raw, err = await _request("PUT", "/AnularCitaDnie", json=body)
    if err:
        return err

    return {"ok": True, "data": raw}


@mcp.tool()
async def modificar_cita_dnie(
    codigo_peticion: str,
    tipo_documento: str,
    numero_documento: str,
    id_comisaria: str,
    fechaCita: str,
    horaCita: str,
    id_tramite: str = "",
) -> dict:
    """
    Modifica la cita de un titular: anula la cita existente y da de alta una
    nueva con los datos indicados.

    Args:
        codigo_peticion:  Identificador de la petición (ej. 'ABC123456').
        tipo_documento:   Tipo de documento: 'X' (NIE) o 'D' (DNI).
        numero_documento: Número de documento (ej. '12345678Z').
        id_comisaria:     Código de la comisaría para la nueva cita (obligatorio).
        fechaCita:        Fecha de la nueva cita (ej. '20240601').
        horaCita:         Hora de la nueva cita (ej. '09:20').
        id_tramite:       Código de trámite para la nueva cita (opcional). 'DNIE' (DNI o NIE), 'PASAPORTE' (pasaporte).

    Returns:
        {
            "ok": true,
            "data": { ...respuesta AltaCitaDnie de la nueva cita... }
        }
    """
    tipo_documento = tipo_documento.upper()
    if tipo_documento not in {"X", "D"}:
        return _error("INVALID_PARAM", "tipo_documento debe ser 'X' o 'D'.")

    body_anular = {
        "codigoPeticion": codigo_peticion,
        "tipotitular":    tipo_documento,
        "Idtitular":      numero_documento,
    }
    raw_anular, err = await _request("PUT", "/AnularCitaDnie", json=body_anular)
    if err:
        return err

    body_alta = _build_alta_body(codigo_peticion, tipo_documento, numero_documento,
                                  id_comisaria, id_tramite,
                                  fecha_cita=fechaCita, hora_cita=horaCita)
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


def _parse_slot(start_time: str):
    """De un startTime ISO devuelve (fechaCita 'AAAAMMDD', horaCita 'HH:MM', display)."""
    try:
        dt = datetime.fromisoformat(str(start_time).replace("Z", "").strip())
        return dt.strftime("%Y%m%d"), dt.strftime("%H:%M"), dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return "", "", str(start_time)


@mcp.tool()
async def consultar_slots_comisaria(
    id_comisaria: str,
    start_date: str,
    end_date: str,
) -> dict:
    """
    Devuelve los huecos (slots) de una comisaría, ya numerados y limpios
    (como maximo MAX_SLOTS), en un rango maximo de 5 dias. Todos los slots que
    devuelve la API se consideran disponibles.

    Args:
        id_comisaria: Identificador de la comisaría.
        start_date: Fecha de inicio (incluida) en formato YYYY-MM-DD.
        end_date: Fecha de fin (incluida) en formato YYYY-MM-DD. Máximo 5 días desde start_date.

    Returns:
        {
            "ok": true,
            "data": {
                "total": 12,
                "mostrados": 8,
                "listado_texto": "1. 02/07/2026 09:20\n2. 02/07/2026 09:50\n...",
                "slots": [
                    {"numero": 1, "fechaCita": "20260702", "horaCita": "09:20", "cuando": "02/07/2026 09:20"}
                ]
            }
        }
    "listado_texto" ya esta numerado y escrito; el agente lo muestra tal cual.
    "slots" sirve para mapear el numero elegido a su fechaCita y horaCita.
    """
    params = {
        "startDate": start_date,
        "endDate": end_date,
    }

    raw, err = await _request("GET", f"/offices/{id_comisaria}/slots", params=params)
    if err:
        return err

    disponibles = raw.get("slots") or [] if isinstance(raw, dict) else []

    slots = []
    for i, s in enumerate(disponibles[:MAX_SLOTS], start=1):
        fecha, hora, cuando = _parse_slot(s.get("startTime", ""))
        slots.append({
            "numero":    i,
            "fechaCita": fecha,
            "horaCita":  hora,
            "cuando":    cuando,
        })

    listado_texto = "\n".join(f"{x['numero']}. {x['cuando']}" for x in slots)

    return {
        "ok": True,
        "data": {
            "total":         len(disponibles),
            "mostrados":     len(slots),
            "slots":         slots,
            "listado_texto": listado_texto,
        },
    }


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
def main():
    mcp.run(transport="stdio")
 
 
if __name__ == "__main__":
    main()
 
