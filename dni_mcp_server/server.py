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

# CSV de codigos INE, ubicado junto a este server.py (ruta relativa)
CSV_CODIGOS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "codigos_ine.csv")

mcp = FastMCP(
    name="kyoe-consultas",
    instructions=(
        "Herramientas para consultar comisarías disponibles, consultar, dar de "
        "alta, anular y modificar citas de DNI/NIE/pasaporte a través de los "
        "servicios de rag.kyoe.es, para enviar SMS y consultar slots disponibles."
    ),
)

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def _error(code: str, message: str) -> dict:
    return {"ok": False, "error": {"code": code, "message": message}}


def _validar_requeridos(**campos):
    """Devuelve un _error si algun campo requerido viene vacio; None si estan
    todos. Evita que el agente llame a la API con parametros en blanco."""
    for nombre, valor in campos.items():
        if not str(valor or "").strip():
            return _error(
                "FALTA_DATO",
                f"Falta el dato '{nombre}'. Preguntaselo al ciudadano antes de continuar.",
            )
    return None


def _validar_id_comisaria(id_comisaria: str):
    """Devuelve un _error si el id de comisaria no es numerico; None si es
    valido. Los codigos de comisaria son siempre digitos (p. ej. '0002')."""
    if not str(id_comisaria or "").strip().isdigit():
        return _error(
            "INVALID_PARAM",
            "El id_comisaria debe estar formado solo por digitos.",
        )
    return None


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
def _resolver_localidad(localidad: str):
    """Resuelve un nombre de localidad a su fila de codigos INE.
    Los nombres de localidad son unicos, asi que se busca por coincidencia exacta
    (normalizada: sin tildes ni mayusculas). Devuelve (fila, error): si existe,
    fila es el dict y error None; si no, fila es None y error es un _error(...)."""
    if not _CODIGOS:
        return None, _error("DATA_ERROR", "No se ha podido cargar la tabla de códigos INE.")

    loc_norm = _normalizar(localidad)
    if not loc_norm:
        return None, _error("INVALID_PARAM", "Debe indicar el nombre de la localidad.")

    fila = next((r for r in _CODIGOS if r["_localidad_norm"] == loc_norm), None)
    if fila is None:
        return None, _error("NOT_FOUND", f"No se ha encontrado la localidad '{localidad}'.")

    return {
        "id_provincia": fila["id_provincia"],
        "provincia":    fila["provincia"],
        "id_localidad": fila["id_localidad"],
        "localidad":    fila["localidad"],
    }, None


@mcp.tool()
async def consultar_comisarias(
    codigo_peticion: str,
    localidad: str,
) -> dict:
    """
    Lista las comisarías disponibles para tramitar DNI/NIE/pasaporte en una localidad.

    Args:
        codigo_peticion: Identificador de la petición.
        localidad: Nombre de la localidad.

    Devuelve "listado_texto" (ya numerado, se muestra tal cual) y "comisarias"
    (mapea el número elegido a su id_comisaria).
    """
    fila, err = _resolver_localidad(localidad)
    if err:
        return err

    params = {
        "codigoPeticion": codigo_peticion,
        "idLocalidad":    fila["id_localidad"],
    }

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
            "comisarias":    comisarias,
            "listado_texto": listado_texto,
        },
    }


def _parsear_cita(raw):
    """Extrae de la respuesta cruda de Consultar/Alta los campos de la cita ya
    formateados. Devuelve (tiene_cita, cita_dict): si no hay fecha real,
    tiene_cita es False y cita_dict es None."""
    datos     = raw if isinstance(raw, dict) else {}
    fecha_raw = str(datos.get("fechaCita", "") or "").strip()
    hora_raw  = str(datos.get("horaCita", "") or "").strip()

    if not fecha_raw:
        return False, None

    fecha_disp, hora_disp = _display_fecha_hora(fecha_raw, hora_raw)
    cita = {
        "fecha":       fecha_disp,
        "hora":        hora_disp,
        "tramite":     str(datos.get("tramiteCita", "") or "").strip(),
        "comisaria":   str(datos.get("desComisariaCita", "") or "").strip(),
        "direccion":   str(datos.get("direccionCita", "") or "").strip(),
        "numero_cita": str(datos.get("numCita", "") or "").strip(),
    }
    return True, cita


def _frase_cita(cita: dict) -> str:
    """Frase con los datos de una cita ya formateada: 'el DD/MM/YYYY a las HH:MM
    en COMISARIA (DIRECCION)'. Omite las partes que falten."""
    frase = f"el {cita['fecha']} a las {cita['hora']}"
    if cita.get("comisaria"):
        frase += f" en {cita['comisaria']}"
    if cita.get("direccion"):
        frase += f" ({cita['direccion']})"
    return frase


async def _obtener_cita(codigo_peticion: str, tipo_documento: str, numero_documento: str):
    """Consulta la cita del titular y devuelve (data, error).

    data tiene el formato: tipo_documento, numero_documento, tiene_cita (bool),
    cita (dict|None) y resumen_texto. Lo usan tanto la tool de consulta como
    alta/anular/modificar para autocomprobarse antes de actuar.
    """
    if not numero_documento or not numero_documento.strip():
        return None, _error(
            "FALTA_DATO",
            "Falta el numero de documento. Preguntaselo al ciudadano antes de continuar.",
        )

    params = {
        "codigoPeticion": codigo_peticion,
        "tipotitular":    tipo_documento,
        "Idtitular":      numero_documento,
    }

    raw, err = await _request("GET", "/ConsultarCitaDnie", params=params)
    if err:
        return None, err

    tiene_cita, cita = _parsear_cita(raw)
    if tiene_cita:
        resumen_texto = f"Cita {_frase_cita(cita)}."
        if cita["numero_cita"]:
            resumen_texto += f" Numero de cita: {cita['numero_cita']}."
    else:
        resumen_texto = "No consta ninguna cita para este documento."

    data = {
        "tipo_documento":   tipo_documento,
        "numero_documento": numero_documento,
        "tiene_cita":       tiene_cita,
        "cita":             cita,
        "resumen_texto":    resumen_texto,
    }
    return data, None


@mcp.tool()
async def consultar_cita_dnie(
    codigo_peticion: str,
    tipo_documento: str,
    numero_documento: str,
) -> dict:
    """
    Consulta la cita de DNI/NIE/pasaporte de un titular. Úsala solo cuando el
    ciudadano pregunta expresamente si tiene cita; el resto de tools ya
    comprueban la cita por su cuenta.

    Args:
        codigo_peticion: Identificador de la petición.
        tipo_documento: 'D' (DNI) o 'X' (NIE).
        numero_documento: Número de documento.
    """
    tipo_documento = tipo_documento.upper()
    if tipo_documento not in {"D", "X"}:
        return _error("INVALID_PARAM", "tipo_documento debe ser 'D' o 'X'.")

    data, err = await _obtener_cita(codigo_peticion, tipo_documento, numero_documento)
    if err:
        return err

    return {"ok": True, "data": data}



def _build_alta_body(codigo_peticion, tipo_documento, numero_documento,
                      id_comisaria, fecha_cita, hora_cita, id_tramite=""):
    body = {
        "codigoPeticion": codigo_peticion,
        "tipotitular":    tipo_documento,
        "idtitular":      numero_documento,
        "idComisaria":    id_comisaria,
        "fechaCita":      fecha_cita,
        "horaCita":       hora_cita,
    }
    if id_tramite:
        body["idTramite"] = id_tramite
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
    Da de alta una cita de DNI/NIE/pasaporte. Antes comprueba si el titular ya
    tiene una cita: si la tiene, informa y NO crea otra.

    Args:
        codigo_peticion: Identificador de la petición.
        tipo_documento: 'D' (DNI) o 'X' (NIE).
        numero_documento: Número de documento.
        id_comisaria: Código de la comisaría (obligatorio).
        fechaCita: Fecha de la cita. Formato AAAAMMDD.
        horaCita: Hora de la cita. Formato HHMM.
        id_tramite: Opcional: 'DNIE' (DNI/NIE) o 'PASAPORTE'.
    """
    tipo_documento = tipo_documento.upper()
    if tipo_documento not in {"X", "D"}:
        return _error("INVALID_PARAM", "tipo_documento debe ser 'X' o 'D'.")

    err = _validar_requeridos(id_comisaria=id_comisaria, fechaCita=fechaCita, horaCita=horaCita)
    if err:
        return err
    err = _validar_id_comisaria(id_comisaria)
    if err:
        return err

    # Autocomprobación: si ya hay cita, informamos y no creamos otra.
    cita_actual, err = await _obtener_cita(codigo_peticion, tipo_documento, numero_documento)
    if err:
        return err
    if cita_actual["tiene_cita"]:
        return {
            "ok": True,
            "data": {
                "creada":        False,
                "tiene_cita":    True,
                "cita":          cita_actual["cita"],
                "resumen_texto": "El titular ya tiene una cita; no se ha creado otra. "
                                 + cita_actual["resumen_texto"],
            },
        }

    body = _build_alta_body(codigo_peticion, tipo_documento, numero_documento,
                             id_comisaria, fechaCita, horaCita, id_tramite)

    raw, err = await _request("POST", "/AltaCitaDnie", json=body)
    if err:
        return err

    return {
        "ok": True,
        "data": {
            "creada":        True,
            "resumen_texto": "Cita creada correctamente.",
        },
    }


@mcp.tool()
async def anular_cita_dnie(
    codigo_peticion: str,
    tipo_documento: str,
    numero_documento: str,
) -> dict:
    """
    Anula la cita de DNI/NIE/pasaporte de un titular. Antes comprueba si existe
    la cita: si no hay ninguna, informa y no llama al servicio.

    Args:
        codigo_peticion: Identificador de la petición.
        tipo_documento: 'D' (DNI) o 'X' (NIE).
        numero_documento: Número de documento.
    """
    tipo_documento = tipo_documento.upper()
    if tipo_documento not in {"X", "D"}:
        return _error("INVALID_PARAM", "tipo_documento debe ser 'X' o 'D'.")

    # Autocomprobación: si no hay cita, no hay nada que anular.
    cita_actual, err = await _obtener_cita(codigo_peticion, tipo_documento, numero_documento)
    if err:
        return err
    if not cita_actual["tiene_cita"]:
        return {
            "ok": True,
            "data": {
                "anulada":       False,
                "resumen_texto": "No consta ninguna cita para este documento; no hay nada que anular.",
            },
        }

    body = {
        "codigoPeticion": codigo_peticion,
        "tipotitular":    tipo_documento,
        "Idtitular":      numero_documento,
    }

    raw, err = await _request("PUT", "/AnularCitaDnie", json=body)
    if err:
        return err

    return {
        "ok": True,
        "data": {
            "anulada":       True,
            "resumen_texto": "Cita anulada correctamente.",
        },
    }


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
    Modifica la cita de un titular. Comprueba si ya tiene cita: si la tiene, la
    anula y crea la nueva; si no la tiene, crea directamente la nueva.

    Args:
        codigo_peticion: Identificador de la petición.
        tipo_documento: 'D' (DNI) o 'X' (NIE).
        numero_documento: Número de documento.
        id_comisaria: Código de la comisaría (obligatorio).
        fechaCita: Fecha de la nueva cita. Formato AAAAMMDD.
        horaCita: Hora de la nueva cita. Formato HHMM.
        id_tramite: Opcional: 'DNIE' (DNI/NIE) o 'PASAPORTE'.
    """
    tipo_documento = tipo_documento.upper()
    if tipo_documento not in {"X", "D"}:
        return _error("INVALID_PARAM", "tipo_documento debe ser 'X' o 'D'.")

    err = _validar_requeridos(id_comisaria=id_comisaria, fechaCita=fechaCita, horaCita=horaCita)
    if err:
        return err
    err = _validar_id_comisaria(id_comisaria)
    if err:
        return err

    # Autocomprobación: solo anulamos si de verdad hay una cita previa.
    cita_actual, err = await _obtener_cita(codigo_peticion, tipo_documento, numero_documento)
    if err:
        return err
    tenia_cita = cita_actual["tiene_cita"]

    if tenia_cita:
        body_anular = {
            "codigoPeticion": codigo_peticion,
            "tipotitular":    tipo_documento,
            "Idtitular":      numero_documento,
        }
        raw_anular, err = await _request("PUT", "/AnularCitaDnie", json=body_anular)
        if err:
            return err

    body_alta = _build_alta_body(codigo_peticion, tipo_documento, numero_documento,
                                  id_comisaria, fechaCita, horaCita, id_tramite)
    raw_alta, err = await _request("POST", "/AltaCitaDnie", json=body_alta)
    if err:
        return err

    if tenia_cita:
        resumen = "Cita modificada correctamente."
    else:
        resumen = "No había cita previa; se ha creado una nueva cita."

    return {
        "ok": True,
        "data": {
            "modificada":    tenia_cita,
            "creada":        not tenia_cita,
            "resumen_texto": resumen,
        },
    }



@mcp.tool()
async def enviar_sms(destinatario: str, mensaje: str) -> dict:
    """
    Envía un SMS a un móvil español. La tool añade el prefijo +34.

    Args:
        destinatario: Móvil de 9 dígitos, sin prefijo.
        mensaje: Texto del SMS.
    """
    # Inyectamos el prefijo +34 dentro de la tool. Aceptamos que llegue ya con
    # prefijo (+34... o 34...) para no duplicarlo.
    numero = str(destinatario).strip().replace(" ", "")
    if numero.startswith("+"):
        telefono = numero
    elif numero.startswith("34"):
        telefono = f"+{numero}"
    else:
        telefono = f"+34{numero}"

    body = {"to": telefono, "message": mensaje}

    raw, err = await _request("POST", "/sms/send", json=body)
    if err:
        return err

    return {
        "ok": True,
        "data": {
            "enviado":       True,
            "resumen_texto": "SMS enviado.",
        },
    }


def _parse_slot(start_time: str):
    """De un startTime ISO devuelve (fechaCita 'AAAAMMDD', horaCita 'HHMM', display)."""
    try:
        dt = datetime.fromisoformat(str(start_time).replace("Z", "").strip())
        return dt.strftime("%Y%m%d"), dt.strftime("%H%M"), dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return "", "", str(start_time)


def _fecha_a_iso(aaaammdd: str) -> str:
    """Convierte una fecha 'AAAAMMDD' a 'YYYY-MM-DD' (formato que espera la API
    de slots). Si ya viene en otro formato, la devuelve tal cual."""
    s = str(aaaammdd).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


def _display_fecha_hora(fecha: str, hora: str):
    """Formatea fechaCita 'AAAAMMDD' -> 'DD/MM/YYYY' y horaCita 'HHMM' -> 'HH:MM'.
    Si no encajan en ese formato, devuelve el valor original sin tocar."""
    f = str(fecha or "").strip()
    h = str(hora or "").strip()
    fecha_disp = f"{f[6:8]}/{f[4:6]}/{f[:4]}" if len(f) == 8 and f.isdigit() else f
    hora_disp  = f"{h[:2]}:{h[2:4]}" if len(h) == 4 and h.isdigit() else h
    return fecha_disp, hora_disp


@mcp.tool()
async def consultar_slots_comisaria(
    id_comisaria: str,
    start_date: str,
) -> dict:
    """
    Devuelve los huecos disponibles de una comisaría para un día.

    Args:
        id_comisaria: Identificador de la comisaría.
        start_date: Día de la cita. Formato AAAAMMDD.

    Devuelve "listado_texto" (ya numerado, se muestra tal cual) y "slots"
    (mapea el número elegido a su fechaCita y horaCita).
    """
    err = _validar_requeridos(id_comisaria=id_comisaria, start_date=start_date)
    if err:
        return err
    err = _validar_id_comisaria(id_comisaria)
    if err:
        return err

    fecha_iso = _fecha_a_iso(start_date)
    params = {
        "startDate": fecha_iso,
        "endDate": fecha_iso,
    }

    raw, err = await _request("GET", f"/offices/{id_comisaria}/slots", params=params)
    if err:
        return err

    todos = raw.get("slots") or [] if isinstance(raw, dict) else []

    # La API mockeada devuelve todos los slots; el agente solo necesita los
    # que tienen available=true.
    disponibles = [s for s in todos if s.get("available") is True]

    slots = []
    for i, s in enumerate(disponibles, start=1):
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
            "slots":         slots,
            "listado_texto": listado_texto,
        },
    }


@mcp.tool()
def crear_codigo_peticion() -> dict:
    """
    Genera un código de petición alfanumérico aleatorio.
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




 
