from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config


def _normalize_endpoint(endpoint_raw: str) -> str:
    endpoint = (endpoint_raw or "").strip().rstrip("/")
    for suffix in ("/openai/v1/responses", "/openai/v1/responses/", "/openai/v1", "/openai", "/openai/v1/"):
        if endpoint.lower().endswith(suffix):
            endpoint = endpoint[: -len(suffix)].rstrip("/")
    return endpoint


def call_chat_completions(
    *,
    prompt: str,
    temperature: float = 0.2,
    max_tokens: int = 128,
    endpoint: str | None = None,
    deployment: str | None = None,
    api_version: str | None = None,
) -> str:
    endpoint_raw = endpoint if endpoint is not None else (getattr(config, "AZURE_OPENAI_ENDPOINT", "") or "")
    api_key = getattr(config, "AZURE_OPENAI_API_KEY", "") or ""
    deployment = deployment if deployment is not None else (getattr(config, "AZURE_OPENAI_DEPLOYMENT", "") or "")
    api_version = api_version if api_version is not None else (
        getattr(config, "AZURE_OPENAI_API_VERSION", "") or "2024-02-15-preview"
    )

    if not str(endpoint_raw).strip() or not str(api_key).strip() or not str(deployment).strip():
        raise RuntimeError("Faltan AZURE_OPENAI_ENDPOINT/AZURE_OPENAI_API_KEY/AZURE_OPENAI_DEPLOYMENT en .env/config.py")

    endpoint_norm = _normalize_endpoint(str(endpoint_raw))
    q = urllib.parse.urlencode({"api-version": str(api_version).strip()})
    url = f"{endpoint_norm}/openai/deployments/{urllib.parse.quote(str(deployment).strip())}/chat/completions?{q}"

    payload = {
        "messages": [
            {"role": "system", "content": "Responde de forma breve."},
            {"role": "user", "content": prompt},
        ],
        "max_completion_tokens": int(max_tokens),
    }
    if str(deployment).strip().lower().startswith("gpt-5"):
        payload["reasoning_effort"] = "minimal"
    if float(temperature) == 1.0:
        payload["temperature"] = 1.0

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json", "api-key": str(api_key).strip()},
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        msg = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else str(exc)
        safe_url = url
        raise RuntimeError(
            "Azure OpenAI error: "
            f"{exc.code} {msg}\n"
            f"URL: {safe_url}\n"
            f"ENDPOINT: {endpoint_norm}\n"
            f"DEPLOYMENT: {str(deployment).strip()}\n"
            f"API_VERSION: {str(api_version).strip()}"
        ) from exc
    except Exception as exc:
        raise RuntimeError(f"No se pudo conectar con Azure OpenAI: {exc}") from exc

    obj = json.loads(body)
    choices = obj.get("choices") or []
    if not choices:
        raise RuntimeError(f"Respuesta sin 'choices': {body[:500]}")
    content = (((choices[0] or {}).get("message") or {}).get("content") or "").strip()
    if not content:
        raise RuntimeError(f"Respuesta vacía: {body[:500]}")
    return str(content)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Test rápido de Azure OpenAI (legacy chat/completions con api-version).")
    parser.add_argument("--prompt", default="Di 'OK' y la fecha de hoy en ISO.", help="Prompt de prueba")
    parser.add_argument("--temperature", type=float, default=0.2)
    parser.add_argument("--max-tokens", type=int, default=128)
    parser.add_argument("--endpoint", default="", help="Override AZURE_OPENAI_ENDPOINT (opcional)")
    parser.add_argument("--deployment", default="", help="Override AZURE_OPENAI_DEPLOYMENT (opcional)")
    parser.add_argument("--api-version", default="", help="Override AZURE_OPENAI_API_VERSION (opcional)")
    args = parser.parse_args(argv)

    out = call_chat_completions(
        prompt=args.prompt,
        temperature=args.temperature,
        max_tokens=args.max_tokens,
        endpoint=(args.endpoint.strip() or None),
        deployment=(args.deployment.strip() or None),
        api_version=(args.api_version.strip() or None),
    )
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

