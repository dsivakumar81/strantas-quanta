from __future__ import annotations

import base64
import shutil
from pathlib import Path


def _submission_body() -> dict[str, str]:
    return {
        "sender": "broker@example.com",
        "recipients": ["quotes@strantas.ai"],
        "subject": "ACME Manufacturing Life, LTD, Dental submission",
        "body_raw": "<p>Employer: ACME Manufacturing</p><p>Broker: Northstar Benefits</p><p>Effective Date: 2026-07-01</p><p>Situs: TX</p>",
        "body_text": "Employer: ACME Manufacturing\nBroker: Northstar Benefits\nEffective Date: 2026-07-01\nSitus: TX\nPlease quote group life, LTD, and dental coverage.",
    }


def test_multipart_pdf_flow_uses_postgres_and_minio(client) -> None:
    payload = _submission_body()
    pdf_bytes = Path("samples/acme_census.pdf").read_bytes()

    listener = client.post(
        "/v1/listener/email-multipart",
        data={
            "sender": payload["sender"],
            "recipients": ",".join(payload["recipients"]),
            "subject": payload["subject"] + " PDF",
            "body_raw": payload["body_raw"],
            "body_text": payload["body_text"],
        },
        files=[("files", ("acme_census.pdf", pdf_bytes, "application/pdf"))],
    )
    assert listener.status_code == 202
    submission_id = listener.json()["submission_id"]

    parse = client.post("/v1/reader/parse", params={"submission_id": submission_id})
    assert parse.status_code == 200
    assert parse.json()["submission_intent"] == "rfp_submission"
    assert parse.json()["document_inventory"][0]["document_type"] == "census"
    assert "tabular" in parse.json()["document_inventory"][0]["tags"]

    extract = client.post("/v1/extractor/run", params={"submission_id": submission_id})
    assert extract.status_code == 200
    case_id = extract.json()["case_id"]

    normalize = client.post("/v1/normalizer/run", params={"submission_id": submission_id})
    assert normalize.status_code == 200
    census = normalize.json()["census"]
    assert census["employee_count"] == 4
    assert census["evidence_references"][0]["page_number"] == 1

    output = client.get(f"/v1/output/{case_id}")
    assert output.status_code == 200
    assert output.json()["census"]["evidenceReferences"][0]["file_name"] == "acme_census.pdf"
    assert any(
        item["evidenceReferences"] and item["evidenceReferences"][0].get("file_name") == "acme_census.pdf"
        for item in output.json()["lobs"]
    )
    assert any(
        warning.startswith("Plan design derived from attachment evidence")
        for lob in normalize.json()["lob_requests"]
        for warning in lob["warnings"]
    )
    output_lob_types = {item["lobType"] for item in output.json()["lobs"]}
    assert {"group_life", "group_ltd", "dental", "vision", "supplemental_ci", "supplemental_accident", "supplemental_hi"} <= output_lob_types


def test_csv_json_listener_flow(client) -> None:
    payload = _submission_body()
    content = base64.b64encode(Path("samples/acme_census.csv").read_bytes()).decode()
    payload["attachments"] = [
        {
            "file_name": "acme_census.csv",
            "content_type": "text/csv",
            "content_base64": content,
        }
    ]

    listener = client.post("/v1/listener/email", json=payload)
    assert listener.status_code == 202
    submission_id = listener.json()["submission_id"]

    normalize = client.post("/v1/normalizer/run", params={"submission_id": submission_id})
    assert normalize.status_code == 200
    census = normalize.json()["census"]
    assert census["employee_count"] == 4
    assert census["states_detected"] == ["LA", "OK", "TX"]


def test_xlsx_json_listener_flow(client) -> None:
    payload = _submission_body()
    xlsx_bytes = Path("samples/acme_census.xlsx").read_bytes()
    payload["attachments"] = [
        {
            "file_name": "acme_census.xlsx",
            "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "content_base64": base64.b64encode(xlsx_bytes).decode(),
        }
    ]

    listener = client.post("/v1/listener/email", json=payload)
    assert listener.status_code == 202
    submission_id = listener.json()["submission_id"]

    parse = client.post("/v1/reader/parse", params={"submission_id": submission_id})
    assert parse.status_code == 200
    assert parse.json()["document_inventory"][0]["file_name"] == "acme_census.xlsx"

    normalize = client.post("/v1/normalizer/run", params={"submission_id": submission_id})
    assert normalize.status_code == 200
    census = normalize.json()["census"]
    assert census["employee_count"] == 4
    assert census["classes_detected"] == ["Executive", "Hourly", "Salaried"]


def test_scanned_pdf_ocr_flow_when_tesseract_present(client) -> None:
    if shutil.which("tesseract") is None:
        import pytest

        pytest.skip("tesseract not installed")

    payload = _submission_body()
    pdf_bytes = Path("samples/acme_census_scanned.pdf").read_bytes()
    listener = client.post(
        "/v1/listener/email-multipart",
        data={
            "sender": payload["sender"],
            "recipients": ",".join(payload["recipients"]),
            "subject": payload["subject"] + " scanned pdf",
            "body_raw": payload["body_raw"],
            "body_text": payload["body_text"],
        },
        files=[("files", ("acme_census_scanned.pdf", pdf_bytes, "application/pdf"))],
    )
    assert listener.status_code == 202
    submission_id = listener.json()["submission_id"]

    normalize = client.post("/v1/normalizer/run", params={"submission_id": submission_id})
    assert normalize.status_code == 200
    census = normalize.json()["census"]
    assert census["employee_count"] >= 4
    assert any("OCR recovered census-like rows" in item["snippet"] for item in census["evidence_references"])


def test_email_precedence_over_conflicting_attachment_fields(client) -> None:
    payload = _submission_body()
    pdf_bytes = Path("samples/acme_census.pdf").read_bytes()
    listener = client.post(
        "/v1/listener/email-multipart",
        data={
            "sender": payload["sender"],
            "recipients": ",".join(payload["recipients"]),
            "subject": payload["subject"] + " precedence",
            "body_raw": payload["body_raw"].replace("TX", "OK"),
            "body_text": payload["body_text"].replace("TX", "OK"),
        },
        files=[("files", ("acme_census.pdf", pdf_bytes, "application/pdf"))],
    )
    assert listener.status_code == 202
    submission_id = listener.json()["submission_id"]

    normalize = client.post("/v1/normalizer/run", params={"submission_id": submission_id})
    assert normalize.status_code == 200
    quote = normalize.json()["quote_request"]
    assert quote["situs_state"] == "OK"
    assert any("situs_state conflict detected" in warning for warning in quote["warnings"])


def test_provider_email_adapter_flow(client) -> None:
    providers = client.get("/v1/listener/providers")
    assert providers.status_code == 200
    assert any(item["provider"] == "microsoft_graph" for item in providers.json())

    pdf_bytes = Path("samples/acme_census.pdf").read_bytes()
    payload = {
        "provider": "microsoft_graph",
        "payload": {
            "subject": "Graph delivered submission",
            "bodyPreview": "Employer: ACME Manufacturing\nBroker: Northstar Benefits\nEffective Date: 2026-07-01\nSitus: TX\nPlease quote vision and accident.",
            "body": {"content": "<p>Graph submission</p>"},
            "from": {"emailAddress": {"address": "broker@example.com"}},
            "toRecipients": [{"emailAddress": {"address": "quotes@strantas.ai"}}],
            "attachments": [
                {
                    "name": "acme_census.pdf",
                    "contentType": "application/pdf",
                    "contentBytes": base64.b64encode(pdf_bytes).decode(),
                }
            ],
        },
    }
    listener = client.post("/v1/listener/provider-email", json=payload)
    assert listener.status_code == 202
    submission_id = listener.json()["submission_id"]

    extract = client.post("/v1/extractor/run", params={"submission_id": submission_id})
    assert extract.status_code == 200
    assert "vision" in extract.json()["detected_lobs"]


def test_smtp_webhook_listener_flow(client) -> None:
    pdf_bytes = Path("samples/acme_census.pdf").read_bytes()
    listener = client.post(
        "/v1/listener/smtp-webhook",
        json={
            "from_email": "broker@example.com",
            "to_emails": ["quotes@strantas.ai"],
            "subject": "SMTP delivered submission",
            "text": "Employer: ACME Manufacturing\nBroker: Northstar Benefits\nEffective Date: 2026-07-01\nSitus: TX\nPlease quote dental and vision.",
            "html": "<p>SMTP delivered submission</p>",
            "attachments": [
                {
                    "file_name": "acme_census.pdf",
                    "content_type": "application/pdf",
                    "content_base64": base64.b64encode(pdf_bytes).decode(),
                }
            ],
        },
    )
    assert listener.status_code == 202
    submission_id = listener.json()["submission_id"]
    extract = client.post("/v1/extractor/run", params={"submission_id": submission_id})
    assert extract.status_code == 200
    assert "dental" in extract.json()["detected_lobs"]


def test_alembic_downgrade_and_upgrade_round_trip() -> None:
    import os
    import subprocess

    root = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env["QUANTA_DATABASE_URL"] = "postgresql+psycopg://quanta:quanta@127.0.0.1:5432/quanta"
    subprocess.run(["alembic", "downgrade", "base"], cwd=root, env=env, check=True)
    subprocess.run(["alembic", "upgrade", "head"], cwd=root, env=env, check=True)
