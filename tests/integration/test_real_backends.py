from __future__ import annotations

import base64
import io
import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
import zipfile

import httpx

from quanta_api.domain.enums import AlertSeverity, EmailProvider, JobStatus, JobType
from quanta_api.domain.models import ConnectorCursor


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

    carrier_output = client.get(f"/v1/output/{case_id}/carrier-rfp")
    assert carrier_output.status_code == 200
    assert carrier_output.json()["employer"]["name"] == "ACME Manufacturing"
    assert carrier_output.json()["groupConfiguration"]["numberOfEligibleEmployees"] == 4
    assert carrier_output.json()["files"][0]["mediaType"] == "application/pdf"
    assert carrier_output.json()["files"][0]["documentType"] == "census"


def test_health_readiness_and_openapi_docs_are_available(client) -> None:
    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"

    readiness = client.get("/readiness")
    assert readiness.status_code == 200
    assert readiness.json()["status"] == "ready"

    openapi = client.get("/openapi.json")
    assert openapi.status_code == 200
    assert openapi.json()["openapi"].startswith("3.")
    assert "/v1/output/{case_id}/carrier-rfp" in openapi.json()["paths"]
    assert openapi.json()["paths"]["/v1/listener/email"]["post"]["requestBody"]["content"]["application/json"]["example"]["sender"] == "broker@example.com"
    assert openapi.json()["paths"]["/v1/output/{case_id}"]["get"]["responses"]["200"]["content"]["application/json"]["example"]["caseId"].startswith("QNT-")

    docs = client.get("/docs")
    assert docs.status_code == 200

    redoc = client.get("/redoc")
    assert redoc.status_code == 200


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


def test_normalizer_reader_handoff_persists_document_type_into_carrier_output(client) -> None:
    payload = _submission_body()
    pdf_bytes = Path("samples/acme_census.pdf").read_bytes()
    listener = client.post(
        "/v1/listener/email-multipart",
        data={
            "sender": payload["sender"],
            "recipients": ",".join(payload["recipients"]),
            "subject": payload["subject"] + " carrier handoff",
            "body_raw": payload["body_raw"],
            "body_text": payload["body_text"],
        },
        files=[("files", ("acme_census.pdf", pdf_bytes, "application/pdf"))],
    )
    assert listener.status_code == 202
    submission_id = listener.json()["submission_id"]

    normalize = client.post("/v1/normalizer/run", params={"submission_id": submission_id})
    assert normalize.status_code == 200
    case_id = normalize.json()["case_id"]

    carrier_output = client.get(f"/v1/output/{case_id}/carrier-rfp")
    assert carrier_output.status_code == 200
    assert carrier_output.json()["files"][0]["documentType"] == "census"


def test_dental_vision_rfp_pdf_extracts_richer_plan_details_and_carrier_fields(client) -> None:
    pdf_bytes = Path("samples/acme_dental_vision_rfp.pdf").read_bytes()
    listener = client.post(
        "/v1/listener/email-multipart",
        headers={"x-quanta-tenant-id": "tenant-alpha"},
        data={
            "sender": "broker@example.com",
            "recipients": "quotes@strantas.ai",
            "subject": "ACME Dental and Vision RFP",
            "body_raw": "<p>Dental and vision submission</p>",
            "body_text": "Employer: ACME Manufacturing; Broker: Northstar Benefits; Effective Date: 2026-07-01; Due Date: 2026-05-15; Situs: TX; Please quote dental and vision.",
        },
        files=[("files", ("acme_dental_vision_rfp.pdf", pdf_bytes, "application/pdf"))],
    )
    assert listener.status_code == 202
    submission_id = listener.json()["submission_id"]

    normalize = client.post(
        "/v1/normalizer/run",
        params={"submission_id": submission_id},
        headers={"x-quanta-tenant-id": "tenant-alpha"},
    )
    assert normalize.status_code == 200
    case_id = normalize.json()["case_id"]

    dental_lob = next(item for item in normalize.json()["lob_requests"] if item["lob_type"] == "dental")
    dental_attrs = dental_lob["requested_plan_designs"][0]["attributes"]
    assert dental_attrs["annual_maximum"] == 2000.0
    assert dental_attrs["deductible"] == 50
    assert dental_attrs["preventive_percent"] == 100
    assert dental_attrs["basic_percent"] == 80
    assert dental_attrs["major_percent"] == 50
    assert dental_attrs["orthodontia_percent"] == 50
    assert dental_attrs["orthodontia_age_limit"] == 19
    assert dental_attrs["office_visit_copay"] == 15
    assert "preventive 100%" in dental_lob["evidence_references"][0]["snippet"].lower()
    dental_classes = " ".join(dental_lob["class_structure"]).lower()
    assert "executive" in dental_classes
    assert "salaried" in dental_classes
    assert "hourly" in dental_classes
    assert "first of the month following 30 days" in " ".join(dental_lob["eligibility_rules"]).lower()
    assert any("100% employee only coverage" in item.lower() for item in dental_lob["contribution_details"]["splits"])
    assert any("dependents voluntary" in item.lower() for item in dental_lob["contribution_details"]["splits"])
    assert any("75%" in item.lower() for item in dental_lob["notes"])

    vision_lob = next(item for item in normalize.json()["lob_requests"] if item["lob_type"] == "vision")
    vision_attrs = vision_lob["requested_plan_designs"][0]["attributes"]
    assert vision_attrs["exam_copay"] == 10
    assert vision_attrs["materials_copay"] == 25
    assert vision_attrs["frame_allowance"] == 180.0
    assert vision_attrs["contact_allowance"] == 150.0
    assert vision_attrs["frequency_months"] == 12
    assert vision_attrs["laser_correction_allowance"] == 250.0
    assert "all benefit eligible employees" in " ".join(vision_lob["eligibility_rules"]).lower()
    assert any("day one" in item.lower() for item in vision_lob["eligibility_rules"])
    assert any("100% for employees" in item.lower() for item in vision_lob["contribution_details"]["splits"])
    assert any("50% for dependents" in item.lower() for item in vision_lob["contribution_details"]["splits"])
    assert any("50%" in item.lower() for item in vision_lob["notes"])

    quote = normalize.json()["quote_request"]
    assert quote["broker_agency_name"] == "Northstar Benefits"
    assert quote["broker_contact_name"] == "Marcus Hale"
    assert quote["broker_contact_email"] == "marcus.hale@northstar.example"
    assert quote["employer_contact_name"] == "Elena Brooks"
    assert quote["employer_contact_email"] == "elena.brooks@acmemfg.example"
    assert quote["field_results"]["broker_contact_name"]["evidence"][0]["file_name"] == "acme_dental_vision_rfp.pdf"
    assert quote["field_results"]["employer_contact_email"]["evidence"][0]["file_name"] == "acme_dental_vision_rfp.pdf"

    carrier = client.get(
        f"/v1/output/{case_id}/carrier-rfp",
        headers={"x-quanta-tenant-id": "tenant-alpha"},
    )
    assert carrier.status_code == 200
    carrier_json = carrier.json()
    assert carrier_json["employer"]["contacts"][0]["fullName"] == "Elena Brooks"
    assert carrier_json["employer"]["contacts"][0]["workEmail"] == "elena.brooks@acmemfg.example"
    assert carrier_json["employer"]["contacts"][0]["fieldResults"]["full_name"]["value"] == "Elena Brooks"
    assert carrier_json["employer"]["contacts"][0]["fieldResults"]["work_email"]["value"] == "elena.brooks@acmemfg.example"
    assert carrier_json["employer"]["locations"][0]["city"] == "Dallas"
    assert carrier_json["employer"]["locations"][0]["addressLine1"] == "2500 Foundry Way"
    assert carrier_json["employer"]["locations"][0]["state"] == "TX"
    assert carrier_json["employer"]["locations"][0]["fieldResults"]["city"]["value"] == "Dallas"
    assert carrier_json["employer"]["locations"][0]["fieldResults"]["postal_code"]["value"] == "75201"
    assert carrier_json["groupConfiguration"]["producers"][0]["broker"]["contact"]["workEmail"] == "marcus.hale@northstar.example"
    assert carrier_json["groupConfiguration"]["producers"][0]["broker"]["contact"]["fullName"] == "Marcus Hale"
    assert carrier_json["groupConfiguration"]["producers"][0]["broker"]["agencyName"] == "Northstar Benefits"
    assert carrier_json["groupConfiguration"]["producers"][0]["broker"]["fieldResults"]["agency_name"]["value"] == "Northstar Benefits"
    assert carrier_json["groupConfiguration"]["producers"][0]["broker"]["contact"]["fieldResults"]["full_name"]["value"] == "Marcus Hale"
    assert carrier_json["groupConfiguration"]["producers"][0]["broker"]["contact"]["fieldResults"]["work_email"]["value"] == "marcus.hale@northstar.example"
    assert carrier_json["groupConfiguration"]["producers"][0]["broker"]["contact"]["fieldResults"]["full_name"]["evidence"][0]["file_name"] == "acme_dental_vision_rfp.pdf"
    assert carrier_json["employer"]["contacts"][0]["fieldResults"]["full_name"]["evidence"][0]["file_name"] == "acme_dental_vision_rfp.pdf"
    dental_coverage = next(item for item in carrier_json["groupConfiguration"]["coverages"] if item["coverageType"] == "dental")
    assert dental_coverage["fieldResults"]["annual_maximum"]["value"] == 2000.0
    assert dental_coverage["fieldResults"]["deductible"]["value"] == 50
    assert dental_coverage["dentalPlanDetails"]["annualMaximum"] == 2000.0
    assert dental_coverage["dentalPlanDetails"]["preventivePercent"] == 100
    assert dental_coverage["dentalPlanDetails"]["basicPercent"] == 80
    assert dental_coverage["dentalPlanDetails"]["majorPercent"] == 50
    assert dental_coverage["dentalPlanDetails"]["orthodontiaPercent"] == 50
    assert dental_coverage["dentalPlanDetails"]["orthodontiaAgeLimit"] == 19
    assert dental_coverage["dentalPlanDetails"]["deductible"] == 50
    assert dental_coverage["dentalPlanDetails"]["officeVisitCopay"] == 15
    assert dental_coverage["dentalPlanDetails"]["serviceWaitingPeriods"] == "basic services 6 months, major services 12 months"
    assert dental_coverage["dentalPlanDetails"]["fieldResults"]["annual_maximum"]["value"] == 2000.0
    assert dental_coverage["dentalPlanDetails"]["fieldResults"]["deductible"]["value"] == 50
    vision_coverage = next(item for item in carrier_json["groupConfiguration"]["coverages"] if item["coverageType"] == "vision")
    assert vision_coverage["visionPlanDetails"]["examCopay"] == 10
    assert vision_coverage["visionPlanDetails"]["materialsCopay"] == 25
    assert vision_coverage["visionPlanDetails"]["frameAllowance"] == 180.0
    assert vision_coverage["visionPlanDetails"]["contactAllowance"] == 150.0
    assert vision_coverage["visionPlanDetails"]["frequencyMonths"] == 12
    assert vision_coverage["visionPlanDetails"]["laserCorrectionAllowance"] == 250.0
    assert vision_coverage["visionPlanDetails"]["fieldResults"]["exam_copay"]["value"] == 10
    assert vision_coverage["visionPlanDetails"]["fieldResults"]["laser_correction_allowance"]["value"] == 250.0
    assert carrier_json["files"][0]["fieldResults"]["document_type"]["value"] == "plan_summary"
    assert carrier_json["fieldResults"]["employer_name"]["value"] == "ACME Manufacturing"
    assert carrier_json["fieldResults"]["location_city"]["value"] == "Dallas"


def test_ltd_percent_parsing_preserves_60_percent(client) -> None:
    payload = _submission_body()
    payload["attachments"] = [
        {
            "file_name": "plan_summary.csv",
            "content_type": "text/csv",
            "content_base64": base64.b64encode(
                (
                    "Employer: ACME Manufacturing\n"
                    "Broker: Northstar Benefits\n"
                    "Effective Date: 07/01/2026\n"
                    "Situs State: TX\n"
                    "LTD benefit: 60% of covered earnings with max monthly benefit $10,000 and elimination period 90 days.\n"
                ).encode()
            ).decode(),
        }
    ]

    listener = client.post("/v1/listener/email", json=payload)
    assert listener.status_code == 202
    submission_id = listener.json()["submission_id"]

    normalize = client.post("/v1/normalizer/run", params={"submission_id": submission_id})
    assert normalize.status_code == 200
    ltd_lob = next(item for item in normalize.json()["lob_requests"] if item["lob_type"] == "group_ltd")
    assert ltd_lob["requested_plan_designs"][0]["benefit_percent"] == 60.0
    assert ltd_lob["field_results"]["benefit_percent"]["value"] == 60.0


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


def test_semicolon_flattened_email_body_is_parsed_cleanly(client) -> None:
    pdf_bytes = Path("samples/acme_census.pdf").read_bytes()
    listener = client.post(
        "/v1/listener/email-multipart",
        data={
            "sender": "broker@example.com",
            "recipients": "quotes@strantas.ai",
            "subject": "Flattened body submission",
            "body_raw": "<p>flattened</p>",
            "body_text": "Employer: ACME Manufacturing; Broker: Northstar Benefits; Effective Date: 2026-07-01; Due Date: 2026-05-15; Situs: TX; Incumbent: Guardian; Please quote Life, LTD, Dental, Vision.",
        },
        files=[("files", ("acme_census.pdf", pdf_bytes, "application/pdf"))],
    )
    assert listener.status_code == 202
    submission_id = listener.json()["submission_id"]

    normalize = client.post("/v1/normalizer/run", params={"submission_id": submission_id})
    assert normalize.status_code == 200
    quote = normalize.json()["quote_request"]
    assert quote["employer_name"] == "ACME Manufacturing"
    assert quote["broker_name"] == "Northstar Benefits"
    assert quote["incumbent_carrier"] == "Guardian"


def test_duplicate_submission_returns_conflict(client) -> None:
    payload = _submission_body()
    response_one = client.post("/v1/listener/email", json=payload)
    assert response_one.status_code == 202

    response_two = client.post("/v1/listener/email", json=payload)
    assert response_two.status_code == 409
    assert response_two.json()["error"] == "conflict"


def test_duplicate_submission_persists_across_app_instances(integration_settings) -> None:
    from fastapi.testclient import TestClient
    from quanta_api.bootstrap import build_service_container
    from quanta_api.main import create_app

    payload = _submission_body()
    first_client = TestClient(create_app(container=build_service_container(integration_settings)))
    first = first_client.post("/v1/listener/email", json=payload)
    assert first.status_code == 202
    first_client.close()

    second_client = TestClient(create_app(container=build_service_container(integration_settings)))
    second = second_client.post("/v1/listener/email", json=payload)
    assert second.status_code == 409
    assert second.json()["details"]["submissionId"].startswith("SUB-")
    second_client.close()


def test_large_attachment_is_rejected(connector_client) -> None:
    connector_client.app.state.container.intake_service.settings.max_attachment_size_bytes = 10
    response = connector_client.post(
        "/v1/listener/email",
        json={
            "sender": "broker@example.com",
            "recipients": ["quotes@strantas.ai"],
            "subject": "Large attachment submission",
            "body_raw": "test",
            "body_text": "test",
            "attachments": [
                {
                    "file_name": "too_large.pdf",
                    "content_type": "application/pdf",
                    "content_base64": base64.b64encode(b"x" * 64).decode(),
                }
            ],
        },
    )
    assert response.status_code == 400
    assert response.json()["error"] == "input_validation_error"


def test_tenant_header_partitions_submission_storage_and_status(client) -> None:
    payload = _submission_body()
    payload["attachments"] = [
        {
            "file_name": "acme_census.csv",
            "content_type": "text/csv",
            "content_base64": base64.b64encode(Path("samples/acme_census.csv").read_bytes()).decode(),
        }
    ]
    response = client.post("/v1/listener/email", headers={"x-quanta-tenant-id": "tenant-alpha"}, json=payload)
    assert response.status_code == 202
    submission_id = response.json()["submission_id"]

    status_response = client.get(f"/v1/submissions/{submission_id}")
    assert status_response.status_code == 404

    status_response = client.get(f"/v1/submissions/{submission_id}", headers={"x-quanta-tenant-id": "tenant-alpha"})
    assert status_response.status_code == 200
    assert status_response.json()["tenant_id"] == "tenant-alpha"

    submission = client.app.state.container.submission_repository.get(submission_id, tenant_id="tenant-alpha")
    assert submission is not None
    assert submission.attachments[0].storage_key.startswith("tenants/tenant-alpha/")


def test_mime_mismatch_is_rejected(client) -> None:
    payload = _submission_body()
    payload["attachments"] = [
        {
            "file_name": "acme_census.pdf",
            "content_type": "text/plain",
            "content_base64": base64.b64encode(Path("samples/acme_census.pdf").read_bytes()).decode(),
        }
    ]
    response = client.post("/v1/listener/email", json=payload)
    assert response.status_code == 400
    assert response.json()["message"] == "Attachment MIME type does not match file content"


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
    status_response = client.get(f"/v1/submissions/{submission_id}")
    assert status_response.status_code == 200
    assert status_response.json()["source_provider"] == "microsoft_graph"

    extract = client.post("/v1/extractor/run", params={"submission_id": submission_id})
    assert extract.status_code == 200
    assert "vision" in extract.json()["detected_lobs"]


def test_inbound_mailbox_config_crud_and_tenant_scope(connector_client) -> None:
    response = connector_client.put(
        "/v1/inbound-mailboxes/gmail",
        headers={"x-quanta-admin-secret": "quanta-admin-secret", "x-quanta-tenant-id": "tenant-alpha"},
        json={
            "provider": "gmail",
            "mailbox_address": "quanta@gmail.com",
            "provider_user_id": "me",
            "access_token": "tenant-gmail-token",
            "mode": "polling",
            "enabled": True,
            "metadata": {"label": "production intake"},
        },
    )
    assert response.status_code == 200
    assert response.json()["mailbox_address"] == "quanta@gmail.com"

    list_response = connector_client.get("/v1/inbound-mailboxes", headers={"x-quanta-tenant-id": "tenant-alpha"})
    assert list_response.status_code == 200
    assert len(list_response.json()) == 1
    assert list_response.json()[0]["provider"] == "gmail"

    empty_other_tenant = connector_client.get("/v1/inbound-mailboxes", headers={"x-quanta-tenant-id": "tenant-beta"})
    assert empty_other_tenant.status_code == 200
    assert empty_other_tenant.json() == []

    delete_response = connector_client.delete(
        "/v1/inbound-mailboxes/gmail",
        headers={"x-quanta-admin-secret": "quanta-admin-secret", "x-quanta-tenant-id": "tenant-alpha"},
    )
    assert delete_response.status_code == 200
    assert delete_response.json()["deleted"] is True


def test_enqueue_inbound_email_job_dedupes_and_runs_with_mailbox_config(connector_client, mock_transport_factory) -> None:
    csv_data = base64.urlsafe_b64encode(Path("samples/acme_census.csv").read_bytes()).decode()
    body_data = base64.urlsafe_b64encode(
        b"Employer: ACME Manufacturing\nBroker: Northstar Benefits\nEffective Date: 07/01/2026\nSitus: TX\nPlease quote dental."
    ).decode()

    mailbox_response = connector_client.put(
        "/v1/inbound-mailboxes/gmail",
        headers={"x-quanta-admin-secret": "quanta-admin-secret", "x-quanta-tenant-id": "tenant-alpha"},
        json={
            "provider": "gmail",
            "mailbox_address": "quanta@gmail.com",
            "provider_user_id": "tenant-user",
            "access_token": "tenant-gmail-token",
            "mode": "polling",
            "enabled": True,
            "metadata": {},
        },
    )
    assert mailbox_response.status_code == 200

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("Authorization") == "Bearer tenant-gmail-token"
        assert "/users/tenant-user/" in request.url.path
        if request.url.path.endswith("/attachments/att-1"):
            return httpx.Response(200, json={"data": csv_data})
        return httpx.Response(
            200,
            json={
                "id": "gmail-queued-1",
                "snippet": "queued gmail message",
                "payload": {
                    "headers": [
                        {"name": "From", "value": "broker@example.com"},
                        {"name": "To", "value": "quotes@strantas.ai"},
                        {"name": "Subject", "value": "Queued Gmail submission"},
                    ],
                    "parts": [
                        {"mimeType": "text/plain", "body": {"data": body_data}},
                        {
                            "mimeType": "text/csv",
                            "filename": "acme_census.csv",
                            "body": {"attachmentId": "att-1"},
                        },
                    ],
                },
            },
        )

    connector_client.app.state.container.gmail_connector.client = mock_transport_factory(handler)

    enqueue_one = connector_client.post(
        "/v1/inbound-emails/enqueue",
        headers={"x-quanta-admin-secret": "quanta-admin-secret", "x-quanta-tenant-id": "tenant-alpha"},
        json={"provider": "gmail", "message_id": "gmail-queued-1", "run_pipeline": False, "event_id": "evt-1"},
    )
    enqueue_two = connector_client.post(
        "/v1/inbound-emails/enqueue",
        headers={"x-quanta-admin-secret": "quanta-admin-secret", "x-quanta-tenant-id": "tenant-alpha"},
        json={"provider": "gmail", "message_id": "gmail-queued-1", "run_pipeline": False, "event_id": "evt-1"},
    )
    assert enqueue_one.status_code == 200
    assert enqueue_two.status_code == 200
    assert enqueue_one.json()["job"]["job_id"] == enqueue_two.json()["job"]["job_id"]

    jobs = connector_client.get("/v1/jobs", headers={"x-quanta-tenant-id": "tenant-alpha"})
    assert jobs.status_code == 200
    assert len(jobs.json()) == 1
    assert jobs.json()[0]["dedupe_key"] == "connector_ingest:tenant-alpha:gmail:gmail-queued-1"

    run_job = connector_client.post(
        "/v1/jobs/run-next",
        headers={"x-quanta-admin-secret": "quanta-admin-secret", "x-quanta-tenant-id": "tenant-alpha"},
    )
    assert run_job.status_code == 200
    assert run_job.json()["status"] == "succeeded"

    submission = connector_client.app.state.container.submission_repository.get("SUB-2026-000001", tenant_id="tenant-alpha")
    assert submission is not None
    assert submission.source_provider == EmailProvider.gmail
    assert submission.connector_message_id == "gmail-queued-1"


def test_gmail_poll_uses_tenant_mailbox_config(connector_client, mock_transport_factory) -> None:
    connector_client.put(
        "/v1/inbound-mailboxes/gmail",
        headers={"x-quanta-admin-secret": "quanta-admin-secret", "x-quanta-tenant-id": "tenant-alpha"},
        json={
            "provider": "gmail",
            "mailbox_address": "customer-quanta@gmail.com",
            "provider_user_id": "tenant-user",
            "access_token": "tenant-gmail-token",
            "mode": "polling",
            "enabled": True,
            "metadata": {"customer": "tenant-alpha"},
        },
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("Authorization") == "Bearer tenant-gmail-token"
        assert request.url.path.endswith("/users/tenant-user/messages")
        return httpx.Response(200, json={"messages": [{"id": "gmail-polled-1"}]})

    connector_client.app.state.container.connector_orchestration.gmail_client = mock_transport_factory(handler)
    response = connector_client.post(
        "/v1/connectors/gmail/poll",
        headers={"x-quanta-admin-secret": "quanta-admin-secret", "x-quanta-tenant-id": "tenant-alpha"},
    )
    assert response.status_code == 200
    assert response.json()["queued"] == 1

    jobs = connector_client.get("/v1/jobs", headers={"x-quanta-tenant-id": "tenant-alpha"})
    assert jobs.status_code == 200
    assert jobs.json()[0]["payload"]["message_id"] == "gmail-polled-1"


def test_graph_poll_uses_tenant_mailbox_config(connector_client, mock_transport_factory) -> None:
    connector_client.put(
        "/v1/inbound-mailboxes/microsoft_graph",
        headers={"x-quanta-admin-secret": "quanta-admin-secret", "x-quanta-tenant-id": "tenant-beta"},
        json={
            "provider": "microsoft_graph",
            "mailbox_address": "rfp@tenant-beta.example",
            "provider_user_id": "tenant-beta-user",
            "access_token": "tenant-graph-token",
            "mode": "polling",
            "enabled": True,
            "metadata": {"customer": "tenant-beta"},
        },
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("Authorization") == "Bearer tenant-graph-token"
        assert request.url.path.endswith("/users/tenant-beta-user/mailFolders/inbox/messages")
        return httpx.Response(200, json={"value": [{"id": "graph-polled-1"}]})

    connector_client.app.state.container.connector_orchestration.graph_client = mock_transport_factory(handler)
    response = connector_client.post(
        "/v1/connectors/microsoft-graph/poll",
        headers={"x-quanta-admin-secret": "quanta-admin-secret", "x-quanta-tenant-id": "tenant-beta"},
    )
    assert response.status_code == 200
    assert response.json()["queued"] == 1

    jobs = connector_client.get("/v1/jobs", headers={"x-quanta-tenant-id": "tenant-beta"})
    assert jobs.status_code == 200
    assert jobs.json()[0]["payload"]["message_id"] == "graph-polled-1"


def test_gmail_push_event_enqueues_connector_poll_job(connector_client) -> None:
    message_data = base64.b64encode(json.dumps({"emailAddress": "tenant-alpha@gmail.com", "historyId": "hist-900"}).encode()).decode()
    response = connector_client.post(
        "/v1/connectors/gmail/events",
        headers={"x-quanta-webhook-secret": "quanta-provider-secret", "x-quanta-tenant-id": "tenant-alpha"},
        json={"message": {"data": message_data, "messageId": "pubsub-1"}},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["queued"] == 1
    assert body["historyId"] == "hist-900"
    assert body["job"]["job_type"] == "connector_poll"
    assert body["job"]["payload"]["provider"] == "gmail"


def test_gmail_poll_job_uses_history_id_for_incremental_fetch(connector_client, mock_transport_factory) -> None:
    connector_client.put(
        "/v1/inbound-mailboxes/gmail",
        headers={"x-quanta-admin-secret": "quanta-admin-secret", "x-quanta-tenant-id": "tenant-hist"},
        json={
            "provider": "gmail",
            "mailbox_address": "tenant-hist@gmail.com",
            "provider_user_id": "tenant-hist-user",
            "access_token": "tenant-hist-token",
            "mode": "polling",
            "enabled": True,
            "metadata": {},
        },
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("Authorization") == "Bearer tenant-hist-token"
        assert request.url.path.endswith("/users/tenant-hist-user/history")
        assert request.url.params["startHistoryId"] == "hist-901"
        return httpx.Response(
            200,
            json={
                "historyId": "hist-902",
                "history": [
                    {"messagesAdded": [{"message": {"id": "gmail-hist-1"}}]},
                    {"messagesAdded": [{"message": {"id": "gmail-hist-2"}}]},
                ],
            },
        )

    connector_client.app.state.container.connector_orchestration.gmail_client = mock_transport_factory(handler)
    job = connector_client.app.state.container.job_queue.enqueue_connector_poll(
        provider=EmailProvider.gmail,
        tenant_id="tenant-hist",
        history_id="hist-901",
        event_id="pubsub-hist-1",
        source="gmail_push",
    )

    run_job = connector_client.post(
        "/v1/jobs/run-next",
        headers={"x-quanta-admin-secret": "quanta-admin-secret", "x-quanta-tenant-id": "tenant-hist"},
    )
    assert run_job.status_code == 200
    assert run_job.json()["job_id"] == job.job_id
    assert run_job.json()["status"] == "succeeded"

    jobs = connector_client.get(
        "/v1/inbound-email-jobs?provider=gmail",
        headers={"x-quanta-admin-secret": "quanta-admin-secret", "x-quanta-tenant-id": "tenant-hist"},
    )
    assert jobs.status_code == 200
    message_ids = {item["payload"].get("message_id") for item in jobs.json() if item["job_type"] == "connector_ingest"}
    assert {"gmail-hist-1", "gmail-hist-2"} <= message_ids


def test_graph_webhook_enqueues_connector_ingest_job(connector_client) -> None:
    connector_client.app.state.container.operations_repository.save_cursor(
        ConnectorCursor(
            provider=EmailProvider.microsoft_graph,
            tenant_id="tenant-graph",
            subscription_id="sub-graph-1",
            status="subscribed",
        )
    )
    response = connector_client.post(
        "/v1/connectors/microsoft-graph/webhook",
        headers={"x-quanta-webhook-secret": "quanta-provider-secret", "x-quanta-tenant-id": "tenant-graph"},
        json={
            "value": [
                {
                    "subscriptionId": "sub-graph-1",
                    "clientState": "quanta",
                    "resource": "/users/tenant-graph/messages/graph-webhook-1",
                    "resourceData": {"id": "graph-webhook-1"},
                }
            ]
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["queued"] == 1
    assert body["jobs"][0]["job_type"] == "connector_ingest"
    assert body["jobs"][0]["payload"]["message_id"] == "graph-webhook-1"


def test_graph_webhook_rejects_invalid_client_state(connector_client) -> None:
    connector_client.app.state.container.operations_repository.save_cursor(
        ConnectorCursor(
            provider=EmailProvider.microsoft_graph,
            tenant_id="tenant-graph",
            subscription_id="sub-graph-1",
            status="subscribed",
        )
    )
    response = connector_client.post(
        "/v1/connectors/microsoft-graph/webhook",
        headers={"x-quanta-webhook-secret": "quanta-provider-secret", "x-quanta-tenant-id": "tenant-graph"},
        json={
            "value": [
                {
                    "subscriptionId": "sub-graph-1",
                    "clientState": "wrong",
                    "resourceData": {"id": "graph-webhook-1"},
                }
            ]
        },
    )
    assert response.status_code == 401


def test_graph_connector_execution_flow(connector_client, mock_transport_factory) -> None:
    pdf_bytes = Path("samples/acme_census.pdf").read_bytes()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("Authorization") == "Bearer graph-token"
        if request.url.path.endswith("/attachments"):
            return httpx.Response(
                200,
                json={
                    "value": [
                        {
                            "name": "acme_census.pdf",
                            "contentType": "application/pdf",
                            "contentBytes": base64.b64encode(pdf_bytes).decode(),
                        }
                    ]
                },
            )
        return httpx.Response(
            200,
            json={
                "subject": "Graph connector submission",
                "bodyPreview": "Employer: ACME Manufacturing\nBroker: Northstar Benefits\nEffective Date: 2026-07-01\nSitus: TX\nRenewal quote for vision and hospital indemnity.",
                "body": {"content": "<p>Graph message body</p>"},
                "from": {"emailAddress": {"address": "broker@example.com"}},
                "toRecipients": [{"emailAddress": {"address": "quotes@strantas.ai"}}],
            },
        )

    connector_client.app.state.container.graph_connector.client = mock_transport_factory(handler)
    response = connector_client.post(
        "/v1/connectors/microsoft-graph/messages/msg-123/ingest",
        headers={"x-quanta-admin-secret": "quanta-admin-secret"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["case_id"].startswith("QNT-")
    assert body["status"] == "normalized"
    assert body["raw_event_storage_key"].startswith("tenants/default/raw-events/graph/")

    output = connector_client.get(f"/v1/output/{body['case_id']}")
    assert output.status_code == 200
    assert output.json()["quoteType"] == "renewal"
    assert "vision" in {item["lobType"] for item in output.json()["lobs"]}


def test_gmail_connector_execution_flow(connector_client, mock_transport_factory) -> None:
    csv_bytes = Path("samples/acme_census.csv").read_bytes()
    pdf_bytes = Path("samples/acme_dental_vision_rfp.pdf").read_bytes()
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as archive:
        archive.writestr("nested/acme_census.csv", csv_bytes)
        archive.writestr("rfp/acme_dental_vision_rfp.pdf", pdf_bytes)
    zip_data = base64.urlsafe_b64encode(zip_buffer.getvalue()).decode()
    body_data = base64.urlsafe_b64encode(
        b"Employer: ACME Manufacturing\nBroker: Northstar Benefits\nEffective Date: 07/01/2026\nSitus: TX\nPlease quote dental, critical illness, accident, and hospital indemnity."
    ).decode()
    html_data = base64.urlsafe_b64encode(
        b"<p>Employer: ACME Manufacturing</p><p>Broker Contact: Marcus Hale</p><p>Please quote dental and vision.</p>"
    ).decode()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("Authorization") == "Bearer gmail-token"
        if request.url.path.endswith("/attachments/att-zip"):
            return httpx.Response(200, json={"data": zip_data})
        return httpx.Response(
            200,
            json={
                "id": "msg-abc",
                "snippet": "Gmail delivered quote request",
                "payload": {
                    "headers": [
                        {"name": "From", "value": "broker@example.com"},
                        {"name": "To", "value": "quotes@strantas.ai"},
                        {"name": "Subject", "value": "Gmail connector submission"},
                    ],
                    "parts": [
                        {
                            "mimeType": "multipart/alternative",
                            "parts": [
                                {"mimeType": "text/plain", "body": {"data": body_data}},
                                {"mimeType": "text/html", "body": {"data": html_data}},
                            ],
                        },
                        {
                            "mimeType": "application/zip",
                            "filename": "gmail_bundle.zip",
                            "body": {"attachmentId": "att-zip"},
                        },
                    ],
                },
            },
        )

    connector_client.app.state.container.gmail_connector.client = mock_transport_factory(handler)
    response = connector_client.post(
        "/v1/connectors/gmail/messages/msg-abc/ingest",
        headers={"x-quanta-admin-secret": "quanta-admin-secret"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["case_id"].startswith("QNT-")
    assert body["raw_event_storage_key"].startswith("tenants/default/raw-events/gmail/")
    assert body["attachment_count"] == 2

    output = connector_client.get(f"/v1/output/{body['case_id']}")
    assert output.status_code == 200
    lob_types = {item["lobType"] for item in output.json()["lobs"]}
    assert {"dental", "vision", "supplemental_ci", "supplemental_accident", "supplemental_hi"} <= lob_types
    assert output.json()["census"]["employeeCount"] > 0

    submission = connector_client.app.state.container.submission_repository.get(body["submission_id"], tenant_id="default")
    assert submission is not None
    assert sorted(item.file_name for item in submission.attachments) == ["acme_census.csv", "acme_dental_vision_rfp.pdf"]
    assert all(item.archive_file_name == "gmail_bundle.zip" for item in submission.attachments)
    assert any(
        "Expanded from archive 'gmail_bundle.zip'" in (evidence.snippet or "")
        for item in submission.attachments
        for evidence in item.evidence_references
    )


def test_provider_email_gmail_payload_accepts_zip_attachments(client) -> None:
    csv_bytes = Path("samples/acme_census.csv").read_bytes()
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as archive:
        archive.writestr("broker/acme_census.csv", csv_bytes)
    zip_data = base64.urlsafe_b64encode(zip_buffer.getvalue()).decode()
    body_data = base64.urlsafe_b64encode(
        b"Employer: ACME Manufacturing\nBroker: Northstar Benefits\nEffective Date: 2026-07-01\nSitus: TX\nPlease quote dental."
    ).decode()

    listener = client.post(
        "/v1/listener/provider-email",
        json={
            "provider": "gmail",
            "payload": {
                "id": "gmail-provider-1",
                "snippet": "provider gmail payload",
                "payload": {
                    "headers": [
                        {"name": "From", "value": "broker@example.com"},
                        {"name": "To", "value": "quotes@strantas.ai"},
                        {"name": "Subject", "value": "Provider Gmail zipped intake"},
                    ],
                    "parts": [
                        {"mimeType": "text/plain", "body": {"data": body_data}},
                        {
                            "mimeType": "application/zip",
                            "filename": "provider_bundle.zip",
                            "body": {"data": zip_data},
                        },
                    ],
                },
            },
        },
    )
    assert listener.status_code == 202
    submission_id = listener.json()["submission_id"]
    assert listener.json()["attachment_count"] == 1
    status_response = client.get(f"/v1/submissions/{submission_id}")
    assert status_response.status_code == 200
    assert status_response.json()["source_provider"] == "gmail"

    submission = client.app.state.container.submission_repository.get(submission_id, tenant_id="default")
    assert submission is not None
    assert [item.file_name for item in submission.attachments] == ["acme_census.csv"]
    assert submission.attachments[0].archive_file_name == "provider_bundle.zip"
    assert submission.attachments[0].archive_member_path == "broker/acme_census.csv"


def test_smtp_webhook_listener_flow(client) -> None:
    pdf_bytes = Path("samples/acme_census.pdf").read_bytes()
    listener = client.post(
        "/v1/listener/smtp-webhook",
        headers={"x-quanta-webhook-secret": "quanta-dev-secret"},
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
    status_response = client.get(f"/v1/submissions/{submission_id}")
    assert status_response.status_code == 200
    assert status_response.json()["source_provider"] == "smtp_webhook"
    assert status_response.json()["raw_event_storage_key"].startswith("tenants/default/raw-events/smtp/")
    extract = client.post("/v1/extractor/run", params={"submission_id": submission_id})
    assert extract.status_code == 200
    assert "dental" in extract.json()["detected_lobs"]


def test_smtp_webhook_rejects_invalid_secret(client) -> None:
    response = client.post(
        "/v1/listener/smtp-webhook",
        headers={"x-quanta-webhook-secret": "wrong-secret"},
        json={
            "from_email": "broker@example.com",
            "to_emails": ["quotes@strantas.ai"],
            "subject": "bad secret",
            "text": "hello",
            "attachments": [],
        },
    )
    assert response.status_code == 401


def test_connector_endpoints_reject_invalid_admin_secret(connector_client) -> None:
    response = connector_client.post(
        "/v1/connectors/microsoft-graph/messages/msg-123/ingest",
        headers={"x-quanta-admin-secret": "wrong-secret"},
    )
    assert response.status_code == 401


def test_metrics_endpoint_exposes_connector_and_job_counters(connector_client, mock_transport_factory) -> None:
    pdf_bytes = Path("samples/acme_census.pdf").read_bytes()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/attachments"):
            return httpx.Response(
                200,
                json={
                    "value": [
                        {
                            "name": "acme_census.pdf",
                            "contentType": "application/pdf",
                            "contentBytes": base64.b64encode(pdf_bytes).decode(),
                        }
                    ]
                },
            )
        return httpx.Response(
            200,
            json={
                "subject": "Graph metrics submission",
                "bodyPreview": "Employer: ACME Manufacturing\nBroker: Northstar Benefits\nEffective Date: 2026-07-01\nSitus: TX\nPlease quote life.",
                "body": {"content": "hello"},
                "from": {"emailAddress": {"address": "broker@example.com"}},
                "toRecipients": [{"emailAddress": {"address": "quotes@strantas.ai"}}],
            },
        )

    connector_client.app.state.container.graph_connector.client = mock_transport_factory(handler)
    response = connector_client.post(
        "/v1/connectors/microsoft-graph/messages/msg-metrics/ingest",
        headers={"x-quanta-admin-secret": "quanta-admin-secret"},
    )
    assert response.status_code == 200

    metrics = connector_client.get("/metrics")
    assert metrics.status_code == 200
    snapshot = metrics.json()
    assert snapshot["counters"]["connector.graph.success"] >= 1
    assert snapshot["counters"]["job.submission.success"] >= 1
    assert snapshot["timings"]["connector.graph.ingest_ms"]["count"] >= 1

    prometheus = connector_client.get("/metrics/prometheus")
    assert prometheus.status_code == 200
    assert "connector_graph_success" in prometheus.text
    assert "job_submission_success" in prometheus.text


def test_otlp_metrics_export_is_attempted(connector_client, mock_transport_factory) -> None:
    deliveries: list[dict] = []

    def otlp_handler(request: httpx.Request) -> httpx.Response:
        deliveries.append(json.loads(request.content.decode()))
        return httpx.Response(200, json={"ok": True})

    connector_client.app.state.container.metrics.settings.metrics_otlp_endpoint = "https://metrics.example.test/v1/metrics"
    connector_client.app.state.container.metrics.settings.metrics_otlp_secret = "metrics-secret"
    connector_client.app.state.container.metrics.client = mock_transport_factory(otlp_handler)

    connector_client.get("/health")
    assert deliveries
    assert deliveries[-1]["resource"]["service.name"] == "strantas-quanta"
    assert "http.request.count" in deliveries[-1]["metrics"]["counters"]


def test_connector_polling_enqueues_and_runs_graph_jobs(connector_client, mock_transport_factory) -> None:
    pdf_bytes = Path("samples/acme_census.pdf").read_bytes()

    def orchestration_handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path.endswith("/subscriptions"):
            return httpx.Response(200, json={"id": "sub-graph-1", "expirationDateTime": "2026-03-13T12:00:00Z"})
        if request.url.path.endswith("/mailFolders/inbox/messages"):
            return httpx.Response(200, json={"value": [{"id": "graph-polled-1"}]})
        if request.url.path.endswith("/graph-polled-1/attachments"):
            return httpx.Response(
                200,
                json={"value": [{"name": "acme_census.pdf", "contentType": "application/pdf", "contentBytes": base64.b64encode(pdf_bytes).decode()}]},
            )
        return httpx.Response(
            200,
            json={
                "subject": "Polled graph submission",
                "bodyPreview": "Employer: ACME Manufacturing\nBroker: Northstar Benefits\nEffective Date: 2026-07-01\nSitus: TX\nPlease quote life and vision.",
                "body": {"content": "<p>polled</p>"},
                "from": {"emailAddress": {"address": "broker@example.com"}},
                "toRecipients": [{"emailAddress": {"address": "quotes@strantas.ai"}}],
            },
        )

    client = mock_transport_factory(orchestration_handler)
    connector_client.app.state.container.graph_connector.client = client
    connector_client.app.state.container.connector_orchestration.graph_client = client

    refresh = connector_client.post(
        "/v1/connectors/microsoft-graph/subscriptions/refresh",
        headers={"x-quanta-admin-secret": "quanta-admin-secret"},
    )
    assert refresh.status_code == 200
    assert refresh.json()["subscription_id"] == "sub-graph-1"

    poll = connector_client.post(
        "/v1/connectors/microsoft-graph/poll",
        headers={"x-quanta-admin-secret": "quanta-admin-secret"},
    )
    assert poll.status_code == 200
    assert poll.json()["queued"] == 1

    jobs = connector_client.get("/v1/jobs")
    assert jobs.status_code == 200
    assert jobs.json()[0]["job_type"] == "connector_ingest"

    run_job = connector_client.post(
        "/v1/jobs/run-next",
        headers={"x-quanta-admin-secret": "quanta-admin-secret"},
    )
    assert run_job.status_code == 200
    assert run_job.json()["status"] == "succeeded"

    alerts = connector_client.get("/v1/alerts")
    assert alerts.status_code == 200

    connector_state = connector_client.get("/v1/connectors/state")
    assert connector_state.status_code == 200
    assert any(item["provider"] == "microsoft_graph" for item in connector_state.json())


def test_tenant_headers_filter_connector_state_jobs_and_alerts(connector_client) -> None:
    connector_client.app.state.container.job_queue.enqueue(
        job_type=JobType.submission_pipeline,
        payload={"submission_id": "SUB-tenant-a", "tenant_id": "tenant-a"},
        tenant_id="tenant-a",
    )
    connector_client.app.state.container.job_queue.enqueue(
        job_type=JobType.submission_pipeline,
        payload={"submission_id": "SUB-tenant-b", "tenant_id": "tenant-b"},
        tenant_id="tenant-b",
    )
    connector_client.app.state.container.alert_service.emit(
        severity=AlertSeverity.warning,
        source="tenant-test",
        message="tenant-a alert",
        tenant_id="tenant-a",
    )
    connector_client.app.state.container.alert_service.emit(
        severity=AlertSeverity.warning,
        source="tenant-test",
        message="tenant-b alert",
        tenant_id="tenant-b",
    )
    connector_client.app.state.container.operations_repository.save_cursor(
        ConnectorCursor(
            provider=EmailProvider.microsoft_graph,
            tenant_id="tenant-a",
            subscription_id="sub-tenant-a",
            cursor="msg-a",
        )
    )
    connector_client.app.state.container.operations_repository.save_cursor(
        ConnectorCursor(
            provider=EmailProvider.gmail,
            tenant_id="tenant-b",
            subscription_id="sub-tenant-b",
            cursor="msg-b",
        )
    )
    jobs = connector_client.get("/v1/jobs", headers={"x-quanta-tenant-id": "tenant-a"})
    assert jobs.status_code == 200
    assert len(jobs.json()) == 1
    assert jobs.json()[0]["tenant_id"] == "tenant-a"

    alerts = connector_client.get("/v1/alerts", headers={"x-quanta-tenant-id": "tenant-b"})
    assert alerts.status_code == 200
    assert all(item["tenant_id"] == "tenant-b" for item in alerts.json())

    cursors = connector_client.get("/v1/connectors/state", headers={"x-quanta-tenant-id": "tenant-a"})
    assert cursors.status_code == 200
    assert len(cursors.json()) == 1
    assert cursors.json()[0]["tenant_id"] == "tenant-a"



def test_worker_tick_auto_refreshes_and_drains_jobs(connector_client, mock_transport_factory) -> None:
    pdf_bytes = Path("samples/acme_census.pdf").read_bytes()

    def graph_handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path.endswith("/subscriptions"):
            return httpx.Response(200, json={"id": "sub-worker-graph", "expirationDateTime": "2026-03-13T12:00:00Z"})
        if request.url.path.endswith("/mailFolders/inbox/messages"):
            return httpx.Response(200, json={"value": [{"id": "graph-worker-1"}]})
        if request.url.path.endswith("/graph-worker-1/attachments"):
            return httpx.Response(200, json={"value": [{"name": "acme_census.pdf", "contentType": "application/pdf", "contentBytes": base64.b64encode(pdf_bytes).decode()}]})
        return httpx.Response(
            200,
            json={
                "subject": "Worker graph submission",
                "bodyPreview": "Employer: ACME Manufacturing\nBroker: Northstar Benefits\nEffective Date: 2026-07-01\nSitus: TX\nPlease quote life.",
                "body": {"content": "worker"},
                "from": {"emailAddress": {"address": "broker@example.com"}},
                "toRecipients": [{"emailAddress": {"address": "quotes@strantas.ai"}}],
            },
        )

    def gmail_handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path.endswith("/watch"):
            return httpx.Response(200, json={"historyId": "hist-123", "expiration": "1773331200000"})
        if request.url.path.endswith("/messages") and request.method == "GET":
            return httpx.Response(200, json={"messages": []})
        return httpx.Response(200, json={})

    graph_client = mock_transport_factory(graph_handler)
    gmail_client = mock_transport_factory(gmail_handler)
    connector_client.app.state.container.graph_connector.client = graph_client
    connector_client.app.state.container.gmail_connector.client = gmail_client
    connector_client.app.state.container.connector_orchestration.graph_client = graph_client
    connector_client.app.state.container.connector_orchestration.gmail_client = gmail_client

    tick = connector_client.post(
        "/v1/worker/tick",
        headers={"x-quanta-admin-secret": "quanta-admin-secret"},
    )
    assert tick.status_code == 200
    body = tick.json()
    assert body["jobsProcessed"] >= 1
    assert any(item["provider"] == "microsoft_graph" for item in body["refreshed"])
    assert any(item["provider"] == "gmail" for item in body["refreshed"])


def test_worker_tick_discovers_tenant_mailboxes_and_drains_connector_jobs(connector_client, mock_transport_factory) -> None:
    pdf_bytes = Path("samples/acme_census.pdf").read_bytes()

    connector_client.put(
        "/v1/inbound-mailboxes/gmail",
        headers={"x-quanta-admin-secret": "quanta-admin-secret", "x-quanta-tenant-id": "tenant-worker"},
        json={
            "provider": "gmail",
            "mailbox_address": "tenant-worker@gmail.com",
            "provider_user_id": "tenant-worker-user",
            "access_token": "tenant-worker-token",
            "mode": "polling",
            "enabled": True,
            "metadata": {"customer": "tenant-worker"},
        },
    )

    def gmail_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/users/tenant-worker-user/messages") and request.method == "GET":
            assert request.headers.get("Authorization") == "Bearer tenant-worker-token"
            return httpx.Response(200, json={"messages": [{"id": "gmail-worker-tenant-1"}]})
        if request.url.path.endswith("/attachments/att-worker-1"):
            assert request.headers.get("Authorization") == "Bearer tenant-worker-token"
            return httpx.Response(200, json={"data": base64.urlsafe_b64encode(pdf_bytes).decode().rstrip("=")})
        if request.url.path.endswith("/users/tenant-worker-user/messages/gmail-worker-tenant-1"):
            assert request.headers.get("Authorization") == "Bearer tenant-worker-token"
            return httpx.Response(
                200,
                json={
                    "id": "gmail-worker-tenant-1",
                    "payload": {
                        "headers": [
                            {"name": "From", "value": "broker@example.com"},
                            {"name": "To", "value": "quotes@tenant-worker.example"},
                            {"name": "Subject", "value": "Worker tenant gmail submission"},
                        ],
                        "parts": [
                            {
                                "mimeType": "text/plain",
                                "body": {
                                    "data": base64.urlsafe_b64encode(
                                        b"Employer: ACME Manufacturing\nBroker: Northstar Benefits\nEffective Date: 2026-07-01\nSitus: TX\nPlease quote life."
                                    ).decode().rstrip("=")
                                },
                            },
                            {
                                "mimeType": "application/pdf",
                                "filename": "acme_census.pdf",
                                "body": {"attachmentId": "att-worker-1"},
                            },
                        ],
                    },
                },
            )
        if request.url.path.endswith("/messages") and request.method == "GET":
            return httpx.Response(200, json={"messages": []})
        if request.method == "POST" and request.url.path.endswith("/watch"):
            return httpx.Response(200, json={"historyId": "hist-worker", "expiration": "1773331200000"})
        return httpx.Response(200, json={})

    client = mock_transport_factory(gmail_handler)
    connector_client.app.state.container.gmail_connector.client = client
    connector_client.app.state.container.connector_orchestration.gmail_client = client

    tick = connector_client.post(
        "/v1/worker/tick",
        headers={"x-quanta-admin-secret": "quanta-admin-secret"},
    )
    assert tick.status_code == 200
    body = tick.json()
    assert body["jobsProcessed"] >= 1
    assert any(item["tenantId"] == "tenant-worker" for item in body["tenants"])

    submission = connector_client.app.state.container.submission_repository.get("SUB-2026-000001", tenant_id="tenant-worker")
    assert submission is not None
    assert submission.source_provider == EmailProvider.gmail
    assert submission.connector_message_id == "gmail-worker-tenant-1"


def test_dead_letter_and_replay_flow_for_failed_job(connector_client, mock_transport_factory) -> None:
    def failing_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "upstream down"})

    client = mock_transport_factory(failing_handler)
    connector_client.app.state.container.gmail_connector.client = client
    connector_client.app.state.container.connector_orchestration.gmail_client = client

    poll = connector_client.post(
        "/v1/connectors/gmail/poll",
        headers={"x-quanta-admin-secret": "quanta-admin-secret"},
    )
    assert poll.status_code == 502 or poll.status_code == 500

    queued = connector_client.app.state.container.job_queue.enqueue(
        job_type=JobType.connector_ingest,
        payload={"provider": "gmail", "message_id": "gmail-fail-1", "run_pipeline": False},
        max_attempts=1,
    )
    run_job = connector_client.post(
        "/v1/jobs/run-next",
        headers={"x-quanta-admin-secret": "quanta-admin-secret"},
    )
    assert run_job.status_code == 200
    assert run_job.json()["status"] == "dead_letter"

    replay = connector_client.post(
        f"/v1/jobs/{queued.job_id}/replay",
        headers={"x-quanta-admin-secret": "quanta-admin-secret"},
    )
    assert replay.status_code == 200
    assert replay.json()["status"] == "queued"

    alerts = connector_client.get("/v1/alerts")
    assert any("dead letter" in item["message"].lower() for item in alerts.json())


def test_inbound_email_dead_letter_dashboard_and_replay_endpoint(connector_client, mock_transport_factory) -> None:
    def failing_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "upstream down"})

    client = mock_transport_factory(failing_handler)
    connector_client.app.state.container.gmail_connector.client = client
    connector_client.app.state.container.connector_orchestration.gmail_client = client

    queued = connector_client.app.state.container.job_queue.enqueue_connector_ingest(
        provider=EmailProvider.gmail,
        message_id="gmail-dead-letter-1",
        tenant_id="tenant-ops",
        run_pipeline=False,
    )
    queued.max_attempts = 1
    connector_client.app.state.container.operations_repository.update_job(queued)

    run_job = connector_client.post(
        "/v1/jobs/run-next",
        headers={"x-quanta-admin-secret": "quanta-admin-secret", "x-quanta-tenant-id": "tenant-ops"},
    )
    assert run_job.status_code == 200
    assert run_job.json()["status"] == "dead_letter"

    dead_letter = connector_client.get(
        "/v1/inbound-email-jobs/dead-letter",
        headers={"x-quanta-admin-secret": "quanta-admin-secret", "x-quanta-tenant-id": "tenant-ops"},
    )
    assert dead_letter.status_code == 200
    assert len(dead_letter.json()) == 1
    assert dead_letter.json()[0]["job_id"] == queued.job_id

    dashboard = connector_client.get(
        "/v1/inbound-email-jobs/dashboard",
        headers={"x-quanta-admin-secret": "quanta-admin-secret", "x-quanta-tenant-id": "tenant-ops"},
    )
    assert dashboard.status_code == 200
    assert dashboard.json()["deadLetterCount"] == 1
    assert dashboard.json()["byProvider"]["gmail"] == 1

    replay = connector_client.post(
        f"/v1/inbound-email-jobs/{queued.job_id}/replay",
        headers={"x-quanta-admin-secret": "quanta-admin-secret"},
    )
    assert replay.status_code == 200
    assert replay.json()["status"] == "queued"
    assert replay.json()["attempts"] == 0


def test_inbound_email_dashboard_includes_per_tenant_stats_and_replay_audit(connector_client) -> None:
    first = connector_client.app.state.container.job_queue.enqueue_connector_ingest(
        provider=EmailProvider.gmail,
        message_id="gmail-dash-1",
        tenant_id="tenant-a",
        run_pipeline=False,
    )
    second = connector_client.app.state.container.job_queue.enqueue_connector_ingest(
        provider=EmailProvider.microsoft_graph,
        message_id="graph-dash-1",
        tenant_id="tenant-b",
        run_pipeline=False,
    )
    first.status = JobStatus.succeeded
    first.completed_at = datetime.now(timezone.utc)
    connector_client.app.state.container.operations_repository.update_job(first)
    second.status = JobStatus.dead_letter
    second.dead_letter_reason = "forced dead letter"
    connector_client.app.state.container.operations_repository.update_job(second)
    connector_client.app.state.container.job_queue.replay(second.job_id)

    dashboard = connector_client.get(
        "/v1/inbound-email-jobs/dashboard?all_tenants=true",
        headers={"x-quanta-admin-secret": "quanta-admin-secret"},
    )
    assert dashboard.status_code == 200
    body = dashboard.json()
    assert body["perTenant"]["tenant-a"]["byProvider"]["gmail"] == 1
    assert body["perTenant"]["tenant-b"]["byProvider"]["microsoft_graph"] == 1
    assert body["lastProcessedMessage"]["messageId"] == "gmail-dash-1"
    assert any(item["job_type"] == "connector_ingest" for item in body["replayAuditTrail"])

    replay_audit = connector_client.get(
        "/v1/inbound-email-jobs/replay-audit?all_tenants=true",
        headers={"x-quanta-admin-secret": "quanta-admin-secret"},
    )
    assert replay_audit.status_code == 200
    assert any(item["job_type"] == "connector_ingest" for item in replay_audit.json())


def test_inbound_email_bulk_replay_and_lag_thresholds(connector_client) -> None:
    gmail_dead = connector_client.app.state.container.job_queue.enqueue_connector_ingest(
        provider=EmailProvider.gmail,
        message_id="gmail-bulk-1",
        tenant_id="tenant-bulk",
        run_pipeline=False,
    )
    graph_dead = connector_client.app.state.container.job_queue.enqueue_connector_ingest(
        provider=EmailProvider.microsoft_graph,
        message_id="graph-bulk-1",
        tenant_id="tenant-bulk",
        run_pipeline=False,
    )
    lagged = connector_client.app.state.container.job_queue.enqueue_connector_poll(
        provider=EmailProvider.gmail,
        tenant_id="tenant-bulk",
        history_id="hist-lagged",
        event_id="lagged-1",
        source="gmail_push",
    )
    gmail_dead.status = JobStatus.dead_letter
    gmail_dead.dead_letter_reason = "gmail failed"
    graph_dead.status = JobStatus.dead_letter
    graph_dead.dead_letter_reason = "graph failed"
    lagged.available_at = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=120)
    connector_client.app.state.container.operations_repository.update_job(gmail_dead)
    connector_client.app.state.container.operations_repository.update_job(graph_dead)
    connector_client.app.state.container.operations_repository.update_job(lagged)

    dashboard = connector_client.get(
        "/v1/inbound-email-jobs/dashboard?lag_threshold_seconds=60",
        headers={"x-quanta-admin-secret": "quanta-admin-secret", "x-quanta-tenant-id": "tenant-bulk"},
    )
    assert dashboard.status_code == 200
    assert dashboard.json()["lagThresholdBreached"] is True
    assert dashboard.json()["perTenant"]["tenant-bulk"]["lagThresholdBreached"] is True

    replay = connector_client.post(
        "/v1/inbound-email-jobs/replay?provider=gmail",
        headers={"x-quanta-admin-secret": "quanta-admin-secret", "x-quanta-tenant-id": "tenant-bulk"},
    )
    assert replay.status_code == 200
    assert replay.json()["replayed"] == 1
    assert replay.json()["jobs"][0]["payload"]["provider"] == "gmail"


def test_queue_monitor_records_lag_metrics_and_alerts(connector_client) -> None:
    lagged = connector_client.app.state.container.job_queue.enqueue_connector_poll(
        provider=EmailProvider.gmail,
        tenant_id="tenant-lag",
        history_id="hist-lag-metric",
        event_id="lag-alert-1",
        source="gmail_push",
    )
    lagged.available_at = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=400)
    connector_client.app.state.container.operations_repository.update_job(lagged)

    dashboard = connector_client.app.state.container.job_queue.monitor_inbound_email_queue(tenant_id="tenant-lag")
    assert dashboard["lagThresholdBreached"] is True

    metrics = connector_client.get("/metrics")
    assert metrics.status_code == 200
    gauges = metrics.json()["gauges"]
    assert gauges["inbound.email.queue.oldest_age_seconds.tenant-lag"] >= 300

    alerts = connector_client.get("/v1/alerts", headers={"x-quanta-tenant-id": "tenant-lag"})
    assert alerts.status_code == 200
    assert any(item["source"] == "job_queue.lag" for item in alerts.json())


def test_alert_service_delivers_webhook_when_configured(connector_client, mock_transport_factory) -> None:
    deliveries: list[dict] = []

    def alert_handler(request: httpx.Request) -> httpx.Response:
        deliveries.append(json.loads(request.content.decode()))
        return httpx.Response(200, json={"ok": True})

    connector_client.app.state.container.alert_service.settings.alert_webhook_url = "https://alerts.example.test/quanta"
    connector_client.app.state.container.alert_service.client = mock_transport_factory(alert_handler)

    connector_client.app.state.container.alert_service.emit(
        severity=AlertSeverity.warning,
        source="integration_test",
        message="Webhook delivery check",
        context={"kind": "test"},
    )
    assert deliveries[0]["source"] == "integration_test"


def test_attachment_conflicts_generate_normalization_warnings(client) -> None:
    payload = _submission_body()
    plan_summary = """
    Employer: ACME Manufacturing
    Broker: Northstar Benefits
    Effective Date: 07/01/2026
    Situs State: TX
    Market Segment: Large Group
    Dental PPO 100 / 80 / 50 annual max $1,500 deductible $50 employer paid.
    Dental PPO 90 / 80 / 50 annual max $2,000 deductible $75 employee paid.
    Vision exam copay $10 materials copay $25 every 12 months frame allowance $150.
    Critical illness $30,000 guarantee issue $10,000 wellness $50.
    Accident off-job benefit $300 er visit benefit $150.
    Hospital indemnity $1,000 admission benefit $1,500.
    """
    payload["attachments"] = [
        {
            "file_name": "plan_summary.csv",
            "content_type": "text/csv",
            "content_base64": base64.b64encode(plan_summary.encode()).decode(),
        }
    ]

    listener = client.post("/v1/listener/email", json=payload)
    assert listener.status_code == 202
    submission_id = listener.json()["submission_id"]

    normalize = client.post("/v1/normalizer/run", params={"submission_id": submission_id})
    assert normalize.status_code == 200
    quote = normalize.json()["quote_request"]
    assert quote["market_segment"] == "large_group"

    dental_lob = next(item for item in normalize.json()["lob_requests"] if item["lob_type"] == "dental")
    assert any("dental contribution_details conflict resolved" in warning for warning in dental_lob["warnings"])
    assert dental_lob["requested_plan_designs"][0]["attributes"]["annual_maximum"] == 2000.0
    assert dental_lob["field_results"]["plan_type"]["confidence"] > 0


def test_field_level_results_present_on_quote_and_lob(client) -> None:
    payload = _submission_body()
    payload["attachments"] = [
        {
            "file_name": "acme_census.csv",
            "content_type": "text/csv",
            "content_base64": base64.b64encode(Path("samples/acme_census.csv").read_bytes()).decode(),
        }
    ]
    listener = client.post("/v1/listener/email", json=payload)
    submission_id = listener.json()["submission_id"]
    normalize = client.post("/v1/normalizer/run", params={"submission_id": submission_id})
    assert normalize.status_code == 200
    quote = normalize.json()["quote_request"]
    assert quote["field_results"]["employer_name"]["value"] == "ACME Manufacturing"
    assert quote["field_results"]["effective_date"]["confidence"] > 0
    lob = next(item for item in normalize.json()["lob_requests"] if item["lob_type"] == "group_life")
    assert lob["field_results"]["plan_type"]["value"] == "basic_life"


def test_multi_document_contradictions_are_normalized_with_warnings(client) -> None:
    payload = _submission_body()
    payload["body_text"] = (
        "Employer: ACME Manufacturing\nBroker: Northstar Benefits\nEffective Date: 2026-07-01\n"
        "Due Date: 2026-05-15\nSitus: TX\nIncumbent: Guardian\nPlease quote dental and vision."
    )
    first_doc = (
        "Employer: ACME Manufacturing\nBroker: Northstar Benefits\nEffective Date: 07/01/2026\n"
        "Due Date: 05/20/2026\nSitus State: TX\nIncumbent Carrier: MetLife\nMarket Segment: Large Group\n"
        "Dental PPO 100 / 80 / 50 annual max $1,500 deductible $50 employer paid."
    )
    second_doc = (
        "Employer: ACME Manufacturing\nBroker: Northstar Benefits\nEffective Date: 07/01/2026\n"
        "Due Date: 05/18/2026\nSitus State: OK\nIncumbent Carrier: Guardian\nMarket Segment: Mid Market\n"
        "Vision exam copay $10 materials copay $25 every 12 months frame allowance $150."
    )
    payload["attachments"] = [
        {
            "file_name": "benefits_one.csv",
            "content_type": "text/csv",
            "content_base64": base64.b64encode(first_doc.encode()).decode(),
        },
        {
            "file_name": "benefits_two.csv",
            "content_type": "text/csv",
            "content_base64": base64.b64encode(second_doc.encode()).decode(),
        },
    ]
    listener = client.post("/v1/listener/email", json=payload)
    submission_id = listener.json()["submission_id"]

    normalize = client.post("/v1/normalizer/run", params={"submission_id": submission_id})
    assert normalize.status_code == 200
    quote = normalize.json()["quote_request"]
    assert quote["situs_state"] == "TX"
    assert quote["incumbent_carrier"] == "Guardian"
    assert quote["response_due_date"] == "2026-05-15"
    assert any("contradiction detected across attachments" in warning for warning in quote["warnings"])


def test_alembic_downgrade_and_upgrade_round_trip() -> None:
    import os
    import subprocess

    root = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env["QUANTA_DATABASE_URL"] = "postgresql+psycopg://quanta:quanta@127.0.0.1:5432/quanta"
    subprocess.run(["alembic", "downgrade", "base"], cwd=root, env=env, check=True)
    subprocess.run(["alembic", "upgrade", "head"], cwd=root, env=env, check=True)
