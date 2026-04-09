from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


def section(title: str, body: str, styles) -> list:
    return [
        Paragraph(title, styles["Heading2"]),
        Spacer(1, 4),
        Paragraph(body, styles["BodyText"]),
        Spacer(1, 10),
    ]


def main() -> None:
    output_path = Path("samples/acme_dental_vision_rfp.pdf")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="SmallCaps", parent=styles["Heading3"], textColor=colors.HexColor("#184b8a")))

    doc = SimpleDocTemplate(str(output_path), pagesize=letter, leftMargin=40, rightMargin=40, topMargin=36, bottomMargin=36)
    story = [
        Paragraph("ACME Manufacturing Dental and Vision RFP", styles["Title"]),
        Spacer(1, 8),
        Paragraph("Synthetic submission for extraction validation", styles["Italic"]),
        Spacer(1, 16),
    ]

    story.extend(
        section(
            "Submission Summary",
            (
                "Employer: ACME Manufacturing<br/>"
                "Employer Contact: Elena Brooks<br/>"
                "Employer Email: elena.brooks@acmemfg.example<br/>"
                "Broker: Northstar Benefits<br/>"
                "Broker Contact: Marcus Hale<br/>"
                "Broker Email: marcus.hale@northstar.example<br/>"
                "Effective Date: 2026-07-01<br/>"
                "Due Date: 2026-05-15<br/>"
                "Situs: TX<br/>"
                "Worksite Address: 2500 Foundry Way<br/>"
                "City: Dallas<br/>"
                "State: TX<br/>"
                "Zip: 75201"
            ),
            styles,
        )
    )

    story.extend(
        section(
            "Dental Plan",
            (
                "Requested dental plan: PPO. Preventive 100%, Basic 80%, Major 50%. "
                "Annual maximum $2,000. Deductible $50. Orthodontia 50% to age 19. "
                "Office visit copay $15. Class 1: Executives. Class 2: Salaried and Hourly. "
                "Eligibility: all full-time employees working 30 or more hours per week on the first of the month following 30 days. "
                "Employer pays 100% employee only coverage. Dependents voluntary. Participation minimum 75% of eligible employees. "
                "Service waiting periods: basic services 6 months, major services 12 months."
            ),
            styles,
        )
    )

    story.extend(
        section(
            "Vision Plan",
            (
                "Requested vision plan: Exam copay $10. Materials copay $25. Lens copay $25. "
                "Frame allowance $180. Contact allowance $150. Frequency every 12 months. "
                "Laser vision correction $250. Class 1: Executives. Eligibility: all benefit eligible employees working 30+ hours per week, day one coverage. "
                "Employer pays 100% for employees and employee pays 50% for dependents. Participation 50% of eligible employees electing coverage."
            ),
            styles,
        )
    )

    story.extend(
        section(
            "Census",
            "The following census is included for testing document classification, evidence extraction, and downstream carrier mapping.",
            styles,
        )
    )

    census_rows = [
        ["employee_id", "first_name", "last_name", "age", "state", "salary", "class", "dependent_count"],
        ["2001", "Elena", "Brooks", "38", "TX", "91000", "Executive", "2"],
        ["2002", "Marcus", "Hale", "44", "TX", "83000", "Salaried", "1"],
        ["2003", "Jade", "Foster", "31", "OK", "61000", "Hourly", "0"],
        ["2004", "Owen", "Price", "52", "LA", "102000", "Salaried", "3"],
    ]
    table = Table(census_rows, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#143d73")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#8aa4c6")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.HexColor("#eef4ff")]),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    story.append(table)

    doc.build(story)


if __name__ == "__main__":
    main()
