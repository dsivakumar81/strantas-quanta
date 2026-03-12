from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Spacer, Table, TableStyle, Paragraph
from reportlab.lib.styles import getSampleStyleSheet


def main() -> None:
    output_path = Path("samples/acme_census.pdf")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    doc = SimpleDocTemplate(str(output_path), pagesize=letter)
    styles = getSampleStyleSheet()
    story = [Paragraph("ACME Manufacturing Census", styles["Title"]), Spacer(1, 12)]
    story.extend(
        [
            Paragraph("Employer: ACME Manufacturing", styles["BodyText"]),
            Paragraph("Broker: Northstar Benefits", styles["BodyText"]),
            Paragraph("Effective Date: 2026-07-01", styles["BodyText"]),
            Paragraph("Situs: TX", styles["BodyText"]),
            Paragraph("Requested lines: Basic Life, LTD, Dental PPO", styles["Heading2"]),
            Paragraph("Basic Life benefit: 2x salary with max benefit $500,000.", styles["BodyText"]),
            Paragraph("LTD benefit: 60% of earnings, elimination period of 90 days, max monthly benefit $10,000.", styles["BodyText"]),
            Paragraph("Dental plan: PPO design requested. Employer paid 100/80/50 coverage tiers.", styles["BodyText"]),
            Paragraph("Vision plan: exam copay $10 and materials copay $25.", styles["BodyText"]),
            Paragraph("Critical illness benefit requested at $30,000.", styles["BodyText"]),
            Paragraph("Accident coverage requested.", styles["BodyText"]),
            Paragraph("Hospital indemnity benefit requested at $2,000.", styles["BodyText"]),
            Spacer(1, 12),
        ]
    )

    data = [
        ["employee_id", "first_name", "last_name", "age", "state", "salary", "class", "dependent_count"],
        ["1001", "Ava", "Lopez", "34", "TX", "72000", "Salaried", "2"],
        ["1002", "Noah", "Patel", "41", "TX", "88000", "Executive", "1"],
        ["1003", "Mia", "Nguyen", "29", "OK", "54000", "Hourly", "0"],
        ["1004", "Liam", "Carter", "53", "LA", "99000", "Salaried", "3"],
    ]
    table = Table(data, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4b99")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.HexColor("#eef4ff")]),
            ]
        )
    )
    story.append(table)
    doc.build(story)


if __name__ == "__main__":
    main()
