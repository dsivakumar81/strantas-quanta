from pathlib import Path

from PIL import Image, ImageDraw, ImageFont
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas


LINES = [
    "employee_id first_name last_name age state salary class dependent_count",
    "1001 Ava Lopez 34 TX 72000 Salaried 2",
    "1002 Noah Patel 41 TX 88000 Executive 1",
    "1003 Mia Nguyen 29 OK 54000 Hourly 0",
    "1004 Liam Carter 53 LA 99000 Salaried 3",
]


def main() -> None:
    output_dir = Path("samples")
    output_dir.mkdir(parents=True, exist_ok=True)
    image_path = output_dir / "acme_census_scanned.png"
    pdf_path = output_dir / "acme_census_scanned.pdf"

    image = Image.new("RGB", (1800, 900), "white")
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default(size=32) if hasattr(ImageFont, "load_default") else ImageFont.load_default()
    y = 80
    for line in LINES:
        draw.text((60, y), line, fill="black", font=font)
        y += 110
    image.save(image_path)

    pdf = canvas.Canvas(str(pdf_path), pagesize=letter)
    pdf.drawImage(str(image_path), 24, 220, width=560, height=280)
    pdf.save()


if __name__ == "__main__":
    main()
