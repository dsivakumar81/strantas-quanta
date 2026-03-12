from pathlib import Path

from openpyxl import Workbook


def main() -> None:
    output_path = Path("samples/acme_census.xlsx")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Census"
    rows = [
        ["employee_id", "first_name", "last_name", "age", "state", "salary", "class", "dependent_count"],
        ["1001", "Ava", "Lopez", 34, "TX", 72000, "Salaried", 2],
        ["1002", "Noah", "Patel", 41, "TX", 88000, "Executive", 1],
        ["1003", "Mia", "Nguyen", 29, "OK", 54000, "Hourly", 0],
        ["1004", "Liam", "Carter", 53, "LA", 99000, "Salaried", 3],
    ]
    for row in rows:
        sheet.append(row)
    workbook.save(output_path)


if __name__ == "__main__":
    main()
