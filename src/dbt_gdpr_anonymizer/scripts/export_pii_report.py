import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import duckdb
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from pydantic import BaseModel
from rich.console import Console
from dbt_gdpr_anonymizer.config.settings import settings
from dbt_gdpr_anonymizer.config.logger import setup_logger

console = Console()


class PIIColumn(BaseModel):
    """Model representing a PII column."""

    model_name: str
    model_schema: str
    column_name: str
    column_description: str
    pii_type: str
    anonymization_method: str
    data_owner: str
    legal_basis: str
    retention_days: int
    k_anonymity_target: str
    report_generated_at: datetime


class PIIReport(BaseModel):
    """Model representing the full PII report."""

    report_date: datetime
    project_version: str
    total_pii_columns: int
    models_with_pii: List[str]
    pii_columns: List[PIIColumn]
    summary: Dict[str, Any]


# ============================================
# FUNCTIONS
# ============================================


def generate_pii_report_data(conn: duckdb.DuckDBPyConnection) -> PIIReport:
    """Generates PII report data from the database."""

    logger.info("Generating PII report...")

    # Execute the dbt macro that generates the report
    # Note: In practice, we should parse schema.yml files
    # Here we simulate it with a direct SQL query
    query = """
    WITH pii_inventory AS (
        SELECT 
            'stg_services_publics' as model_name,
            'staging' as model_schema,
            'contact_email' as column_name,
            'Adresse email de contact du service' as column_description,
            'direct_identifier' as pii_type,
            'hash_sha256' as anonymization_method,
            'DPO Services Publics' as data_owner,
            'RGPD Art. 6.1.e' as legal_basis,
            730 as retention_days,
            'N/A' as k_anonymity_target,
            CURRENT_TIMESTAMP as report_generated_at
        
        UNION ALL
        
        SELECT 
            'stg_services_publics',
            'staging',
            'contact_phone',
            'Numéro de téléphone de contact du service',
            'direct_identifier',
            'mask_partial',
            'DPO Services Publics',
            'RGPD Art. 6.1.e',
            730,
            'N/A',
            CURRENT_TIMESTAMP
        
        UNION ALL
        
        SELECT 
            'stg_services_publics',
            'staging',
            'latitude',
            'Coordonnée GPS - Latitude',
            'quasi_identifier',
            'round_2_decimals',
            'DPO Services Publics',
            'RGPD Art. 6.1.e',
            730,
            '5',
            CURRENT_TIMESTAMP
        
        UNION ALL
        
        SELECT 
            'stg_services_publics',
            'staging',
            'longitude',
            'Coordonnée GPS - Longitude',
            'quasi_identifier',
            'round_2_decimals',
            'DPO Services Publics',
            'RGPD Art. 6.1.e',
            730,
            '5',
            CURRENT_TIMESTAMP
        
        UNION ALL
        
        SELECT 
            'stg_services_publics',
            'staging',
            'street_address',
            'Adresse postale complète du service',
            'quasi_identifier',
            'aggregate_to_city',
            'DPO Services Publics',
            'RGPD Art. 6.1.e',
            730,
            'N/A',
            CURRENT_TIMESTAMP
    )
    SELECT * FROM pii_inventory
    ORDER BY model_name, column_name
    """

    df = conn.execute(query).fetchdf()

    # Conversion in Pydantic model
    pii_columns = [PIIColumn(**row.to_dict()) for _, row in df.iterrows()]

    models_with_pii = df["model_name"].unique().tolist()

    summary = {
        "total_models": len(models_with_pii),
        "pii_by_type": df.groupby("pii_type").size().to_dict(),
        "pii_by_anonymization_method": df.groupby("anonymization_method")
        .size()
        .to_dict(),
        "models_list": models_with_pii,
    }

    # Complete report
    report = PIIReport(
        report_date=datetime.now(),
        project_version="1.0.0",
        total_pii_columns=len(pii_columns),
        models_with_pii=models_with_pii,
        pii_columns=pii_columns,
        summary=summary,
    )

    return report


def export_json(report: PIIReport, output_path: Path) -> None:
    """Exports the report as JSON."""

    logger.info(f"Export JSON to : {output_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(
            report.model_dump(mode="json"), f, indent=2, ensure_ascii=False, default=str
        )

    logger.success(f"JSON report exported to : {output_path}")


def export_csv(report: PIIReport, output_path: Path) -> None:
    """Exports the report as CSV."""

    logger.info(f"Export CSV to : {output_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Conversion in DataFrame
    df = pd.DataFrame([col.model_dump() for col in report.pii_columns])

    # Export CSV
    df.to_csv(output_path, index=False, encoding="utf-8")

    logger.success(f"CSV report exported to : {output_path}")


def display_summary(report: PIIReport) -> None:
    """Displays a summary of the report using rich."""

    from rich.table import Table

    console.print("\n[bold cyan] RESUME OF PII REPORT [/bold cyan]\n")

    # Main Statistics
    console.print(f"Report date: {report.report_date.strftime('%Y-%m-%d %H:%M:%S')}")
    console.print(f"Projet version: {report.project_version}")
    console.print(f"PII Columns detected: {report.total_pii_columns}")
    console.print(f"Concerned models: {len(report.models_with_pii)}")
    console.print()

    # Table by PII type
    table = Table(title="PII Type repartition")
    table.add_column("Type", style="cyan")
    table.add_column("Number", justify="right", style="magenta")

    for pii_type, count in report.summary["pii_by_type"].items():
        table.add_row(pii_type, str(count))

    console.print(table)
    console.print()

    # Table by anonymizer method
    table2 = Table(title="Anonymizer method used")
    table2.add_column("Method", style="cyan")
    table2.add_column("Number", justify="right", style="magenta")

    for method, count in report.summary["pii_by_anonymization_method"].items():
        table2.add_row(method, str(count))

    console.print(table2)
    console.print()


# ============================================
# MAIN
# ============================================


def main() -> None:
    """Main entry point."""

    import argparse

    parser = argparse.ArgumentParser(description="Export of PII report for GDPR audit")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("dbt_project/logs/pii_report.json"),
        help="Path of the output file",
    )
    parser.add_argument(
        "--format",
        choices=["json", "csv"],
        default="json",
        help="Export format (json or csv)",
    )

    args = parser.parse_args()

    # Configuration
    load_dotenv()
    setup_logger(settings.logging)

    console.print("\n[bold cyan] GDPR Anonymizer - Export PII Report[/bold cyan]\n")

    # Connection to DuckDB
    try:
        conn = duckdb.connect(settings.duckdb_path, read_only=True)
        logger.info(f"Connection to {settings.duckdb_path}")
    except Exception as e:
        logger.error(f"Impossible to connect to DuckDB: {e}")
        sys.exit(1)

    try:
        # Generate the report
        report = generate_pii_report_data(conn)

        display_summary(report)

        # Export according to the chosen format
        if args.format == "json":
            export_json(report, args.output)
        else:
            csv_path = args.output.with_suffix(".csv")
            export_csv(report, csv_path)

        console.print("[bold green] PII Report exported successfully[/bold green]\n")

    except Exception as e:
        logger.exception(f"Error during export: {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
