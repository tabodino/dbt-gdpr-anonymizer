import re
import sys
from pathlib import Path
from typing import List, Tuple

import duckdb
from dotenv import load_dotenv
from loguru import logger
from rich.console import Console
from rich.table import Table
from dbt_gdpr_anonymizer.config.settings import settings


console = Console()


# ============================================
# PII DETECTION PATTERNS
# ============================================


# Standard (non-anonymized) email pattern
EMAIL_PATTERN = re.compile(
    r"\b[A-Za-z0-9._%+-]+@(?!anonymized\.gouv\.fr)[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
)


# French phone number pattern with all digits (not masked)
PHONE_PATTERN = re.compile(r"\+33\s*[1-9]\s*\d{2}\s*\d{2}\s*\d{2}\s*\d{2}(?!\s*XX)")


# Address pattern with street number
ADDRESS_PATTERN = re.compile(
    r"\d+\s+(?:rue|avenue|boulevard|place|impasse)\s+[\w\s]+", re.IGNORECASE
)


# ============================================
# VALIDATION FUNCTIONS
# ============================================


def get_schema(conn: duckdb.DuckDBPyConnection) -> str:
    """Get the schema name where int_services_enriched is located."""
    result = conn.execute(
        """
        SELECT table_schema
        FROM information_schema.tables
        WHERE table_name = 'int_services_enriched'
    """
    ).fetchone()

    if result:
        return result[0]
    else:
        logger.error("Table int_services_enriched not found in any schema")
        sys.exit(1)


def check_no_pii_in_marts(conn: duckdb.DuckDBPyConnection) -> List[Tuple[str, str]]:
    """
    Ensure that no PII is present in marts tables.

    Returns:
        List of tuples (table, column) containing potential PII
    """
    logger.info("Checking for PII in marts tables...")

    issues = []

    # Get all tables from the marts schema
    marts_tables = conn.execute(
        """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'marts'
    """
    ).fetchall()

    for (table_name,) in marts_tables:
        logger.debug(f"Analyzing table: marts.{table_name}")

        # Get text columns
        columns = conn.execute(
            f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'marts' 
              AND table_name = '{table_name}'
              AND data_type IN ('VARCHAR', 'TEXT')
        """
        ).fetchall()

        for col_name, _ in columns:
            # Sample a few values
            sample_query = f"""
                SELECT DISTINCT "{col_name}"
                FROM marts.{table_name}
                WHERE "{col_name}" IS NOT NULL
                LIMIT 100
            """

            try:
                samples = conn.execute(sample_query).fetchall()

                for (value,) in samples:
                    if value:
                        # Check PII patterns
                        if EMAIL_PATTERN.search(str(value)):
                            issues.append(
                                (
                                    f"marts.{table_name}",
                                    col_name,
                                    "Non-anonymized email detected",
                                )
                            )

                        if PHONE_PATTERN.search(str(value)):
                            issues.append(
                                (
                                    f"marts.{table_name}",
                                    col_name,
                                    "Unmasked phone number detected",
                                )
                            )

                        if ADDRESS_PATTERN.search(str(value)):
                            issues.append(
                                (
                                    f"marts.{table_name}",
                                    col_name,
                                    "Precise address detected",
                                )
                            )

            except Exception as e:
                logger.warning(f"Error while analyzing {table_name}.{col_name}: {e}")

    return issues


def check_anonymization_quality(conn: duckdb.DuckDBPyConnection, schema: str) -> dict:
    """
    Check anonymization quality in int_services_enriched.

    Returns:
        Dictionary with quality metrics
    """
    logger.info("Checking anonymization quality...")

    metrics = {}

    # Check anonymized emails
    result = conn.execute(
        f"""
        SELECT 
            COUNT(*) as total_emails,
            SUM(CASE WHEN contact_email_anon LIKE '%@anonymized.gouv.fr' THEN 1 ELSE 0 END) as properly_anonymized,
            SUM(CASE WHEN contact_email_anon NOT LIKE '%@anonymized.gouv.fr' 
                     AND contact_email_anon IS NOT NULL THEN 1 ELSE 0 END) as improperly_anonymized
        FROM {schema}.int_services_enriched
        WHERE contact_email_anon IS NOT NULL
    """
    ).fetchone()

    metrics["emails"] = {
        "total": result[0],
        "properly_anonymized": result[1],
        "improperly_anonymized": result[2],
        "success_rate": (result[1] / result[0] * 100) if result[0] > 0 else 0,
    }

    # Check masked phone numbers
    result = conn.execute(
        f"""
        SELECT 
            COUNT(*) as total_phones,
            SUM(CASE WHEN contact_phone_anon LIKE '%XX XX XX XX' THEN 1 ELSE 0 END) as properly_masked,
            SUM(CASE WHEN contact_phone_anon NOT LIKE '%XX XX XX XX' 
                     AND contact_phone_anon IS NOT NULL THEN 1 ELSE 0 END) as improperly_masked
        FROM {schema}.int_services_enriched
        WHERE contact_phone_anon IS NOT NULL
    """
    ).fetchone()

    metrics["phones"] = {
        "total": result[0],
        "properly_masked": result[1],
        "improperly_masked": result[2],
        "success_rate": (result[1] / result[0] * 100) if result[0] > 0 else 0,
    }

    result = conn.execute(
        f"""
        SELECT
            COUNT(*) AS total_coords,
            SUM(CASE
                WHEN (latitude_anon * 100) = FLOOR(latitude_anon * 100)
                AND (longitude_anon * 100) = FLOOR(longitude_anon * 100)
                THEN 1 ELSE 0 END) AS properly_rounded
        FROM {schema}.int_services_enriched
        WHERE latitude_anon IS NOT NULL
        AND longitude_anon IS NOT NULL
    """
    ).fetchone()

    metrics["coordinates"] = {
        "total": result[0],
        "properly_rounded": result[1],
        "success_rate": (result[1] / result[0] * 100) if result[0] > 0 else 0,
    }

    return metrics


def check_k_anonymity(conn: duckdb.DuckDBPyConnection, schema: str, k: int = 5) -> bool:
    """
    Check that quasi-identifiers comply with k-anonymity.

    Args:
        conn: DuckDB connection
        k: k-anonymity threshold (default 5)

    Returns:
        True if k-anonymity is satisfied
    """
    logger.info(f"Checking {k}-anonymity...")

    result = conn.execute(
        f"""
        WITH groups AS (
            SELECT 
                organization_category,
                COUNT(*) AS group_size
            FROM {schema}.int_services_enriched
            GROUP BY organization_category
        )
        SELECT *
        FROM groups
        WHERE group_size < {k}
        ORDER BY group_size ASC;
    """
    ).fetchall()

    if result:
        logger.warning(f"{len(result)} groups with less than {k} individuals")
        return False
    else:
        logger.success(f"All groups comply with {k}-anonymity")
        return True


def display_results(issues: List[Tuple], metrics: dict, k_anonymity_ok: bool) -> None:
    """Display validation results using rich."""

    console.print("\n[bold cyan]ANONYMIZATION VALIDATION REPORT[/bold cyan]\n")

    # PII issues table
    if issues:
        console.print("[bold red]PROBLEMS DETECTED[/bold red]\n")

        table = Table(title="Sensitive data detected in marts")
        table.add_column("Table", style="cyan")
        table.add_column("Column", style="magenta")
        table.add_column("PII type", style="red")

        for table_name, col_name, issue_type in issues:
            table.add_row(table_name, col_name, issue_type)

        console.print(table)
    else:
        console.print("[bold green]No PII detected in marts[/bold green]\n")

    # Quality metrics table
    console.print("\n[bold cyan]QUALITY METRICS[/bold cyan]\n")

    metrics_table = Table()
    metrics_table.add_column("Data type", style="cyan")
    metrics_table.add_column("Total", justify="right")
    metrics_table.add_column("Properly anonymized", justify="right", style="green")
    metrics_table.add_column("Success rate", justify="right")

    for data_type, data in metrics.items():
        metrics_table.add_row(
            data_type.title(),
            str(data["total"]),
            str(
                data.get(
                    "properly_anonymized",
                    data.get("properly_masked", data.get("properly_rounded", 0)),
                )
            ),
            f"{data['success_rate']:.1f}%",
        )

    console.print(metrics_table)

    # K-anonymity
    console.print("\n[bold cyan]K-ANONYMITY[/bold cyan]\n")
    if k_anonymity_ok:
        console.print("[bold green]K-anonymity satisfied (k ≥ 5)[/bold green]")
    else:
        console.print("[bold red]K-anonymity NOT satisfied[/bold red]")


# ============================================
# MAIN
# ============================================


def main() -> None:
    """Main entrypoint."""

    import argparse

    parser = argparse.ArgumentParser(description="Validate data anonymization")
    parser.add_argument("--duckdb", type=Path, help="Path to the DuckDB database")
    parser.add_argument("--verbose", action="store_true", help="Verbose mode")

    args = parser.parse_args()

    # Configuration
    load_dotenv()

    if args.duckdb:
        settings.duckdb_path = str(args.duckdb)

    # Logger
    logger.remove()
    logger.add(
        sys.stderr,
        level="DEBUG" if args.verbose else "INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}",
    )

    console.print("\n[bold cyan]RGPD Anonymizer - Validation[/bold cyan]\n")

    # DuckDB connection
    try:
        conn = duckdb.connect(settings.duckdb_path, read_only=True)
        logger.info(f"Connected to {settings.duckdb_path}")
    except Exception as e:
        logger.error(f"Unable to connect to DuckDB: {e}")
        sys.exit(1)

    try:
        # Validation
        schema = get_schema(conn)
        issues = check_no_pii_in_marts(conn)
        metrics = check_anonymization_quality(conn, schema)
        k_anonymity_ok = check_k_anonymity(conn, schema, k=5)

        # Display
        display_results(issues, metrics, k_anonymity_ok)

        # Exit code
        if issues or not k_anonymity_ok:
            console.print("\n[bold red]Validation FAILED[/bold red]\n")
            sys.exit(1)
        else:
            console.print(
                "\n[bold green]Validation PASSED - Data is GDPR compliant[/bold green]\n"
            )
            sys.exit(0)

    except Exception as e:
        logger.exception(f"Error during validation: {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
