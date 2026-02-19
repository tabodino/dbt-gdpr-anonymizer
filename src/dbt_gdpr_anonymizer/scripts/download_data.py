import json
import sys
from pathlib import Path
from typing import Optional

import httpx
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from pydantic import BaseModel
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from dbt_gdpr_anonymizer.config.settings import settings
from dbt_gdpr_anonymizer.config.logger import setup_logger


console = Console()


class ServicePublic(BaseModel):
    """Data model representing a public service."""

    service_id: str
    service_name: str
    parent_organization: Optional[str] = None
    organization_type: Optional[str] = None
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    website: Optional[str] = None
    street_address: Optional[str] = None
    postal_code: Optional[str] = None
    city: Optional[str] = None
    commune: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    insee_code: Optional[str] = None
    last_updated: Optional[str] = None


def fetch_dataset_info() -> dict:
    """Retrieve dataset metadata from the data.gouv.fr API."""

    url = f"{settings.datagouv.api_url}/datasets/{settings.datagouv.dataset_id}/"

    logger.info(f"Fetching dataset metadata: {settings.datagouv.dataset_id}")

    with httpx.Client(timeout=settings.datagouv.http_timeout) as client:
        for attempt in range(settings.datagouv.http_retries):
            try:
                response = client.get(url)
                response.raise_for_status()
                data = response.json()

                logger.success(f"Metadata retrieved: {data.get('title', 'N/A')}")
                return data

            except httpx.HTTPError as e:
                logger.warning(
                    f"Attempt {attempt + 1}/{settings.datagouv.http_retries} failed: {e}"
                )
                if attempt == settings.datagouv.http_retries - 1:
                    logger.error("Failed to retrieve dataset metadata")
                    raise


def download_json_data(url: str) -> list[dict]:
    """Download the JSONL file containing public service records."""

    logger.info(f"Downloading data from: {url}")

    with httpx.Client(timeout=settings.datagouv.http_timeout) as client:
        response = client.get(url)
        response.raise_for_status()

        # The file is JSONL: one JSON object per line
        lines = response.text.splitlines()
        data = [json.loads(line) for line in lines if line.strip()]

        logger.success(f"Download complete: {len(data)} services retrieved")
        return data


def parse_service(raw_service: dict) -> Optional[ServicePublic]:
    """Convert a raw service entry into a structured ServicePublic model."""

    try:
        geo = raw_service.get("geo", {})
        address = raw_service.get("writeAddress", {})

        service = ServicePublic(
            service_id=raw_service.get("id", ""),
            service_name=raw_service.get("name", ""),
            parent_organization=raw_service.get("parent_name"),
            organization_type=raw_service.get("type"),
            contact_email=raw_service.get("contact_email"),
            contact_phone=raw_service.get("contact_phone"),
            website=(
                raw_service.get("website", [None])[0]
                if raw_service.get("website")
                else None
            ),
            street_address=address.get("streetAddress"),
            postal_code=address.get("postalCode"),
            city=address.get("addressLocality"),
            commune=geo.get("commune"),
            latitude=geo.get("latitude"),
            longitude=geo.get("longitude"),
            insee_code=geo.get("insee_comm"),
            last_updated=raw_service.get("update"),
        )

        return service

    except Exception as e:
        logger.warning(
            f"Parsing error for service {raw_service.get('id', 'unknown')}: {e}"
        )
        return None


def convert_to_dataframe(services: list[ServicePublic]) -> pd.DataFrame:
    """Convert a list of ServicePublic objects into a pandas DataFrame."""

    logger.info("Converting to DataFrame...")

    data = [service.model_dump() for service in services]
    df = pd.DataFrame(data)

    logger.success(f"DataFrame created: {len(df)} rows, {len(df.columns)} columns")
    logger.info(f"Columns: {', '.join(df.columns.tolist())}")

    return df


def save_to_csv(
    df: pd.DataFrame, output_path: Path, sample_size: Optional[int] = None
) -> None:
    """Save the DataFrame to a CSV file."""

    if sample_size:
        logger.info(f"Sampling {sample_size} rows...")
        df = df.sample(n=min(sample_size, len(df)), random_state=42)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")

    logger.success(f"Data saved to: {output_path}")
    logger.info(f"File size: {output_path.stat().st_size / 1024:.2f} KB")


def display_summary(df: pd.DataFrame) -> None:
    """Display a summary of the dataset using rich tables."""

    from rich.table import Table

    table = Table(title=" Downloaded Data Summary")

    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")

    table.add_row("Total services", str(len(df)))
    table.add_row("With email", str(df["contact_email"].notna().sum()))
    table.add_row("With phone", str(df["contact_phone"].notna().sum()))
    table.add_row("With address", str(df["street_address"].notna().sum()))
    table.add_row("With GPS coordinates", str(df["latitude"].notna().sum()))
    table.add_row("Organization types", str(df["organization_type"].nunique()))
    table.add_row("Distinct communes", str(df["commune"].nunique()))

    console.print(table)


# ============================================
# MAIN
# ============================================


def main() -> None:
    """Main entry point of the script."""

    import argparse

    parser = argparse.ArgumentParser(
        description="Download public service data from data.gouv.fr"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("dbt_project/seeds/services_publics_raw.csv"),
        help="Output CSV file path",
    )
    parser.add_argument(
        "--sample",
        type=int,
        help="Optional number of rows to sample",
    )

    args = parser.parse_args()

    load_dotenv()
    setup_logger(settings.logging)

    console.print("\n[bold cyan] GDPR Anonymizer - Data Download[/bold cyan]\n")

    try:
        # Step 1: Fetch metadata
        with console.status("[bold green]Fetching metadata..."):
            dataset_info = fetch_dataset_info()

        resources = dataset_info.get("resources", [])
        json_resource = next((r for r in resources if r.get("format") == "json"), None)

        if not json_resource:
            logger.error("No JSON resource found in dataset")
            sys.exit(1)

        download_url = json_resource.get("url")

        # Step 2: Download data
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Downloading data...", total=None)
            raw_data = download_json_data(download_url)
            progress.update(task, completed=True)

        # Step 3: Parse data
        with console.status("[bold green]Parsing data..."):
            services = []
            for raw_service in raw_data:
                service = parse_service(raw_service)
                if service:
                    services.append(service)

            logger.info(f"{len(services)}/{len(raw_data)} services parsed successfully")

        # Step 4: Convert to DataFrame
        df = convert_to_dataframe(services)

        # Step 5: Save CSV
        save_to_csv(df, args.output, args.sample)

        # Step 6: Display summary
        display_summary(df)

        console.print("\n[bold green] Download completed successfully![/bold green]\n")

    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        console.print(f"\n[bold red] Error: {e}[/bold red]\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
