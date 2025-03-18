#!/usr/bin/env python3
"""
Hungarian Police Missing Persons Scraper

This script scrapes data about missing persons from the Hungarian Police website
and creates a CSV file with detailed information about each person.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urljoin
import logging
from datetime import datetime

logger = logging.getLogger()


def scrape_missing_persons(
    base_url="https://www.police.hu/hu/koral/eltunt-szemelyek",
    params={
        "ent_szemely_eltunt_viselt_nev_teljes": "",
        "ent_szemely_eltunt_szuletesi_hely": "",
        "ent_szemely_eltunt_kore_szerv_fk_kod_ertekek": "All",
        "ent_szemely_eltunt_kori_szerv_fk_kod_ertekek": "All",
        "ent_szemely_eltunt_szuletesi_datum[min]": "2012-06-06",
        "ent_szemely_eltunt_szuletesi_datum[max]": "",
        "ent_szemely_eltunt_nem_fk_kod_ertekek": "All",
        "min": "2012-06-06",
        "max": "",
    },
):
    """
    Scrape missing persons data and return a DataFrame with collected information.

    Args:
        base_url: The base URL of the missing persons page
        params: Query parameters for filtering

    Returns:
        DataFrame containing all scraped data
    """
    all_persons = []
    page = 0

    # Set up session for consistent connections
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.122 Safari/537.36"
        }
    )

    while True:
        # Add page parameter to query params
        page_params = params.copy()
        if page > 0:
            page_params["page"] = page

        logger.debug(f"Scraping page {page}...")

        try:
            # Get the page
            response = session.get(base_url, params=page_params)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # Check if we've reached a page with no results
            if "Nincs találat" in response.text:
                logger.debug(f"No more results found at page {page}. Stopping.")
                break

            # Find missing person container
            persons_container = soup.find_all("div", class_="flex-grid person eltunt")

            if not persons_container:
                logger.warning(
                    f"No missing person container found on page {page}. Stopping."
                )
                break

            persons_div = persons_container[0].find_all("div", class_="col overlay")

            logger.info(f"Found {len(persons_div)} missing persons on page {page}")

            # Process each person entry
            for person_div in persons_div:
                try:
                    # Find the link to the person's details page
                    links = person_div.find_all("a", href=True)
                    if not links:
                        continue

                    person_link = links[0]["href"]

                    # Convert relative links to absolute URLs
                    if not person_link.startswith("http"):
                        person_link = urljoin(base_url, person_link)

                    # Extract basic info from the list page
                    name = ""
                    name_div = person_div.find("div", class_="name")
                    if name_div:
                        name = " ".join(name_div.get_text(strip=False).split())

                    birth_date = ""
                    caption_div = person_div.find("div", class_="caption")
                    if caption_div:
                        birth_date_div = caption_div.find("div", class_="szul_datum")
                        if birth_date_div:
                            birth_text = birth_date_div.get_text(strip=True)
                            if ":" in birth_text:
                                birth_date = birth_text.split(":", 1)[1].strip()

                    # Get detailed information from the person's page
                    person_data = scrape_person_details(
                        person_link, session, name, birth_date
                    )
                    if person_data:
                        all_persons.append(person_data)

                    # Add a small delay between requests
                    time.sleep(1)

                except Exception as e:
                    logger.error(f"Error processing person entry: {e}")
                    continue

            # Move to the next page
            page += 1
            time.sleep(2)

        except Exception as e:
            logger.error(f"Error fetching page {page}: {e}")
            break

    # Create DataFrame
    if all_persons:
        df = pd.DataFrame(all_persons)
        return df
    else:
        logger.warning("No data found.")
        return pd.DataFrame()


def scrape_person_details(person_url, session, pre_name="", pre_birth_date=""):
    """
    Scrape details of a single missing person.

    Args:
        person_url: URL of the person's detailed page
        session: Session for making requests
        pre_name: Name extracted from the list page
        pre_birth_date: Birth date extracted from the list page

    Returns:
        Dictionary containing the person's details
    """
    try:
        logger.debug(f"Scraping details from {person_url}")
        response = session.get(person_url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        # Initialize data dictionary with pre-extracted values
        person_data = {
            "Név": pre_name,
            "Nem": "",
            "Születési hely": "",
            "Születési dátum": pre_birth_date,
            "Születési ország": "",
            "Eltűnés dátuma": "",
            "Körözést elrendelő szerv": "",
            "Körözési eljárás határozat száma": "",
        }

        # If we don't have a name yet, try to extract it from the title
        if not person_data["Név"]:
            name_element = soup.find("h1", class_="page-title")
            if name_element:
                person_data["Név"] = name_element.text.strip()

        # Find all detail rows
        detail_rows = soup.find_all("div", class_="line")

        for row in detail_rows:
            try:
                logger.debug(f"Checking row: {str(row).strip()}")

                # Find the label and value columns
                field_name = row.find("label").text.strip()
                field_value = row.text.split(":", 1)[1].strip()

                # Map fields to our data dictionary
                if field_name == "Nem":
                    person_data["Nem"] = field_value
                elif field_name == "Születési hely":
                    person_data["Születési hely"] = field_value
                elif (
                    field_name == "Születési dátum"
                    and not person_data["Születési dátum"]
                ):
                    person_data["Születési dátum"] = field_value
                elif field_name == "Születési ország":
                    person_data["Születési ország"] = field_value
                elif field_name == "Eltűnés dátuma":
                    person_data["Eltűnés dátuma"] = field_value
                elif field_name == "Körözést elrendelő szerv":
                    person_data["Körözést elrendelő szerv"] = field_value
                elif field_name == "Körözési eljárás határozat száma":
                    person_data[
                        "Körözési eljárás határozat száma, eljárás iktatószáma"
                    ] = field_value
            except Exception as e:
                logger.debug(f"Skipping row due to exception: {e}")
                continue

        return person_data

    except Exception as e:
        logger.error(f"Error scraping {person_url}: {e}")
        # Return partial data if available
        return person_data if pre_name or pre_birth_date else None


def save_to_excel(df):
    default_path = "missing_persons.xlsx"
    try:
        df.to_excel(default_path, index=False)
        logger.info(f"Saved {len(df)} missing persons to {default_path}")
    except PermissionError:
        # File is likely open in another program
        current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        alternative_filename = f"missing_persons_{current_date}.xlsx"
        df.to_excel(alternative_filename, index=False)
        logger.warning(
            f"Could not save to {default_path} (file may be open). Saved to {alternative_filename} instead."
        )
    except Exception as e:
        # Handle other potential errors
        current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        alternative_filename = f"missing_persons_{current_date}.xlsx"
        df.to_excel(alternative_filename, index=False)
        logger.error(
            f"Error saving to {default_path}: {e}. Saved to {alternative_filename} instead."
        )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("scraper.log"), logging.StreamHandler()],
    )

    try:
        logger.info("Starting missing persons data scraper")
        df = scrape_missing_persons()
        logger.info(f"Successfully scraped data for {len(df)} missing persons.")

        save_to_excel(df)
    except Exception as e:
        logger.critical(f"Fatal error in main scraper: {e}")
