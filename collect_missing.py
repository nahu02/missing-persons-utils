#!/usr/bin/env python3
"""
Hungarian Police Missing Persons Scraper

This script scrapes data about missing persons from the Hungarian Police website
and creates a CSV file with detailed information about each person.
"""

import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
import asyncio
from urllib.parse import urljoin
import logging
from datetime import datetime

logger = logging.getLogger()


async def scrape_missing_persons(
    base_url="https://www.police.hu/hu/koral/eltunt-szemelyek",
    name="",
    birth_place="",
    investigating_authority="All",
    requesting_authority="All",
    birth_date_min="2012-06-06",
    birth_date_max="",
    gender="All",
) -> pd.DataFrame:
    """
    Scrape missing persons data and return a DataFrame with collected information.

    Args:
        base_url: The base URL of the missing persons page
        name: Filter by person's name
        birth_place: Filter by birth place
        investigating_authority: Filter by investigating authority (FK code)
        requesting_authority: Filter by requesting authority (FK code)
        birth_date_min: Filter by minimum birth date (YYYY-MM-DD)
        birth_date_max: Filter by maximum birth date (YYYY-MM-DD)
        gender: Filter by gender (All or code for gender)ű

    Returns:
        DataFrame containing all scraped data
    """
    all_persons = []
    page = 0

    # Construct params dictionary from function parameters
    params = {
        "ent_szemely_eltunt_viselt_nev_teljes": name,
        "ent_szemely_eltunt_szuletesi_hely": birth_place,
        "ent_szemely_eltunt_kore_szerv_fk_kod_ertekek": investigating_authority,
        "ent_szemely_eltunt_kori_szerv_fk_kod_ertekek": requesting_authority,
        "ent_szemely_eltunt_szuletesi_datum[min]": birth_date_min,
        "ent_szemely_eltunt_szuletesi_datum[max]": birth_date_max,
        "ent_szemely_eltunt_nem_fk_kod_ertekek": gender,
    }

    no_of_results = None

    # Set up session for consistent connections
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.122 Safari/537.36"
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            # Add page parameter to query params
            page_params = params.copy()
            if page > 0:
                page_params["page"] = page

            logger.debug(f"Scraping page {page}...")

            try:
                # Get the page
                async with session.get(base_url, params=page_params) as response:
                    if response.status != 200:
                        logger.error(
                            f"Error: Status {response.status} when fetching page {page}"
                        )
                        break

                    html_content = await response.text()

                soup = BeautifulSoup(html_content, "html.parser")

                # Check if we've reached a page with no results
                if "Nincs találat" in html_content:
                    logger.debug(f"No more results found at page {page}. Stopping.")
                    break

                if no_of_results is None:
                    try:
                        # Find all results
                        all_div = soup.find_all("div", class_="all-results")[0]
                        # This has "Összes találat: XX"
                        no_of_results = int(all_div.text.split(":")[1].strip())
                        logger.debug(
                            f"Total number of missing persons: {no_of_results}"
                        )
                    except Exception as e:
                        no_of_results = False
                        logger.debug(f"Error finding total number of results: {e}")

                # Find missing person container
                persons_container = soup.find_all(
                    "div", class_="flex-grid person eltunt"
                )

                if not persons_container:
                    logger.warning(
                        f"No missing person container found on page {page}. Stopping."
                    )
                    break

                persons_div = persons_container[0].find_all("div", class_="col overlay")

                logger.debug(f"Found {len(persons_div)} missing persons on page {page}")

                # Process each person entry
                person_tasks = []
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
                            birth_date_div = caption_div.find(
                                "div", class_="szul_datum"
                            )
                            if birth_date_div:
                                birth_text = birth_date_div.get_text(strip=True)
                                if ":" in birth_text:
                                    birth_date = birth_text.split(":", 1)[1].strip()

                        # Create task for getting person details
                        task = asyncio.create_task(
                            scrape_person_details(
                                person_link, session, name, birth_date
                            )
                        )
                        person_tasks.append(task)

                    except Exception as e:
                        logger.error(f"Error processing person entry: {e}")
                        continue

                # Gather all person details
                person_results = await asyncio.gather(
                    *person_tasks, return_exceptions=True
                )

                for result in person_results:
                    if isinstance(result, Exception):
                        logger.error(f"Error during person detail scrape: {result}")
                    elif result:
                        all_persons.append(result)

                if no_of_results:
                    msg = f"Scraped data for {len(all_persons)} missing persons out of {no_of_results}"
                else:
                    msg = f"Scraped data for {len(all_persons)} missing persons"
                logger.info(msg)

                # Move to the next page
                page += 1
                await asyncio.sleep(2)

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


async def scrape_person_details(person_url, session, pre_name="", pre_birth_date=""):
    """
    Scrape details of a single missing person.

    Args:
        person_url: URL of the person's detailed page
        session: aiohttp ClientSession for making requests
        pre_name: Name extracted from the list page
        pre_birth_date: Birth date extracted from the list page

    Returns:
        Dictionary containing the person's details
    """
    try:
        logger.debug(f"Scraping details from {person_url}")

        current_date = datetime.now().strftime("%Y-%m-%d")

        # Initialize data dictionary with pre-extracted values
        person_data = {
            "Név": pre_name,
            "Nem": "",
            "Születési hely": "",
            "Születési dátum": pre_birth_date,
            "Születési ország": "",
            "Körözést elrendelő szerv": "",
            "Körözési eljárás határozat száma": "",
            f"Eltűnés dátuma {current_date}": "",
        }

        async with session.get(person_url) as response:
            if response.status != 200:
                logger.error(
                    f"Error: Status {response.status} when fetching {person_url}"
                )
                return person_data if pre_name or pre_birth_date else None

            html_content = await response.text()

        soup = BeautifulSoup(html_content, "html.parser")

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
                    person_data[f"Eltűnés dátuma {current_date}"] = field_value
                elif field_name == "Körözést elrendelő szerv":
                    person_data["Körözést elrendelő szerv"] = field_value
                elif (
                    field_name
                    == "Körözési eljárás határozat száma, eljárás iktatószáma"
                ):
                    person_data["Körözési eljárás határozat száma"] = field_value
            except Exception as e:
                logger.debug(f"Skipping row due to exception: {e}")
                continue

        # Small delay to avoid overwhelming the server
        await asyncio.sleep(0.5)
        return person_data

    except Exception as e:
        logger.error(f"Error scraping {person_url}: {e}")
        # Return partial data if available
        return person_data if pre_name or pre_birth_date else None


async def save_to_excel(df):
    default_path = "missing_persons.xlsx"
    try:
        # Use run_in_executor for blocking I/O operations
        await asyncio.to_thread(df.to_excel, default_path, index=False)
        logger.info(f"Saved {len(df)} missing persons to {default_path}")
    except PermissionError:
        # File is likely open in another program
        current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        alternative_filename = f"missing_persons_{current_date}.xlsx"
        await asyncio.to_thread(df.to_excel, alternative_filename, index=False)
        logger.warning(
            f"Could not save to {default_path} (file may be open). Saved to {alternative_filename} instead."
        )
    except Exception as e:
        # Handle other potential errors
        current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        alternative_filename = f"missing_persons_{current_date}.xlsx"
        await asyncio.to_thread(df.to_excel, alternative_filename, index=False)
        logger.error(
            f"Error saving to {default_path}: {e}. Saved to {alternative_filename} instead."
        )


async def main():
    try:
        logger.info("Starting missing persons data scraper")
        df = await scrape_missing_persons()
        logger.info(f"Successfully scraped data for {len(df)} missing persons.")

        await save_to_excel(df)
    except Exception as e:
        logger.critical(f"Fatal error in main scraper: {e}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("scraper.log"), logging.StreamHandler()],
    )

    # Run the async main function
    asyncio.run(main())
