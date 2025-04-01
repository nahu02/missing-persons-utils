import logging
import asyncio
import streamlit as st
from datetime import datetime, timedelta
import io
import pandas as pd

from collect_missing import scrape_missing_persons

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("missing-persons.log"), logging.StreamHandler()],
)

# Page configuration
st.set_page_config(
    page_title="Eltűnt Személyek",
    page_icon="🔍",
    layout="wide",
)

# Create tabs for navigation
tab1, tab2 = st.tabs(["Keresés", "Összehasonlítás"])

# TAB 1: SEARCH PAGE
with tab1:
    # Main title for the app
    st.title("🔍 Eltűnt Személy Kereső")

    st.write("""
        Ez az alkalmazás a [Rendőrség weboldaláról](https://www.police.hu/hu/koral/eltunt-szemelyek) gyűjt adatokat az eltűnt személyekről
        és részletes információkat jelenít meg róluk.
    """)

    # Calculate default min birth date (12 years ago from today)
    default_min_birth_date = (datetime.now() - timedelta(days=365.25 * 12)).strftime(
        "%Y-%m-%d"
    )

    # Search filters section
    st.header("Keresési Feltételek")

    st.info(
        "Add meg a keresési feltételeket, majd kattints az 'Eltűnt Személyek Keresése' gombra!"
    )

    # Create a two-column layout for the name and birth place fields
    col1, col2 = st.columns(2)

    with col1:
        name = st.text_input(
            "Név", value="", help="Szűrés név alapján", key="search_name"
        )

    with col2:
        birth_place = st.text_input(
            "Születési Hely",
            value="",
            help="Szűrés születési hely alapján",
            key="search_birthplace",
        )

    # Create a layout for birth date filters
    col1, col2 = st.columns(2)

    with col1:
        birth_date_min = st.date_input(
            "Születési Dátum (minimum)",
            datetime.strptime(default_min_birth_date, "%Y-%m-%d"),
            help="Szűrés minimum születési dátum alapján (alapértelmezett: 12 éve)",
            key="search_min_date",
        )

    with col2:
        use_max_date = st.checkbox(
            "Születési Dátum (maximum) beállítása", value=False, key="use_max_date"
        )
        birth_date_max = None
        if use_max_date:
            birth_date_max = st.date_input(
                "Születési Dátum (maximum)",
                datetime.now(),
                help="Szűrés maximum születési dátum alapján",
                key="search_max_date",
            )

    # Search button across the full width
    search_button = st.button(
        "Eltűnt Személyek Keresése",
        type="primary",
        use_container_width=True,
        key="search_button",
    )

    # Main content area for search
    if search_button:
        # Convert dates to string format for API
        min_birth_date_str = birth_date_min.strftime("%Y-%m-%d")
        max_birth_date_str = birth_date_max.strftime("%Y-%m-%d") if use_max_date else ""

        progress_placeholder = st.empty()

        with st.spinner(
            "Adatok lekérdezése a Rendőrség weboldaláról. Ez eltarthat egy ideig..."
        ):
            try:
                # Create a progress display
                progress_container = progress_placeholder.container()
                progress_container.text("Keresés indítása...")

                # Execute the async function
                df = asyncio.run(
                    scrape_missing_persons(
                        name=name,
                        birth_place=birth_place,
                        birth_date_min=min_birth_date_str,
                        birth_date_max=max_birth_date_str,
                    )
                )

                # Clear progress display when done
                progress_placeholder.empty()

                if df.empty:
                    st.warning("Nem található eredmény a megadott feltételekkel.")
                else:
                    st.success(
                        f"A keresési feltételek alapján találtam {len(df)} eltűnt személyt."
                    )

                    # Display data
                    st.subheader("Eredmények")

                    st.dataframe(df, use_container_width=True, hide_index=True)

                    # Add download button
                    # Create a BytesIO buffer for Excel file
                    buffer = io.BytesIO()
                    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                        df.to_excel(writer, index=False)
                    buffer.seek(0)

                    st.download_button(
                        "Letöltés Excel fájlként",
                        buffer,
                        f"eltunt-szemelyek_{datetime.now().strftime('%Y-%m-%d')}_min-szuletes-{min_birth_date_str}.xlsx",
                        "application/vnd.ms-excel",
                        key="download-excel",
                        icon="⬇️",
                    )

            except Exception as e:
                st.error(f"Hiba a keresés során: {e}")
                logging.error(f"Error in Streamlit app: {e}")

# TAB 2: COMPARISON PAGE
with tab2:
    st.title("🔄 Eltűnt Személyek Adatbázis Összehasonlítás és Egyesítés")
    st.write("""
        Ez a funkció lehetővé teszi egy meglévő összehasonlítási adatbázis és új adatok egyesítését:
        1. Az új személyek hozzáadásra kerülnek a meglévő adatbázishoz
        2. A mai dátummal egy új oszlop is létrejön az aktuális eltűnési dátummal
        Ez segít nyomon követni, hogy ki mikor tűnt el, kit találtak meg, és ki az, aki esetleg újra eltűnt.
    """)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Meglévő összehasonlítási adatbázis")
        st.write(
            "Ez a folyamatosan frissített adatbázisod, amely az eltűnt személyek előzményeit tartalmazza. "
            + "Ha még nincs ilyen adatbázisod, töltsd fel az első fájlt."
        )
        ongoing_file = st.file_uploader(
            "Válaszd ki a meglévő adatbázis Excel fájlt",
            type=["xlsx", "xls"],
            key="ongoing_file",
        )

    with col2:
        st.subheader("Új adatok")
        st.write(
            "Ez a legfrissebb eltűnt személyek listája, amelyet hozzá szeretnél adni az összehasonlítási adatbázishoz."
        )
        new_file = st.file_uploader(
            "Válaszd ki az új adatokat tartalmazó Excel fájlt",
            type=["xlsx", "xls"],
            key="new_file",
        )

    compare_button = st.button(
        "Adatok egyesítése", type="primary", use_container_width=True
    )

    if compare_button:
        logging.info("Merging data...")
        if not ongoing_file or not new_file:
            st.error("Kérlek tölts fel mindkét Excel fájlt az egyesítéshez!")
            logging.error("Missing one or both files for merging.")
        else:
            try:
                with st.spinner("Adatok egyesítése folyamatban..."):
                    # Read Excel files
                    ongoing_df = pd.read_excel(ongoing_file)
                    new_df = pd.read_excel(new_file)

                    # Get file names for reference
                    ongoing_file_name = ongoing_file.name
                    new_file_name = new_file.name

                    # Check if the dataframes have any content
                    if ongoing_df.empty or new_df.empty:
                        st.warning("Az egyik vagy mindkét fájl üres!")
                    else:
                        st.success(
                            f"Sikeres beolvasás! A meglévő adatbázis {len(ongoing_df)} sort, az új adatok {len(new_df)} sort tartalmaznak."
                        )

                        # Find a column in the new data that matches "Eltűnés dátuma XXX" pattern
                        date_columns = [
                            col for col in new_df.columns if "Eltűnés dátuma" in col
                        ]

                        if date_columns:
                            # Use the found "Eltűnés dátuma XXX" column
                            eltunes_column_name = date_columns[0]
                            missing_date_source_column = date_columns[0]
                            logging.debug(
                                f"A következő oszlop használata: '{eltunes_column_name}'"
                            )
                        elif "Eltűnés dátuma" in new_df.columns:
                            # Fall back to default if no "Eltűnés dátuma XXX" exists but "Eltűnés dátuma" does
                            current_date = datetime.now().strftime("%Y-%m-%d")
                            eltunes_column_name = f"Eltűnés dátuma {current_date}"
                            missing_date_source_column = "Eltűnés dátuma"
                            logging.debug(
                                f"Nincs 'Eltűnés dátuma XXX' oszlop az új adatokban, '{missing_date_source_column}' oszlop használata és új '{eltunes_column_name}' oszlop létrehozása."
                            )
                        else:
                            st.error(
                                "Az új adatokban nem található 'Eltűnés dátuma' oszlop!"
                            )
                            st.stop()

                        # Check for required columns in new data
                        required_columns = [
                            "Név",
                            "Születési dátum",
                            missing_date_source_column,
                        ]
                        if not all(col in new_df.columns for col in required_columns):
                            missing_cols = [
                                col
                                for col in required_columns
                                if col not in new_df.columns
                            ]
                            st.error(
                                f"Az új adatokból hiányoznak kötelező oszlopok: {', '.join(missing_cols)}"
                            )
                            st.stop()

                        # Display basic info about uploaded files
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**{ongoing_file_name}** oszlopai:")
                            st.write(", ".join(ongoing_df.columns.tolist()))
                        with col2:
                            st.write(f"**{new_file_name}** oszlopai:")
                            st.write(", ".join(new_df.columns.tolist()))

                        # Create a backup of original ongoing data
                        original_ongoing_df = ongoing_df.copy()

                        # Define person identifier columns
                        id_columns = ["Név", "Születési dátum"]

                        # Ensure all required columns exist in ongoing data
                        required_base_columns = [
                            "Név",
                            "Nem",
                            "Születési hely",
                            "Születési dátum",
                            "Születési ország",
                        ]
                        for col in required_base_columns:
                            if col not in ongoing_df.columns:
                                if col in new_df.columns:
                                    ongoing_df[col] = None
                                else:
                                    ongoing_df[col] = None
                                    st.warning(
                                        f"A '{col}' oszlop hiányzik mindkét fájlból. Üres oszlop beszúrva."
                                    )

                        # Add the new column for current missing date to ongoing data if it doesn't exist already
                        if eltunes_column_name not in ongoing_df.columns:
                            ongoing_df[eltunes_column_name] = None

                        # Find people who exist in both datasets
                        common_people = pd.merge(
                            ongoing_df[id_columns],
                            new_df[id_columns],
                            on=id_columns,
                            how="inner",
                        )

                        # Update existing records with new missing date
                        for _, row in common_people.iterrows():
                            # Create mask to find the person in both dataframes
                            ongoing_mask = (ongoing_df["Név"] == row["Név"]) & (
                                ongoing_df["Születési dátum"] == row["Születési dátum"]
                            )
                            new_mask = (new_df["Név"] == row["Név"]) & (
                                new_df["Születési dátum"] == row["Születési dátum"]
                            )

                            # Get the missing date from new data
                            missing_date = new_df.loc[
                                new_mask, missing_date_source_column
                            ].values[0]

                            # Update the current missing date in ongoing data
                            ongoing_df.loc[ongoing_mask, eltunes_column_name] = (
                                missing_date
                            )

                        # Find people who are only in the new data
                        new_only_people = pd.merge(
                            new_df[id_columns],
                            ongoing_df[id_columns],
                            on=id_columns,
                            how="left",
                            indicator=True,
                        )
                        new_only_people = new_only_people[
                            new_only_people["_merge"] == "left_only"
                        ].drop(columns=["_merge"])

                        # Create new records for people only in new data
                        if len(new_only_people) > 0:
                            # Get all records from new data that are new people
                            new_records = pd.merge(
                                new_df, new_only_people, on=id_columns
                            )

                            # Ensure new records have all the columns from ongoing data
                            for col in ongoing_df.columns:
                                if (
                                    col not in new_records.columns
                                    and col != eltunes_column_name
                                ):
                                    new_records[col] = None

                            # Set the current missing date
                            new_records[eltunes_column_name] = new_records[
                                missing_date_source_column
                            ]

                            # Select only the columns that are in ongoing_df
                            new_records = new_records[ongoing_df.columns]

                            # Append new records to ongoing data
                            ongoing_df = pd.concat(
                                [ongoing_df, new_records], ignore_index=True
                            )

                        # Display results
                        st.subheader("Adatbázis egyesítés eredménye")

                        num_common = len(common_people)
                        num_new = len(new_only_people)
                        num_only_in_ongoing = len(ongoing_df) - num_common - num_new

                        # Create a download button for the updated database
                        buffer = io.BytesIO()

                        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                            ongoing_df.to_excel(
                                writer, sheet_name="Adatbázis", index=False
                            )

                            # Add a summary sheet
                            summary_data = {
                                "Leírás": [
                                    "Meglévő adatbázis",
                                    "Új adatok",
                                    "Frissítés dátuma",
                                    "Használt eltűnési dátum oszlop",
                                    "Meglévő adatbázis rekordok száma",
                                    "Új adatok száma",
                                    "Frissített adatok (továbbra is keresettek, vagy újra eltűntek) száma",
                                    "Hozzáadott új rekordok (újonnan, ez előtt még sosem eltűntek) száma",
                                    "Csak meglévőben található rekordok (már nem keresettek) száma",
                                    "Egyesített adatbázis rekordok száma",
                                ],
                                "Érték": [
                                    ongoing_file_name,
                                    new_file_name,
                                    datetime.now().strftime("%Y-%m-%d"),
                                    eltunes_column_name,
                                    len(original_ongoing_df),
                                    len(new_df),
                                    num_common,
                                    num_new,
                                    num_only_in_ongoing,
                                    len(ongoing_df),
                                ],
                            }
                            pd.DataFrame(summary_data).to_excel(
                                writer,
                                sheet_name=f"Összesítés {datetime.now().strftime('%Y-%m-%d')}",
                                index=False,
                            )

                            # Copy all other sheets from the ongoing file (if any)
                            try:
                                ongoing_excel = pd.ExcelFile(
                                    ongoing_file, engine="openpyxl"
                                )
                                # Check if there are more than 1 sheets
                                if len(ongoing_excel.sheet_names) > 1:
                                    for sheet_name in ongoing_excel.sheet_names:
                                        # Skip the first sheet as we already created the main data sheet
                                        if sheet_name != ongoing_excel.sheet_names[0]:
                                            # Read the sheet and write it to the new file
                                            sheet_df = pd.read_excel(
                                                ongoing_file, sheet_name=sheet_name
                                            )
                                            sheet_df.to_excel(
                                                writer,
                                                sheet_name=sheet_name,
                                                index=False,
                                            )
                            except Exception as e:
                                logging.warning(
                                    f"Could not copy additional sheets from ongoing file: {e}"
                                )

                        buffer.seek(0)

                        # Show tabs with results
                        tab1, tab2, tab3, tab4 = st.tabs(
                            [
                                "Összesített adatok",
                                f"Frissített rekordok ({num_common})",
                                f"Új rekordok ({num_new})",
                                f"Csak meglévőben ({num_only_in_ongoing})",
                            ]
                        )

                        with tab1:
                            st.write("Az új egyesített adatbázis:")
                            st.dataframe(
                                ongoing_df, use_container_width=True, hide_index=True
                            )

                            st.download_button(
                                "Frissített adatbázis letöltése Excel formátumban",
                                buffer,
                                f"eltunt_szemelyek_adatbazis_{datetime.now().strftime('%Y-%m-%d')}.xlsx",
                                "application/vnd.ms-excel",
                                key="database-download",
                                icon="⬇️",
                            )

                        with tab2:
                            if num_common == 0:
                                st.info(
                                    "Nincs olyan személy, aki mindkét adatbázisban szerepel."
                                )
                            else:
                                st.write(
                                    f"Az alábbi {num_common} személy mindkét adatbázisban szerepel, az eltűnési dátumuk frissítve lett. "
                                    + "Kéken kiemelve azok a rekordok, ahol az eltűnési dátum változott (ha van ilyen):"
                                )
                                common_records = pd.merge(
                                    ongoing_df, common_people, on=id_columns
                                )

                                # Find all date columns that contain "Eltűnés dátuma"
                                date_columns = [
                                    col
                                    for col in common_records.columns
                                    if "Eltűnés dátuma" in col
                                ]

                                if len(date_columns) >= 2:
                                    # Sort date columns by date (newest first)
                                    date_columns.sort(reverse=True)

                                    # Get current and previous date columns
                                    current_date_col = date_columns[0]  # newest
                                    previous_date_col = date_columns[1]  # second newest

                                    # Create a style function to highlight rows with different dates
                                    def highlight_changed_dates(row):
                                        if pd.notna(row[current_date_col]) and pd.notna(
                                            row[previous_date_col]
                                        ):
                                            if (
                                                row[current_date_col]
                                                != row[previous_date_col]
                                            ):
                                                return [
                                                    "background-color: CornflowerBlue"
                                                ] * len(row)
                                        return [""] * len(row)

                                    # Apply styling
                                    styled_df = common_records.style.apply(
                                        highlight_changed_dates, axis=1
                                    )
                                    st.dataframe(
                                        styled_df,
                                        use_container_width=True,
                                        hide_index=True,
                                    )
                                else:
                                    # Not enough date columns for comparison
                                    st.dataframe(
                                        common_records,
                                        use_container_width=True,
                                        hide_index=True,
                                    )

                        with tab3:
                            if num_new == 0:
                                st.info(
                                    "Nincs olyan személy, aki csak az új adatokban szerepel."
                                )
                            else:
                                st.write(
                                    f"Az alábbi {num_new} új személy hozzáadásra került az adatbázishoz (újonnan eltűntek):"
                                )
                                new_records_display = pd.merge(
                                    ongoing_df, new_only_people, on=id_columns
                                )
                                st.dataframe(
                                    new_records_display,
                                    use_container_width=True,
                                    hide_index=True,
                                )

                        with tab4:
                            only_in_ongoing_mask = ~ongoing_df["Név"].isin(
                                common_people["Név"]
                            ) & ~ongoing_df["Név"].isin(new_only_people["Név"])
                            only_in_ongoing = ongoing_df[only_in_ongoing_mask]

                            if len(only_in_ongoing) == 0:
                                st.info(
                                    "Nincs olyan személy, aki csak a meglévő adatbázisban szerepel."
                                )
                            else:
                                st.write(
                                    f"Az alábbi {len(only_in_ongoing)} személy csak a meglévő adatbázisban szerepel (már nem keresik őket):"
                                )
                                st.dataframe(
                                    only_in_ongoing,
                                    use_container_width=True,
                                    hide_index=True,
                                )

            except Exception as e:
                st.error(f"Hiba az adatok egyesítése során: {str(e)}")
                logging.error(f"Error in merger feature: {e}")
                # Add detailed traceback for debugging
                import traceback

                logging.error(traceback.format_exc())
    else:
        st.info(
            "Töltsd fel a meglévő adatbázist és az új adatokat tartalmazó Excel fájlokat, majd kattints az 'Adatok egyesítése' gombra!"
        )
