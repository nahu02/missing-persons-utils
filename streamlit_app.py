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

st.title("🔍 Eltűnt Személy Kereső")
st.write("""
    Ez az alkalmazás a [Rendőrség weboldaláról](https://www.police.hu/hu/koral/eltunt-szemelyek) gyűjt adatokat az eltűnt személyekről
    és részletes információkat jelenít meg róluk.
""")

# Calculate default min birth date (12 years ago from today)
default_min_birth_date = (datetime.now() - timedelta(days=365.25 * 12)).strftime(
    "%Y-%m-%d"
)

# Sidebar for search filters
with st.sidebar:
    st.header("Keresési Feltételek")

    name = st.text_input("Név", value="", help="Szűrés név alapján")
    birth_place = st.text_input(
        "Születési Hely", value="", help="Szűrés születési hely alapján"
    )

    birth_date_min = st.date_input(
        "Születési Dátum (minimum)",
        datetime.strptime(default_min_birth_date, "%Y-%m-%d"),
        help="Szűrés minimum születési dátum alapján (alapértelmezett: 12 éve)",
    )

    use_max_date = st.checkbox("Születési Dátum (maximum) beállítása", value=False)
    birth_date_max = None
    if use_max_date:
        birth_date_max = st.date_input(
            "Születési Dátum (maximum)",
            datetime.now(),
            help="Szűrés maximum születési dátum alapján",
        )

    search_button = st.button(
        "Eltűnt Személyek Keresése", type="primary", use_container_width=True
    )

    st.info(
        "Megjegyzés: A keresés több percig is eltarthat a találatok számától függően."
    )

# Main content area
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
else:
    st.info(
        "Add meg a keresési feltételeket a bal oldali sávban, majd kattints az 'Eltűnt Személyek Keresése' gombra!"
    )
