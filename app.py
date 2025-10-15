import streamlit as st
import pandas as pd
from io import BytesIO
import numpy as np
import re
import requests
import io
import pvlib
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Functies voor dataverwerking (ongewijzigd) ---

def process_energy_file(file_upload, register_type: str) -> pd.DataFrame:
    if file_upload is None: return pd.DataFrame()
    try:
        df = pd.read_csv(file_upload, sep=';', decimal=',')
        df['Tijdstip'] = pd.to_datetime(
            df['Van (datum)'].astype(str) + ' ' + df['Van (tijdstip)'].astype(str), dayfirst=True
        )
        df_filtered = df[df['Register'] == register_type].copy()
        df_filtered['Volume'] = pd.to_numeric(df_filtered['Volume'])
        return df_filtered[['Tijdstip', 'Volume']]
    except Exception as e:
        st.error(f"Fout bij het verwerken van Standaard CSV '{file_upload.name}': {e}")
        return pd.DataFrame()

def process_amr_file(file_upload) -> pd.DataFrame:
    if file_upload is None: return pd.DataFrame()
    try:
        df = pd.read_csv(file_upload, sep=';', skiprows=4, header=None)
        df_kwt = df[df.iloc[:, 7] == 'KWT'].copy()
        if df_kwt.empty:
            st.warning(f"Geen rijen met 'KWT' in de 8e kolom gevonden in '{file_upload.name}'.")
            return pd.DataFrame()
        
        df_kwt['start_datetime'] = pd.to_datetime(df_kwt.iloc[:, 0], format='%d%m%Y %H:%M', errors='coerce')
        df_kwt.dropna(subset=['start_datetime'], inplace=True)
        
        value_cols = list(range(10, 10 + 96))
        df_to_process = df_kwt[['start_datetime'] + value_cols]

        df_long = pd.melt(df_to_process, id_vars=['start_datetime'], value_vars=value_cols,
                          var_name='interval_index', value_name='Volume')
        
        df_long['Volume'] = pd.to_numeric(df_long['Volume'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)

        time_offset_minutes = (df_long['interval_index'] - 10 + 1) * 15
        df_long['Tijdstip'] = df_long['start_datetime'] + pd.to_timedelta(time_offset_minutes, unit='m')

        return df_long[['Tijdstip', 'Volume']]
    except Exception as e:
        st.error(f"Fout bij het verwerken van AMR-bestand '{file_upload.name}': {e}")
        return pd.DataFrame()

def process_belpex_file() -> pd.DataFrame:
    belpex_path = "BelpexFilter.csv"
    try:
        df_belpex = pd.read_csv(belpex_path, sep=';', encoding='cp1252')
        df_belpex.columns = df_belpex.columns.str.strip()
        df_belpex['Tijdstip_uur'] = pd.to_datetime(df_belpex['Date'], dayfirst=True)
        numeric_text = df_belpex['Euro'].str.extract(r'(-?[\d,]+)', expand=False)
        clean_price = pd.to_numeric(numeric_text.str.replace(',', '.'), errors='coerce')
        df_belpex['BELPEX_EUR_KWH'] = clean_price / 1000
        return df_belpex[['Tijdstip_uur', 'BELPEX_EUR_KWH']]
    except FileNotFoundError:
        st.error(f"Fout: Het bestand '{belpex_path}' niet gevonden. Zorg dat het bestand lokaal of op GitHub in dezelfde map staat.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Fout bij het laden van het Belpex-bestand: {e}")
        return pd.DataFrame()

# --- VERNIEUWDE FUNCTIE (HYBRIDE AANPAK) ---
@st.cache_data
def process_pvgis_hybrid(segments, lat, lon, loss):
    """Haalt TMY-weerdata op via de API en berekent de PV-productie lokaal met pvlib."""
    if not segments:
        return pd.DataFrame()

    with st.spinner(f"TMY-weerdata voor locatie ({lat}, {lon}) ophalen..."):
        try:
            # Stap 1: Haal TMY weerdata op via de API
            tmy_api_url = f"https://re.jrc.ec.europa.eu/api/tmy?lat={lat}&lon={lon}&outputformat=csv"
            
            # Gebruik een robuuste sessie met retries
            session = requests.Session()
            retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('https://', adapter)
            
            response = session.get(tmy_api_url, timeout=30)
            response.raise_for_status()

            # Converteer de tekst-respons naar een virtueel bestand voor pvlib
            tmy_buffer = io.StringIO(response.text)
            
            # Lees de TMY data met pvlib
            weather, _, _, _ = pvlib.iotools.read_pvgis_tmy(tmy_buffer, map_variables=True)
            st.info("Weerdata succesvol opgehaald.")

        except requests.exceptions.RequestException as e:
            st.error(f"Kon geen weerdata ophalen bij PVGIS na 3 pogingen: {e}")
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Fout bij verwerken van TMY-weerdata: {e}")
            return pd.DataFrame()

    # Stap 2: Bereken de PV-productie lokaal met de opgehaalde weerdata
    with st.spinner(f"Lokale PV-simulatie uitvoeren..."):
        location = pvlib.location.Location(latitude=lat, longitude=lon, tz='Europe/Brussels')
        total_ac_power = pd.Series(0.0, index=weather.index)

        for i, segment in enumerate(segments):
            system = pvlib.pvsystem.PVSystem(
                surface_tilt=segment['slope'], surface_azimuth=segment['azimuth'],
                module_parameters={'pdc0': segment['kwp'], 'gamma_pdc': -0.004},
                inverter_parameters={'pdc0': segment['kwp']},
                losses_parameters=dict(losses_percent=loss)
            )
            mc = pvlib.modelchain.ModelChain(system, location, aoi_model='physical', spectral_model='no_loss')
            mc.run_model(weather)
            total_ac_power += mc.results.ac.fillna(0)
    
    total_energy_kwh_hourly = total_ac_power / 1000
    df_resampled = pd.DataFrame(total_energy_kwh_hourly, columns=['PVGIS_kwh'])
    df_resampled = df_resampled.resample('15min').ffill() / 4
    df_resampled.reset_index(inplace=True)
    df_resampled.rename(columns={'index': 'Tijdstip'}, inplace=True)
    
    return df_resampled

def to_excel(df: pd.DataFrame) -> bytes:
    """Converteert een DataFrame naar een Excel-bestand in het geheugen."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    return output.getvalue()

# --- Streamlit App Interface ---

st.set_page_config(layout="wide", page_title="Energie Data Combiner")
st.title("🔌 Energie Data Combiner")
st.markdown("Combineert Fluvius verbruiks-, injectie- en PV-data met Belpex-prijzen.")

if 'processed_data' not in st.session_state: st.session_state.processed_data = None
if 'pvgis_data' not in st.session_state: st.session_state.pvgis_data = None

st.header("Stap 1: Upload je bestanden")

with st.expander("Upload hier je energiebestanden", expanded=True):
    file_type = st.radio("**Kies het type energiebestand:**", ('Normale CSV (Fluvius)', 'AMR Bestand (Fluvius)'), horizontal=True)
    col1, col2 = st.columns(2)
    with col1:
        file_import = st.file_uploader("1. Afname (import) [.csv]", type="csv")
        file_injectie = st.file_uploader("2. Injectie (export) [.csv]", type="csv")
    with col2:
        file_pv = st.file_uploader("3. Hulpverbruik (PV-opbrengst) [.csv]", type="csv")

with st.expander("Optioneel: Voeg PV-productie simulatie toe (Hybride API + pvlib)"):
    st.markdown("Definieer je locatie en voeg tot 5 PV-segmenten toe. De weerdata wordt live opgehaald, de berekening gebeurt lokaal.")
    
    col_loc1, col_loc2, col_loc3 = st.columns(3)
    with col_loc1:
        lat = st.number_input("Breedtegraad", value=51.22, format="%.4f")
    with col_loc2:
        lon = st.number_input("Lengtegraad", value=5.08, format="%.4f")
    with col_loc3:
        loss = st.number_input("Systeemverlies (%)", value=14, min_value=0, max_value=100, step=1)

    # --- HIER IS DE AANPASSING ---
    # De standaardwaarde (value) is nu 0 in plaats van 1.
    num_segments = st.number_input("Aantal PV-segmenten", min_value=0, max_value=5, value=0, step=1)
    
    pvgis_segments_hybrid = []
    if num_segments > 0:
        for i in range(num_segments):
            st.markdown(f"--- \n **Segment {i+1}**")
            col3, col4, col5 = st.columns(3)
            with col3:
                kwp = st.number_input("Vermogen (kWp)", min_value=0.1, value=4.0, step=0.1, key=f"pvgis_kwp_{i}")
            with col4:
                slope = st.number_input("Helling °", min_value=0, max_value=90, value=35, key=f"pvgis_slope_{i}")
            with col5:
                azimuth = st.number_input("Azimuth ° (0=Z, -90=O)", min_value=-180, max_value=180, value=0, key=f"pvgis_azimuth_{i}")
            pvgis_segments_hybrid.append({'kwp': kwp, 'slope': slope, 'azimuth': azimuth})


if st.button("Verwerk bestanden en simuleer PV-productie", type="primary"):
    if not (file_import or file_injectie or file_pv):
        st.warning("Upload ten minste één energiebestand om door te gaan.")
    else:
        with st.spinner("Data wordt verwerkt..."):
            if file_type == 'Normale CSV (Fluvius)':
                df_import = process_energy_file(file_import, "Afname Actief")
                df_injectie = process_energy_file(file_injectie, "Injectie Actief")
                df_pv = process_energy_file(file_pv, "Hulpverbruik Actief")
            else:
                df_import = process_amr_file(file_import)
                df_injectie = process_amr_file(file_injectie)
                df_pv = process_amr_file(file_pv)
            
            dataframes = []
            if df_import is not None and not df_import.empty: dataframes.append(df_import.rename(columns={'Volume': 'import_kwh'}))
            if df_injectie is not None and not df_injectie.empty: dataframes.append(df_injectie.rename(columns={'Volume': 'injection_kwh'}))
            if df_pv is not None and not df_pv.empty: dataframes.append(df_pv.rename(columns={'Volume': 'pv_kwh'}))

            if not dataframes:
                st.error("Geen geldig energiebestand gevonden of verwerkt.")
                st.session_state.processed_data = None
            else:
                finale_df = dataframes[0]
                for df_to_merge in dataframes[1:]:
                    finale_df = pd.merge(finale_df, df_to_merge, on='Tijdstip', how='outer')
                df_belpex = process_belpex_file()
                if df_belpex is not None and not df_belpex.empty:
                    st.success("Belpex-data succesvol geladen.")
                    finale_df['Tijdstip_uur'] = finale_df['Tijdstip'].dt.floor('H')
                    finale_df = pd.merge(finale_df, df_belpex, on='Tijdstip_uur', how='left')
                    finale_df.drop(columns=['Tijdstip_uur'], inplace=True)
                else:
                    st.error("Kon Belpex-data niet laden. De BELPEX-kolom zal leeg zijn.")
                finale_df.rename(columns={'Tijdstip': 'Date', 'BELPEX_EUR_KWH': 'BELPEX'}, inplace=True)
                for col in ['import_kwh', 'injection_kwh', 'pv_kwh', 'BELPEX']:
                    if col not in finale_df.columns: finale_df[col] = 0
                finale_df.fillna(0, inplace=True)
                finale_df.sort_values('Date', inplace=True)
                st.session_state.processed_data = finale_df
                st.success("✅ Energiebestanden succesvol verwerkt!")

            # Voer de hybride PVGIS-simulatie uit
            if pvgis_segments_hybrid:
                st.session_state.pvgis_data = process_pvgis_hybrid(pvgis_segments_hybrid, lat, lon, loss)
                if st.session_state.pvgis_data is not None and not st.session_state.pvgis_data.empty:
                    st.success("✅ Hybride PV-simulatie succesvol uitgevoerd!")
            else:
                st.session_state.pvgis_data = None


if st.session_state.processed_data is not None:
    st.header("Stap 2: Selecteer datumbereik en download")
    
    df = st.session_state.processed_data.copy()
    
    if st.session_state.pvgis_data is not None and not st.session_state.pvgis_data.empty:
        pvgis_df = st.session_state.pvgis_data
        
        pvgis_df['match_key'] = pvgis_df['Tijdstip'].dt.strftime('%m-%d %H:%M')
        df['match_key'] = df['Date'].dt.strftime('%m-%d %H:%M')
        
        df = pd.merge(df, pvgis_df[['match_key', 'PVGIS_kwh']], on='match_key', how='left').drop(columns=['match_key'])
        df['PVGIS_kwh'].fillna(0, inplace=True)

    min_date, max_date = df['Date'].min().date(), df['Date'].max().date()

    col3, col4 = st.columns(2)
    with col3:
        start_date = st.date_input("Startdag", min_value=min_date, max_value=max_date, value=min_date)
    with col4:
        end_date = st.date_input("Einddag", min_value=min_date, max_value=max_date, value=max_date)

    if start_date > end_date:
        st.error("Fout: De startdag kan niet na de einddag liggen.")
    else:
        mask = (df['Date'].dt.date >= start_date) & (df['Date'].dt.date <= end_date)
        filtered_df = df.loc[mask].copy()

        st.markdown(f"**Voorbeeld van de geselecteerde data ({len(filtered_df)} rijen):**")
        st.dataframe(filtered_df.head())

        excel_data = to_excel(filtered_df)
        st.download_button(
            label="📥 Download geselecteerde data als Excel",
            data=excel_data,
            file_name=f"gefilterde_energiemix_{start_date}_tot_{end_date}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
