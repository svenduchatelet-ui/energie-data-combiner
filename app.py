import streamlit as st
import pandas as pd
from io import BytesIO
import numpy as np
import re
import requests # Nodig voor de verbeterde API-aanroep
import io # Nodig om tekst als een bestand te behandelen

# --- Functies voor dataverwerking ---

def process_energy_file(file_upload, register_type: str) -> pd.DataFrame:
    """Leest een geüpload normaal energie-CSV-bestand (type 1)."""
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
    """Verwerkt het specifieke AMR-formaat (skip 4 rows, filter on KWT, expand 96 values)."""
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
    """Leest het Belpex CSV-bestand vanuit de repository."""
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

# --- VERNIEUWDE FUNCTIE OM PVGIS DATA LIVE OP TE HALEN ---
@st.cache_data
def process_pvgis_live(segments, lat, lon, year, loss):
    """Haalt voor een lijst van segmenten de data live op bij PVGIS en telt de kWh-waarden op."""
    if not segments:
        return pd.DataFrame()

    all_power_series = []
    api_url = 'https://re.jrc.ec.europa.eu/api/seriescalc'

    with st.spinner(f"PVGIS data voor {year} ophalen voor {len(segments)} segment(en)..."):
        for i, segment in enumerate(segments):
            try:
                params = {
                    'lat': lat, 'lon': lon, 'startyear': year, 'endyear': year,
                    'outputformat': 'csv', 'pvcalculation': 1,
                    'peakpower': segment['kwp'], 'loss': loss,
                    'angle': segment['slope'], 'aspect': segment['azimuth'],
                    'raddatabase': 'PVGIS-ERA5'
                }
                
                response = requests.get(api_url, params=params)
                response.raise_for_status() # Stopt als de HTTP-request mislukt

                # Dynamische verwerking van de CSV-tekst
                all_lines = response.text.strip().splitlines()
                
                header_index = -1
                for j, line in enumerate(all_lines):
                    if line.startswith('time,'):
                        header_index = j
                        break
                
                if header_index == -1:
                    raise ValueError("Header-regel 'time,...' niet gevonden in de PVGIS-respons.")
                
                # Creëer een virtueel bestand in het geheugen voor pandas
                csv_buffer = io.StringIO('\n'.join(all_lines[header_index:]))
                
                df_pvgis = pd.read_csv(csv_buffer)
                
                df_pvgis['time'] = pd.to_datetime(df_pvgis['time'], format='%Y%m%d:%H%M')
                df_pvgis['P'] = pd.to_numeric(df_pvgis['P'], errors='coerce')
                
                all_power_series.append(df_pvgis.set_index('time')['P'])
                st.info(f"Segment {i+1} ({segment['kwp']} kWp) succesvol opgehaald.")

            except requests.exceptions.RequestException as e:
                st.error(f"Fout bij de verbinding met PVGIS voor segment {i+1}: {e}")
                continue
            except Exception as e:
                st.error(f"Fout bij verwerken van PVGIS data voor segment {i+1}: {e}")
                continue
    
    if not all_power_series:
        return pd.DataFrame()

    # Combineer de series en tel ze op
    total_power_w = pd.concat(all_power_series, axis=1).sum(axis=1)
    
    # Converteer van W naar kWh per uur
    total_energy_kwh_hourly = total_power_w / 1000
    
    # Resample van uurwaarden naar kwartierwaarden
    df_resampled = pd.DataFrame(total_energy_kwh_hourly, columns=['PVGIS_kwh'])
    df_resampled = df_resampled.resample('15min').ffill() / 4 # Verdeel de uur-energie over 4 kwartieren
    
    df_resampled.reset_index(inplace=True)
    df_resampled.rename(columns={'time': 'Tijdstip'}, inplace=True)

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

if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None
if 'pvgis_data' not in st.session_state:
    st.session_state.pvgis_data = None

st.header("Stap 1: Upload je bestanden")

with st.expander("Upload hier je energiebestanden", expanded=True):
    file_type = st.radio(
        "**Kies het type energiebestand:**",
        ('Normale CSV (Fluvius)', 'AMR Bestand (Fluvius)'), horizontal=True
    )
    col1, col2 = st.columns(2)
    with col1:
        file_import = st.file_uploader("1. Afname (import) [.csv]", type="csv")
        file_injectie = st.file_uploader("2. Injectie (export) [.csv]", type="csv")
    with col2:
        file_pv = st.file_uploader("3. Hulpverbruik (PV-opbrengst) [.csv]", type="csv")

with st.expander("Optioneel: Voeg live PVGIS-productie toe"):
    st.markdown("Definieer je locatie en voeg tot 5 PV-segmenten toe. De data wordt live van de PVGIS-server gehaald.")
    
    col_loc1, col_loc2, col_loc3, col_loc4 = st.columns(4)
    with col_loc1:
        lat = st.number_input("Breedtegraad", value=51.22, format="%.4f")
    with col_loc2:
        lon = st.number_input("Lengtegraad", value=5.08, format="%.4f")
    with col_loc3:
        year = st.number_input("Simulatiejaar", value=2020, min_value=2005, max_value=2020, step=1, help="PVGIS-ERA5 data is beschikbaar t/m 2020.")
    with col_loc4:
        loss = st.number_input("Systeemverlies (%)", value=14, min_value=0, max_value=100, step=1)

    num_segments = st.number_input("Aantal PVGIS-segmenten", min_value=0, max_value=5, value=1, step=1)
    
    pvgis_segments_live = []
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
            
            pvgis_segments_live.append({'kwp': kwp, 'slope': slope, 'azimuth': azimuth})


if st.button("Verwerk bestanden en haal PVGIS data op", type="primary"):
    if not (file_import or file_injectie or file_pv):
        st.warning("Upload ten minste één energiebestand om door te gaan.")
    else:
        with st.spinner("Data wordt verwerkt..."):
            # Bestaande logica voor Fluvius/AMR
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

            # Haal PVGIS-data live op en bewaar het apart
            if pvgis_segments_live:
                st.session_state.pvgis_data = process_pvgis_live(pvgis_segments_live, lat, lon, year, loss)
                if st.session_state.pvgis_data is not None and not st.session_state.pvgis_data.empty:
                    st.success("✅ PVGIS-data succesvol opgehaald en verwerkt!")
            else:
                st.session_state.pvgis_data = None


if st.session_state.processed_data is not None:
    st.header("Stap 2: Selecteer datumbereik en download")
    
    df = st.session_state.processed_data.copy()
    
    if st.session_state.pvgis_data is not None and not st.session_state.pvgis_data.empty:
        pvgis_df = st.session_state.pvgis_data
        
        # Omdat de PVGIS-data voor een specifiek jaar is (bv. 2020),
        # en de energiemetingen voor een ander jaar kunnen zijn, moeten we de
        # dag en maand matchen, ongeacht het jaar.
        pvgis_df['match_key'] = pvgis_df['Tijdstip'].dt.strftime('%m-%d %H:%M')
        df['match_key'] = df['Date'].dt.strftime('%m-%d %H:%M')
        
        df = pd.merge(
            df,
            pvgis_df[['match_key', 'PVGIS_kwh']],
            on='match_key',
            how='left'
        ).drop(columns=['match_key'])
        
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