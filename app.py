import streamlit as st
import pandas as pd
from io import BytesIO
import numpy as np
import re

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

# --- AANGEPAST: BELPEX WORDT NU ALS LOKAAL BESTAND GELEZEN ---
def process_belpex_file() -> pd.DataFrame:
    """Leest het Belpex CSV-bestand vanuit de repository."""
    belpex_path = "BelpexFilter.csv" # Simpel pad naar het bestand
    
    try:
        df_belpex = pd.read_csv(belpex_path, sep=';', encoding='cp1252')
        df_belpex.columns = df_belpex.columns.str.strip()
        df_belpex['Tijdstip_uur'] = pd.to_datetime(df_belpex['Date'], dayfirst=True)
        
        numeric_text = df_belpex['Euro'].str.extract(r'(-?[\d,]+)', expand=False)
        clean_price = pd.to_numeric(numeric_text.str.replace(',', '.'), errors='coerce')
        df_belpex['BELPEX_EUR_KWH'] = clean_price / 1000
        
        return df_belpex[['Tijdstip_uur', 'BELPEX_EUR_KWH']]
    except FileNotFoundError:
        st.error(f"Fout: Het bestand '{belpex_path}' niet gevonden in de repository. Zorg dat het bestand op GitHub staat.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Fout bij het laden van het Belpex-bestand: {e}")
        return pd.DataFrame()

def to_excel(df: pd.DataFrame) -> bytes:
    """Converteert een DataFrame naar een Excel-bestand in het geheugen."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    return output.getvalue()

# --- Streamlit App Interface (verder ongewijzigd) ---

st.set_page_config(layout="wide", page_title="Energie Data Combiner")
st.title("🔌 Energie Data Combiner")
st.markdown("Combineert Fluvius verbruiks-, injectie- en PV-data met Belpex-prijzen.")

if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None

st.header("Stap 1: Upload je energiebestanden")

with st.expander("Upload hier je bestanden", expanded=True):
    file_type = st.radio(
        "**Kies het type energiebestand:**",
        ('Normale CSV (Fluvius)', 'AMR Bestand (Fluvius)'),
        horizontal=True
    )
    
    col1, col2 = st.columns(2)
    with col1:
        file_import = st.file_uploader("1. Afname (import) [.csv]", type="csv")
        file_injectie = st.file_uploader("2. Injectie (export) [.csv]", type="csv")
    with col2:
        file_pv = st.file_uploader("3. Hulpverbruik (PV-opbrengst) [.csv]", type="csv")

if st.button("Verwerk bestanden", type="primary"):
    if not (file_import or file_injectie or file_pv):
        st.warning("Upload ten minste één energiebestand (import, injectie of PV) om door te gaan.")
    else:
        with st.spinner("Data wordt verwerkt..."):
            if file_type == 'Normale CSV (Fluvius)':
                df_import = process_energy_file(file_import, "Afname Actief")
                df_injectie = process_energy_file(file_injectie, "Injectie Actief")
                df_pv = process_energy_file(file_pv, "Hulpverbruik Actief")
            else: # AMR Bestand (Fluvius)
                df_import = process_amr_file(file_import)
                df_injectie = process_amr_file(file_injectie)
                df_pv = process_amr_file(file_pv)
            
            dataframes = []
            if df_import is not None and not df_import.empty:
                dataframes.append(df_import.rename(columns={'Volume': 'import_kwh'}))
            if df_injectie is not None and not df_injectie.empty:
                dataframes.append(df_injectie.rename(columns={'Volume': 'injection_kwh'}))
            if df_pv is not None and not df_pv.empty:
                dataframes.append(df_pv.rename(columns={'Volume': 'pv_kwh'}))

            if not dataframes:
                st.error("Geen geldig energiebestand gevonden of verwerkt. Controleer het bestandstype of de inhoud van de bestanden.")
                st.session_state.processed_data = None
            else:
                finale_df = dataframes[0]
                for df_to_merge in dataframes[1:]:
                    finale_df = pd.merge(finale_df, df_to_merge, on='Tijdstip', how='outer')

                df_belpex = process_belpex_file()
                
                if df_belpex is not None and not df_belpex.empty:
                    st.success("Belpex-data succesvol geladen uit repository.")
                    finale_df['Tijdstip_uur'] = finale_df['Tijdstip'].dt.floor('H')
                    finale_df = pd.merge(finale_df, df_belpex, on='Tijdstip_uur', how='left')
                    finale_df.drop(columns=['Tijdstip_uur'], inplace=True)
                else:
                    st.error("Kon Belpex-data niet laden. De BELPEX-kolom zal leeg zijn.")

                finale_df.rename(columns={'Tijdstip': 'Date', 'BELPEX_EUR_KWH': 'BELPEX'}, inplace=True)
                
                for col in ['import_kwh', 'injection_kwh', 'pv_kwh', 'BELPEX']:
                    if col not in finale_df.columns:
                        finale_df[col] = 0
                
                finale_df.fillna(0, inplace=True)
                finale_df.sort_values('Date', inplace=True)
                
                st.session_state.processed_data = finale_df
                st.success("✅ Bestanden succesvol verwerkt! Ga naar Stap 2.")

if st.session_state.processed_data is not None:
    st.header("Stap 2: Selecteer datumbereik en download")
    
    df = st.session_state.processed_data
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
        filtered_df = df.loc[mask]

        st.markdown(f"**Voorbeeld van de geselecteerde data ({len(filtered_df)} rijen):**")
        st.dataframe(filtered_df.head())

        excel_data = to_excel(filtered_df)
        st.download_button(
            label="📥 Download geselecteerde data als Excel",
            data=excel_data,
            file_name=f"gefilterde_energiemix_{start_date}_tot_{end_date}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )