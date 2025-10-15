import streamlit as st
import pandas as pd
from io import BytesIO

# --- Functies uit het originele script, aangepast voor Streamlit ---

def process_energy_file(file_upload, register_type: str) -> pd.DataFrame:
    """
    Leest een geüpload energie-CSV-bestand, filtert op een specifiek 'Register',
    en creëert een schone DataFrame met Tijdstip en Volume.
    """
    if file_upload is None:
        return pd.DataFrame()

    try:
        df = pd.read_csv(file_upload, sep=';')
        
        # Converteer datum- en tijdkolommen naar datetime-objecten
        df['Tijdstip'] = pd.to_datetime(
            df['Van (datum)'].astype(str) + ' ' + df['Van (tijdstip)'].astype(str),
            dayfirst=True
        )
        
        # Filter op het gewenste register
        df_filtered = df[df['Register'] == register_type].copy()
        
        # Converteer de volumekolom naar een numeriek formaat
        df_filtered['Volume'] = df_filtered['Volume'].str.replace(',', '.', regex=False).astype(float)
        df_clean = df_filtered[['Tijdstip', 'Volume']].copy()
        
        return df_clean
    except Exception as e:
        st.error(f"Fout bij het verwerken van '{file_upload.name}': {e}")
        return pd.DataFrame()

def process_belpex_file(file_upload) -> pd.DataFrame:
    """ Leest het geüploade Belpex CSV-bestand en formatteert de data. """
    if file_upload is None:
        return pd.DataFrame()
        
    try:
        df_belpex = pd.read_csv(file_upload, sep=';', encoding='cp1252')
        df_belpex.columns = df_belpex.columns.str.strip()

        df_belpex['Tijdstip_uur'] = pd.to_datetime(df_belpex['Date'], dayfirst=True)
        
        # Extraheer en converteer de prijs naar numeriek formaat
        numeric_text = df_belpex['Euro'].str.extract(r'(-?[\d,]+)', expand=False)
        clean_price = pd.to_numeric(numeric_text.str.replace(',', '.'), errors='coerce')
        df_belpex['BELPEX_EUR_KWH'] = clean_price / 1000
        
        return df_belpex[['Tijdstip_uur', 'BELPEX_EUR_KWH']]
    except Exception as e:
        st.error(f"Fout bij het verwerken van het Belpex-bestand: {e}")
        return pd.DataFrame()

# Functie om de data te converteren naar een Excel-bestand in het geheugen
def to_excel(df: pd.DataFrame) -> bytes:
    """Converteert een DataFrame naar een Excel-bestand in het geheugen (bytes)."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    processed_data = output.getvalue()
    return processed_data

# --- Streamlit App Interface ---

st.set_page_config(layout="wide", page_title="Energie Data Combiner")

st.title("🔌 Energie Data Combiner")
st.markdown("Deze tool combineert Fluvius verbruiks-, injectie- en PV-data met Belpex-prijzen.")

# Gebruik session state om de data tussen te stappen te bewaren
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = None

# --- Stap 1: Bestanden uploaden ---
st.header("Stap 1: Upload je CSV-bestanden")

with st.expander("Upload hier je bestanden", expanded=True):
    col1, col2 = st.columns(2)
    with col1:
        file_import = st.file_uploader("1. Afname (import)", type="csv")
        file_injectie = st.file_uploader("2. Injectie (export)", type="csv")
    with col2:
        file_pv = st.file_uploader("3. Hulpverbruik (PV-opbrengst)", type="csv")
        file_belpex = st.file_uploader("4. Belpex Prijzen (BelpexFilter.csv)", type="csv")

if st.button("Verwerk bestanden", type="primary"):
    # Vereiste is minstens één energiebestand en het Belpex-bestand
    if not (file_import or file_injectie or file_pv) or not file_belpex:
        st.warning("Upload ten minste één energiebestand (import, injectie of PV) én het Belpex-bestand.")
    else:
        with st.spinner("Data wordt verwerkt..."):
            # Verwerk de geüploade energiebestanden
            df_import = process_energy_file(file_import, "Afname Actief")
            df_injectie = process_energy_file(file_injectie, "Injectie Actief")
            df_pv = process_energy_file(file_pv, "Hulpverbruik Actief")
            
            # Hernoem de kolommen
            df_import.rename(columns={'Volume': 'import_kwh'}, inplace=True)
            df_injectie.rename(columns={'Volume': 'injection_kwh'}, inplace=True)
            df_pv.rename(columns={'Volume': 'pv_kwh'}, inplace=True)
            
            # Combineer de dataframes
            finale_df = pd.DataFrame(columns=['Tijdstip'])
            if not df_import.empty:
                finale_df = pd.merge(finale_df, df_import, on='Tijdstip', how='outer')
            if not df_injectie.empty:
                finale_df = pd.merge(finale_df, df_injectie, on='Tijdstip', how='outer')
            if not df_pv.empty:
                finale_df = pd.merge(finale_df, df_pv, on='Tijdstip', how='outer')

            # Verwerk en voeg Belpex-data toe
            df_belpex = process_belpex_file(file_belpex)
            if not df_belpex.empty:
                finale_df['Tijdstip_uur'] = finale_df['Tijdstip'].dt.floor('H')
                finale_df = pd.merge(
                    finale_df, 
                    df_belpex[['Tijdstip_uur', 'BELPEX_EUR_KWH']], 
                    on='Tijdstip_uur', 
                    how='left'
                )
                finale_df.drop(columns=['Tijdstip_uur'], inplace=True)

            # Data opschonen
            finale_df.rename(columns={'Tijdstip': 'Date', 'BELPEX_EUR_KWH': 'BELPEX'}, inplace=True)
            finale_df.fillna(0, inplace=True)
            finale_df.sort_values('Date', inplace=True)
            
            # Bewaar de data in de sessie
            st.session_state.processed_data = finale_df
            st.success("✅ Bestanden succesvol verwerkt! Ga naar Stap 2.")


# --- Stap 2: Datumbereik selecteren en downloaden ---
if st.session_state.processed_data is not None:
    st.header("Stap 2: Selecteer datumbereik en download")
    
    df = st.session_state.processed_data
    
    # Haal de min en max datum uit de data voor de date picker
    min_date = df['Date'].min().date()
    max_date = df['Date'].max().date()

    col3, col4 = st.columns(2)
    with col3:
        start_date = st.date_input("Startdag", min_value=min_date, max_value=max_date, value=min_date)
    with col4:
        end_date = st.date_input("Einddag", min_value=min_date, max_value=max_date, value=max_date)

    if start_date > end_date:
        st.error("Fout: De startdag kan niet na de einddag liggen.")
    else:
        # Filter de data op basis van de geselecteerde range
        mask = (df['Date'].dt.date >= start_date) & (df['Date'].dt.date <= end_date)
        filtered_df = df.loc[mask]

        st.markdown(f"**Voorbeeld van de geselecteerde data ({len(filtered_df)} rijen):**")
        st.dataframe(filtered_df.head())

        # Downloadknop
        excel_data = to_excel(filtered_df)
        st.download_button(
            label="📥 Download geselecteerde data als Excel",
            data=excel_data,
            file_name=f"gefilterde_energiemix_{start_date}_tot_{end_date}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )