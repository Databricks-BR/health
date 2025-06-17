import os
import re
from unidecode import unidecode
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderUnavailable, GeocoderTimedOut, GeocoderServiceError
import speech_recognition as sr
from streamlit_geolocation import streamlit_geolocation
from geopy.distance import geodesic
from st_audiorec import st_audiorec
import io
import pydeck as pdk

# Configuration
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml"

def sql_query(query: str) -> pd.DataFrame:
    cfg = Config()
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

def clean_text(text):
    if text is None:
        return ""
    # Remove accents and special characters, keep letters, numbers, spaces, basic punctuation
    text = unidecode(str(text))
    cleaned = re.sub(r'[^\w\s,.-]', ' ', text)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

def get_address(lat, lon):
    geolocator = Nominatim(user_agent="streamlit_app", timeout=10)
    try:
        location = geolocator.reverse((lat, lon), language='pt-BR')
        return location.address if location else None
    except (GeocoderUnavailable, GeocoderTimedOut, GeocoderServiceError):
        return None

def get_city_from_coords(lat, lon):
    geolocator = Nominatim(user_agent="streamlit_app", timeout=10)
    try:
        location = geolocator.reverse((lat, lon), language='pt-BR')
        if location and 'address' in location.raw:
            return location.raw['address'].get('city') or location.raw['address'].get('town')
    except (GeocoderUnavailable, GeocoderTimedOut, GeocoderServiceError):
        pass
    return None

def make_waze_link(lat, lon):
    return f"https://waze.com/ul?ll={lat},{lon}&navigate=yes"

def make_googlemaps_link(lat, lon):
    return f"https://www.google.com/maps?q={lat},{lon}"

# Main app
st.set_page_config(layout="wide")
st.image(
    "https://raw.githubusercontent.com/Databricks-BR/health/refs/heads/main/image/busca_medicamento.png",
    use_column_width=True
)
# st.header("Buscador de Medicamentos")

# Automatic geolocation
st.subheader("Localização Automática")
location = streamlit_geolocation()
user_coords = (location['latitude'], location['longitude']) if location else None

if user_coords:
    address = get_address(*user_coords)
    if address:
        address_clean = clean_text(address)
        st.success(f"Localização detectada: {address_clean}; Coordenadas: {user_coords}")
    else:
        st.warning("Endereço não disponível (serviço de geocodificação indisponível ou sem resposta)")
else:
    st.warning("Permita acesso à localização para continuar")

# Medication input section
if user_coords:
    user_city = get_city_from_coords(*user_coords)
    if user_city:
        user_city_clean = clean_text(user_city)
        st.success(f"Cidade inferida: {user_city_clean}")
        available_cities = sql_query("SELECT DISTINCT cidade FROM demo_health.buscador_medicamento.estoque_filial")['cidade'].tolist()
        available_cities_clean = [clean_text(c) for c in available_cities]
        
        if user_city_clean in available_cities_clean:
            # Get medications for this city
            original_city = available_cities[available_cities_clean.index(user_city_clean)]
            medications = sql_query(f"""
                SELECT DISTINCT LOWER(remedio) AS remedio
                FROM demo_health.buscador_medicamento.estoque_filial 
                WHERE cidade = '{original_city}'
            """)['remedio'].tolist()
            
            # Input method selection
            input_method = st.radio("Selecione o método de entrada:", 
                                  ["Gravar áudio", "Selecionar manualmente"],
                                  horizontal=True)
            
            selected_med = None
            # Voice input
            if input_method == "Gravar áudio":
                st.subheader("Diga o nome do medicamento")
                wav_audio_data = st_audiorec()
                
                if wav_audio_data:
                    st.audio(wav_audio_data, format='audio/wav')
                    audio_file = io.BytesIO(wav_audio_data)
                    r = sr.Recognizer()
                    try:
                        with sr.AudioFile(audio_file) as source:
                            audio = r.record(source)
                            try:
                                text = r.recognize_google(audio, language='pt-BR')
                                selected_med = text.lower()
                                selected_med = clean_text(selected_med)
                                st.success(f"Medicamento reconhecido: {selected_med}")
                            except sr.UnknownValueError:
                                st.error("Não foi possível reconhecer o áudio")
                            except sr.RequestError as e:
                                st.error(f"Erro ao acessar o serviço de reconhecimento: {e}")
                    except Exception as e:
                        st.error(f"Erro ao processar o áudio: {str(e)}")
            
            # Manual selection
            if input_method == "Selecionar manualmente":
                st.subheader("Seleção Manual")
                selected_med = st.selectbox("Selecione o medicamento:", 
                                          sorted(medications),
                                          index=0,
                                          format_func=lambda x: x.lower())
                selected_med = clean_text(selected_med)
            
            # Process when medication is selected
            if selected_med:
                results = sql_query(f"""
                    SELECT filial, cidade, remedio, estoque, latitude, longitude, endereco
                    FROM demo_health.buscador_medicamento.estoque_filial
                    WHERE cidade = '{original_city}'
                      AND LOWER(remedio) = '{selected_med}'
                      AND estoque > 0
                """)
                if not results.empty:
                    results['distance'] = results.apply(
                        lambda row: geodesic(user_coords, (row['latitude'], row['longitude'])).km, 
                        axis=1
                    ).round(2)
                    results['waze_link'] = results.apply(
                        lambda row: make_waze_link(row['latitude'], row['longitude']),
                        axis=1
                    )
                    results['gmap_link'] = results.apply(
                        lambda row: make_googlemaps_link(row['latitude'], row['longitude']),
                        axis=1
                    )
                    st.subheader("Filiais Disponíveis")
                    # Prepare data for pydeck
                    pharmacy_df = results.assign(
                        lat=lambda x: x['latitude'],
                        lon=lambda x: x['longitude']
                    )
                    user_df = pd.DataFrame({
                        'lat': [user_coords[0]],
                        'lon': [user_coords[1]]
                    })
                    # Create pydeck layers
                    pharmacy_layer = pdk.Layer(
                        'ScatterplotLayer',
                        data=pharmacy_df,
                        get_position='[lon, lat]',
                        get_color='[255, 0, 0, 160]',  # Red color for pharmacies
                        get_radius=100,
                        pickable=True
                    )
                    user_layer = pdk.Layer(
                        'ScatterplotLayer',
                        data=user_df,
                        get_position='[lon, lat]',
                        get_color='[0, 0, 255, 160]',  # Blue color for user
                        get_radius=150
                    )
                    # Set the viewport
                    view_state = pdk.ViewState(
                        latitude=user_coords[0],
                        longitude=user_coords[1],
                        zoom=12,
                        pitch=0
                    )
                    # Create and display the deck
                    deck = pdk.Deck(
                        layers=[pharmacy_layer, user_layer],
                        initial_view_state=view_state,
                        map_style='mapbox://styles/mapbox/light-v10',
                        tooltip={
                            'html': '<b>{filial}</b><br>Estoque: {estoque}',
                            'style': {
                                'backgroundColor': 'white',
                                'color': 'black'
                            }
                        }
                    )
                    st.pydeck_chart(deck)
                    # Show table with distance, address, Waze link, and Google Maps link
                    st.dataframe(
                        results[['filial', 'endereco', 'distance', 'estoque', 'waze_link', 'gmap_link']],
                        column_config={
                            "filial": "Unidade",
                            "endereco": "Endereço",
                            "distance": st.column_config.NumberColumn(
                                "Distância (km)", 
                                format="%.2f km"
                            ),
                            "estoque": st.column_config.NumberColumn(
                                "Estoque", 
                                format="%d unidades"
                            ),
                            "waze_link": st.column_config.LinkColumn(
                                "Waze",
                                help="Abrir no Waze"
                            ),
                            "gmap_link": st.column_config.LinkColumn(
                                "Google Maps",
                                help="Abrir no Google Maps"
                            )
                        },
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.warning("Nenhum estoque disponível para o medicamento selecionado")
                    st.write("Consulta usada:", f"cidade='{original_city}', remedio='{selected_med}'")
        else:
            st.warning(f"Cidade {user_city_clean} não encontrada no sistema")
    else:
        st.warning("Não foi possível determinar a cidade a partir da localização")






