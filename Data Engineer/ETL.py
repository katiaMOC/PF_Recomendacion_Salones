import pandas as pd
import numpy as np
import re
import requests
import warnings
warnings.filterwarnings("ignore", message="Downcasting.*", category=FutureWarning)
from ast import literal_eval

#  2. Carga de Datos

# Business solo es el csv que se transformo del pkl
business = pd.read_csv('Data/Business.csv') 

# Review-1 esta en el drive en la carpeta Data 12/03/24 ------  Es el unico archivo que te faltaria pasar al datalake
Review = pd.read_csv('Data/Review-1.csv')

# User es del json transformado a CSV nomas
User = pd.read_csv('Data/User.csv', dtype={'Column_name': str})

'''
# Funciones de Transformacion
def transformation_Business(df):
    # Eliminar filas con valores NaN en la columna 'categories'
    Business = df.dropna(subset=['categories'])

    # Filtrar las filas que contienen 'Nail Salons' en la columna 'categories'
    Business = Business[Business['categories'].str.contains('Nail Salons')]


    # De limitamos el proyecto basado en la Categoria 'Nail Salon' o 'Salon de Uñas'


    #  Limpieza y Normalizacion

    columnas_a_eliminar = ['is_open', 'categories','state', 
        'business_id.1', 'name.1', 'address.1', 'city.1', 'state.1', 
        'postal_code.1', 'latitude.1', 'longitude.1', 'stars.1', 'review_count.1',
            'is_open.1', 'attributes.1', 'categories.1', 'hours.1']

    # Eliminar las columnas especificadas
    Business = Business.drop(columnas_a_eliminar, axis=1)

    # Define una función para obtener el estado desde la latitud y longitud
    def reverse_geocode(lat, lon):
        # Aquí va tu código para obtener el estado desde la latitud y longitud
        url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lon}"
        response = requests.get(url)
        data = response.json()
        if 'address' in data:
            state = data['address'].get('state', '')
            return state

    # Aplica la función a cada fila del DataFrame y crea una lista de resultados
    states = [reverse_geocode(row['latitude'], row['longitude']) for _, row in Business.iterrows()]

    # Asigna la lista de estados a la columna 'state' del DataFrame
    Business['state'] = states
    # Usando la API OpenStreetMap (OSM) se realizo una geocodificacion inversa que me devuelve los diferentes estados('state') basado en las columnas 'latitude' y 'longitude' 

    # Aplicamos el filtro con referencia en la columna 'name'
    Business = Business[Business['state'] == 'Florida']

    # Crea un nuevo DataFrame solo con las columnas 'business_id' y 'opening_hours_id'
    Atributes = Business[['business_id', 'attributes']].copy()

    # Crea un nuevo DataFrame solo con las columnas 'business_id' y 'opening_hours_id'
    opening_hour = Business[['business_id', 'hours']].copy()

    # Utilizo el método drop() de pandas para eliminar las columnas especificadas
    Business = Business.drop(['attributes', 'hours', 'state'], axis=1)

    return Atributes, opening_hour, Business

Atributes, opening_hour, Business = transformation_Business(business)
'''
def transformation_Business(df):
    
    # Crea un nuevo DataFrame solo con las columnas 'business_id' y 'opening_hours_id'
    opening_hour = df[['business_id', 'hours']].copy()

    # Crea un nuevo DataFrame solo con las columnas 'business_id' y 'opening_hours_id'
    Atributes = df[['business_id', 'attributes']].copy()
    
    # Utilizo el método drop() de pandas para eliminar las columnas especificadas
    Business = df.drop(['attributes', 'hours'], axis=1)

    return Atributes, opening_hour, Business

Atributes, opening_hour, Business = transformation_Business(business)


def guardar_business_csv(df_business, nombre_archivo):
    try:
        df_business.to_csv(nombre_archivo, index=False)
        print(f"Los datos de las reseñas se han guardado exitosamente en '{nombre_archivo}'.")
    except Exception as e:
        print(f"Ocurrió un error al intentar guardar los datos de las reseñas en '{nombre_archivo}': {e}")

# Uso de la función y tambien especifica la ruta
guardar_business_csv(Business, 'Datos1/C_Business.csv')


#Business = pd.read_csv('Datos/Prueba_Business.csv')

def transformation_Review(df):
    # Obtener los id únicos del DataFrame business
    business_ids_to_keep = Business['business_id'].unique()

    # Friltamos el dfreviewYelp basado en los balores unico de lao id de dfbusinessYelp
    Review = df[df['business_id'].isin(business_ids_to_keep)]

    # Convertir la columna 'date' a tipo datetime
    Review.loc[:, 'date'] = pd.to_datetime(Review['date'])

    columnas_a_eliminar = ['useful', 'funny', 'cool']
    # Eliminar las columnas especificadas
    Review = Review.drop(columnas_a_eliminar, axis=1)

    # Aplicar la limpieza de texto directamente sobre la columna 'text'
    Review['text'] = Review['text'].str.lower().str.replace(r'[^a-z0-9\s]', '')

    # Modificamos el tipo de dato a un valor flotante
    Review['stars'] = Review['stars'].astype('float64')

    # Renombrar la columna review_stars
    Review = Review.rename(columns={'stars' : 'review_stars'})

    # Genero la tabla date
    date = Review[['date']].copy()
    # Guardar
    #Review.to_csv('Datos1/C_Review.csv', index = False)    
    return Review

Reviews = transformation_Review(Review)

def guardar_reviews_csv(df_reviews, nombre_archivo):
    try:
        df_reviews.to_csv(nombre_archivo, index=False)
        print(f"Los datos de las reseñas se han guardado exitosamente en '{nombre_archivo}'.")
    except Exception as e:
        print(f"Ocurrió un error al intentar guardar los datos de las reseñas en '{nombre_archivo}': {e}")

# Uso de la función
guardar_reviews_csv(Reviews, 'Datos1/C_Reviews.csv')


def create_calendar_table(df):  
    
    # Convertir la columna 'date' a formato de fecha y hora
    df['date'] = pd.to_datetime(df['date'])

    # Extraer año, mes y día
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    
    # Calcular el semestre
    df['semester'] = (df['date'].dt.month - 1) // 6 + 1
    
    # Calcular el trimestre
    df['quarter'] = (df['date'].dt.month - 1) // 3 + 1
    
    # Agrupar por año, mes, día, semestre y trimestre y contar el número de eventos
    calendar = df.groupby(['year', 'month', 'day', 'semester', 'quarter']).size().reset_index(name='event_count')
    
    calendar.to_csv('Datos1/C_Calender.csv', index=False)

create_calendar_table(Reviews)


def transformation_User(df):

    # Obtener los id únicos del DataFrame dfbusinessYelp
    business_ids_to_keep = Reviews['user_id'].unique()

    # Friltamos el dfreviewYelp basado en los balores unico de lao id de dfbusinessYelp
    User = df[df['user_id'].isin(business_ids_to_keep)]

    # Eliminar filas duplicadas del DataFrame User
    User = User.drop_duplicates()

    #Creo la tabla Compliment_User
    Compliment = User[['user_id','useful', 'funny', 'cool', 'compliment_profile']].copy()

    #Columnas a eliminar de la tabla User
    columnas_a_eliminar = ['elite', 'friends', 'compliment_hot','compliment_more', 'compliment_profile',
            'compliment_cute', 'compliment_list', 'compliment_note', 
            'compliment_plain', 'compliment_cool', 'compliment_funny', 
            'compliment_writer', 'compliment_photos','useful', 'funny', 'cool']

    # Eliminar las columnas especificadas
    User = User.drop(columnas_a_eliminar, axis=1)

    # Es la tabla Compiment_User
    # Utilizar la función melt para convertir las columnas en filas
    Compliment = pd.melt(Compliment, id_vars=['user_id'], var_name='name_compliment', value_name='counts')

    # Ordenar el DataFrame por user_id
    Compliment = Compliment.sort_values(by='user_id')

    # Creamos la tabla name_compliment
    data = {
        'compliment_id': ['com-001', 'com-002', 'com-003', 'com-004'],
        'name_compliment': ['useful', 'funny', 'cool', 'compliment_profile']}

    name_compliment = pd.DataFrame(data)

    # Unir las tablas
    Compliment = pd.merge(Compliment, name_compliment, left_on='name_compliment', right_on='name_compliment')

    # Eliminar la columna 'compliment_name'
    Compliment = Compliment.drop(columns=['name_compliment'])

    #Realice un copiado por un warning que salia
    Compliment = Compliment[['user_id', 'compliment_id', 'counts']].copy()

    #Cramos la columna correo
    User['correo'] = User['name'] + User.index.astype(str) + '@gmail.com' 
    User['correo'] = User['correo'].str.lower()

    User.to_csv('Datos1/C_User.csv', index= False)
    Compliment.to_csv('Datos1/C_Compliment_User.csv', index= False)
    name_compliment.to_csv('Datos1/C_Compliment.csv', index= False)

transformation_User(User)


def transformation_Attribute(df):
    # Rellenar los valores nulos con un valor predeterminado
    df.loc[:, 'attributes'] = df['attributes'].fillna('False')

    # Convertir la columna 'attributes' en un DataFrame separado
    Attributes = df['attributes'].apply(literal_eval).apply(pd.Series)

    # Agregar la columna 'business_id' de nuevo al DataFrame de atributos
    Attributes['business_id'] = df['business_id']

    # Reorganizar las columnas para colocar 'business_id' al principio
    Attributes = Attributes[['business_id'] + list(Attributes.columns[:-1])]

    # Eliminar las columnas no deseadas
    columns_to_drop = ['AcceptsInsurance','RestaurantsDelivery', 'GoodForMeal', 
                    'RestaurantsAttire', 'RestaurantsTableService', 'DogsAllowed', 
                    'RestaurantsGoodForGroups', 'Music',  'OutdoorSeating', 'HasTV', 
                    'RestaurantsReservations', 'Alcohol', 'Ambience', 'HappyHour',
                    'GoodForDancing', 'Smoking', 'NoiseLevel', 'AcceptsInsurance', 
                    'HairSpecializesIn', 'RestaurantsTakeOut', 0, 'BusinessAcceptsBitcoin', 
                    'GoodForKids', 'WheelchairAccessible']
    Attributes = Attributes.drop(columns_to_drop, axis=1)

    # Remplazar los valores nan con un diccionario vacío
    Attributes['BusinessParking'] = Attributes['BusinessParking'].fillna('{}')

    # Convertir la columna 'BusinessParking' a un diccionario
    Attributes['BusinessParking'] = Attributes['BusinessParking'].apply(literal_eval)

    # Desanidar el diccionario
    CarParking = pd.json_normalize(Attributes['BusinessParking'])

    # Verificar si al menos una de las columnas tiene True en cada fila
    CarParking['Parking'] = CarParking[['garage', 'street', 'validated', 'lot', 'valet']].any(axis=1)

    # Eliminar las columnas no deseadas de la tabla parking
    CarParking = CarParking.drop(['garage', 'street', 'validated', 'lot', 'valet'], axis=1)

    # Combinar la tabla parking con la tabla Attributes
    Attributes = Attributes.merge(CarParking, left_index=True, right_index=True)

    # Eliminar la columna 'BusinessParking' de la tabla Attributes
    Attributes = Attributes.drop('BusinessParking', axis=1)

    # Normalizar la columna 'WiFi'
    def normalize_wifi(value):
        if value in ["u'free'", "'free'"]:
            return True
        else:
            return False
    Attributes['WiFi'] = Attributes['WiFi'].apply(normalize_wifi)

    # Mapear los valores de 'RestaurantsPriceRange2' y convertir a booleanos
    mapping = {2.0: True, 1.0: True, 3.0: True, 4.0: True}
    Attributes['RestaurantsPriceRange2'] = Attributes['RestaurantsPriceRange2'].map(mapping).astype(bool)

    # Reemplazar NaN por False en el DataFrame Attributes
    Attributes = Attributes.fillna(False)

    # Renombrar la columna 'RestaurantsPriceRange2' a 'Food'
    Attributes = Attributes.rename(columns={'RestaurantsPriceRange2': 'Food'})

    # Renombrar la columna 'Parking' a 'CarParking'
    Attributes = Attributes.rename(columns={'Parking': 'CarParking'})

    # Agregar una columna 'Attribute_id' que contiene el ID único para cada fila
    Attributes['attribute_id'] = ['at-' + str(i).zfill(3) for i in range(1, len(Attributes) + 1)]

    # Lista con el orden deseado de las columnas
    column_order = ['business_id', 'attribute_id', 'BusinessAcceptsCreditCards', 'CarParking', 
                    'BikeParking', 'WiFi', 'Food', 'ByAppointmentOnly']

    # Reordenar las columnas del DataFrame
    Attributes = Attributes.reindex(columns=column_order)

    # Obtener los nombres de las columnas
    columnas = Attributes.columns

    # Convertir los nombres de las columnas a minúsculas
    columnas_en_minuscula = [columna.lower() for columna in columnas]

    # Asignar los nuevos nombres de columnas al DataFrame
    Attributes.columns = columnas_en_minuscula

    # Convertir las columnas de tipo booleano a tipo objeto (str)
    Attributes['carparking'] = Attributes['carparking'].astype(str)
    Attributes['wifi'] = Attributes['wifi'].astype(str)
    Attributes['food'] = Attributes['food'].astype(str)

    # Lista de atributos
    attributes = ['businessacceptscreditcards', 'carparking', 'bikeparking', 'wifi', 'food', 'byappointmentonly']

    # Crear un nuevo DataFrame
    df_new = pd.DataFrame(columns=['business_id', 'attributes', 'value'])

    # Iterar sobre cada fila del DataFrame original
    for index, row in Attributes.iterrows():
        business_id = row['business_id']
        for attribute in attributes:
            value = row[attribute]
            df_new = pd.concat([df_new, pd.DataFrame({'business_id': [business_id], 'attributes': [attribute], 'value': [value]})], ignore_index=True)

    Attributes = df_new

    # Filtrar el DataFrame para mantener solo las filas donde el valor es True
    Attributes = Attributes[Attributes['value'] == 'True']

    Attributes = Attributes.drop(['value'], axis = 1)

    # Crear un DataFrame con los nombres de los atributos
    data = {'type_attributes_id': ['t-001', 't-002', 't-003', 't-004', 't-005', 't-006'],
            'name_attributes': ['businessacceptscreditcards', 'carparking', 'bikeparking', 
                                'wifi', 'food', 'byappointmentonly']}

    # Crear el DataFrame
    Type_Attribute = pd.DataFrame(data)

    # Realizar la unión por la columna 'attributes' y 'name_attributes'
    df = Attributes.merge(Type_Attribute, left_on='attributes', right_on='name_attributes', how='left')

    # Mantener solo las columnas necesarias
    Attributes = df[['business_id', 'type_attributes_id']]

    # Crear una copia profunda de 'Attributes' para asegurarte de que es un DataFrame independiente
    Attributes1 = Attributes.copy(deep=True)

    # Utilizar .loc para evitar SettingWithCopyWarning
    # Crear una columna 'opening_hours_id' que sea incremental
    Attributes1.loc[:, 'attributes_id'] = ['at-' + str(i).zfill(3) for i in range(1, len(Attributes) + 1)]
    
    
    Attributes1 = Attributes1[['attributes_id', 'business_id','type_attributes_id']]

    Attributes1.to_csv('Datos1/C_Attributes.csv', index=False)
    Type_Attribute.to_csv('Datos1/C_Type_Attributes.csv', index=False)

transformation_Attribute(Atributes)



def transformation_Opening_Hours(df):
    # Crear una lista vacía para almacenar las filas de la nueva tabla
    opening_hours = []

    # Iterar sobre cada fila en la tabla 'Business'
    for idx, row in df.iterrows():
        # Extraer los horarios de operación
        hours_str = row['hours']
        try:
            # Convertir la cadena de texto a un diccionario
            hours = literal_eval(hours_str)
            # Iterar sobre cada día de la semana en los horarios de operación
            for day, time in hours.items():
                # Dividir el horario en 'hora de apertura' y 'hora de cierre'
                opening_time, closing_time = time.split('-')
                # Agregar una nueva fila a la lista
                opening_hours.append([row['business_id'], day, opening_time, closing_time])
        except (ValueError, SyntaxError):
            # Manejar el caso en el que la cadena no sea un diccionario válido
            pass

    # Convertir la lista en una tabla de pandas
    opening_hours = pd.DataFrame(opening_hours, columns=['business_id', 'day_of_week', 'opening_time', 'closing_time'])

    # Creamos la tabla Days
    Days = {'days_id': ['d-001', 'd-002', 'd-003', 'd-004', 'd-005', 'd-006', 'd-007'],
    'name_days': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']}
    Days = pd.DataFrame(Days)

    # Crear un nuevo DataFrame combinando opening_hours y Days usando merge
    opening_hours = pd.merge(opening_hours, Days, left_on='day_of_week', right_on='name_days')

    # Eliminar la columna 'name_days' ya que ya no es necesaria
    opening_hours.drop(['name_days', 'day_of_week'], axis=1, inplace=True)

    # Unir las columnas opening_time y closing_time con un guión "-"
    opening_hours['combined_time'] = opening_hours['opening_time'] + '-' + opening_hours['closing_time']

    # Copio la tabla opening_hours
    hours = opening_hours[['combined_time']]

    # Obtener valores únicos de la columna 'combined_time'
    unique_times = hours['combined_time'].unique()

    # Crear un DataFrame con los valores únicos
    unique_df = pd.DataFrame({'combined_time': unique_times})

    # Agregar una columna 'hours_id' con valores incrementales
    unique_df['hours_id'] = ['h-' + str(i).zfill(3) for i in range(1, len(unique_df) + 1)]

    hours = unique_df

    # Unir las tablas opening_hours y hours usando la columna 'combined_time'
    result = pd.merge(opening_hours, hours[['combined_time', 'hours_id']], on='combined_time', how='left')

    # Eliminar la columna 'combined_time' si no la necesitas
    result.drop('combined_time', axis=1, inplace=True)

    opening_hours = result

    # Eliminamos las columnas opening_time y closing_time
    opening_hours = opening_hours.drop(['opening_time', 'closing_time'], axis= 1)

    # Crear una columna 'opening_hours_id' que sea incremental
    opening_hours['opening_hours_id'] = ['oh-' + str(i).zfill(3) for i in range(1, len(opening_hours) + 1)]

    # Seleccionar las columnas en el orden deseado
    opening_hours = opening_hours[['opening_hours_id', 'business_id', 'days_id', 'hours_id']]

    # Cambiar el nombre de la columna 'combined_time' a 'Horario'
    hours = hours.rename(columns={'combined_time': 'Schedule'})

    #return opening_hours, hours, Days
    # Guardar CSV
    opening_hours.to_csv('Datos1/C_Opening_Hours.csv', index= False)
    Days.to_csv('Datos1/C_Days.csv', index= False)
    hours.to_csv('Datos1/C_Schedule.csv', index= False)

transformation_Opening_Hours(opening_hour)


