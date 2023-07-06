#Contiene todas las funciones necesarias para ejecutar la aplicación
#
#
# Realizado por Ismael Franco Hernando.

# -- Imports --
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
from mysql.connector import (connection)
import pandas as pd
import numpy as np
import ipywidgets as widgets
import IPython.display as dply
import datetime
import os
import tensorflow as tf
tf.get_logger().setLevel('ERROR')
warnings.filterwarnings("ignore", category=DeprecationWarning)



def cargaDatos(use, pas, host, base):
    datos1D, clasif = conexionBase(use, pas, host, base)
    muestraBobinas(datos1D, clasif)

def conexionBase(use, pas, host, base):    
    try:
        cnx = connection.MySQLConnection(user=use, 
                                         password=pas,
                                         host=host,
                                         database=base)
        
        q01 = "SELECT * FROM GR_LPValues"
        datos1D = pd.read_sql(q01,cnx)
        q02 = "SELECT * FROM V_Coils"
        clasif = pd.read_sql(q02,cnx)
        cnx.close()
        return datos1D, clasif

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Usuario o contraseña incorrecto")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("La base de datos no existe")
        else:
            print(err)


def muestraBobinas(datos1D, datos):
    # Obtener los valores únicos de las bobinas
    valores_unicos = datos['SID'].unique()

    # Crear el desplegable
    desplegable = widgets.Dropdown(
        options=valores_unicos,
        description="Selecciona la bobina que desees:"
    )

    # Función que se ejecuta cuando se selecciona un valor en el desplegable
    def seleccionar_valor(valor):
        idBobina = desplegable.value
        print("\n---------------------------------------------------------------------------------------------")
        print("\nBobina seleccionada:", idBobina)
        registrosBobina = datos1D[datos1D['COILID'] == idBobina]
        registrosBobina = registrosBobina.sort_values(['COILID', 'MID', 'TILEID']).reset_index(drop=True)
        caractersiticas = get_features(registrosBobina, datos)
        dply.display(caractersiticas)
        creaCarpeta()
        caractersiticas.to_csv('./bobinas/'+str(idBobina)+'.csv', mode='w', index=False)
        evaluaBobina(caractersiticas, idBobina)

    # Vincular la función con el desplegable
    desplegable.observe(seleccionar_valor, names='value')

    # Mostrar el desplegable
    display(desplegable)

    
def get_features(datos, clasificados):
    # Creamos el DF que se devolverá con las diferentes columnas
    rDF = pd.DataFrame(columns = ['COILID' , 'MID', 'ZNMAX_FAILURES','ZNMIN_FAILURES', 'CALIBRATED', 'TOTAL_TILEID', 'L_DIS','R_DIS', 'MAP'])
    
    # Inicializamos los parámetros con los que se trabajará a lo largo de la función
    errorAntes = False # Será False en caso de que en la anterior iteración no hubiera ningún error,
                        # y True en caso contrario
    midAnterior = -1 # Contiene el ID del sensor de la anterior iteración 
    idAnterior = -1 # Contiene la ID de la bobina de la anterior iteración
    calibradosTotales = 0 # Contador de calibrados
    fallosTotalesMax = 0 # Contador de fallos por exceso de Zn
    fallosTotalesMin = 0 # Contador de fallos por falta de Zn
    contadorID = -1 # Contador para añadir los registros por orden en pandas
    mapa = []
    
    # Se recorren todas las tejas de todas las bobinas para evaluarlas
    for i in range(len(datos)):
        # ID de la bobina
        id = datos['COILID'][i]
        # ID del sensor que ha medido los datos
        mid = datos['MID'][i]
        
        # Comparamos las ID de los sensores y en caso de ser diferentes es que hemos pasado a analizar
        # los datos de otro sensor, por lo que se pueden guardar los datos evaluado del anterior sensor
        if mid != midAnterior:
            # Este contador permite no guardar datos en la primera iteración.
            if contadorID >= 0:
                # Obtenemos el número de tejas que tiene cada bobina por sensor
                tejasTotales = len(datos[(datos['COILID'] == idAnterior) & (datos['MID'] == midAnterior)])
                # Obtenemos la mitad de la bobina
                mitad = int(tejasTotales/2)                
                # Inicializamos la distancia a 0 por si no se encuntrar errores
                ldis = 0
                rdis = 0                
                # Recorremos desde la mitad hasta el inicio de la bobina hasta encontrar un fallo
                for j in range(mitad):
                    if mapa[mitad - 1 - j] != 0:
                        ldis = mitad - j
                        break                        
                # Recorremos desde la mitad hasta el final de la bobina hasta encontrar un fallo        
                for j in range(mitad):
                    if mapa[mitad + j] != 0:
                        rdis = mitad - j
                        break
                        
                # Guardamos los datos
                rDF.loc[contadorID] = [idAnterior,  midAnterior, fallosTotalesMax, fallosTotalesMin, calibradosTotales, tejasTotales, ldis, rdis, str(mapa)]
                
                # Se reinician los contadores de fallos, calibrados y booleano de error
                fallosTotalesMax = 0
                fallosTotalesMin = 0
                calibradosTotales = 0
                errorAntes = False
                mapa = []
            # Se aumenta el contador en 1 para guardar correctamente los datos en el DF    
            contadorID = contadorID + 1
            # Se actualiza la ID del sensior
            midAnterior = mid
            
        # En caso de que las IDs de las bobinas no coincidan se obtienen los datos de la nueva bobina        
        if idAnterior != id:
            # Se obtiene el registro de la bobina evaluada
            registro = clasificados.loc[clasificados['SID']==id]
            # Se guardan su ID, valores max y min de ZN
            idAnterior = registro['SID'].iloc[0]
            znMax = registro['ZnMax'].iloc[0]
            znMin = registro['ZnMin'].iloc[0]
            label = registro['CLASSLABEL'].iloc[0]
            # Se actualiza la ID de la nueva bobina
            idAnterior = id
            
        
        # Obtenemoss el ZN max y min que se ha medido en la teja
        datosMax = datos['MAX'][i]
        datosMin = datos['MIN'][i]
        # Obtenemos la ID de la teja a evaluar
        tileid = datos['TILEID'][i]            
        # Obtenemos el sensor que ha medido los datos
        mid = datos['MID'][i]
        # Obtenemos el valor medio de ZN en la teja
        mean = datos['MEAN'][i]
        
        codificado = 0
        # Evaluamos si cumple o no con los valores máximos de ZN
        if round(datosMax,1) > znMax:
            # En caso de que en la anterior iteración no hubiera un error se aumenta el número de fallos,
            # en caso contrario no se actualiza ya que seguimos en el mismo fallo
            if errorAntes != True:
                fallosTotalesMax = fallosTotalesMax + 1
                errorAntes = True
                
            codificado = 1
        # Evaluamos si cumple o no con los valores mínimos de ZN y no es una calibración
        elif  round(datosMin,1) < znMin and (datosMax > 0 and datosMin > 0):
            # En caso de que en la anterior iteración no hubiera un error se aumenta el número de fallos,
            # en caso contrario no se actualiza ya que seguimos en el mismo fallo
            if errorAntes != True:
                fallosTotalesMin = fallosTotalesMin + 1
                errorAntes = True
                
            codificado = -1
        # En caso de que los valores sean 0 se actualzia el contador de calibrados
        elif datosMax == 0 and datosMin == 0:
            calibradosTotales = calibradosTotales + 1
            # Se cambia a False el error, ya que los calibrados no son errores
            errorAntes = False
        else:
            # Se cambia a False ya que la teja evaluada cumple con los requisitos
            errorAntes = False 
        
        mapa.append(codificado)
    # Obtenemos el número de tejas que tiene cada bobina por sensor
    tejasTotales = len(datos[(datos['COILID'] == idAnterior) & (datos['MID'] == midAnterior)])
    # Obtenemos la mitad de la bobina
    mitad = int(tejasTotales/2)
    # Inicializamos la distancia a 0 por si no se encuntrar errores
    ldis = 0
    rdis = 0  
    # Recorremos desde la mitad hasta el inicio de la bobina hasta encontrar un fallo 
    for j in range(mitad):
        if mapa[mitad - 1 - j] != 0:
            ldis = mitad - j
            break            
    # Recorremos desde la mitad hasta el final de la bobina hasta encontrar un fallo 
    for j in range(mitad):
        if mapa[mitad + j] != 0:
            rdis = mitad - j
            break
            
    # Guardamos los datos
    rDF.loc[contadorID] = [id,  mid, fallosTotalesMax, fallosTotalesMin, calibradosTotales, tejasTotales, ldis, rdis, str(mapa)]  
    
    # Se devuelve el DF generado
    return rDF

def cargaModelos():
    directorio = './modelos'

    modelos = os.listdir(directorio)

    modelosCargados = []

    for m in modelos:
        rutaModelo = os.path.join(directorio, m)
        modelo = tf.keras.models.load_model(rutaModelo)
        modelosCargados.append(modelo)
        
    return modelosCargados

def unionMapas1D(datos):
    bobinas = datos['COILID'].unique()
    sensores = [(123.0, 124.0), (201.0, 202.0)]
    X = np.zeros(int(len(datos)/2)).tolist()    
    contadorID = 0
    
    for bob in bobinas:
        for s in sensores:
            mapa1 = datos.loc[datos['COILID']==bob].loc[datos['MID']==s[0]].MAP.iloc[0]      
            mapa1 = np.array(mapa1[1:-1].split(',')).astype(int)
            mapa2 = datos.loc[datos['COILID']==bob].loc[datos['MID']==s[1]].MAP.iloc[0]      
            mapa2 = np.array(mapa2[1:-1].split(',')).astype(int)
            X[contadorID] = np.concatenate((mapa1, mapa2), axis=0)
            contadorID+=1
            
    return X

def preprocesado(X, maxLongitud=208):
    nuevoX = []
    for x in X:
        nuevoX.append(np.pad(x, (0,maxLongitud-len(x)), 'constant', constant_values= 0))
        
    return np.expand_dims(nuevoX, axis=-1)

def evaluaBobina(datos, idBobina):
    mapa = unionMapas1D(datos)
    mapaProcesado = preprocesado(mapa)
    modelos = cargaModelos()
    
    predicc = []       
    for m in modelos:
        predicc.append(m.predict(mapaProcesado))
    
    sensores1 = predicc[:(len(predicc)//2)]
    sensores2 = predicc[(len(predicc)//2):]
    
    valorS1 = np.sum(sensores1)/len(sensores1)
    valorS2 = np.sum(sensores2)/len(sensores2)
    print("\nPredicción según los sensores: 123 y 124")
    if valorS1 <= 0.3:
        print("La predicción de la bobina es la clase OK")
        lab1 = "OK"
    else:
        print("La predicción de la bobina es la clase NOK")
        lab1 = "NOK"
        
    print("\nPredicción según los sensores: 201 y 202")
    if valorS2 <= 0.3:
        print("La predicción de la bobina es la clase OK")
        lab2 = "OK"
    else:
        print("La predicción de la bobina es la clase NOK")
        lab2 = "NOK"
        
    historialPath = 'historial.txt'

    # Verificar si el archivo existe
    if not os.path.exists(historialPath):
        # Crear el archivo vacío
        open(historialPath, 'w').close()

    # Obtener la fecha actual
    fecha = datetime.date.today()

    # Abrir el archivo en modo lectura
    with open(historialPath, 'r') as archivo:
        lineas = archivo.readlines()

    # Agregar una línea con la fecha actual y dos unos al final
    lineaNueva = f"{fecha}::{idBobina}::123-124::{lab1}\n"
    lineas.append(lineaNueva)
    lineaNueva = f"{fecha}::{idBobina}::201-202::{lab2}\n"
    lineas.append(lineaNueva)

    # Guardar el archivo modificado
    with open(historialPath, 'w') as archivo:
        archivo.writelines(lineas)
        
def creaCarpeta():
    ruta = './bobinas'

    if not os.path.exists(ruta):
        os.makedirs(ruta)