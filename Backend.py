from flask import Flask, request, render_template, redirect, url_for
import os
import pandas as pd
import pyodbc
import glob

app = Flask(__name__)

# Ruta donde se almacenarán temporalmente los archivos
UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Asegúrate de que la carpeta de subida exista
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload_files', methods=['POST'])
def upload_files():
    if 'files' not in request.files:
        return 'No se enviaron archivos', 400
    
    files = request.files.getlist('files')
    
    for file in files:
        if file.filename == '':
            return 'No se seleccionó ningún archivo', 400
        
        if file and file.filename.endswith('.xlsx'):
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
            file.save(filepath)
            
            # Procesar el archivo usando la función existente
            PROCESO(filepath)
        else:
            return 'Archivo no válido. Solo se permiten archivos Excel (.xlsx)', 400
    
    return redirect(url_for('index'))

def PROCESO(carga):

    ArchivosExcel = glob.glob(os.path.join(carga.strip('"'), "*.xlsx"))

    conn = None
    cursor = None
    
    # AL RECORRER EL PATH CON LOS ARCHIVOS EXCEL
    for Formato in ArchivosExcel:
        print(f"Procesando archivo: {Formato}")
        try:
            Data = pd.read_excel(Formato, sheet_name="DATOS", header=None)
        except Exception as e:
            print(f"Error al leer el archivo {Formato}: {e}")
            continue
        
        tipoForm = Data.iat[0,0]
        print(f"Tipo de Formato detectado: {tipoForm}")

        #%% CONDICIONAL PARA CARGAR LOS FORMATO 1
        if tipoForm == "FORMATO 1":
            Formato_1 =  Formato
            Hoja_F1 = 'DATOS'

            # Leer el archivo Excel, utilizando la fila 20 como encabezado
            Data_F1 = pd.read_excel(Formato_1, sheet_name = Hoja_F1, header=23)

            # Seleccionar el rango de filas y columnas
            Data_F1 = Data_F1.iloc[0:4, 0:8]
            Niveles = ["Nivel 1 (Conocer)", "Nivel 2 (Comprender)", "Nivel 3 (Aplicar)", "Nivel 4 (Evaluar)"]
            Data_F1[Niveles] = Data_F1[Niveles].fillna(0)

            # Cambiar los títulos de las columnas 11 y 12 a "De conocimiento" y "De desempeño"
            Data_F1.rename(columns={Data_F1.columns[6]: "De conocimiento", Data_F1.columns[7]: "De desempeño"}, inplace=True)
            Data_F1["De conocimiento"] = Data_F1["De conocimiento"].fillna("NA")
            Data_F1["De desempeño"] = Data_F1["De desempeño"].fillna("NA")

            # Leer el archivo Excel auxiliar
            DataAUX_F1 = pd.read_excel(Formato_1, sheet_name=Hoja_F1)
            DataAUX_F1 = DataAUX_F1.iloc[11:19, 1:3]

            # Extraer información adicional
            nombreAsign = DataAUX_F1.iloc[0, 1]
            codigoAsign = DataAUX_F1.iloc[1, 1]
            grupoClase = DataAUX_F1.iloc[5, 1]
            nombreProf = DataAUX_F1.iloc[7,1]
            nucleoForm = DataAUX_F1.iloc[3, 1]
            centroEst = DataAUX_F1.iloc[4, 1]
            periodoAcad = DataAUX_F1.iloc[6, 1]

            # Crear un DataFrame con la misma cantidad de estudiantes que DataF1 y agregar las columnas repetidas
            num_filas = len(Data_F1)
            DataAUX_F1 = pd.DataFrame({
                'Asignatura': [nombreAsign] * num_filas,
                'Codigo': [codigoAsign] * num_filas,
                'Nucleo de Formación': [nucleoForm] * num_filas,
                'Centro de Estudios': [centroEst] * num_filas,
                'Grupo de Clase': [grupoClase] * num_filas,
                'Periodo Académico': [periodoAcad] * num_filas,
                'Profesor': [nombreProf] * num_filas
            })

            Data_F1 = pd.concat([DataAUX_F1, Data_F1], axis=1)

            # Crear una conexión a la base de datos de Access
            conn_str = (
                r'DRIVER={SQL Server};'
                r'SERVER=servidorrap.database.windows.net;'
                r'DATABASE=BaseDatosRAP;'
                r'UID=adminrap;'
                r'PWD=M@mey0315;'
            )

            try:
                conn = pyodbc.connect(conn_str)
                cursor = conn.cursor()

                for index, row in Data_F1.iterrows():
                    # Verificar si ya existe la información
                    cursor.execute("""
                    SELECT COUNT(*) FROM FORMATO_1
                    WHERE Asignatura = ? AND Codigo = ? AND [Nucleo de Formación] = ? AND [Periodo Académico] = ? AND [Codigo RAP] = ?
                    AND [Grupo de Clase] = ?
                    """, row['Asignatura'], row['Codigo'], row['Nucleo de Formación'], row['Periodo Académico'], row['Código'], row['Grupo de Clase'])
                    
                    count = cursor.fetchone()[0]
                    
                    if count == 0:
                        # Insertar los datos en la tabla existente si no existen
                        cursor.execute("""
                        INSERT INTO FORMATO_1 (Asignatura, Codigo, [Nucleo de Formación], [Centro de Estudios], [Grupo de Clase],
                                               [Periodo Académico], [Profesor], [Codigo RAP], [Resultado de Aprendizaje], 
                                               [Nivel 1], [Nivel 2], [Nivel 3], [Nivel 4], [Evidencias de Conocimiento], [Evidencias de Desempeño])
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, row['Asignatura'], row['Codigo'], row['Nucleo de Formación'], row['Centro de Estudios'], row['Grupo de Clase'], row['Periodo Académico'], row['Profesor'],row['Código'], row['Resultado de Aprendizaje del Programa o del Departamento'], row['Nivel 1 (Conocer)'], row['Nivel 2 (Comprender)'], row['Nivel 3 (Aplicar)'], row['Nivel 4 (Evaluar)'], row['De conocimiento'], row['De desempeño'])
                    else:
                        print(f"\nEl registro con Código: {row['Codigo']}, Asignatura: {row['Asignatura']}, Periodo Académico: {row['Periodo Académico']} y Código RAP: {row['Código']} ya está cargado en la base de datos.")

                conn.commit()
                print("\nProceso de carga completado.")
            except pyodbc.Error as e:
                print(f"Error al conectar con la base de datos de Access o cargar datos: {e}")
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()

        #%% CONDICIONAL PARA CARGAR LOS FORMATO 1A
        
        elif tipoForm == "FORMATO 1A":

            Formato_1A = Formato
            Hoja_F1A = 'DATOS'

            #%% DATOS ESTUDIANTES Y CALIFICACIONES RAP
            try:
                # Leer el archivo Excel
                Data_F1A = pd.read_excel(Formato_1A, Hoja_F1A)

                # Seleccionar el rango de filas y columnas
                Data_F1A = Data_F1A.iloc[22:56, 1:6]  # Recordar que iloc es cero-indexado

                Data_F1A.columns = ['Estudiante', 'Calificación RAP 1', 'Calificación RAP 2', 'Calificación RAP 3', 'Calificación RAP 4']

                primerFilVacia = Data_F1A[Data_F1A['Estudiante'].isna()].index
                
                # Si hay registros vacíos, eliminar desde el primer registro vacío en adelante
                
                if not primerFilVacia.empty:
                    Data_F1A = Data_F1A.loc[:primerFilVacia[0]-1]

                # Reiniciar el índice de Data_F1A
                Data_F1A.reset_index(drop=True, inplace=True)

                # Mostrar el DataFrame condicionado
                #print("DataFrame")
                #print(Data_F1A)

                DataAUX_F1A = pd.read_excel(Formato_1A, Hoja_F1A)
                # Seleccionar el rango de filas y columnas
                DataAUX_F1A = DataAUX_F1A.iloc[11:19, 1:3]

                nombreAsign = DataAUX_F1A.iloc[0, 1]
                codigoAsign = DataAUX_F1A.iloc[1, 1]
                nucleoForm = DataAUX_F1A.iloc[3, 1]
                centroEst = DataAUX_F1A.iloc[4, 1]
                grupoClas = DataAUX_F1A.iloc[5, 1]
                periodoAcad = DataAUX_F1A.iloc[6, 1]
                nombreProf = DataAUX_F1A.iloc[7, 1]

                # Crear un DataFrame con la misma cantidad de estudiantes que Data_F1A y agregar las columnas repetidas
                num_estudiantes = len(Data_F1A)
                DataAUX_F1A = pd.DataFrame({
                    'Asignatura': [nombreAsign] * num_estudiantes,
                    'Codigo': [codigoAsign] * num_estudiantes,
                    'Nucleo de Formación': [nucleoForm] * num_estudiantes,
                    'Centro de Estudios': [centroEst] * num_estudiantes,
                    'Grupo de Clase': [grupoClas] * num_estudiantes,
                    'Periodo Académico': [periodoAcad] * num_estudiantes,
                    'Profesor': [nombreProf] * num_estudiantes
                })

                # Mostrar DataAUX_F1A con las columnas adicionales
                print("\nDataFrame DataAUX_F1A con información del curso")
                #print(DataAUX_F1A)

                #%% UNIR LOS DATAFRAMES

                # Unir los dos DataFrames horizontalmente
                Data_F1A = pd.concat([DataAUX_F1A, Data_F1A], axis=1)

                # Mostrar el DataFrame unido
                print("\nDataFrame completo")
                #print(Data_F1A)
                Niveles = ["Calificación RAP 1", "Calificación RAP 2", "Calificación RAP 3", "Calificación RAP 4"]
                Data_F1A[Niveles] = Data_F1A[Niveles].fillna(0)

                # Crear una conexión a la base de datos de Access
                conn_str = (
                    r'DRIVER={SQL Server};'
                    r'SERVER=servidorrap.database.windows.net;'
                    r'DATABASE=BaseDatosRAP;'
                    r'UID=adminrap;'
                    r'PWD=M@mey0315;'
                )

                try:
                    conn = pyodbc.connect(conn_str)
                    cursor = conn.cursor()

                    for index, row in Data_F1A.iterrows():
                        # Verificar si ya existe la información
                        cursor.execute("""
                        SELECT COUNT(*) FROM FORMATO_1A
                        WHERE Asignatura = ? AND Codigo = ? AND [Nucleo de Formación] = ? AND [Periodo Académico] = ? AND Estudiante = ?
                        """, row['Asignatura'], row['Codigo'], row['Nucleo de Formación'], row['Periodo Académico'], row['Estudiante'])
                        
                        count = cursor.fetchone()[0]
                        
                        if count == 0:
                            # Insertar los datos en la tabla existente si no existen
                            cursor.execute("""
                            INSERT INTO FORMATO_1A (Asignatura, Codigo, [Nucleo de Formación], [Centro de Estudios], [Grupo de Clase], [Periodo Académico], Profesor, Estudiante, [Calificación RAP 1], [Calificación RAP 2], [Calificación RAP 3], [Calificación RAP 4])
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, row['Asignatura'], row['Codigo'], row['Nucleo de Formación'], row['Centro de Estudios'], row['Grupo de Clase'], row['Periodo Académico'], row['Profesor'], row['Estudiante'], row['Calificación RAP 1'], row['Calificación RAP 2'], row['Calificación RAP 3'], row['Calificación RAP 4'])
                        else:
                            print(f"\nEl registro con Código: {row['Codigo']}, Asignatura: {row['Asignatura']}, Periodo Académico: {row['Periodo Académico']} y Codigo RAP: {row['Código']} ya está cargado en la base de datos.")

                    conn.commit()
                    print("\nProceso de carga completado.")
                except pyodbc.Error as e:
                    print(f"Error al conectar con la base de datos de Access o cargar datos: {e}")
                finally:
                    if cursor:
                        cursor.close()
                    if conn:
                        conn.close()
            
            except Exception as e:
                print(f"Error al procesar el archivo {Formato_1A}: {e}")

        #%% CONDICIONAL PARA CARGAR LOS FORMATO 2
        
        elif tipoForm == "FORMATO 2":
            
            Formato_2 = Formato
            Hoja_F2 = 'DATOS'

            #%% DATOS ESTUDIANTES Y CALIFICACIONES RAP
            
            try:
                # Leer el archivo Excel para la primera información
                Data_F2A = pd.read_excel(Formato_2, sheet_name=Hoja_F2,header=21)
                
                Data_F2A = Data_F2A.iloc[0:4, 0:11]
                
                #Volver 0 valores que 
                Niveles = ["Nivel 1 (Conocer)", "Nivel 2 (Comprender)", "Nivel 3 (Aplicar)", "Nivel 4 (Evaluar)"]
                Data_F2A[Niveles] = Data_F2A[Niveles].fillna(0)
                
                Data_F2A.rename(columns={Data_F2A.columns[6]: "Evidencias", Data_F2A.columns[7]: "Fortalezas", 
                                        Data_F2A.columns[8]: "Acción de Mantenimiento",
                                        Data_F2A.columns[10]: "Acción de Mejora"}, inplace=True)
                
                Niveles = ["Evidencias", "Fortalezas", "Acción de Mantenimiento", "Oportunidades de Mejora", "Acción de Mejora"]
                Data_F2A[Niveles] = Data_F2A[Niveles].fillna("NA")
                
                Data_F2A.reset_index(drop=True, inplace=True)
                
                DataAUX_F2A = pd.read_excel(Formato_2, Hoja_F2)
                # Seleccionar el rango de filas y columnas
                DataAUX_F2A = DataAUX_F2A.iloc[11:17, 1:3]
                
                nombreAsign = DataAUX_F2A.iloc[0, 1]
                codigoAsign = DataAUX_F2A.iloc[1, 1]
                periodoAcad = DataAUX_F2A.iloc[3, 1]
                nucleoForm = DataAUX_F2A.iloc[4, 1]
                centroEst = DataAUX_F2A.iloc[5, 1]
                
                
                # Crear un DataFrame con la misma cantidad de estudiantes que Data_F2A y agregar las columnas repetidas
                num_col = len(Data_F2A)
                DataAUX_F2A = pd.DataFrame({
                    'Asignatura': [nombreAsign] * num_col,
                    'Codigo': [codigoAsign] * num_col,
                    'Periodo Académico': [periodoAcad] * num_col,
                    'Nucleo de Formación': [nucleoForm] * num_col,
                    'Centro de Estudios': [centroEst] * num_col
                })
                
                # Mostrar DataAUX_F2A con las columnas adicionales
                print("\nDataFrame DataAUX_F2A con información del curso")
                print(DataAUX_F2A)
                
                #%% UNIR LOS DATAFRAMES F2A
                
                # Unir los dos DataFrames horizontalmente
                Data_F2A = pd.concat([DataAUX_F2A, Data_F2A], axis=1)
                
                #%% INICIO Recopilación Datos F2B
                
                # Leer el archivo Excel para la primera información
                Data_F2B = pd.read_excel(Formato_2, sheet_name=Hoja_F2,header=28)
                
                
                Data_F2B = Data_F2B.iloc[0:5, 1:6]
                
                Data_F2B.rename(columns={Data_F2B.columns[1]: "Fortalezas", Data_F2B.columns[2]: "Acción de Mantenimiento",
                                        Data_F2B.columns[3]: "Oportunidades de Mejora", Data_F2B.columns[4]: "Acción de Mejora"}, inplace=True)
                
                Niveles = ["Otros Aspectos Valorados", "Fortalezas", "Acción de Mantenimiento", "Oportunidades de Mejora", "Acción de Mejora"]
                Data_F2B[Niveles] = Data_F2B[Niveles].fillna("NA")
                
                Data_F2B.reset_index(drop=True, inplace=True)
                
                DataAUX_F2B = pd.read_excel(Formato_2, Hoja_F2)
                # Seleccionar el rango de filas y columnas
                DataAUX_F2B = DataAUX_F2B.iloc[35:42, 1:3]
                
                
                planMante = DataAUX_F2B.iloc[0, 1]
                planMejo = DataAUX_F2B.iloc[3, 1]
                
                # Crear un DataFrame con la misma cantidad de estudiantes que Data_F2B y agregar las columnas repetidas
                num_col = len(Data_F2B)
                DataAUX_F2B = pd.DataFrame({
                    'Avances Plan de Mantenimiento': [planMante] * num_col,
                    'Avances Plan de Mejoramiento': [planMejo] * num_col
                })
                
                DataAUX_F2B = DataAUX_F2B.fillna("NA")
                
                # Mostrar DataAUX_F2A con las columnas adicionales
                print("\nDataFrame DataAUX_F2B con información del curso")
                print(DataAUX_F2B)
                
                #%% UNIR LOS DATAFRAMES F2B CON INFORMACIÓN ADICIONAL
                
                # Unir los dos DataFrames horizontalmente
                Data_F2B = pd.concat([Data_F2B, DataAUX_F2B], axis=1)
                
                DataAUX_F2B = pd.read_excel(Formato_2, Hoja_F2, header=None)
                # Seleccionar el rango de filas y columnas
                DataAUX_F2B = DataAUX_F2B.iloc[12:18, 1:3]
                
                nombreAsign = DataAUX_F2B.iloc[0, 1]
                codigoAsign = DataAUX_F2B.iloc[1, 1]
                periodoAcad = DataAUX_F2B.iloc[3, 1]
                nucleoForm = DataAUX_F2B.iloc[4, 1]
                centroEst = DataAUX_F2B.iloc[5, 1]
                
                
                # Crear un DataFrame con la misma cantidad de estudiantes que Data_F2A y agregar las columnas repetidas
                num_col = len(Data_F2B)
                DataAUX_F2B = pd.DataFrame({
                    'Asignatura': [nombreAsign] * num_col,
                    'Codigo': [codigoAsign] * num_col,
                    'Periodo Académico': [periodoAcad] * num_col,
                    'Nucleo de Formación': [nucleoForm] * num_col,
                    'Centro de Estudios': [centroEst] * num_col
                })
                
                # Mostrar DataAUX_F2A con las columnas adicionales
                print("\nDataFrame DataAUX_F2A con información del curso")
                print(DataAUX_F2B)
                
                Data_F2B = pd.concat([DataAUX_F2B, Data_F2B], axis=1)
                # Crear una conexión a la base de datos de Access
                conn_str = (
                    r'DRIVER={SQL Server};'
                    r'SERVER=servidorrap.database.windows.net;'
                    r'DATABASE=BaseDatosRAP;'
                    r'UID=adminrap;'
                    r'PWD=M@mey0315;'
                )
                
                try:
                    conn = pyodbc.connect(conn_str)
                    cursor = conn.cursor()
                
                    for index, row in Data_F2A.iterrows():
                        # Verificar si ya existe la información
                        cursor.execute("""
                        SELECT COUNT(*) FROM FORMATO_2A
                        WHERE Asignatura = ? AND Codigo = ? AND [Periodo Académico] = ? AND [Nucleo de Formación] = ? AND [Codigo RAP] = ?
                        """, row['Asignatura'], row['Codigo'], row['Periodo Académico'], row['Nucleo de Formación'], row['Código'])
                        
                        count = cursor.fetchone()[0]
                        
                        if count == 0:
                            # Insertar los datos en la tabla existente si no existen
                            cursor.execute("""
                            INSERT INTO FORMATO_2A (Asignatura, Codigo, [Periodo Académico], [Nucleo de Formación], [Centro de Estudios], [Codigo RAP], [Resultado de Aprendizaje], [Nivel 1], [Nivel 2], [Nivel 3], [Nivel 4], Evidencias, Fortalezas, [Acción de Mantenimiento], [Oportunidades de Mejora], [Acción de Mejora])
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, row['Asignatura'], row['Codigo'], row['Periodo Académico'], row['Nucleo de Formación'], row['Centro de Estudios'], row['Código'], row['Resultado de Aprendizaje del Programa o del Departamento'], row['Nivel 1 (Conocer)'], row['Nivel 2 (Comprender)'], row['Nivel 3 (Aplicar)'], row['Nivel 4 (Evaluar)'], row['Evidencias'], row['Fortalezas'], row['Acción de Mantenimiento'], row['Oportunidades de Mejora'], row['Acción de Mejora'])
                        else:
                            print(f"\nEl registro con Código: {row['Codigo']}, Asignatura: {row['Asignatura']}, Periodo Académico: {row['Periodo Académico']} y Codigo RAP: {row['Código']} ya está cargado en la base de datos.")
                
                    conn.commit()
                    print("\nProceso de carga completado.")
                except pyodbc.Error as e:
                    print(f"Error al conectar con la base de datos de Access o cargar datos: {e}")
                finally:
                    if cursor:
                        cursor.close()
                    if conn:
                        conn.close()
                    
                try:
                    conn = pyodbc.connect(conn_str)
                    cursor = conn.cursor()
                
                    for index, row in Data_F2B.iterrows():
                        
                        # Verificar si ya existe la información por ASIGNATURA - PERIODO ACADEMMICO - COLUMNA CON DATOS distntos
                        cursor.execute("""
                        SELECT COUNT(*) FROM FORMATO_2B
                        WHERE Asignatura = ? AND Codigo = ? AND [Nucleo de Formación] = ? AND [Periodo Académico] = ? AND [Otros Aspectos Valorados] = ?
                        """, row['Asignatura'], row['Codigo'], row['Nucleo de Formación'], row['Periodo Académico'], row['Otros Aspectos Valorados'])
                        
                        count = cursor.fetchone()[0]
                        
                        if count == 0:
                            # Insertar los datos en la tabla existente si no existen
                            cursor.execute("""
                            INSERT INTO FORMATO_2B (Asignatura, Codigo, [Periodo Académico], [Nucleo de Formación], [Centro de Estudios], [Otros Aspectos Valorados], Fortalezas, [Acción de Mantenimiento], [Oportunidades de Mejora], [Acción de Mejora], [Avances Plan de Mantenimiento], [Avances Plan de Mejoramiento])
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, row['Asignatura'], row['Codigo'], row['Periodo Académico'], row['Nucleo de Formación'], row['Centro de Estudios'], row['Otros Aspectos Valorados'], row['Fortalezas'], row['Acción de Mantenimiento'], row['Oportunidades de Mejora'], row['Acción de Mejora'], row['Avances Plan de Mantenimiento'], row['Avances Plan de Mejoramiento'])
                        else:
                            print(f"\nEl registro con Código: {row['Codigo']}, Asignatura: {row['Asignatura']}, Periodo Académico: {row['Periodo Académico']} y Codigo RAP: {row['Código']} ya está cargado en la base de datos.")
                
                    conn.commit()
                    print("\nProceso de carga completado.")
                except pyodbc.Error as e:
                    print(f"Error al conectar con la base de datos de Access o cargar datos: {e}")
                finally:
                    if cursor:
                        cursor.close()
                    if conn:
                        conn.close()
                    
                    
            except Exception as e:
                print(f"Error al procesar el archivo {Formato_2}: {e}")
                
#%% FUNCIÓN PARA CARGAR LA CARPETA
def CARGA():
    print("Bienvenido a la carga de formatos de Resultados de aprendizaje: ")
    carga = input("Ingrese la ruta de acceso a los links: ")
    PROCESO(carga)

CARGA()

# Ejecutar la app
if __name__ == '__main__':
    app.run(debug=True)