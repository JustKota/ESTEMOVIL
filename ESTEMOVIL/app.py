#     PROCESO DE ETL CON BASE DE DATOS ESTEMOVIL

# IMPORT DE TODAS LAS BIBLIOTECAS UTILIZADAS

from flask import Flask, Blueprint, render_template, g
import mysql.connector
import plotly.express as px
import plotly.io as pio
from plotly.io import to_json 
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn import datasets
import numpy as np
import json, os


# CREACION DE LA APLICACION 
app = Flask(__name__)
plot_bp = Blueprint('plot', __name__)


# CONEXION A LA BASE DE DATOS
def get_db_connection():
      if 'db' not in g:
        g.db = mysql.connector.connect(user='root', password='', 
                                   host='localhost',
                                   database='estemovil',
                                   port='3306')
        return g.db
      # CAPTURA DE ERROR, SI NO HAY BASE DE DATOS SE CIERRA EL PRIMER PROCESO
@app.teardown_appcontext
def close_db_connection(exception):
        db = g.pop('db', None)
        if db is not None:
             db.close()


# INDEX    Y LLAMADO DE TABLAS ORIGINALES
@app.route('/')
def index():

        conexion = get_db_connection()
        cursor = conexion.cursor()
        cursor.execute("select * from recargas")
        consulta = cursor.fetchall()
        return render_template('index.html', datos=consulta)


@app.route('/activaciones')
def activaciones():

        conexion = get_db_connection()
        cursor = conexion.cursor()
        cursor.execute("select * from activaciones")
        consulta = cursor.fetchall()
        return render_template('tabla_activaciones.html', datos=consulta)


@app.route('/vencimientos')
def vencimientos():

        conexion = get_db_connection()
        cursor = conexion.cursor()
        cursor.execute("select * from vencimientoex")
        consulta = cursor.fetchall()
        return render_template('vencimientoex.html', datos=consulta)


# TRANSFORMACIONES
@app.route('/reactivacion')
def reactivacion():

        conexion = get_db_connection()
        cursor = conexion.cursor()
        cursor.execute("""
        SELECT activaciones.msisdn, vencimientoex.fecha, vencimientoex.hora,
                activaciones.ccpdv, activaciones.idpdv, activaciones.nombrepdv, activaciones.estado, activaciones.vence
        FROM vencimientoex
        JOIN activaciones ON vencimientoex.numero = activaciones.msisdn;
        """)
        consulta = cursor.fetchall()
        return render_template('reactivacion.html', datos=consulta)


@app.route('/registros')
def registros():

        conexion = get_db_connection()
        cursor = conexion.cursor()
        cursor.execute("""
        SELECT activaciones.ccpdv, activaciones.idpdv, activaciones.nombrepdv, COUNT(activaciones.msisdn) AS total_numero
                FROM activaciones
        GROUP BY activaciones.idpdv, activaciones.ccpdv, activaciones.nombrepdv; 
        """)
        consulta = cursor.fetchall()
        return render_template('registros.html', datos=consulta)


@app.route('/paquetex')
def paquetex():

        conexion = get_db_connection()
        cursor = conexion.cursor()
        cursor.execute("""
        SELECT vencimientoex.fecha, vencimientoex.hora, vencimientoex.numero, vencimientoex.valor,
                recargas.paquete
        FROM vencimientoex
        JOIN recargas ON vencimientoex.numero = recargas.msisdn_linea;
        """)
        consulta = cursor.fetchall()
        return render_template('paqueteex.html', datos=consulta)


@plot_bp.route('/plot')
def plot():
    conexion = get_db_connection()
    cursor = conexion.cursor()

    # COUNT PARA CONTAR ACTIVACIONES EN CADA PUNTO DE VENTA (DEBE SUPERAR LAS 30 ACTIVACIONES)
    cursor.execute("""
    SELECT idpdv, COUNT(msisdn) as total_activaciones 
    FROM activaciones 
    GROUP BY idpdv
    HAVING COUNT(msisdn) > 30
    """)
    data = cursor.fetchall()

    # CONVIERTE LOS DATOS EN UN DF
    df_pie = pd.DataFrame(data, columns=['Punto de Venta', 'Activaciones'])

    # CREA GRAFICO DE PASTEL EN BASE A LOS DATOS ANTERIORES
    fig_pie = px.pie(df_pie, names='Punto de Venta', values='Activaciones', 
                     title='Porcentaje de Activaciones por Punto de Venta')
    graph_html_pie = pio.to_html(fig_pie, full_html=False)

    cursor.close()
    conexion.close()

    
    return render_template('plot.html', graph_html_pie=graph_html_pie)
app.register_blueprint(plot_bp)


@app.route('/predicciones')
def predicciones():
    
    conexion = get_db_connection()
    cursor = conexion.cursor()

    # CONSULTA LLAMANDO LOS DATOS DE PUNTOS DE VENTA QUE HAN ACTIVADO SIMS EN LOS ULTIMOS 2 MESES

    cursor.execute("""
    SELECT idpdv, DATE_FORMAT(diact, '%Y-%m') as mes, COUNT(msisdn) as sim_cards_activadas
    FROM activaciones
    WHERE diact >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
    GROUP BY idpdv, mes
    """)
    data = cursor.fetchall()

    # CONVIERTE LOS DATOS EN UN DATAFRAME DE PANDAS
    df = pd.DataFrame(data, columns=['Punto de Venta', 'Mes', 'SIM Cards activadas'])

    # REEMPLAZAR (LIMPIAR) DATOS NULOS Y FECHAS INVALIDAS
    df['Mes'] = df['Mes'].replace('0000-00', pd.NaT)  
    df['Mes'] = pd.to_datetime(df['Mes'], errors='coerce')  # CONVIERTE EL DATO A DATETIME
    df = df.dropna(subset=['Mes'])  # ELIMINA VALORES NULOS EN LA COLUMNA "MES"

    # ENTRENAMIENTO EN REGRESION PARA PREDECIR EL PROXIMO MES
    df['Mes_Num'] = df['Mes'].view('int64') // 10**9  # CONVIERTE LA FECHA A SEGUNDOS

    # CREACION DE UNA BIBLIOTECA PARA GUARDAR LOS DATOS DE PREDICCION
    predicciones_por_punto = {}

    # DIFERENCIAR CADA PUNTO DE VENTA UNICO
    for punto_venta in df['Punto de Venta'].unique():
        df_punto = df[df['Punto de Venta'] == punto_venta]
        X = df_punto[['Mes_Num']]              # EL PUNTO X VA A SER EL MES, O NUMERO DE MES
        y = df_punto['SIM Cards activadas']     # EL PUNTO Y ES EL TOTAL DE SIMS ACTIVADAS POR PUNTO DE VENTA

        # CREACION Y ENTRENAMIENTO DE LA IA EN REGRESION, TOMANDO LOS DATOS EN X.Y
        model = LinearRegression()
        model.fit(X, y)

        # HCREAR LA PREDICCION PARA EL PROXIMO MES
        next_month = df_punto['Mes'].max() + pd.DateOffset(months=1)
        next_month_num = int(next_month.timestamp())  
        prediccion = model.predict([[next_month_num]])[0]

        # SI ES MAS DE DOS O MAS PREDICCIONES SE GUARDAN EN EL DICCIONARIO
        if prediccion >= 2:
            predicciones_por_punto[punto_venta] = round(prediccion, 2)

    # CREA UN PLOTLY, O GRAFICA DE LINEAS DEPENDIENDO DEL LOS DATOS X.Y
    fig = px.line(df, x='Mes', y='Sims activadas', color='Punto de Venta',
                  title='Activacion de sims por punto de Venta')
    graph_json = fig.to_json()

    cursor.close()
    conexion.close()

    return render_template('predicciones.html', 
                           datos=data, 
                           graph_json=graph_json, 
                           predicciones_por_punto=predicciones_por_punto)

# AQUI EJECUTA EL PROGRAMA EN EL PUERTO "5000"
if __name__ == '__main__':
    app.run(port=5000,debug=True)