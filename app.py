from flask import Flask, Blueprint, render_template, g
import mysql.connector
import plotly.express as px
import plotly.io as pio
from plotly.io import to_json  # Agrega esta línea
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import numpy as np
import json




app = Flask(__name__)
plot_bp = Blueprint('plot', __name__)


def get_db_connection():
      if 'db' not in g:
        g.db = mysql.connector.connect(user='root', password='', 
                                   host='localhost',
                                   database='estemovil',
                                   port='3306')
        return g.db
@app.teardown_appcontext
def close_db_connection(exception):
        db = g.pop('db', None)
        if db is not None:
             db.close()


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
        SELECT activaciones.ccpdv, activaciones.idpdv, activaciones.nombrepdv, COUNT(activaciones.msisdn) AS total_msisdn
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
    # Conexión a la base de datos y obtención de datos de activaciones por punto de venta
    conexion = get_db_connection()
    cursor = conexion.cursor()

    # Consulta SQL para contar las activaciones por punto de venta
    cursor.execute("""
    SELECT idpdv, COUNT(msisdn) as total_activaciones 
    FROM activaciones 
    GROUP BY idpdv
    """)
    data = cursor.fetchall()

    # Convertir los datos a un DataFrame de Pandas
    df_pie = pd.DataFrame(data, columns=['Punto de Venta', 'Activaciones'])

    # Crear un gráfico de pastel con Plotly
    fig_pie = px.pie(df_pie, names='Punto de Venta', values='Activaciones', 
                     title='Porcentaje de Activaciones por Punto de Venta')
    graph_html_pie = pio.to_html(fig_pie, full_html=False)

    # Cerrar la conexión a la base de datos
    cursor.close()
    conexion.close()

    # Renderizar la plantilla con ambos gráficos
    return render_template('plot.html', graph_html_pie=graph_html_pie)
app.register_blueprint(plot_bp)


@app.route('/predicciones')
def predicciones():
    # 1. Obtener los datos de activaciones por punto de venta
    conexion = get_db_connection()
    cursor = conexion.cursor()

    # Consulta para obtener el número de SIM cards vendidas por mes por punto de venta
    cursor.execute("""
    SELECT idpdv, DATE_FORMAT(diact, '%Y-%m') as mes, COUNT(msisdn) as sim_cards_vendidas
    FROM activaciones
    GROUP BY idpdv, mes
    """)
    data = cursor.fetchall()

    # Convertir los datos a un DataFrame de Pandas
    df = pd.DataFrame(data, columns=['Punto de Venta', 'Mes', 'SIM Cards Vendidas'])

    # 2. Limpieza de datos: Reemplazar fechas inválidas y eliminar filas nulas
    df['Mes'] = df['Mes'].replace('0000-00', pd.NaT)  # Reemplazar '0000-00' con NaT
    df['Mes'] = pd.to_datetime(df['Mes'], errors='coerce')  # Convertir a datetime, ignorando errores
    df = df.dropna(subset=['Mes'])  # Eliminar las filas con valores nulos en 'Mes'

    # 3. Entrenar un modelo de regresión para predecir el próximo mes
    df['Mes_Num'] = df['Mes'].view('int64') // 10**9  # Convertir fechas a segundos

    X = df[['Mes_Num']]
    y = df['SIM Cards Vendidas']

    # Crear y entrenar el modelo de regresión
    model = LinearRegression()
    model.fit(X, y)

    # Hacer una predicción para el siguiente mes
    next_month = df['Mes'].max() + pd.DateOffset(months=1)
    next_month_num = int(next_month.timestamp())  # Convertir a timestamp
    prediccion = model.predict([[next_month_num]])[0]

    # 4. Crear un gráfico con Plotly
    fig = px.line(df, x='Mes', y='SIM Cards Vendidas', color='Punto de Venta',
                  title='Ventas de SIM Cards por Punto de Venta')
    graph_json = to_json(fig)  # Usa to_json en lugar de json.dumps

    # Cerrar conexión a la base de datos
    cursor.close()
    conexion.close()

    # 5. Renderizar la plantilla
    return render_template('predicciones.html', datos=data, prediccion=round(prediccion, 2), graph_json=graph_json)




if __name__ == '__main__':
    app.run(port=5000,debug=True)