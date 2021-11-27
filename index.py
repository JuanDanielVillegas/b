import pymongo
import os

MONGO_HOST="localhost"
MONGO_PUERTO="27017"
MONGO_TIME_OUT=1000

MONGO_URI="mongodb://" + MONGO_HOST + ":" + MONGO_PUERTO + "/"

MONGO_BASE_DATOS="scottEmbedding"
MONGO_COLECCION="hr"


def getMaxEmpno():
    pipeline = [
        { "$sort" : { "empno" : -1 } },
        { "$limit" : 1 },
        { "$project": {"_id":0, "empno": 1 } }
    ]
    resultado = coleccion.aggregate(pipeline)
    for documento in resultado:
        return documento["empno"]

def getDatos():
    pipeline = [
        {"$project": {"_id":0, "empno": 1, "ename": 1, "job":1, "sal":1, "departamento.dname": 1, "departamento.loc": 1}},
        { "$sort" : { "empno" : -1 }}
    ]
    resultados = coleccion.aggregate(pipeline)
    for documento in resultados:
        dname = documento["departamento"]['dname']
        loc = documento["departamento"]['loc']
        print(f"{documento['empno']}  { documento['ename']} {documento['job']} {documento['sal']}  {dname}  {loc}")
        # print(documento)

def insertarDatos(empno, nombre, puesto, salario, departamento, localidad):
    documento = {
        "empno": empno,
        "ename": nombre,
        "job": puesto,
        "sal": salario,
        "departamento": {
            "dname": departamento,
            "loc": localidad
        }
    }
    # print(documento)
    coleccion.insert_one(documento)

def buscarEmpleado(empno):
    pipeline = [
        { "$match" : { "empno" : empno }},
        {"$project": {"_id":0, "empno": 1, "ename": 1, "job":1, "sal":1, "departamento.dname": 1, "departamento.loc": 1}}
    ]
    resultados = coleccion.aggregate(pipeline)
    for documento in resultados:
        return documento

def eliminarEmpleado(empno):
    query = {"empno": empno}
    documento = buscarEmpleado(empno)
    imprimirUno(documento)
    
    decision = input("Desea eliminar al empleado S/N: ").upper()
    if(decision == "S"):
        coleccion.delete_one(query)
        print("Empleado eliminado\n")
        getDatos()
    else:
        print("Operación cancelada")

def actualizarEmpleado(empno, opcion, cambio):
    atributos = ["ename", "job","sal","deptno", "loc"]
    documento = buscarEmpleado(empno)
    if opcion <= 3:
        query = {"empno": empno},{"$set": {atributos[opcion-1]: cambio}}
    else:
        atributo = "departamento" + atributos[opcion-1]
        query = {"empno": empno},{"$set": {atributo: cambio}}

    imprimirUno(documento)
    
    
    coleccion.update(query)

def capturarDatos():
    maxEmpno = getMaxEmpno()
    nombre = input("Ingrese el nombre: ")
    puesto = input("Ingrese el puesto: ")
    salario = int(input("Ingrese el salario: "))
    departamento = input("Ingrese el departamento: ")
    localidad = input("Ingrese la localidad ")
    # print(f"{nombre} {puesto} {salario} {departamento} {localidad}")
    insertarDatos(maxEmpno+1, nombre.upper(), puesto.upper(), salario, departamento.upper(), localidad.upper())

def continuar():
    input("\nPress Enter to continue...")

def imprimirUno(documento):
    print(f"\n{documento['empno']}  { documento['ename']} {documento['job']} {documento['sal']}  {documento['departamento']['dname']}  {documento['departamento']['loc']}")

def menuActualizar():
    menu = [
        "Nombre del empleado",
        "Puesto",
        "Salario",
        "Nombre del Departamento",
        "Localidad del Departamento"
    ]
    empno = int(input("Numero de empleado: "))
    documento = buscarEmpleado(empno)
    imprimirUno(documento)

    print("Actualizar")
    print(f"1. {menu[0]}")
    print(f"2. {menu[1]}")
    print(f"3. {menu[2]}")
    print(f"4. {menu[3]}")
    print(f"5. {menu[4]}")
    opcion = int(input("Opcion: "))

    if(opcion==3):
        cambio = int(input(f"Nuevo {menu[opcion-1]}"))
    else:
        cambio = input(f"Nuevo {menu[opcion-1]}")

    actualizarEmpleado(empno, opcion, cambio)
    

def menu():
    while True:
        os.system("cls")
        print("1. Leer Registros")
        print("2. Buscar Registro")
        print("3. Insertar Registro")
        print("4. Eliminar Registro")
        print("5. Actualizar Registro")
        print("6. Salir")

        opcion = int(input("Selecione una opcion: "))
        if opcion == 1:
            print("")
            getDatos()
            continuar()
        elif opcion == 2:
            empno = int(input("Ingrese el numero de empleado: "))
            documento = buscarEmpleado(empno)
            imprimirUno(documento)
            continuar()
        elif opcion == 3:
            capturarDatos()
            continuar()
        elif opcion == 4:
            empno = int(input("Numero de empleado: "))
            eliminarEmpleado(empno)
            continuar()
        elif opcion == 5:
            menuActualizar()
            continuar()
        elif opcion == 6:
            break
        else:
            print("Opcion invalida")
            continuar()
        print(" ")



try:
    cliente=pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_TIME_OUT)
    baseDatos=cliente[MONGO_BASE_DATOS]
    coleccion=baseDatos[MONGO_COLECCION]
    menu()
    cliente.close()
except pymongo.errors.ServerSelectionTimeoutError as errorTiempo:
    print("Tiempo exedido "+errorTiempo)
except pymongo.errors.ConnetionFailure as errorConexion:
    print("Fallo al conectarse a mongodb "+errorConexion)







