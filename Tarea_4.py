"""
UNIVERSIDAD NACIONAL ABIERTA Y A DISTANCIA – UNAD
Escuela de Ciencias Básicas, Tecnología e Ingeniería – ECBTI
Curso: Big Data – Tarea 4
Grupo: 87
Autor: JHON HECTOR PINZON RODRIGUEZ
Año: 2025

Repositorio Consolidado de Consultas en MongoDB

Descripción:
Este archivo contiene la recopilación completa de consultas implementadas
durante el desarrollo de la Tarea 4 – MongoDB. Su propósito es servir como
guía técnica para la ejecución de operaciones de CRUD, consultas con
operadores y pipelines de agregación dentro del entorno mongosh.

Nota:
El archivo no ejecuta las consultas directamente en Python; está diseñado
como un script de referencia para copiar y ejecutar en la consola de MongoDB.

Contenido del archivo:
1. Selección y administración de la base de datos.
2. Operaciones CRUD sobre las colecciones.
3. Consultas con filtros y operadores.
4. Consultas de agregación para análisis estadístico.

Este documento acompaña el desarrollo reportado en el archivo:
Tarea_4_grupo87.pdf
"""


# ============================================================
# 1. SELECCIÓN DE BASE DE DATOS
# ============================================================

# En mongosh, selecciona/crea la base de datos:
use catalogo_comercio_electronico

# Ver colecciones disponibles
show collections

# ============================================================
# 2. CRUD BÁSICO (insertar, consultar, actualizar, eliminar)
# ============================================================

# 2.1 Insertar documentos masivos desde archivos JSON
# (Ejemplo de carga con mongoimport desde terminal)
# mongoimport --db catalogo_comercio_electronico --collection categorias --file categorias.json --jsonArray
# mongoimport --db catalogo_comercio_electronico --collection productos  --file productos.json  --jsonArray
# mongoimport --db catalogo_comercio_electronico --collection usuarios   --file usuarios.json   --jsonArray
# mongoimport --db catalogo_comercio_electronico --collection pedidos    --file pedidos.json    --jsonArray
# mongoimport --db catalogo_comercio_electronico --collection resenas    --file resenas.json    --jsonArray

# 2.2 Consultar (find)
# Obtener todos los documentos de productos
db.productos.find()

# Consultar un producto por id
db.productos.find({ _id: "prd_ukoa1nsr18" })

# 2.3 Actualizar (updateOne)
# Cambiar datos de un usuario (ejemplo del caso real)
db.usuarios.updateOne(
  { _id: "usr_rcgb7361qu" },
  {
    $set: {
      nombre: "Gabriela Gómez R.",
      telefono: "3005552211"
    }
  }
)
# Resultado esperado: matchedCount=1, modifiedCount=1 si el id existe.

# 2.4 Eliminar
# Eliminar un producto específico por id
db.productos.deleteOne({ _id: "prd_b29co0vs85" })

# Eliminar varios productos por ids
db.productos.deleteMany({
  _id: { $in: ["prd_b29co0vs85", "prd_u8f32xpuai"] }
})

# Eliminar una colección completa
db.resenas.drop()

# Eliminar toda la base de datos (¡cuidado!)
db.dropDatabase()

# ============================================================
# 3. CONSULTAS CON FILTROS Y OPERADORES
# ============================================================

# 3.1 Productos con stock menor a 20 (proyección limitada a nombre y stock)
db.productos.find(
  { stock: { $lt: 20 } },
  { nombre: 1, stock: 1 }
)
# Devuelve solo productos con pocas unidades disponibles.

# 3.2 Usuarios registrados desde 2024 en adelante
db.usuarios.find(
  { fecha_registro: { $gte: "2024-01-01" } },
  { nombre: 1, fecha_registro: 1 }
)

# 3.3 Productos pertenecientes a varias categorías (uso de $in)
db.productos.find(
  { categoria_id: { $in: ["cat_a7pf17yp91", "cat_wblre8nf3e"] } }
)

# 3.4 Productos activos y con stock mayor a 3 (uso de $and)
db.productos.find({
  $and: [
    { activo: true },
    { stock: { $gt: 3 } }
  ]
})

# 3.5 Usuarios con rol "cliente" o "premium" (uso de $or)
db.usuarios.find({
  $or: [
    { rol: "cliente" },
    { rol: "premium" }
  ]
})

# 3.6 Pedidos cuyo estado NO es "entregado" (uso de $ne)
db.pedidos.find({
  estado: { $ne: "entregado" }
})
# Útil para identificar pedidos pendientes, pagados o en proceso.

# ============================================================
# 4. CONSULTAS DE AGREGACIÓN (PIPELINES)
# Nota: todas cierran con ])  (error común corregido)
# ============================================================

# 4.1 Contar productos por categoría
db.productos.aggregate([
  {
    $group: {
      _id: "$categoria_id",
      total_productos: { $sum: 1 }
    }
  }
])

# 4.2 Promedio de stock por marca
db.productos.aggregate([
  {
    $group: {
      _id: "$marca",
      promedio_stock: { $avg: "$stock" }
    }
  }
])

# 4.3 Gasto total y número de pedidos por usuario
db.pedidos.aggregate([
  {
    $group: {
      _id: "$usuario_id",
      gasto_total: { $sum: "$resumen.total.valor" },
      pedidos_realizados: { $sum: 1 }
    }
  },
  { $sort: { gasto_total: -1 } }
])

# 4.4 Calificación promedio y total de reseñas por producto
db.resenas.aggregate([
  {
    $group: {
      _id: "$producto_id",
      promedio_calificacion: { $avg: "$calificacion" },
      total_resenas: { $sum: 1 }
    }
  },
  { $sort: { promedio_calificacion: -1 } }
])

# 4.5 Top 5 productos con más reseñas
db.resenas.aggregate([
  {
    $group: {
      _id: "$producto_id",
      total_resenas: { $sum: 1 }
    }
  },
  { $sort: { total_resenas: -1 } },
  { $limit: 5 }
])

# 4.6 Stock total por categoría
db.productos.aggregate([
  {
    $group: {
      _id: "$categoria_id",
      stock_total: { $sum: "$stock" }
    }
  }
])

# 4.7 Valor total del inventario por marca
# (precio.valor * stock) acumulado por marca
db.productos.aggregate([
  {
    $group: {
      _id: "$marca",
      valor_inventario: {
        $sum: { $multiply: ["$precio.valor", "$stock"] }
      }
    }
  },
  { $sort: { valor_inventario: -1 } }
])

# 4.8 Promedio general del valor de pedidos
db.pedidos.aggregate([
  {
    $group: {
      _id: null,
      promedio_valor_pedido: { $avg: "$resumen.total.valor" }
    }
  }
])

# 4.9 Top 5 productos más vendidos (sumando cantidades en items)
db.pedidos.aggregate([
  { $unwind: "$items" },
  {
    $group: {
      _id: "$items.producto_id",
      total_vendido: { $sum: "$items.cantidad" }
    }
  },
  { $sort: { total_vendido: -1 } },
  { $limit: 5 }
])

# ========================= FIN DEL ARCHIVO =========================
