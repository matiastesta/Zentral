# Arquitectura de Datos: Producto vs Lote

## 📊 Niveles de Información

### Nivel 1: PRODUCTO MAESTRO
El producto maestro contiene información **compartida** por todos los lotes del mismo producto.

**Campos copiados al crear producto nuevo:**
- ✅ **Nombre** - Ej: "Coca Cola"
- ✅ **Categoría** - Ej: "Bebidas"
- ✅ **Código interno** - Ej: "COCBEB06" (el nuevo)
- ✅ **Descripción** - Texto descriptivo del producto
- ✅ **Precio de venta** - Precio al que se vende
- ✅ **Unidad** - Ej: "Unidad", "Litro", "Kg"
- ✅ **Método** - FIFO/LIFO para cálculo de costos
- ✅ **Stock mínimo** - Alerta de stock bajo
- ✅ **Punto de reorden** - Cuando pedir más
- ✅ **Imagen** - Foto del producto
- ✅ **Uses lots** - Si usa lotes (siempre True)
- ✅ **Stock ilimitado** - Flag especial

**Campos NO compartidos:**
- ❌ **Proveedor** - Es específico de cada lote
- ❌ **Cantidad** - Es específica de cada lote
- ❌ **Costo unitario** - Puede variar entre lotes
- ❌ **Vencimiento** - Es específico de cada lote

### Nivel 2: LOTE (Inventory Lot)
Cada lote representa una **carga específica** de inventario.

**Campos específicos del lote:**
- ✅ **Proveedor** - Ej: "Distribuidora Pepsi" (puede cambiar entre lotes)
- ✅ **Cantidad inicial** - Ej: 20 unidades
- ✅ **Cantidad disponible** - Stock actual de este lote
- ✅ **Costo unitario** - Precio pagado en esta compra
- ✅ **Vencimiento** - Fecha de expiración de este lote específico
- ✅ **Fecha de ingreso** - Cuándo se recibió
- ✅ **Received at** - Timestamp de la tanda

---

## 🔄 Al Crear Producto Nuevo por Cambio de Código

### Ejemplo: COCBEB05 → COCBEB06

**Situación:**
- Tanda original tiene: Coca Cola, código COCBEB05, proveedor "Distribuidora Pepsi"
- Usuario cambia código a COCBEB06

**Resultado (CORRECTO):**

#### 1. Se Crea Producto NUEVO
```python
Product(
    name='Coca Cola',                        # ✅ Copiado
    internal_code='COCBEB06',               # ✅ El nuevo código
    category_id=...,                        # ✅ Copiado
    description='...',                      # ✅ Copiado
    sale_price=1500.0,                      # ✅ Copiado
    unit_name='Unidad',                     # ✅ Copiado
    method='FIFO',                          # ✅ Copiado
    min_stock=5.0,                          # ✅ Copiado
    reorder_point=10.0,                     # ✅ Copiado
    image_filename='cocacola.jpg',          # ✅ Copiado
    costo_unitario_referencia=1200.0,       # ✅ Copiado
    # NOTA: NO incluye proveedor (no existe en este nivel)
)
```

#### 2. El Lote se Reasocia al Producto Nuevo
```python
InventoryLot(
    product_id=<nuevo_producto_id>,         # ✅ Cambia al nuevo producto
    supplier_name='Distribuidora Pepsi',    # ✅ SE MANTIENE (no se pierde)
    qty_initial=20,                         # ✅ SE MANTIENE
    qty_available=20,                       # ✅ SE MANTIENE
    unit_cost=1500.0,                       # ✅ SE MANTIENE
    expiration_date='2026-12-31',           # ✅ SE MANTIENE
    received_at='2026-03-10 16:21:54'       # ✅ SE MANTIENE
)
```

---

## 📋 Verificación Visual

### Legajo del Producto (Imagen 1)
```
Nombre: Coca Cola                          ✅ Copiado
Categoría: Bebidas                         ✅ Copiado
Código interno: COCBEB05                   ✅ Código original
Estado: Activo                             ✅ Default
Proveedor: [vacío]                         ✅ CORRECTO - No existe en este nivel

Historial de lotes:
  LOTE 08MM01QGT
  - Proveedor: Distribuidora Pepsi         ✅ En el lote específico
  - Cantidad: 20
  - Costo: $1,500
```

### Lista de Stock (Imagen 3)
```
Tanda N°1
  Coca Cola - COCBEB05
  Proveedor: Distribuidora Pepsi           ✅ Del lote

Tanda N°2  
  Coca Cola - COCBEB06                     ✅ Producto nuevo
  Proveedor: Distribuidora Pepsi           ✅ Del lote (se mantiene)
```

---

## ✅ Confirmación de Comportamiento CORRECTO

### Campo "Proveedor" en Legajo del Producto
**Estado actual:** Vacío  
**¿Es correcto?** ✅ **SÍ**

**Explicación:**
- El campo "Proveedor" en el legajo del producto es un **campo de display/referencia**
- El proveedor real está en cada **lote individual**
- Esto permite que el mismo producto tenga diferentes proveedores según la compra

**Ejemplo real:**
```
Producto: Coca Cola (COCBEB06)
  Lote 1: Proveedor = "Distribuidora Pepsi" (compra de enero)
  Lote 2: Proveedor = "Mayorista XYZ" (compra de febrero)
  Lote 3: Proveedor = "Distribuidora Pepsi" (compra de marzo)
```

Si el proveedor estuviera en el producto maestro, los 3 lotes tendrían el mismo proveedor forzosamente, lo cual es incorrecto.

### Campo "Descripción"
**Estado actual:** Se copia del producto original  
**¿Es correcto?** ✅ **SÍ**

Si el producto original tiene descripción, el nuevo producto la hereda. Si no tiene, queda vacía (puede agregarse después editando el legajo del producto).

### Otros Campos Consolidados
Todos los campos del producto maestro se copian correctamente:
- ✅ Precio de venta
- ✅ Unidad de medida
- ✅ Método de costeo (FIFO/LIFO)
- ✅ Stocks mínimos
- ✅ Punto de reorden
- ✅ Imagen del producto
- ✅ Costo unitario de referencia

---

## 🔍 Cómo Verificar

### Test 1: Crear Producto Nuevo
```
1. Tanda original: Coca Cola, COCBEB05
2. Modificar tanda
3. Cambiar código a COCBEB06
4. Confirmar
5. Ir al legajo del producto COCBEB06
6. ✓ Verificar nombre = "Coca Cola"
7. ✓ Verificar categoría = "Bebidas"
8. ✓ Verificar precio de venta copiado
9. ✓ Verificar descripción copiada (si existía)
10. ✓ Verificar campo Proveedor vacío (CORRECTO)
11. ✓ Ver historial de lotes → proveedor en cada lote
```

### Test 2: Verificar Proveedor en Lote
```
1. Ir al legajo del producto COCBEB06
2. Sección "Historial de lotes"
3. ✓ Cada lote muestra su proveedor específico
4. ✓ Lote puede tener proveedor diferente
```

---

## 📝 Resumen

### Campos a Nivel Producto (Compartidos)
Estos se **copian completamente** al crear producto nuevo:
- Nombre, categoría, código interno
- Descripción, precio de venta, unidad
- Método FIFO/LIFO, stocks mínimos, imagen
- Flags (uses_lots, stock_ilimitado)

### Campos a Nivel Lote (Específicos)
Estos se **mantienen en el lote** al reasociarlo:
- Proveedor (cada lote puede tener el suyo)
- Cantidad, costo unitario, vencimiento
- Fecha de ingreso, received_at

### Arquitectura Correcta
```
Producto COCBEB05 (Coca Cola)
├── Lote 1: 20 unidades, proveedor A, vence 2026-06
└── Lote 2: 30 unidades, proveedor B, vence 2026-09

Producto COCBEB06 (Coca Cola) ← NUEVO
└── Lote 3: 20 unidades, proveedor A, vence 2026-12
    (reasociado desde COCBEB05)
```

**Resultado:** Cada producto tiene sus lotes, cada lote su proveedor. Todo consolidado correctamente. ✅

---

**Fecha:** 10/03/2026  
**Estado:** ✅ Verificado - Comportamiento correcto  
**Conclusión:** El campo "Proveedor" vacío en el legajo del producto es la arquitectura correcta
