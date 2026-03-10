# Corrección Crítica: Modificar Tanda NO Debe Alterar Producto Maestro Global

## 🔴 Problema Crítico Identificado

Al editar una tanda y cambiar el código interno de una fila, el sistema estaba **modificando el producto maestro global**, afectando todos los lotes históricos de otras tandas que compartían ese producto.

### Comportamiento Incorrecto (ANTES)

```
Usuario en Tanda N°1:
  - Edita "Coca Cola" con código COCBEB05
  - Cambia código a COCBEB06
  
Sistema (incorrecto):
  ❌ Modifica el producto maestro "Coca Cola" globalmente
  ❌ Cambia el código de COCBEB05 a COCBEB06 en TODAS las tandas
  ❌ Afecta Tanda N°2, N°3, etc. que tenían ese producto
  ❌ Pierde trazabilidad histórica
```

**Consecuencia:** El código `COCBEB06` aparece en múltiples tandas que originalmente tenían `COCBEB05`, rompiendo completamente la integridad de datos.

---

## ✅ Comportamiento Correcto (AHORA)

### Regla Conceptual

**Modificar tanda = Resolver la imputación de esa carga/lote**  
**Legajo de producto = Modificar el producto maestro global**

**NO mezclar ambos comportamientos.**

### Lógica Implementada

Cuando el usuario cambia el código interno dentro de "Modificar tanda", el sistema decide:

#### Caso A: Código Nuevo NO Existe
```
Usuario cambia: COCBEB05 → COCBEB06
Sistema verifica: COCBEB06 no existe en la base de datos

Acción:
✅ Crear producto NUEVO con:
   - Nombre: Coca Cola (copiado de la fila)
   - Categoría: Bebidas (copiada de la fila)
   - Código interno: COCBEB06 (el nuevo)
   - Unidad: heredada del producto original
✅ Reasociar el lote de esta fila al producto nuevo
✅ Actualizar movimientos asociados al producto nuevo
✅ Marcar producto original como potencialmente huérfano
✅ NO tocar el producto original COCBEB05
✅ NO afectar otras tandas
```

#### Caso B: Código Nuevo YA Existe
```
Usuario cambia: COCBEB05 → COCBEB04
Sistema verifica: COCBEB04 ya existe en la base de datos

Acción:
✅ NO crear producto nuevo
✅ Reasociar el lote de esta fila al producto existente COCBEB04
✅ Actualizar movimientos asociados al producto existente
✅ Marcar producto original COCBEB05 como potencialmente huérfano
✅ NO modificar el producto existente COCBEB04
✅ NO afectar otras tandas
```

#### Caso C: Código NO Cambió
```
Usuario NO cambia el código (solo edita proveedor, cantidad, etc.)

Acción:
✅ Actualizar solo datos del LOTE:
   - Proveedor
   - Cantidad
   - Costo unitario
   - Vencimiento
❌ NO modificar el producto maestro
❌ NO tocar otros lotes
```

---

## 🔧 Implementación Técnica

### Backend: `routes_tanda_update.py`

**Líneas 123-216: Nueva lógica completa**

```python
old_product_id = lote.product_id
codigo_interno_nuevo = change.get('codigo_interno', '').strip()

# Determinar si código interno cambió
producto_original = db.session.get(Product, old_product_id)
codigo_interno_original = producto_original.internal_code if producto_original else None
codigo_cambio = (codigo_interno_nuevo != codigo_interno_original)

# Caso 1: Código cambió a uno EXISTENTE → Reasociar lote
if codigo_cambio and change.get('is_reclassification'):
    target_product_id = change['reclassify_to_product_id']
    lote.product_id = target_product_id
    # Actualizar movimientos
    db.session.query(InventoryMovement).filter(...).update(...)
    productos_huerfanos.append(old_product_id)

# Caso 2: Código cambió a uno NUEVO → Crear producto nuevo
elif codigo_cambio and codigo_interno_nuevo:
    existe_producto = db.session.query(Product).filter(
        Product.internal_code == codigo_interno_nuevo
    ).first()
    
    if not existe_producto:
        # Crear producto nuevo
        producto_nuevo = Product(
            company_id=cid,
            name=change.get('nombre'),
            internal_code=codigo_interno_nuevo,
            category_id=...,
            unit=producto_original.unit
        )
        db.session.add(producto_nuevo)
        db.session.flush()
        
        # Reasociar lote al producto nuevo
        lote.product_id = producto_nuevo.id
        # Actualizar movimientos
        db.session.query(InventoryMovement).filter(...).update(...)
        productos_huerfanos.append(old_product_id)

# Caso 3: Código NO cambió → Solo actualizar datos del lote
else:
    # CRÍTICO: NO modificar el producto maestro
    if 'proveedor' in change:
        lote.supplier_name = change['proveedor']
    if 'cantidad' in change:
        lote.qty_initial = change['cantidad']
    # etc.
```

**Garantías:**
- ✅ NUNCA modifica `producto.name`, `producto.internal_code`, `producto.category_id` del producto original
- ✅ Solo crea productos nuevos o reasocia lotes
- ✅ Solo afecta el lote específico de esa fila
- ✅ Preserva integridad histórica

### Frontend: `index.html`

**Función `detectReclassification()` (líneas 8120-8154)**

```javascript
function detectReclassification(row) {
    const original = originalData.find(o => o.lot_id === row.lot_id);
    const codigoOriginal = String(original.codigo_interno || '').trim().toLowerCase();
    const codigoNuevo = String(row.codigo_interno || '').trim().toLowerCase();
    
    // Si no cambió, no hay reclasificación
    if (codigoOriginal === codigoNuevo) return null;
    
    // Buscar si el código NUEVO ya existe
    const existing = allProducts.find(p => 
        String(p.internal_code || '').trim().toLowerCase() === codigoNuevo &&
        p.id !== row.product_id
    );
    
    if (existing) {
        // CASO A: Código existe → Reasociar
        return {
            type: 'reassign_to_existing',
            existingProductId: existing.id,
            existingProductName: existing.name,
            willBecomeLot: true
        };
    } else if (codigoNuevo) {
        // CASO B: Código NO existe → Crear nuevo
        return {
            type: 'create_new_product',
            newCode: codigoNuevo,
            willCreateNew: true
        };
    }
    
    return null;
}
```

**Estados Visuales en Tabla Editable:**
- `🔒 Bloqueado` - Tiene movimientos posteriores
- `➕ Nuevo producto` - Verde - Se creará producto nuevo
- `→ Lote existente` - Ámbar - Se reasociará a producto existente
- `Editable` - Gris - Sin cambios de código

**Resumen de Confirmación (líneas 8398-8410):**

```javascript
if (item.reclassification.type === 'create_new_product') {
    html += '<div class="bg-green-50 border-green-200">';
    html += '<i class="fas fa-plus-circle"></i>';
    html += `Resultado: Se creará un producto nuevo con código interno ${newCode}`;
    html += '</div>';
} else if (item.reclassification.type === 'reassign_to_existing') {
    html += '<div class="bg-amber-50 border-amber-200">';
    html += '<i class="fas fa-arrow-right"></i>';
    html += `Resultado: Este registro pasará a agregarse como lote al producto existente: ${existingName}`;
    html += '</div>';
}
```

---

## 📊 Ejemplos de Casos Reales

### Ejemplo 1: Cambiar COCBEB05 a COCBEB06 (Código Nuevo)

**Situación inicial:**
- Tanda N°1 tiene: Coca Cola, código COCBEB05, 20 unidades
- Producto maestro "Coca Cola" tiene código COCBEB05

**Acción del usuario:**
1. Modificar tanda N°1
2. Cambiar código de Coca Cola a COCBEB06
3. Click "Aplicar cambios"

**Modal de confirmación muestra:**
```
Coca Cola
  Código interno: COCBEB05 → COCBEB06
  
✅ Resultado: Se creará un producto nuevo con código interno COCBEB06
```

**Al confirmar:**
- ✅ Se crea producto nuevo "Coca Cola" con código COCBEB06
- ✅ El lote de 20 unidades de Tanda N°1 ahora pertenece al producto nuevo
- ✅ El producto maestro original "Coca Cola COCBEB05" sigue existiendo
- ✅ Si hay Tanda N°2 con COCBEB05, NO se afecta

**Resultado en UI:**
- Tanda N°1: Coca Cola - COCBEB06
- Tanda N°2: Coca Cola - COCBEB05 (sin cambios)

### Ejemplo 2: Cambiar COCBEB05 a COCBEB04 (Código Existente)

**Situación inicial:**
- Tanda N°1 tiene: Coca Cola, código COCBEB05, 20 unidades
- Ya existe producto "Coca Cola" con código COCBEB04

**Acción del usuario:**
1. Modificar tanda N°1
2. Cambiar código de Coca Cola a COCBEB04
3. Click "Aplicar cambios"

**Modal de confirmación muestra:**
```
Coca Cola
  Código interno: COCBEB05 → COCBEB04
  
🔄 Resultado: Este registro pasará a agregarse como lote al producto existente: Coca Cola COCBEB04
```

**Al confirmar:**
- ✅ El lote de 20 unidades de Tanda N°1 ahora pertenece al producto COCBEB04
- ✅ NO se crea producto duplicado
- ✅ El producto COCBEB05 queda huérfano (se limpia si no tiene otros lotes)
- ✅ Otras tandas con COCBEB05 NO se afectan

**Resultado en UI:**
- Tanda N°1: Coca Cola - COCBEB04
- Stock de COCBEB04: suma las 20 unidades de este lote

### Ejemplo 3: Solo Cambiar Proveedor (Sin Cambio de Código)

**Situación inicial:**
- Tanda N°1 tiene: Coca Cola, código COCBEB05, proveedor "Distribuidora A"

**Acción del usuario:**
1. Modificar tanda N°1
2. Cambiar proveedor a "Distribuidora B"
3. NO cambiar código interno
4. Click "Aplicar cambios"

**Modal de confirmación muestra:**
```
Coca Cola
  Proveedor: Distribuidora A → Distribuidora B
  
Sin conversiones ni creaciones
```

**Al confirmar:**
- ✅ Solo se actualiza `lote.supplier_name = 'Distribuidora B'`
- ❌ NO se modifica el producto maestro
- ❌ NO se crean productos nuevos
- ❌ NO se reasocian lotes
- ✅ Otras tandas NO se afectan

---

## 🔒 Validaciones de Seguridad

### Bloqueo por Movimientos Posteriores

Si el lote ya tiene ventas, consumos o ajustes:
```
❌ NO se permite cambiar código interno
❌ NO se permite crear producto nuevo
❌ NO se permite reasociar lote
```

**Estado visual:** `🔒 Bloqueado`

**Mensaje:** "No se puede modificar: tiene movimientos posteriores"

### Limpieza de Productos Huérfanos

Cuando un producto queda sin lotes después de una reasociación:

```python
# Verificar si tiene lotes
tiene_lotes = db.session.query(InventoryLot.id).filter(
    InventoryLot.product_id == prod_id
).first() is not None

# Verificar si tiene movimientos directos
tiene_movimientos = db.session.query(InventoryMovement.id).filter(
    InventoryMovement.product_id == prod_id
).first() is not None

# Si no tiene nada, marcar como eliminado
if not tiene_lotes and not tiene_movimientos:
    producto.deleted_at = datetime.utcnow()
```

**Seguridad:** Solo limpia si está 100% huérfano.

---

## 📋 Comparación Antes vs Ahora

### ANTES (Incorrecto) ❌

| Acción | Resultado |
|--------|-----------|
| Cambiar código COCBEB05 → COCBEB06 | Modifica producto maestro globalmente |
| Tanda N°2 con COCBEB05 | Se ve afectada, ahora muestra COCBEB06 |
| Histórico | Se pierde, todo cambia a COCBEB06 |
| Lotes de otras tandas | Todos cambian de código |

### AHORA (Correcto) ✅

| Acción | Resultado |
|--------|-----------|
| Cambiar código COCBEB05 → COCBEB06 | Crea producto nuevo con COCBEB06 |
| Tanda N°2 con COCBEB05 | NO se afecta, mantiene COCBEB05 |
| Histórico | Se preserva intacto |
| Lotes de otras tandas | NO se modifican |

---

## 📁 Archivos Modificados

### Backend
**`app/inventory/routes_tanda_update.py` (líneas 123-263)**
- Revertida lógica que modificaba producto maestro
- Implementado Caso A: Crear producto nuevo
- Implementado Caso B: Reasociar a producto existente
- Implementado Caso C: Solo actualizar lote

### Frontend
**`app/templates/inventory/index.html`**
- `detectReclassification()` (líneas 8120-8154): Detecta tipo de cambio
- Renderizado de estados (líneas 8167-8181): Visual claro de cada caso
- Modal de confirmación (líneas 8398-8410): Explicación del resultado
- Payload al backend (líneas 8433-8448): Envío correcto de flags

---

## ✅ Checklist de Validación

### Persistencia Correcta
- [x] Cambio a código nuevo crea producto nuevo
- [x] Cambio a código existente reasocia lote
- [x] Sin cambio de código NO modifica producto maestro
- [x] Solo se afecta el lote de la fila editada
- [x] Otras tandas NO se ven afectadas

### Estados Visuales
- [x] "Nuevo producto" en verde para código nuevo
- [x] "→ Lote existente" en ámbar para código existente
- [x] "Editable" en gris para sin cambios
- [x] "🔒 Bloqueado" en rojo para con movimientos

### Resumen de Confirmación
- [x] Muestra claramente: "Se creará producto nuevo"
- [x] Muestra claramente: "Pasará a agregarse como lote a..."
- [x] Solo lista productos con cambios reales
- [x] Formato antes → después legible

### Seguridad
- [x] Bloquea edición si hay movimientos posteriores
- [x] Limpia productos huérfanos automáticamente
- [x] Transacción completa con rollback en error
- [x] Validaciones en frontend y backend

---

## 🧪 Testing Recomendado

### Test 1: Crear Producto Nuevo
```
1. Tanda tiene: Coca Cola, COCBEB05
2. Modificar tanda
3. Cambiar código a COCBEB06 (no existe)
4. Aplicar cambios
5. ✓ Modal muestra: "Se creará producto nuevo COCBEB06"
6. Confirmar
7. ✓ Verificar producto nuevo creado
8. ✓ Verificar lote asociado al nuevo producto
9. ✓ Verificar COCBEB05 sigue existiendo
10. ✓ Verificar otras tandas NO afectadas
```

### Test 2: Reasociar a Producto Existente
```
1. Ya existe producto con COCBEB04
2. Tanda tiene: Coca Cola, COCBEB05
3. Modificar tanda
4. Cambiar código a COCBEB04
5. Aplicar cambios
6. ✓ Modal muestra: "Pasará a lote de COCBEB04"
7. Confirmar
8. ✓ Verificar lote ahora en COCBEB04
9. ✓ Verificar COCBEB05 eliminado (si quedó huérfano)
10. ✓ Verificar otras tandas NO afectadas
```

### Test 3: Sin Cambio de Código
```
1. Tanda tiene: Coca Cola, COCBEB05, proveedor A
2. Modificar tanda
3. Cambiar solo proveedor a B
4. NO cambiar código
5. Aplicar cambios
6. ✓ Modal muestra solo: "Proveedor: A → B"
7. ✓ NO muestra "crear producto" ni "reasociar"
8. Confirmar
9. ✓ Verificar solo cambió proveedor del lote
10. ✓ Verificar producto maestro intacto
```

### Test 4: Múltiples Tandas No Se Afectan
```
1. Crear Tanda N°1: Coca Cola COCBEB05 (20 unidades)
2. Crear Tanda N°2: Coca Cola COCBEB05 (30 unidades)
3. Modificar solo Tanda N°1
4. Cambiar código a COCBEB06
5. Confirmar
6. ✓ Tanda N°1: COCBEB06 (20 unidades)
7. ✓ Tanda N°2: COCBEB05 (30 unidades) - SIN CAMBIOS
8. ✓ Stock COCBEB05: 30 unidades
9. ✓ Stock COCBEB06: 20 unidades
```

---

## 🎯 Resultado Final

### Problema Resuelto
✅ Modificar una tanda **NO modifica el producto maestro global**  
✅ Cada tanda resuelve su imputación de forma independiente  
✅ Trazabilidad histórica preservada  
✅ Integridad de datos garantizada  

### Comportamiento Correcto
- **Código nuevo** → Crea producto nuevo para esa fila
- **Código existente** → Reasocia lote al producto existente
- **Sin cambio** → Solo actualiza datos del lote

### UX Mejorada
- Estados visuales claros en tabla editable
- Resumen de confirmación explica exactamente qué pasará
- Usuario tiene control total del resultado

---

**Fecha:** 10/03/2026  
**Estado:** ✅ Corrección crítica completada  
**Prioridad:** 🔴🔴🔴 Máxima - Corregía comportamiento destructivo
