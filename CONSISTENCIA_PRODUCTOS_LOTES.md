# Consistencia de Productos y Lotes - Ciclo de Vida Completo

## 🔄 Problemas Corregidos

### Problema 1: Editar Lote Individual No Abría Modal de Tanda
**Antes:** Click "Editar lote" → Abría modal simple limitado  
**Ahora:** Click "Editar lote" → Abre modal de tanda completo con tabla editable ✅

### Problema 2: Productos Huérfanos Desaparecían Permanentemente
**Antes:**  
```
COCBEB05 → COCBEB06 (crea nuevo producto, COCBEB05 se elimina físicamente)
COCBEB06 → COCBEB05 (intenta crear pero código ya usado, falla)
Resultado: Producto perdido en backend "huérfano"
```

**Ahora:**  
```
COCBEB05 → COCBEB06 (crea nuevo producto, COCBEB05 se marca soft-deleted)
COCBEB06 → COCBEB05 (detecta que existe soft-deleted, lo REVIVE)
Resultado: Producto COCBEB05 vuelve a estar activo ✅
```

---

## 🔄 Ciclo de Vida Completo de un Producto

### Escenario Completo: Crear → Reasociar → Volver Independiente

#### Estado Inicial
```
Tanda N°1:
  - Coca Cola
  - Código: COCBEB05
  - 20 unidades
  - Proveedor: Distribuidora Pepsi

Backend:
  Product(id=100, internal_code='COCBEB05', deleted_at=NULL, active=True)
  InventoryLot(id=200, product_id=100, qty=20)
```

#### Paso 1: Cambiar COCBEB05 → COCBEB06
**Usuario:** Modifica tanda, cambia código a COCBEB06  
**Sistema:** Detecta código nuevo (no existe)

**Acciones:**
```python
# 1. Crear producto nuevo
Product(id=101, internal_code='COCBEB06', deleted_at=NULL, active=True)

# 2. Reasociar lote
InventoryLot(id=200, product_id=101)  # Cambió de 100 a 101

# 3. Marcar producto original como huérfano (SOFT DELETE)
Product(id=100, internal_code='COCBEB05', deleted_at='2026-03-10', active=False)
```

**Estado resultante:**
```
Productos:
  - COCBEB05 (soft-deleted) ❌
  - COCBEB06 (activo) ✅

Lotes:
  - Lote 200 → Producto COCBEB06 ✅
```

#### Paso 2: Volver COCBEB06 → COCBEB05
**Usuario:** Modifica tanda nuevamente, cambia código a COCBEB05  
**Sistema:** Detecta código existente PERO soft-deleted

**Acciones:**
```python
# 1. Buscar producto (INCLUYENDO soft-deleted)
producto = Product.query.filter_by(internal_code='COCBEB05').first()
# Encuentra: Product(id=100, deleted_at='2026-03-10')

# 2. REVIVIR producto
producto.deleted_at = None
producto.active = True

# 3. Reasociar lote al producto revivido
InventoryLot(id=200, product_id=100)  # Vuelve a 100

# 4. Marcar COCBEB06 como huérfano (SOFT DELETE)
Product(id=101, internal_code='COCBEB06', deleted_at='2026-03-10', active=False)
```

**Estado resultante:**
```
Productos:
  - COCBEB05 (activo) ✅ ← REVIVIÓ
  - COCBEB06 (soft-deleted) ❌

Lotes:
  - Lote 200 → Producto COCBEB05 ✅
```

---

## 🎯 Lógica de Decisión al Cambiar Código

### Diagrama de Flujo

```
Usuario cambia código interno de X a Y
  ↓
¿Código Y existe?
  ├─ NO existe
  │   ↓
  │   Crear producto nuevo con código Y
  │   Reasociar lote al nuevo producto
  │   Soft-delete producto original X
  │
  ├─ SÍ existe y está ACTIVO
  │   ↓
  │   Reasociar lote al producto existente Y
  │   Soft-delete producto original X
  │
  └─ SÍ existe pero está SOFT-DELETED
      ↓
      REVIVIR producto Y (deleted_at=NULL, active=True)
      Reasociar lote al producto revivido
      Soft-delete producto original X
```

### Casos de Uso

#### Caso 1: Código Nuevo (No Existe)
```
COCBEB05 → COCBEB99
```
**Resultado:**
- ✅ Crea `Product(internal_code='COCBEB99')`
- ✅ Reasocia lote
- ✅ Soft-delete `COCBEB05`

#### Caso 2: Código Existente Activo
```
COCBEB05 → COCBEB04 (COCBEB04 ya existe y está activo)
```
**Resultado:**
- ✅ NO crea producto duplicado
- ✅ Reasocia lote a `COCBEB04` existente
- ✅ Soft-delete `COCBEB05`

#### Caso 3: Código Existente Soft-Deleted (NUEVO)
```
COCBEB06 → COCBEB05 (COCBEB05 existe pero deleted_at='2026-03-10')
```
**Resultado:**
- ✅ NO crea producto duplicado
- ✅ REVIVE `COCBEB05` (deleted_at=NULL)
- ✅ Reasocia lote a `COCBEB05` revivido
- ✅ Soft-delete `COCBEB06`

---

## 🔧 Implementación Técnica

### Backend: `routes_tanda_update.py`

**Búsqueda que incluye soft-deleted:**
```python
# ANTES (incorrecto)
existe_producto = db.session.query(Product).filter(
    Product.company_id == cid,
    Product.internal_code == codigo_interno_nuevo,
    Product.deleted_at == None  # ❌ Excluía soft-deleted
).first()

# AHORA (correcto)
existe_producto = db.session.query(Product).filter(
    Product.company_id == cid,
    Product.internal_code == codigo_interno_nuevo
    # ✅ No filtra por deleted_at, busca TODOS
).first()
```

**Lógica de revivir:**
```python
if existe_producto and existe_producto.deleted_at is not None:
    # REVIVIR producto soft-deleted
    existe_producto.deleted_at = None
    existe_producto.active = True
    
    # Reasociar lote
    lote.product_id = existe_producto.id
    
    # Actualizar movimientos
    db.session.query(InventoryMovement).filter(
        InventoryMovement.lot_id == lote.id
    ).update({
        InventoryMovement.product_id: existe_producto.id
    })
    
    # Marcar producto viejo como huérfano
    productos_huerfanos.append(old_product_id)
```

**Soft delete de huérfanos:**
```python
# ANTES (incorrecto)
if not tiene_lotes and not tiene_movimientos:
    db.session.delete(producto)  # ❌ Eliminación física

# AHORA (correcto)
if not tiene_lotes and not tiene_movimientos:
    producto.deleted_at = datetime.utcnow()
    producto.active = False
    # ✅ NO eliminar físicamente
```

### Frontend: `index.html`

**Editar lote individual abre modal de tanda:**
```javascript
Array.from(tbodyLots.querySelectorAll('.btn-lot-edit')).forEach(btn => {
    btn.addEventListener('click', function () {
        const lotId = this.getAttribute('data-lot-id') || '';
        
        // Buscar received_at del lote
        const lot = allLots.find(l => String(l.id) === String(lotId));
        
        if (lot && lot.received_at) {
            // ✅ Abrir modal de tanda (mismo que "Modificar tanda")
            window.openTandaEditor(lot.received_at);
        } else {
            // Fallback al editor simple
            openEditLot(lotId);
        }
    });
});
```

---

## 📊 Matriz de Estados

| Código Original | Código Nuevo | Existe Nuevo | Estado Nuevo | Acción |
|----------------|--------------|--------------|--------------|--------|
| COCBEB05 | COCBEB06 | NO | - | Crear producto nuevo |
| COCBEB05 | COCBEB04 | SÍ | Activo | Reasociar a existente |
| COCBEB06 | COCBEB05 | SÍ | Soft-deleted | **REVIVIR** producto |
| COCBEB05 | [vacío] | - | - | Solo actualizar lote |

---

## ✅ Garantías de Consistencia

### 1. No Hay Productos Duplicados
- ✅ Busca TODOS los productos (activos y soft-deleted)
- ✅ Si existe, reasocia o revive
- ✅ Solo crea nuevo si NO existe

### 2. Productos No Desaparecen
- ✅ Soft delete en lugar de eliminación física
- ✅ Productos huérfanos quedan en BD con `deleted_at`
- ✅ Se pueden revivir automáticamente

### 3. Trazabilidad Completa
- ✅ Historial de cambios preservado
- ✅ Productos soft-deleted tienen timestamp
- ✅ Movimientos mantienen integridad referencial

### 4. Edición Consistente
- ✅ Lote individual = Tanda completa (mismo modal)
- ✅ Misma lógica de detección de cambios
- ✅ Mismo resumen de confirmación

---

## 🧪 Testing del Ciclo Completo

### Test 1: Crear → Volver
```
1. Tanda con COCBEB05
2. Modificar: COCBEB05 → COCBEB06
3. ✓ Producto COCBEB06 creado
4. ✓ COCBEB05 soft-deleted
5. Modificar: COCBEB06 → COCBEB05
6. ✓ Producto COCBEB05 REVIVIDO
7. ✓ COCBEB06 soft-deleted
8. ✓ No hay duplicados
```

### Test 2: Crear → Reasociar → Volver
```
1. Tanda con COCBEB05
2. Modificar: COCBEB05 → COCBEB06 (crea nuevo)
3. Modificar: COCBEB06 → COCBEB04 (reasocia a existente)
4. ✓ COCBEB05 soft-deleted
5. ✓ COCBEB06 soft-deleted
6. ✓ Lote en COCBEB04
7. Modificar: COCBEB04 → COCBEB05
8. ✓ COCBEB05 REVIVE
9. ✓ Lote vuelve a COCBEB05
```

### Test 3: Editar Lote Individual
```
1. Ver stock individual (Imagen 2)
2. Click menú ⋮ en lote
3. Click "Editar lote"
4. ✓ Se abre modal de tanda (Imagen 1)
5. ✓ Tabla editable con todas las columnas
6. ✓ Resumen de cambios
7. ✓ Confirmación previa
```

---

## 🔍 Verificación en UI

### Vista Stock Individual (Imagen 2)
**Antes:**
```
Coca Cola - COCBEB06
  [Menú]
    - Editar lote → Modal simple ❌
```

**Ahora:**
```
Coca Cola - COCBEB06
  [Menú]
    - Editar lote → Modal de tanda ✅
    - Ir al legajo
    - Eliminar lote
```

### Legajo del Producto
**Antes:** Producto desaparecía si quedaba huérfano  
**Ahora:** Producto se marca como inactivo pero se puede revivir

```
Backend:
  Product(id=100, internal_code='COCBEB05')
  - Si tiene lotes: active=True, deleted_at=NULL
  - Si es huérfano: active=False, deleted_at='2026-03-10'
  - Si se revive: active=True, deleted_at=NULL
```

---

## 📁 Archivos Modificados

### Backend
**`app/inventory/routes_tanda_update.py`**
- Líneas 159-182: Buscar productos incluyendo soft-deleted y revivir
- Líneas 312-317: Soft delete de huérfanos (no eliminación física)

### Frontend
**`app/templates/inventory/index.html`**
- Líneas 5507-5526: Editar lote individual abre modal de tanda

---

## 🎯 Resultado Final

### Problema Original
❌ Productos desaparecían al hacer cambios de código  
❌ Editar lote individual tenía funcionalidad limitada  
❌ Inconsistencia entre editar tanda vs editar lote  

### Solución Implementada
✅ Productos se preservan con soft delete  
✅ Productos se reviven automáticamente si se necesitan  
✅ Editar lote = Editar tanda (mismo modal y funcionalidad)  
✅ Consistencia total en el ciclo de vida  
✅ Trazabilidad completa sin pérdida de datos  

---

**Fecha:** 10/03/2026  
**Estado:** ✅ Corrección completada  
**Prioridad:** 🔴 Crítica - Corregía pérdida de datos e inconsistencia
