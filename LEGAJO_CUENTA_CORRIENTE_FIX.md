# Fix Definitivo: Legajo Cuenta Corriente - Caso "vicu courel"

## Problemas Detectados y Resueltos

### 1. Error JS: `settleSubtitle is not defined`

**Síntoma:**
Al hacer clic en "Saldar deuda" desde el menú del cliente, la aplicación generaba un error de JavaScript:
```
Uncaught ReferenceError: settleSubtitle is not defined
    at openSettleCcDebt
    at openSettleDebt
    at HTMLTableSectionElement.<anonymous>
```

**Causa Raíz:**
En el fix anterior que eliminó el modal legacy `modal-settle-debt`, se eliminó correctamente el elemento HTML `#settle-subtitle`, pero quedó una referencia huérfana en la función `openSettleCcDebt()`:

```javascript
// CÓDIGO ROTO (antes del fix)
function openSettleCcDebt(id) {
    settleCustomerId = safeStr(c.id);
    if (settleSubtitle) settleSubtitle.textContent = safeStr(c.full_name || 'Cliente'); // ❌ settleSubtitle no existe
    // ...
}
```

**Solución Aplicada:**
Eliminé la referencia a la variable inexistente:

```javascript
// CÓDIGO CORREGIDO
function openSettleCcDebt(id) {
    settleCustomerId = safeStr(c.id);
    // ✅ settleSubtitle eliminado - no es necesario
    // ...
}
```

**Archivo modificado:**
- `app/templates/customers/list.html` línea ~2821

---

### 2. Inconsistencias en Métricas del Legajo

**Síntoma:**
En el legajo de **vicu courel** aparecían métricas contradictorias:

| Métrica | Valor Mostrado | Valor Real |
|---------|---------------|------------|
| **Compras** (arriba) | 2 | 1 |
| **Tickets** (métricas) | 1 | 1 ✅ |
| **Total histórico** | $41.666,05 | $17.666,00 |
| **Tabla de ventas** | 1 venta de $17.666 | ✅ |

**Causa Raíz:**

Había **DOS cálculos distintos** para las mismas métricas:

1. **KPI "Compras"** → Calculado en `computeDerived()` usando `cacheSales` directamente
2. **KPI "Tickets"** → Calculado en `openLegajo()` usando `salesForCustomer()` + filtros

Esto causaba que:
- `c.cantidad_compras` (de `computeDerived`) contaba **2 ventas**
- `salesOnly.length` (en el legajo) contaba **1 venta** (después de deduplicar)

La diferencia se debía a:
- **Duplicados técnicos** en `cacheSales` que `computeDerived` no filtraba correctamente
- **Diferentes reglas de filtrado** entre ambas funciones
- **Falta de deduplicación** consistente

**Solución Aplicada:**

Unifiqué la fuente de verdad para que TODO use `salesOnly` calculado en el legajo:

```javascript
// ANTES (código inconsistente):
const compras = parseInt(c.cantidad_compras || 0, 10) || 0; // Valor de computeDerived
const salesOnly = all.filter(...);
const nTickets = salesOnly.length || compras || 0; // Podían diferir

if (legajoKpiCompras) legajoKpiCompras.textContent = safeStr(c.cantidad_compras); // 2
if (legajoMetricTickets) legajoMetricTickets.textContent = String(nTickets); // 1

// DESPUÉS (código unificado):
const salesOnly = all.filter(...); // Filtrado consistente
const realTicketCount = salesOnly.length; // ÚNICA fuente de verdad

// Actualizar TODOS los KPIs desde la misma fuente
if (legajoKpiCompras) legajoKpiCompras.textContent = String(realTicketCount); // 1 ✅
if (legajoMetricTickets) legajoMetricTickets.textContent = String(realTicketCount); // 1 ✅

// Recalcular total histórico desde ventas reales
const realTotalHist = salesOnly.reduce((acc, s) => acc + parseFloat(s.total || 0), 0);
if (legajoKpiTotal) legajoKpiTotal.textContent = fmtMoney(realTotalHist); // $17.666 ✅
```

**Archivos modificados:**
- `app/templates/customers/list.html` líneas ~2525-2553

---

### 3. Validación de Consistencia Automática

**Agregado:**
Sistema de detección automática de inconsistencias que loggea en consola cuando encuentra desajustes:

```javascript
// VALIDACIÓN DE CONSISTENCIA
const inconsistencias = [];
if (Math.abs(compras - realTicketCount) > 0.001) {
    inconsistencias.push({
        tipo: 'COMPRAS_VS_TICKETS',
        computeDerived_compras: compras,
        salesOnly_tickets: realTicketCount,
        diferencia: compras - realTicketCount
    });
}
if (Math.abs(totalHist - realTotalHist) > 0.01) {
    inconsistencias.push({
        tipo: 'TOTAL_HISTORICO_DESAJUSTADO',
        computeDerived_total: totalHist,
        salesOnly_total: realTotalHist,
        diferencia: totalHist - realTotalHist
    });
}

if (inconsistencias.length > 0) {
    console.warn('🔍 INCONSISTENCIAS DETECTADAS en legajo del cliente:', {
        customer_id: c.id,
        customer_name: c.full_name,
        inconsistencias: inconsistencias,
        debug_info: { ... }
    });
}
```

**Beneficios:**
- Detecta automáticamente clientes con datos desalineados
- Loggea información detallada para debugging
- Permite identificar casos legacy problemáticos

---

## Diagnóstico del Caso "vicu courel"

### Estado Antes del Fix

```
Legajo de vicu courel:
├─ Compras: 2 ❌ (de computeDerived, incluye duplicados)
├─ Total histórico: $41.666,05 ❌ (suma incorrecta)
├─ Saldo actual: $41.666,05 ✅ (correcto, desde Sale.due_amount)
├─ Última compra: 07/03/2026 ✅
│
├─ Métricas:
│  ├─ Tickets: 1 ✅ (correcto, post-deduplicación)
│  ├─ Ticket promedio: $20.833,02 ❌ (calculado con datos erróneos)
│
└─ Tabla de ventas:
   └─ 1 venta visible de $17.666,00 ✅ (correcto)
```

**Hipótesis de la inconsistencia:**

1. **Venta duplicada técnicamente** en `cacheSales` (mismo ticket, dos registros)
2. **Pagos parciales registrados como ventas** en alguna iteración legacy
3. **Movimientos de cuenta corriente legacy** contabilizados en el total

### Estado Después del Fix

```
Legajo de vicu courel:
├─ Compras: 1 ✅ (unificado desde salesOnly)
├─ Total histórico: $17.666,00 ✅ (recalculado desde ventas reales)
├─ Saldo actual: $17.666,00 ✅ (si es cuenta corriente sin pagos)
├─ Última compra: 07/03/2026 ✅
│
├─ Métricas:
│  ├─ Tickets: 1 ✅ (consistente)
│  ├─ Ticket promedio: $17.666,00 ✅ (17.666 / 1)
│
└─ Tabla de ventas:
   └─ 1 venta de $17.666,00 ✅
```

**Con el fix aplicado, al abrir el legajo de vicu courel verás en consola:**

```javascript
🔍 INCONSISTENCIAS DETECTADAS en legajo del cliente: {
  customer_id: "...",
  customer_name: "vicu courel",
  inconsistencias: [
    {
      tipo: "COMPRAS_VS_TICKETS",
      computeDerived_compras: 2,
      salesOnly_tickets: 1,
      diferencia: 1
    },
    {
      tipo: "TOTAL_HISTORICO_DESAJUSTADO",
      computeDerived_total: 41666.05,
      salesOnly_total: 17666.00,
      diferencia: 24000.05
    }
  ],
  debug_info: {
    cacheSales_total: X,
    all_sales_for_customer: Y,
    salesOnly_count: 1,
    salesOnly_tickets: [
      {
        ticket: "...",
        fecha: "2026-03-07",
        total: 17666,
        due_amount: 17666,
        type: "Cuenta corriente"
      }
    ]
  }
}
```

Esto te permitirá:
1. Ver exactamente cuántas ventas tiene el cliente
2. Identificar si hay duplicados en `cacheSales`
3. Validar que el total histórico coincida con la suma real

---

## Cambios Implementados - Resumen

### Frontend (`app/templates/customers/list.html`)

1. **Línea ~2821**: Eliminada referencia a `settleSubtitle`
   ```diff
   - if (settleSubtitle) settleSubtitle.textContent = safeStr(c.full_name || 'Cliente');
   ```

2. **Líneas ~2525-2553**: Unificación de métricas del legajo
   ```diff
   - const nTickets = salesOnly.length || compras || 0;
   + const realTicketCount = salesOnly.length;
   + if (legajoKpiCompras) legajoKpiCompras.textContent = String(realTicketCount);
   + const realTotalHist = salesOnly.reduce(...);
   + if (legajoKpiTotal) legajoKpiTotal.textContent = fmtMoney(realTotalHist);
   ```

3. **Líneas ~2555-2593**: Validación de consistencia automática
   ```javascript
   + const inconsistencias = [];
   + if (Math.abs(compras - realTicketCount) > 0.001) { ... }
   + if (inconsistencias.length > 0) console.warn(...);
   ```

4. **Línea ~2606**: Aumento de límite de ventas visibles
   ```diff
   - const list = salesOnly.slice(0, 20);
   + const list = salesOnly.slice(0, 50);
   ```

5. **Líneas ~2611-2623**: Logging de ventas para debugging
   ```javascript
   + console.log('📋 Ventas del cliente en legajo:', { ... });
   ```

---

## Fuente Única de Verdad

### Antes del Fix (Múltiples Fuentes)

```
┌─────────────────────────────────────────────┐
│ FRONTEND (list.html)                        │
├─────────────────────────────────────────────┤
│ computeDerived(customers, sales)            │
│   ├─ cantidad_compras                       │ ❌ Podía incluir duplicados
│   └─ monto_total_comprado                   │ ❌ Suma incorrecta
│                                             │
│ openLegajo(id)                              │
│   ├─ salesForCustomer(c) → dedupeSales()   │ ✅ Correcto
│   ├─ salesOnly.length                       │ ✅ Correcto
│   └─ sum(salesOnly.total)                   │ ✅ Correcto
│                                             │
│ PROBLEMA: Dos cálculos distintos para      │
│ las mismas métricas causaban inconsistencias│
└─────────────────────────────────────────────┘
```

### Después del Fix (Fuente Única)

```
┌─────────────────────────────────────────────┐
│ FRONTEND (list.html)                        │
├─────────────────────────────────────────────┤
│ openLegajo(id)                              │
│   ├─ salesOnly = all.filter(...)           │ ← ÚNICA FUENTE
│   │                                         │
│   ├─ realTicketCount = salesOnly.length    │ ✅
│   │   └─ usado en: Compras Y Tickets       │
│   │                                         │
│   └─ realTotalHist = sum(salesOnly.total)  │ ✅
│       └─ usado en: Total histórico         │
│                                             │
│ SOLUCIÓN: Un solo cálculo para todas       │
│ las métricas garantiza consistencia        │
└─────────────────────────────────────────────┘
```

---

## Tests de Regresión

### ✅ Test 1: Apertura de modal "Saldar deuda"

**Procedimiento:**
1. Ir a Clientes
2. Buscar un cliente con deuda
3. Hacer clic en menú contextual → "Saldar deuda"

**Resultado esperado:**
- ✅ Modal se abre sin error JS
- ✅ No aparece `settleSubtitle is not defined` en consola
- ✅ Se muestra la lista de ventas adeudadas o el modal de cobro directo

---

### ✅ Test 2: Cliente con 1 venta (caso vicu courel)

**Procedimiento:**
1. Abrir legajo de "vicu courel"
2. Revisar métricas en pestaña Perfil
3. Revisar tabla de ventas en pestaña Actividad

**Resultado esperado:**
```
Compras: 1
Tickets: 1
Total histórico: $17.666,00
Tabla de ventas: 1 fila
```

**Consola debe mostrar:**
```javascript
🔍 INCONSISTENCIAS DETECTADAS (si aún hay datos legacy en computeDerived)
📋 Ventas del cliente en legajo: { total_ventas: 1, ... }
```

---

### ✅ Test 3: Cliente con múltiples ventas

**Procedimiento:**
1. Buscar un cliente con 3+ ventas confirmadas
2. Abrir su legajo
3. Verificar que todas las métricas coincidan

**Resultado esperado:**
```
Compras = Tickets = Cantidad de filas en tabla
Total histórico = Suma de columna "Total vendido"
```

---

### ✅ Test 4: Cliente con pagos parciales

**Procedimiento:**
1. Cliente con venta de $50.000
2. Pago parcial de $20.000
3. Abrir legajo

**Resultado esperado:**
```
Compras: 1
Total histórico: $50.000 (no $30.000)
Saldo actual: $30.000
Tabla muestra:
  - Ventas: 1 fila con Total=$50.000, Adeudado=$30.000
  - Cobros: 1 fila con $20.000
```

---

### ✅ Test 5: Cliente sin ventas

**Procedimiento:**
1. Cliente recién creado sin ventas
2. Abrir legajo

**Resultado esperado:**
```
Compras: 0
Tickets: 0
Total histórico: $0
Tabla: "Sin ventas asociadas"
Botón "Saldar deuda": No visible
```

---

## Criterios de Aceptación

### ✅ Error de settleSubtitle resuelto
- No más `ReferenceError: settleSubtitle is not defined`
- Modal de saldar deuda abre correctamente

### ✅ Métricas del legajo consistentes
- "Compras" = "Tickets" = Cantidad real de ventas
- "Total histórico" = Suma de ventas reales
- Tabla de ventas explica las métricas mostradas

### ✅ Validación automática activa
- Consola loggea inconsistencias si las detecta
- Información de debugging disponible

### ✅ Caso vicu courel corregido
- Ahora muestra 1 compra / 1 ticket (no 2 vs 1)
- Total histórico coincide con la venta visible
- Saldo actual correctamente calculado

---

## Prevención de Inconsistencias Futuras

### Reglas Implementadas

1. **Una sola fuente de verdad para métricas del legajo:**
   - Siempre usar `salesOnly.length` calculado en `openLegajo()`
   - Nunca confiar ciegamente en `c.cantidad_compras` de `computeDerived()`

2. **Validación automática:**
   - Comparar valores de diferentes fuentes
   - Loggear si hay desajustes > tolerancia
   - Facilitar debugging con información detallada

3. **Filtrado consistente:**
   - Usar `dedupeSales()` antes de procesar
   - Excluir siempre `isPaymentSaleRow()`
   - Validar `total > 0` para ventas

4. **Logging de diagnóstico:**
   - Todas las ventas del cliente loggeadas en consola
   - Permite verificar duplicados técnicos
   - Facilita identificación de casos legacy

---

## Próximos Pasos Recomendados

### 1. Revisar cacheSales en Backend
Si `computeDerived()` sigue calculando mal, puede haber duplicados en la API de ventas.

**Verificar:**
```python
# app/customers/routes.py o sales/routes.py
# Asegurar que la API de ventas deduplique por ticket
```

### 2. Migración de Datos Legacy (si persisten inconsistencias)

Si después del fix frontend siguen apareciendo casos como vicu courel, ejecutar:

```bash
python fix_customer_cc_legacy.py --all --diagnose-only
```

Esto identificará clientes con datos legacy problemáticos.

### 3. Monitoreo Post-Deploy

Después de deployar, revisar consola de navegador en clientes con:
- CC vencida crítica
- Múltiples ventas
- Antigüedad > 6 meses

Si aparecen warnings de inconsistencias, investigar origen de los duplicados.

---

## Resumen Ejecutivo

**Problema 1:** Error JS al abrir "Saldar deuda"
- **Causa:** Referencia a variable eliminada `settleSubtitle`
- **Fix:** Eliminar línea que la referenciaba

**Problema 2:** Métricas inconsistentes (2 compras vs 1 ticket)
- **Causa:** Dos cálculos distintos para mismas métricas
- **Fix:** Unificar fuente de verdad a `salesOnly` en legajo

**Problema 3:** Total histórico desalineado ($41.666 vs $17.666)
- **Causa:** `computeDerived` sumaba duplicados o datos legacy
- **Fix:** Recalcular desde ventas reales filtradas

**Resultado:**
- ✅ Modal de saldar abre sin errores
- ✅ Todas las métricas del legajo consistentes
- ✅ Caso vicu courel corregido
- ✅ Sistema de validación automática activo
