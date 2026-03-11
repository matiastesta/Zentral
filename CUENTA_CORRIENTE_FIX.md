# Fix Definitivo: Cuenta Corriente Legacy

## Problema Resuelto

Se eliminó el bug crítico donde clientes con deuda real mostraban $0 en el modal de saldar cuenta corriente debido a la coexistencia de dos flujos:

- **Modal viejo** (`modal-settle-debt`): Mostraba datos incorrectos/cacheados → **ELIMINADO**
- **Modal nuevo** (`modal-cobro`): Calcula desde la fuente de verdad → **AHORA ES EL ÚNICO**

## Cambios Realizados

### 1. Frontend - Eliminación de Modal Legacy

**Archivo:** `app/templates/customers/list.html`

**Eliminado:**
- Modal completo `#modal-settle-debt` con su HTML
- Referencias a `modalSettle`, `btnCloseSettle`, `btnCancelSettle`, `settleSubtitle`
- Función legacy que poblaba el modal con datos incorrectos
- Event listeners del modal viejo

**Mantenido:**
- `modal-settle-type`: Selector de tipo de deuda (CC vs Cuotas)
- `modal-cobro`: Modal unificado que funciona correctamente
- `openSettleCcDebt()`: Ahora usa solo el flujo correcto

### 2. Backend - Fuente de Verdad Unificada

**Archivo:** `app/customers/routes.py`

**Endpoint existente validado:**
```python
@bp.get('/api/customers/<customer_id>/debt-summary')
```

Este endpoint **YA calcula correctamente** desde:
```python
Sale.company_id == company_id
Sale.customer_id == customer_id
Sale.sale_type == 'Venta'
Sale.status != 'Reemplazada'
Sale.due_amount > 0  # ← FUENTE DE VERDAD
```

**Nuevo endpoint de diagnóstico:**
```python
@bp.get('/api/customers/diagnose-cc')
```

Permite detectar clientes con deuda en producción sin ejecutar scripts externos.

### 3. Script de Diagnóstico y Reparación

**Archivo:** `fix_customer_cc_legacy.py`

**Uso:**

```bash
# Solo diagnóstico (sin cambios)
python fix_customer_cc_legacy.py --company-id <company_id> --diagnose-only

# Diagnóstico de todas las empresas
python fix_customer_cc_legacy.py --all --diagnose-only

# Simulación de reparación (dry-run)
python fix_customer_cc_legacy.py --company-id <company_id> --dry-run

# Reparación real
python fix_customer_cc_legacy.py --company-id <company_id>
```

**Nota:** Como la fuente de verdad es `Sale.due_amount`, el script principalmente **verifica** que no existan campos legacy cacheados incorrectamente. Si en el futuro se detectan, el script los normalizaría.

## Fuente de Verdad Actual

### Saldo de Cuenta Corriente

**SIEMPRE** se calcula desde:

```python
Sale.due_amount > 0
```

**NUNCA** desde:
- Campos cacheados en Customer ❌
- Variables legacy ❌
- Snapshots desactualizados ❌

### Cálculo en Frontend

El frontend **siempre** obtiene el saldo desde:
```javascript
const real_data = await fetch('/customers/api/customers/<id>/debt-summary');
```

Y luego muestra el modal de cobro con:
```javascript
openCobroModal({
    amount: due_amount,  // ← Desde Sale.due_amount real
    due_amount: due_amount,
    sale_id: sid,
    ...
});
```

## Flujo Correcto Actual

### Usuario hace click en "Saldar deuda"

1. `openSettleDebt(id)` → Verifica si tiene CC y/o Cuotas
2. Si tiene ambas → Abre `modal-settle-type` (selector)
3. Si solo CC → Llama directamente `openSettleCcDebt(id)`
4. `openSettleCcDebt(id)` → Busca ventas con `due_amount > 0`
5. Si 1 venta → Abre `modal-cobro` con esa venta
6. Si múltiples → Muestra lista y permite elegir
7. Al elegir → Llama `openCcCheckoutForSaleId(saleId)`
8. `openCcCheckoutForSaleId()` → Abre `modal-cobro` con datos reales

### Modal de Cobro (`modal-cobro`)

Muestra:
- Monto original de la venta
- Deuda actual (desde `Sale.due_amount`)
- Permite pago parcial o total
- Al confirmar → POST `/sales/api/sales/settle` con `sale_id` y `amount`

## Validaciones Implementadas

### Backend

1. **Endpoint `/api/customers/<id>/debt-summary`:**
   - Calcula desde `Sale.due_amount` en tiempo real
   - No usa cache ni campos legacy
   - Devuelve `cc_total_balance`, `cc_count`, `cc_status`

2. **Endpoint `/sales/api/sales/settle`:**
   - Valida que `sale_id` exista
   - Valida que `due_amount > 0`
   - Valida que `amount <= due_amount`
   - Crea registro de pago tipo `CobroCC`

### Frontend

1. **Modal de cobro:**
   - No renderiza si `due_amount` es 0
   - Muestra siempre el saldo real recalculado
   - Valida monto ingresado ≤ deuda

2. **Badge de estado:**
   - Se calcula desde el resumen real del cliente
   - Solo muestra "CC Vencida Crítica" si hay deuda vencida real

## Tests de Regresión Manuales

Verificar que:

### ✅ Caso 1: Cliente con deuda real
- Badge: "CC Vencida" o "CC Vencida Crítica"
- Legajo pestaña Finanzas: Saldo correcto
- Click "Saldar deuda": Modal muestra monto correcto
- Se puede pagar parcial o total

### ✅ Caso 2: Cliente sin deuda
- Badge: Sin estado de deuda
- Botón "Saldar deuda": No aparece o está deshabilitado
- Modal: No se abre con $0

### ✅ Caso 3: Cliente con pago parcial previo
- Venta de $50.000
- Pago previo de $20.000
- Modal muestra: $30.000 pendientes
- Al pagar $30.000 → Deuda queda en $0

### ✅ Caso 4: Multiempresa
- Clientes de empresa A no afectan empresa B
- Saldos aislados por `company_id`

## Diagnóstico en Producción

### Opción 1: Endpoint HTTP

```bash
curl -X GET "https://zentral.example.com/customers/api/customers/diagnose-cc" \
  -H "Cookie: session=..." \
  -H "Content-Type: application/json"
```

Devuelve:
```json
{
  "ok": true,
  "total_customers": 150,
  "customers_with_debt": 12,
  "results": [
    {
      "customer_id": "abc123",
      "customer_name": "Juan Pérez",
      "real_cc_balance": 41666.05,
      "sales_count": 2,
      "status": "OK - Fuente de verdad desde Sale.due_amount"
    },
    ...
  ]
}
```

### Opción 2: Script Python

```bash
python fix_customer_cc_legacy.py --all --diagnose-only
```

Genera reporte en consola con:
- Total clientes procesados
- Clientes con cuenta corriente
- Saldo real por cliente
- Ventas pendientes por cliente

## Criterios de Aceptación

### ✅ Eliminado modal viejo
- `modal-settle-debt` no existe en el código
- No quedan referencias a `settleSubtitle`, `settle-sale`, etc.

### ✅ Fuente de verdad unificada
- Todo saldo viene de `Sale.due_amount`
- No hay campos legacy en uso

### ✅ Consistencia garantizada
- Badge, legajo y modal muestran lo mismo
- Imposible tener "CC Vencida" + modal $0

### ✅ Multiempresa funcional
- Cada tenant tiene datos aislados
- Script funciona con `--all`

### ✅ Sistema de diagnóstico
- Endpoint HTTP disponible
- Script Python ejecutable
- Logs informativos

## Campos Legacy Identificados

**Nota:** En el modelo `Customer` actual **NO hay campos legacy** de saldo cacheado.

Si en el futuro se detectaran campos como:
- `saldo_cc` (deprecated)
- `ultima_deuda` (deprecated)
- `cache_*` (deprecated)

El script `fix_customer_cc_legacy.py` puede extenderse para:
1. Detectarlos
2. Compararlos con el saldo real
3. Reportar inconsistencias
4. Opcionalmente limpiarlos o normalizarlos

## Migración Completada

No se requiere migración de datos porque:

1. **No hay campos legacy en Customer**
2. **La fuente de verdad siempre fue `Sale.due_amount`**
3. **El problema era solo en el modal viejo del frontend**

## Logs de Diagnóstico

Al ejecutar el script, se genera un log como:

```
============================================================
  Diagnóstico de Cuenta Corriente
============================================================
Total clientes procesados: 150
Clientes con cuenta corriente: 12
Clientes reparados: 0
Inconsistencias encontradas: 0
Errores: 0

Detalles (mostrando primeros 20):

  1. Cliente: Juan Pérez
     ID: abc123
     Saldo real: $41,666.05
     Ventas pendientes: 2
     Vencidas: 1 ($21,666.05)

  2. Cliente: María García
     ID: def456
     Saldo real: $15,300.00
     Ventas pendientes: 1

...
============================================================
```

## Conclusión

El sistema ahora:

✅ Usa una **única fuente de verdad** (`Sale.due_amount`)  
✅ Eliminó el **modal legacy** que causaba el bug  
✅ Unificó el **flujo de saldar deuda** al modal correcto  
✅ Provee **diagnóstico en tiempo real** vía endpoint y script  
✅ Funciona **correctamente en multiempresa**  
✅ **Imposibilita** el caso "badge rojo + modal $0"  

No se requieren migraciones de datos porque el problema era únicamente de presentación en el frontend legacy.
