# Sistema de Tandas de Carga - Inventario

## Descripción General

El sistema de **Tandas de Carga** permite agrupar ingresos de inventario que fueron cargados juntos (especialmente importaciones masivas desde Excel) para facilitar su gestión y permitir la eliminación masiva segura de tandas completas.

## Características Principales

### 1. Creación Automática de Tandas
- **Importación Excel**: Cada vez que importás productos desde Excel, se crea automáticamente una tanda de carga que agrupa todos los lotes importados.
- **Identificador único**: Cada tanda recibe un identificador del formato `TANDA-YYYYMMDD-HHMMSS`.
- **Estadísticas**: La tanda registra cantidad de ítems, unidades totales, fecha/hora, usuario, y tipo de origen.

### 2. Vista Agrupada en Stock Ingreso
- **Toggle de agrupación**: En la vista "Stock ingreso", activá el checkbox "Agrupar por tandas de carga" para cambiar entre vista individual y agrupada.
- **Expansión/colapso**: Hacé clic en cualquier tanda para expandir y ver sus ítems individuales.
- **Filtros**: Podés filtrar tandas por tipo de origen (Excel o Manual).

### 3. Eliminación Masiva Segura
- **Un solo clic**: Eliminá una tanda completa con todos sus lotes de una sola vez.
- **Validaciones críticas**: El sistema **NO permite** eliminar tandas si:
  - Algún lote ya fue vendido
  - Algún lote fue consumido parcialmente
  - Hubo ajustes o movimientos posteriores
- **Feedback claro**: Si una tanda no se puede eliminar, el sistema te muestra exactamente qué lotes están comprometidos y por qué.

## Migración de Datos Existentes

### Script de Migración Retroactiva

El script `migrate_tandas_retroactivo.py` agrupa automáticamente los ingresos existentes en tandas basándose en:
- Misma empresa (tenant)
- Mismo día de creación
- Ventana de tiempo de 5 minutos entre lotes
- Detección de origen (Excel o manual)

#### Uso del Script

**Modo DRY RUN (ver qué haría sin aplicar cambios):**
```bash
python migrate_tandas_retroactivo.py
```

**Aplicar migración:**
```bash
python migrate_tandas_retroactivo.py --commit
```

**Revertir migración (eliminar todas las tandas):**
```bash
python migrate_tandas_retroactivo.py --revert --commit
```

## Aplicar Migración de Base de Datos

**IMPORTANTE:** Antes de usar el sistema, debés aplicar la migración de base de datos:

```bash
flask db upgrade
```

Esto creará:
- La tabla `tanda_carga`
- Los campos `tanda_carga_id` en `inventory_lot` e `inventory_movement`
- Los índices necesarios

## API Endpoints

### Listar Tandas
```
GET /inventory/api/tandas-carga
Query params:
  - page (default: 1)
  - per_page (default: 50, max: 200)
  - tipo_origen (excel | manual)
  - estado (activa)
  - fecha_desde (YYYY-MM-DD)
  - fecha_hasta (YYYY-MM-DD)
```

### Detalle de Tanda
```
GET /inventory/api/tandas-carga/<tanda_id>
Returns: tanda info + array de lotes con detalles
```

### Eliminar Tanda
```
DELETE /inventory/api/tandas-carga/<tanda_id>
Validaciones:
  - Rechaza si algún lote tiene ventas
  - Rechaza si algún lote fue consumido parcialmente
  - Elimina lotes, movimientos y egresos asociados
```

## Flujo de Uso Típico

### 1. Importar productos desde Excel
1. Ir a Inventario → Crear ítem → Importar Excel
2. Seleccionar archivo Excel con productos
3. Confirmar importación
4. El sistema crea automáticamente una tanda y asigna todos los lotes

### 2. Revisar tandas
1. Ir a Inventario → Stock ingreso
2. Activar "Agrupar por tandas de carga"
3. Ver lista de tandas con resumen (fecha, cantidad ítems, unidades totales)
4. Clic en una tanda para expandir y ver detalles

### 3. Eliminar tanda incorrecta
1. En la vista agrupada, identificar la tanda a eliminar
2. Clic en botón rojo "Eliminar tanda"
3. Confirmar la acción
4. Si algún lote fue vendido, el sistema rechaza la operación y muestra detalles
5. Si todo está OK, se eliminan todos los lotes de la tanda

## Modelo de Datos

### Tabla `tanda_carga`
```sql
- id (PK)
- company_id (tenant)
- identificador (ej: TANDA-20260310-143052)
- tipo_origen ('excel' | 'manual')
- fecha_hora_creacion
- user_id (FK a user.id)
- cantidad_items (int)
- cantidad_total_unidades (float)
- observacion (text)
- estado ('activa')
- created_at, updated_at
```

### Relaciones
- `inventory_lot.tanda_carga_id` → `tanda_carga.id`
- `inventory_movement.tanda_carga_id` → `tanda_carga.id`

## Notas Importantes

### Integridad de Datos
- ✅ No rompe stock actual
- ✅ No rompe historial de movimientos
- ✅ Respeta integridad entre ventas y lotes
- ✅ Multi-tenant seguro
- ✅ Mantiene filtros existentes

### Performance
- Paginación en lista de tandas (50 items por página)
- Lazy loading de detalles de tanda (solo se cargan al expandir)
- Índices en campos clave (tanda_carga_id, tipo_origen, fecha_hora_creacion)

### Casos de Uso NO Soportados (Por Diseño)
- ❌ No se puede editar una tanda (solo crear y eliminar)
- ❌ No se puede mover lotes entre tandas
- ❌ No se puede eliminar tandas con lotes vendidos o consumidos
- ❌ No se pueden crear tandas manualmente desde UI (solo por importación o script)

### Extensibilidad Futura
El sistema está preparado para:
- Cargas manuales (campo `tipo_origen` permite 'manual')
- Diferentes estados de tanda (campo `estado`)
- Observaciones personalizadas

## Troubleshooting

### "No se puede eliminar esta tanda"
**Causa**: Algún lote de la tanda ya fue vendido o consumido.
**Solución**: No se puede eliminar. Esta validación es intencional para proteger la integridad del stock.

### "Error de conexión al eliminar la tanda"
**Causa**: Problema de red o servidor.
**Solución**: Verificar que el servidor esté corriendo y reintentar.

### No aparecen tandas en la vista
**Causa**: 
1. No se aplicó la migración de BD
2. No hay lotes con tanda asignada
3. Filtros activos

**Solución**:
1. Ejecutar `flask db upgrade`
2. Ejecutar script de migración retroactiva
3. Revisar filtros

## Seguridad

- ✅ Requiere autenticación (`@login_required`)
- ✅ Requiere permiso de módulo inventario (`@module_required('inventory')`)
- ✅ Respeta multi-tenancy (solo ve tandas de su empresa)
- ✅ Validaciones robustas antes de eliminar
- ✅ Rollback automático en caso de error
