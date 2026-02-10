"""
Logic Agent - Converts MySQL stored procedures/functions to PostgreSQL PL/pgSQL.
"""

from langchain_core.messages import HumanMessage

from src.agents.base_agent import BaseAgent
from src.state import MigrationState, MigrationPhase, MigrationStatus, ConvertedProcedure
from src.tools.artifact_manager import get_artifact_manager


class LogicAgent(BaseAgent):
    """
    Agent responsible for converting stored procedures and functions.
    Produces: procedures/*.sql artifacts
    """
    
    def __init__(self):
        super().__init__(
            name="Stored Logic Conversion Agent",
            description="Converts MySQL stored procedures and functions to PostgreSQL PL/pgSQL",
            use_complex_model=True,  # Complex model for code translation
            system_prompt="""You are the Stored Logic Conversion Agent for PostgreSQL 16.

## Your Job
1. Convert MySQL stored procedures to PostgreSQL functions
2. Translate MySQL views to PostgreSQL views
3. Handle parameter modes (IN, OUT, INOUT)
4. Map MySQL functions to PostgreSQL equivalents

## Key Syntax Conversions
- DELIMITER $$ ... $$ → Not needed in PostgreSQL
- DECLARE → Must be in declaration section before BEGIN
- LEAVE → EXIT
- ITERATE → CONTINUE
- SELECT ... INTO → Works the same in PL/pgSQL

## CRITICAL: Table Names (Sakila Database)
- ALL table names are SINGULAR, never plural!
- Use "payment" NOT "payments"
- Use "rental" NOT "rentals"
- Use "staff" NOT "staffs"
- Use "customer" NOT "customers"
- Use "inventory" NOT "inventories"

---

## PostgreSQL 16 View Conversion Examples

### STRING_AGG with DISTINCT (IMPORTANT!)
In PostgreSQL, you CANNOT use DISTINCT inside STRING_AGG with ORDER BY in the same call.

❌ WRONG (MySQL style):
```sql
GROUP_CONCAT(DISTINCT c.name ORDER BY c.name)
```

❌ WRONG (PostgreSQL error):
```sql
STRING_AGG(DISTINCT c.name, ', ' ORDER BY c.name)  -- ERROR!
```

✅ CORRECT (PostgreSQL 16):
```sql
-- Option 1: Use subquery
(SELECT STRING_AGG(DISTINCT name, ', ') FROM category WHERE ...)

-- Option 2: Remove DISTINCT if order matters
STRING_AGG(c.name, ', ' ORDER BY c.name)

-- Option 3: For actor_info view pattern, use array_agg with DISTINCT
array_to_string(ARRAY_AGG(DISTINCT c.name ORDER BY c.name), ', ')
```

### View with Complex Aggregation
```sql
-- PostgreSQL 16 view example
CREATE VIEW actor_info AS
SELECT 
    a.actor_id,
    a.first_name,
    a.last_name,
    STRING_AGG(
        c.name || ': ' || (
            SELECT STRING_AGG(f.title, ', ')
            FROM film f
            WHERE ...
        ),
        '; '  -- No ORDER BY here if you need it inside
    ) AS film_info
FROM actor a
...
GROUP BY a.actor_id, a.first_name, a.last_name;
```

---

## PostgreSQL 16 Procedure/Function Examples

### OUT Parameters with SETOF Returns
When a function has OUT parameters AND returns a set, use this pattern:

❌ WRONG:
```sql
CREATE FUNCTION rewards_report(
    IN min_purchases SMALLINT,
    OUT count_rewardees INTEGER
) RETURNS SETOF customer AS $$
```

✅ CORRECT:
```sql
CREATE FUNCTION rewards_report(
    min_purchases SMALLINT,
    min_amount NUMERIC
) RETURNS TABLE (
    customer_id SMALLINT,
    first_name VARCHAR,
    ...
) AS $$
```

### Function with OUT Parameter Only
```sql
-- For simple OUT parameter, return the type directly
CREATE FUNCTION film_in_stock(
    p_film_id INT,
    p_store_id INT
) RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM inventory
    WHERE film_id = p_film_id
      AND store_id = p_store_id;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;
```

### SETOF Returns (Multiple Rows)
```sql
-- Return multiple rows of a single value
CREATE FUNCTION get_film_ids(p_store_id INT)
RETURNS SETOF INTEGER AS $$
BEGIN
    RETURN QUERY
    SELECT inventory_id FROM inventory WHERE store_id = p_store_id;
END;
$$ LANGUAGE plpgsql;
```

---

## Trigger Conversion

```sql
-- Trigger function (returns trigger)
CREATE OR REPLACE FUNCTION my_trigger_fn()
RETURNS trigger AS $$
BEGIN
    NEW.last_update := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger definition
CREATE TRIGGER my_trigger
BEFORE INSERT OR UPDATE ON my_table
FOR EACH ROW
EXECUTE FUNCTION my_trigger_fn();
```

---

Always return valid PostgreSQL 16 PL/pgSQL code without markdown formatting."""
        )
        self.artifact_manager = get_artifact_manager()
    
    def run(self, state: MigrationState) -> MigrationState:
        """Convert all stored procedures and functions."""
        self.log("Starting stored logic conversion...")
        
        if not state.schema_metadata:
            self.log("No schema metadata found!", "error")
            return state
        
        try:
            schema = state.schema_metadata
            converted_procs: list[ConvertedProcedure] = []
            
            for proc in schema.procedures:
                self.log(f"Converting {proc.type}: {proc.name}")
                
                # Use LLM for conversion
                pg_code, notes = self._convert_procedure(proc)
                
                # Save SQL artifact
                file_path = self.artifact_manager.save_procedure_sql(proc.name, pg_code)
                
                converted = ConvertedProcedure(
                    name=proc.name,
                    procedure_type=proc.type,
                    source_code=proc.source_code,
                    target_code=pg_code,
                    conversion_notes=notes,
                    file_path=str(file_path),
                    status=MigrationStatus.PENDING,
                )
                converted_procs.append(converted)
                
                self.log(f"  ✓ Saved to {file_path}")
            
            # Convert triggers
            for trigger in schema.triggers:
                self.log(f"Converting trigger: {trigger.name}")
                
                pg_code, notes = self._convert_trigger(trigger)
                
                file_path = self.artifact_manager.save_sql(
                    pg_code,
                    f"trigger_{trigger.name}.sql",
                    subdir="procedures",
                    header_comment=f"Trigger: {trigger.name} on {trigger.table_name}"
                )
                
                converted = ConvertedProcedure(
                    name=trigger.name,
                    procedure_type="trigger",
                    source_code=trigger.source_code,
                    target_code=pg_code,
                    conversion_notes=notes,
                    file_path=str(file_path),
                    status=MigrationStatus.PENDING,
                )
                converted_procs.append(converted)
            
            # Save summary
            proc_summary = {
                "procedures": len([p for p in converted_procs if p.procedure_type == "procedure"]),
                "functions": len([p for p in converted_procs if p.procedure_type == "function"]),
                "triggers": len([p for p in converted_procs if p.procedure_type == "trigger"]),
                "conversions": [p.model_dump() for p in converted_procs],
            }
            self.artifact_manager.save_json(proc_summary, "converted_procedures.json")
            
            # Update state
            state.converted_procedures = converted_procs
            state.current_phase = MigrationPhase.LOGIC_CONVERSION
            state.artifact_paths["converted_procedures"] = str(
                self.artifact_manager.artifacts_dir / "converted_procedures.json"
            )
            
            self.log(f"Converted {len(converted_procs)} objects", "success")
            
        except Exception as e:
            self.log(f"Logic conversion failed: {str(e)}", "error")
            state.errors.append({
                "phase": MigrationPhase.LOGIC_CONVERSION,
                "error_type": "conversion_error",
                "error_message": str(e)
            })
        
        return state
    
    def _convert_procedure(self, proc) -> tuple[str, str]:
        """Convert a MySQL procedure/function to PL/pgSQL."""
        prompt = f"""Convert this MySQL {proc.type} to PostgreSQL 16 PL/pgSQL:

```sql
{proc.source_code}
```

## CRITICAL PostgreSQL 16 Rules:

### Parameters: {proc.parameters}
### Return type hint: {proc.return_type or 'void'}

### OUT Parameter Handling (CRITICAL!)
PostgreSQL does NOT support OUT parameters with RETURNS void.

❌ WRONG:
CREATE FUNCTION my_func(IN p_id INT, OUT p_count INT) RETURNS void AS $$

✅ CORRECT - For single OUT value, use RETURNS:
CREATE FUNCTION film_in_stock(p_film_id INT, p_store_id INT)
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM inventory WHERE ...;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

✅ CORRECT - For SETOF with table columns:
CREATE FUNCTION rewards_report(min_purchases SMALLINT, min_amount NUMERIC)
RETURNS TABLE(
    customer_id SMALLINT,
    store_id SMALLINT,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR
) AS $$
BEGIN
    RETURN QUERY SELECT c.customer_id, c.store_id, ... FROM customer c WHERE ...;
END;
$$ LANGUAGE plpgsql;

### Other Rules:
1. Use CREATE OR REPLACE FUNCTION
2. Use $$ delimiters
3. DECLARE section before BEGIN
4. Table names are SINGULAR: payment, rental, customer, inventory

Return ONLY the PostgreSQL code, no explanations or markdown."""

        # Use invoke_with_retry for automatic API key rotation on rate limits
        response = self.invoke_with_retry([HumanMessage(content=prompt)])
        pg_code = self.extract_text_content(response).strip()
        
        # Clean up any markdown code blocks
        if pg_code.startswith("```"):
            lines = pg_code.split("\n")
            pg_code = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
        
        return pg_code, f"Converted {proc.type} to PL/pgSQL function"
    
    def _convert_trigger(self, trigger) -> tuple[str, str]:
        """Convert a MySQL trigger to PostgreSQL."""
        prompt = f"""Convert this MySQL trigger to PostgreSQL:

Trigger: {trigger.name}
Table: {trigger.table_name}
Timing: {trigger.timing}
Event: {trigger.event}
Body:
```sql
{trigger.source_code}
```

Requirements:
1. Create a trigger function first
2. Then create the trigger that calls this function
3. PostgreSQL triggers return TRIGGER type
4. Use NEW/OLD records appropriately

Return ONLY the PostgreSQL code (function + trigger), no explanations."""

        # Use invoke_with_retry for automatic API key rotation on rate limits
        response = self.invoke_with_retry([HumanMessage(content=prompt)])
        pg_code = self.extract_text_content(response).strip()
        
        if pg_code.startswith("```"):
            lines = pg_code.split("\n")
            pg_code = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
        
        return pg_code, f"Converted trigger to PL/pgSQL"
    
    def _generate_fallback(self, proc) -> str:
        """Generate fallback function template."""
        params = ", ".join([
            f"{p.get('name', 'param')} {p.get('type', 'TEXT')}"
            for p in proc.parameters
        ]) if proc.parameters else ""
        
        return f"""-- TODO: Manual conversion required
CREATE OR REPLACE FUNCTION {proc.name}({params})
RETURNS {proc.return_type or 'void'}
LANGUAGE plpgsql
AS $$
BEGIN
    -- Original MySQL code:
    -- {proc.source_code[:200]}...
    RAISE NOTICE 'Function {proc.name} needs manual conversion';
END;
$$;
"""
    
    def _generate_trigger_fallback(self, trigger) -> str:
        """Generate fallback trigger template."""
        return f"""-- TODO: Manual conversion required
CREATE OR REPLACE FUNCTION {trigger.name}_func()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    -- Original trigger code needs manual conversion
    RETURN NEW;
END;
$$;

CREATE TRIGGER {trigger.name}
    {trigger.timing} {trigger.event} ON "{trigger.table_name}"
    FOR EACH ROW
    EXECUTE FUNCTION {trigger.name}_func();
"""


def logic_node(state: dict) -> dict:
    """LangGraph node function for logic conversion."""
    agent = LogicAgent()
    
    if isinstance(state, dict):
        migration_state = MigrationState(**state)
    else:
        migration_state = state
    
    updated_state = agent.run(migration_state)
    return updated_state.model_dump()
