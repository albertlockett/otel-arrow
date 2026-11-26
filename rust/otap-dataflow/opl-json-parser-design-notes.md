# OPL Variables -- Design and Implementation Plan

This document describes adding **variables** to OPL (the OpenTelemetry
Pipeline Language) and evaluating them in the OTAP query engine.

It is written in two halves:

- **Part 1 (human-facing)** -- what variables are, the exact rules, and worked
  examples. This half is the authoritative statement of behaviour and is intended
  to seed updates to the OPL grammar
  ([`opl.pest`](https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/crates/query-engine-languages/src/opl/opl.pest))
  and the OPL user guide
  ([`opl-user-guide`](https://github.com/open-telemetry/otel-arrow/tree/main/rust/otap-dataflow/crates/query-engine-languages/docs/opl-user-guide)).
- **Part 2 (implementation)** -- the end-to-end plan. It names concrete files and
  functions, and calls out the subtle hazards.

---

## Part 1: Variables in OPL (behaviour and rules)

### Overview

A **variable** holds a per-record (column-aligned) value computed earlier
in a pipeline, so it can be reused later without recomputing it and without
writing it into the output data.

Declare a variable with `let`:

```
let var_name = <expr>
```

Then read it by using its name as an identifier in a later expression:

```
logs
| let sev = severity_text
| where sev == "ERROR"
| set body = sev
```

A variable is not written into the output OTAP batch -- it exists only for the
duration of processing one batch and is discarded afterward.

### The rules

#### Declaration and reads

- `let name = <expr>` declares (or re-declares) a variable in the **current**
  pipeline and makes it readable by **later** stages in that pipeline.
- An identifier expression (e.g. `name` in `where name == "x"`) resolves to a
  **variable read** if a variable of that name is in scope at that point;
  otherwise it is a **record-field reference** (the existing behaviour). The
  parser tracks declared names to make this decision.
- A variable may be read anywhere an expression is accepted: the predicate of a
  `where` clause, the target (right-hand side) of a `set`, arithmetic, function
  arguments, etc. Eventually a variable will also be a valid `set` destination
  (left-hand side); see "Not supported in this pass" for why that is deferred.

#### Scope

- A variable is in scope only **after** it is declared, within the pipeline that
  declares it and within that pipeline's nested pipelines.
- A **nested** pipeline (e.g. the body of `apply attributes { ... }`, an `if`/
  `else` branch, or a `fork` branch) can **read** variables declared in an
  enclosing pipeline (**inner-sees-outer**).
- A variable declared **inside** a nested pipeline goes **out of scope** when that
  nested pipeline ends; it cannot be referenced after the nested pipeline closes.
  (Rationale: an outer record maps to *many* inner rows -- a log has many
  attributes -- so an inner value has no single well-defined value at the outer
  scope.)

#### Redefinition and shadowing

- An existing variable may be **re-declared** by a later `let` in the same
  pipeline; the new declaration replaces the old one. Its type may change.
- A nested pipeline may declare a variable with the **same name** as an outer
  variable (**shadowing**). Inside the nested pipeline, the name refers to the
  inner (shadowing) variable. When the nested pipeline ends, the inner binding is
  discarded and the **outer binding is restored** unchanged.

#### `let name = name` (self-reference on the right-hand side)

The right-hand side is resolved **before** the new name is registered. So a
reference to the name being declared resolves to whatever that name means *before*
this `let`:

- **No variable of that name in scope yet**: `let x = x` reads the **record field**
  `x` (or an outer variable `x`, if one is in scope). If `x` is neither a field nor
  an in-scope variable, the right-hand side is an unknown identifier and is
  rejected -- `let x = x` is not special-cased, it fails like any other reference
  to an undefined name.
- **A variable of that name is already in scope**: `let tmp = tmp + 1` reads the
  **existing** variable `tmp`, because `tmp` is already in scope from an earlier
  `let`, and this `let` re-declares it.

This single rule ("register the name after resolving the right-hand side")
produces both behaviours. **Both cases must be covered by unit tests** so the
semantics cannot silently regress.

#### Variable names

- A variable name is an identifier: starts with a letter or `_`, followed by
  letters, digits, or `_` (inherited from the OPL grammar's `ident` rule).
- A name may **not** be a reserved word (all OPL keywords) and may **not** be a
  known function name. These are rejected at parse time.
- A name may **not** shadow a field of the current record type (e.g. naming a
  variable `severity_text` on a logs pipeline). This is rejected at planning time
  (the parser does not know the data model; the planner does). The check runs
  against whatever record type is current at that point, so a name can be legal at
  one level and rejected at a nested level with a different record type.

#### Worked example

```
logs
| let var_x = 5         // var_x = 5
| let var_x = 6         // re-declared; var_x = 6
| apply attributes {
    let var_y = 7                 // inner variable, attributes scope
    | where var_x + var_y > 10    // OK: outer var_x readable inside
    | let var_x = 8               // shadows outer var_x for this block only
    // end of block: var_y dropped; the shadowing var_x is discarded
  }
| where var_x == 6      // true: outer var_x is unchanged (still 6)
| where var_y > 10      // INVALID: var_y is out of scope here
```

### Not supported in this pass

#### Variable mutation (`set <var> = ...`)

You can **re-declare** a variable with `let`, but you cannot **mutate** one with
`set`. Attempting `set x = ...` where `x` is a declared variable is rejected at
parse time, with a message that points you at `let`.

This *will* be supported eventually, but it is deliberately out of this pass
because it is a different and harder feature than redefinition:

- Redefinition is lexical: a new `let` creates a binding in the current scope,
  and nested bindings vanish when the scope closes.
- Mutation assigns to the **existing** binding wherever it was declared -- it must
  *reach up* the scope chain and persist, the opposite of a nested shadow.

Consider:

```
logs
| let filter_threshold = 10          // default
| if (resource.attributes["service.name"] == "critical_service") {
    set filter_threshold = 5         // must update the OUTER binding...
  }
| where severity_number > filter_threshold
```

The `if` runs on a **filtered subset** of records, so after the branches merge,
`filter_threshold` must be a **per-record** value: 5 for records that took the
branch, 10 for the rest. So mutation inside a conditional **overwrites only some
rows** of a variable declared in an outer scope, and that change must **persist**
past the branch (unlike a shadow, which is discarded). It also raises a type
question:

```
logs
| let x = null
| if (severity_number > 5) { set x = "string" } else { set x = 5 }
// what is the type of x here? string | int?
```

Supporting mutation therefore needs a way to assign to a binding in an outer
scope, a way to combine the per-branch changes into a single per-record value, and
a rule for reconciling types across branches. All of that can be future work; see
Part 2's "Future: variable mutation" for the sketch.

#### Assigning a variable into a destination with a one-to-many relationship

This is not allowed, and the planner must actively reject it, because the result
is ambiguous:

```
logs
| let v = severity_text                       // v holds one value per log
| set resource.attributes["x"] = v            // INVALID: one resource, many logs
```

A single resource owns **many** logs, each with its own `severity_text`. The
destination `resource.attributes["x"]` has one slot per resource, but `v` supplies
a different value for each of that resource's logs, so there is no single
well-defined value to store. Whenever the value being assigned has **more than one
value per destination slot**, the assignment is ambiguous and is rejected at
planning time. (A variable is what makes this reachable: it can carry a per-log
value into a `set` whose destination is per-resource; see Part 2 section B.3.)

---

# Part 2: Implementation plan

## Where things stand (exists vs. missing)

- **Expression AST (`rust/contrib/data_engine/expressions`)**: COMPLETE, no
  changes needed.
  - `ScalarExpression::Variable(VariableScalarExpression { name, accessor })` -- reads.
  - `MutableValueExpression::Variable(VariableScalarExpression)` -- writes (a valid
    `set` destination alongside `Source` and `Argument`).
  - Documented AST semantics: variables are scoped to the execution of a query for a
    given record; each run starts with no variables defined; they are not shared
    across runs. This matches our per-batch model.
- **OPL parser (`crates/query-engine-languages/src/opl`)**: no `let` syntax; needs
  a grammar rule, AST wiring, declared-name tracking, and read-side resolution.
- **query-engine planner (`crates/query-engine/src/pipeline`)**: rejects variable
  reads (`expr/planner.rs::plan_scalar` has no `Variable` arm) and variable
  `set`-destinations (`planner.rs::plan_sets` rejects non-`Source` destinations).
- **query-engine execution**: no variable storage. `ExecutionState` is the home.

## How it will work end-to-end (the shape, before the detail)

1. **Parser** tracks declared variable names per pipeline level (not globally),
   so a bare identifier is resolved to either a variable read or a field read, and
   emits the existing AST (`Set` with a `Variable` destination for `let`;
   `ScalarExpression::Variable` for reads).
2. **Planner** turns a `let` into a dedicated `AssignVariablePipelineStage` (a
   **new** pipeline stage introduced by this work; it must not touch the output
   batch), and turns a variable read into a **new** `ScopedExpr::VariableRead`
   variant. It enforces the field-shadow and 1:many rules.
3. **Runtime** stores variables in `ExecutionState` as a **scope stack** of
   id-carrying values. Reads **lazily realign** a stored value to the batch shape
   at the read site, by id, with a cheap pointer-equality fast-path.

The part to be most cautious about is runtime alignment (step 3): a stored
variable outlives the batch it was computed on, so a later `where`/branch can
reshape the rows out from under it. Getting this wrong produces silently incorrect
results rather than an error. Section C.3 (the "EqualScope hazard") covers it in
full.

## Part A -- OPL front-end (`crates/query-engine-languages/src/opl`)

Files: `opl.pest`, `parser/assignment.rs` (or a new `let` stage parser),
`parser/operator.rs`, `parser/pipeline.rs` (builder trait + name tracking),
`parser/expression.rs` (read-side resolution).

### A.0 Where variable tracking lives: the `PipelineBuilder`, NOT `ParserState`

Variable names are tracked **per builder level**, reusing the existing builder
nesting (an `InnerPipelineBuilder` is created exactly when descending into a
nested pipeline such as `apply` / if-else / fork). `ParserState` is left unchanged
(it tracks functions only).

Why not `ParserState`: functions are effectively global and `InnerPipelineBuilder`
forwards `push_function_definition` to the parent (hoisted). Variables are
lexically scoped; putting them on the single `ParserState` would flatten the
hierarchy and leak inner declarations outward.

Why not reuse `ParserScope` / `ParserStateScope` (the shared KQL mechanism): its
`is_local_variable_defined` checks only the scope's own local set and does **not**
walk to the parent's locals (only globals/constants/functions are shared by ref),
so an outer `let` would be invisible inside a nested block -- the opposite of our
inner-sees-outer requirement. It also bakes in a global/local split and a
no-shadowing rule (KQL `KS201`). KQL gets away with this only because its top-level
`let`s are folded into globals/constants (we deliberately do not fold). Reusing it
would mean either adopting KQL's fold-to-global model or modifying the shared crate
and perturbing KQL. Decision: keep our own builder-level name sets with a
local-then-parent walk, self-contained in the OPL crate.

Extend the `PipelineBuilder` trait (mirrors the function methods, different
delegation):

- `declare_variable(&mut self, name: &str)` -- registers the name on the
  **current** level only. Idempotent (re-declaration is fine).
  `InnerPipelineBuilder` does **NOT** forward this to the parent (unlike
  `push_function_definition`); that is what keeps inner declarations from leaking
  outward.
- `is_variable(&self, name: &str) -> bool` -- checks the local set, then delegates
  to the parent (`self.parent.is_variable(name)`), giving **inner-sees-outer**
  resolution. Same delegation shape as the existing `get_function`.

Each of `RootPipelineBuilder` and `InnerPipelineBuilder` holds its own set of
locally-declared names (a `HashSet<String>` / `BTreeSet<String>` -- ordered vs.
hashed does not matter; pick one). Root has no parent to delegate to.

The parser tracks only **names in scope** -- it has no notion of types or values.
Type and value resolution happens later, in the planner and at runtime.

### A.1 Grammar (`opl.pest`)

- Add `let_operator_call = { "let" ~ identifier_expression ~ "=" ~ expression }`.
- Add `let_operator_call` as a choice in the `operator_call` rule (the `|`-separated
  list of operators).
- OPL has **no** reserved-word rule today; keywords (`where`, `set`, `and`, `or`,
  `not`, `is`, `as`, `true`, `false`, `null`, etc.) are matched positionally and
  are not excluded from `ident`. So `let and = 1` currently lexes `and` as a valid
  identifier. Reserved-word rejection is therefore done in the parser (A.2), not
  the grammar, for simpler, better diagnostics.

### A.2 Variable name validation (parser-enforced)

At `declare_variable` time (in the `let` stage parser), reject a proposed name
that collides with either of the following, with a clear `ParserError`:

- **Reserved words: reject ALL keywords.** This covers both expression-level
  keywords (`and`, `or`, `not`, `is`, `as`, `true`, `false`, `null`) and operator
  keywords (`let`, `where`, `set`, `extend`, `apply`, `fork`, `rename`, `remove`,
  `project-away`, `drop`, `route_to`, `scale_metric`, `if`, `else`). Maintain an
  explicit reserved-word set in the OPL crate and keep it in sync with the grammar.
- **Function names**: if the proposed name resolves to a known function
  (`PipelineBuilder::get_function` / the function table), reject it. Functions are
  global/hoisted, so consult the function table regardless of nesting level.

Field-name shadowing (e.g. a variable named `severity_text`) is **not** checked
here -- the parser has no reference to the OTel data model. That check lives in the
planner (B.1).

### A.3 `let` stage parser

- Validate the name per A.2 at declaration.
- Parse the right-hand side `expression` exactly like `set` does, **before**
  calling `declare_variable(name)` on the current builder. This ordering is what
  makes `let name = name` behave correctly (Part 1 "self-reference"):
  - No variable of that name in scope yet: in `let x = x`, `x` is not yet a
    variable at this level, so the RHS reads the field (or an outer `x`); if `x` is
    neither, the RHS is an unknown identifier and fails downstream.
  - Re-declaration: in `let tmp = tmp`, `tmp` is already in scope, so the RHS reads
    the existing variable.
- Emit `DataExpression::Transform(TransformExpression::Set(...))` with the
  destination
  `MutableValueExpression::Variable(VariableScalarExpression::new(loc, name, ValueAccessor::new()))`.
  (Reuses the existing `Set` AST node; only the destination variant differs.)

### A.4 Variable reads (`parser/expression.rs`)

- The primary decision point is `parse_primitive_expression` (currently ~line
  786), where a bare `identifier_expression` unconditionally becomes a
  `SourceScalarExpression`. Branch on `is_variable(name)`:
  - in scope as a variable -> `ScalarExpression::Variable(...)`
  - otherwise -> `ScalarExpression::Source(...)` (unchanged)
- Apply the same leading-identifier check at the two other spots that start a
  dotted/bracketed access from a bare identifier, so `tmp.foo` / `tmp["x"]` read
  into the variable:
  - `parse_index_expression` (~line 666)
  - `parse_attribute_selection_expression` (~line 705)
  Carry any trailing accessor path into the `Variable`'s `ValueAccessor`.
- Because `where`, `set` RHS, math, and function args all route through
  `parse_expression -> parse_primitive_expression`, reads work uniformly across
  operators with no per-operator changes.

### A.5 Variable mutation -> parser rejection (this pass)

Mutating a variable (`set <var> = ...`) is not supported this pass and is rejected
in the parser, because that is the only layer that both resolves the `set`
destination and knows which names are variables (`is_variable`).

Today `parse_assignment_expression` (`parser/assignment.rs`) always builds the
`set` destination (its `left`) as a `SourceScalarExpression` -- it never consults
`is_variable` for the destination. Add that check: when the `set` destination is a
bare `identifier_expression` whose name `is_variable`, reject with
`ParserError::SyntaxNotSupported` (the parser's existing "not supported" variant --
the parser has NO `NotYetSupported` variant; that one belongs to the query-engine
`Error` type, not `ParserError`).

Message e.g.: "'filter_threshold' is a variable; use `let` to redefine it.
Mutating a variable (assignment that persists across scopes) is not yet supported."
See "Future: variable mutation" for the eventual design. When mutation is
implemented, this rejection is replaced by building a `Variable` destination (the
mirror of A.3's `let`) and handling it in the planner.

## Part B -- query-engine planner (`crates/query-engine/src/pipeline`)

Files: `planner.rs` (`plan_sets`), `expr/planner.rs` (`plan_scalar`), `expr.rs`
(new `ScopedExpr::VariableRead` variant), `assign.rs` (1:many cardinality walks),
new stage module.

### B.0 Variable writes -> a separate `AssignVariablePipelineStage`

In `planner.rs::plan_sets` (~line 1008), the current
`let MutableValueExpression::Source(dest) = ... else { NotYetSupported }`
hard-rejects non-`Source` destinations. Add a `MutableValueExpression::Variable`
arm that emits a **separate `AssignVariablePipelineStage`**, NOT an overload of
`AssignPipelineStage`. Reasons grounded in the current code:

- `AssignPipelineStage`'s `Assignment` is built around `dest_column: ColumnAccessor`
  (assign.rs ~91-100); a variable has no column.
- `AssignPipelineStage` batches consecutive column sets and reasons about
  referenced columns via `check_combine` / `cols_or_keys_referenced` for Arrow
  efficiency; a variable write has no column dest and must not participate.
- `AssignPipelineStage` mutates the OTAP record batch; a variable write must not
  touch the batch.

Steps for the arm:
1. Run the field-shadow check (B.1).
2. Plan the RHS via `ExprPlanner::plan_scalar`.
3. **Flush any pending column assignments first** (mirror the existing
   nested-pipeline flush at planner.rs ~1038-1044: build an `AssignPipelineStage`
   from the batched assignments, push it, clear `assignments` +
   `cols_or_keys_referenced`), THEN push the `AssignVariablePipelineStage`. This
   preserves left-to-right order when `let` writes interleave with column `set`s
   (a later `set` may read the variable; the variable RHS may read a just-set
   column).
   - TODO (optimization): this flush is conservative. If no later `set` whose
     assignments are still batched actually references the variable being set (and
     the variable's RHS does not reference a not-yet-flushed column), the flush is
     unnecessary and more column assignments could be coalesced into a single
     `AssignPipelineStage` than this always-flush scheme produces. A later pass can
     check referenced columns/variables (the machinery already exists in
     `cols_or_keys_referenced`) and only flush when there is a real dependency.

### B.1 Field-name shadowing check (planner-enforced, strict)

The planner is the only layer that knows the current record's valid fields -- it
carries `self.record_type: RecordType`, used throughout `plan_sets` (e.g.
`ColumnAccessor::try_from_value_accessor(..., &self.record_type)`).

Rule (strict reject everywhere): when planning a variable-destination `Set`, before
registering the variable, attempt to resolve the name against the current
`record_type`'s known columns/fields. If it resolves to a real field, reject
(e.g. "variable name 'severity_text' conflicts with a field of Log records").
Strict-everywhere is chosen because it is easy to loosen later without breaking
valid programs, whereas tightening later would break them.

The valid-field set narrows mid-pipeline (after `is Gauge`, or inside
`apply attributes { ... }` where `record_type` becomes `Attributes`). Because the
planner already re-plans nested pipelines with a fresh `record_type`
(`Self::new(nested_pipeline_record_type, ...)`), the check naturally runs against
the correct model at each level.

### B.2 Variable reads (`expr/planner.rs::plan_scalar`)

- Add a `ScalarExpression::Variable(_)` arm producing a **new `ScopedExpr` variant**
  -- `ScopedExpr::VariableRead { name }`. NOT a `LeafEval` variant: `LeafEval` is
  entangled with `DatafusionExpr`/projection/`BatchEval` (the `JoinAndEval` path
  destructures it as `DatafusionExpr`, see `eval.rs::join_and_eval_value` ~line
  458), and a variable read has no RecordBatch and no DataFusion column.
- Planner-time error if the referenced variable name was never declared (defensive;
  the parser already resolved names, but validate here too for good diagnostics).

### B.3 The 1:many assignment guard (reuse the existing cardinality walk)

This guard already exists and already does exactly what Part 1's 1:many rule needs
-- we only have to teach it about the new leaf. `validate_assign` (assign.rs:2144,
called from `AssignPipelineStage::try_new`) calls two recursive `ScopedExpr`-tree
walks that read each leaf's `DataScope` and reject a source whose scope is **finer**
than the destination:

- `validate_expr_cardinality` (assign.rs:2265) -- for attribute destinations.
  Its docstring example is literally `resource.attributes["x"] = event_name`.
- `validate_struct_col_assign_cardinality` (assign.rs:2366) -- for resource/scope
  struct-field destinations.

Both walks currently match only `ScopedExpr::Eval`, `JoinAndEval`, and `Bitmap*`.
**Add a `ScopedExpr::VariableRead` arm to both**, contributing the variable's
**stored provenance `DataScope`** exactly as an `Eval` leaf contributes its own.
Because a variable carries its provenance scope explicitly (the stored
`DataScope`, see C.1), a record-scoped variable assigned into a coarser
(resource/scope) destination is detected and rejected here -- this is the enforcement
point for Part 1's permanently-disallowed 1:many case.

(Omitting these arms is a Rust compiler error -- the match is exhaustive -- so the
build will force the implementer to handle it; the correct handling is to surface
the provenance scope, not to default it to something permissive.)

## Part C -- Execution (`crates/query-engine/src/pipeline`)

Files: `state.rs`, new `assign_variable.rs` (or similar), `expr.rs` (new
`ScopedExpr::VariableRead`), `expr/eval.rs` (realign-on-read leaf), `expr/join.rs`
(expose id-gather primitives / a `realign_by_id` helper + a `ptr_eq` fast-path),
`apply.rs` / `conditional.rs` / `fork.rs` (scope push/pop around child-stage loops).

### C.0 Mutability model (confirmed from source)

`PipelineStage::execute` receives `exec_state: &mut ExecutionState`
(pipeline.rs:91), and `execute_with_state` threads a single `&mut ExecutionState`
sequentially through every stage (pipeline.rs:491-502). So the **write side** has
direct `&mut` access (insert / push / pop), and the **read side** borrows
`&ExecutionState` shared via `EvalContext` (eval.rs:70,119) during a stage's own
`execute`. No interior mutability / `RefCell` needed.

### C.1 Variable storage in `ExecutionState` (`state.rs`)

Add a first-class **scope stack**:
`variables: Vec<HashMap<String, StoredVariable>>` (innermost frame on top), where
`StoredVariable` holds the evaluated value as captured at `let` time: `values`
(`ColumnarValue`), `scope` (`DataScope`), and the **definition-time id
`ArrayRef`(s)** (`ids` / `parent_ids`). The ids are the self-describing
"id -> value" key used to realign on read (C.3).

Accessors:

- `set_variable(name, stored)` -- insert into the **top** frame (declaration /
  re-declaration / shadow).
- `get_variable(name)` -- walk frames **top-down**, return the first match
  (inner-sees-outer).
- `push_scope()` / `pop_scope()` -- enter/leave a nested pipeline. These govern
  **visibility only** (shadow-restore on pop). They do **not** realign anything;
  alignment is lazy, at read (C.3).

Clear per batch: a `reset`/clear path consistent with `reset_counters`; a reused
`ExecutionState` across batches must start with a single empty root frame.

Perf note (pinned, deferred): names-as-`String` keys in a per-frame `HashMap` are
fine for now -- no worse than Arrow carrying field names in a schema. A later
optimisation can pre-scan the program for all `let` names and index variables into
a sized `Vec` slot. Do **not** bake slot indices into the AST now.

### C.2 New `AssignVariablePipelineStage { name, source: ScopedExpr }`

- `execute`: evaluate `source` via `EvalContext` to a `ScopedValue`, capture its
  ids, `exec_state.set_variable(name, stored)`, return the OTAP batch
  **unchanged**.
- `parent_behavior()` -> `ParentBehavior::Preserves` (no record/parent change).
- Also implement `execute_on_attributes` and `execute_on_metric_data_points` (and
  `supports_exec_on` for those scopes) so `let` works at any nesting level (inside
  `apply attributes { ... }`, inside data-point pipelines).

### C.3 Read alignment -- the EqualScope hazard and the lazy fix

**The hazard (a silent-wrong-answer bug if done naively).**
`compute_join_alignment` (join.rs:285) takes a **positional** fast-path --
`JoinAlignment::EqualScope`, "no reordering needed" -- whenever
`left.data_scope == right.data_scope`, and it never consults ids in that case.
This is safe everywhere today because two same-scope values always come from the
same batch instantiation. A stored variable breaks that invariant: it outlives the
batch it was computed on. **Any row-reshaping stage between the `let` and the
read** -- a top-level `where`, a conditional branch's `filter_otap_batch`
(conditional.rs:312-313), etc. -- leaves the stored column at the old length/order
while the current batch is a reordered subset. Same `DataScope`, different rows ->
the positional path reads the **wrong rows** (or panics on a length mismatch).

Example that is NOT a conditional (shows it is the general case):

```
logs
| let var_x = severity_text              // captured over all rows
| where event_name == "foo"              // reshapes the root batch
| set attributes["x"] = concat(severity_text, var_x)   // var_x now misaligned
```

**Rejected approach: eager re-filter at every reshaping boundary.** Correct but
wasteful -- it realigns every live variable at every `where`/branch entry even if
the variable is never read again, and front-loads work the lazy path does once, on
demand. In the example, `var_x` is read once, far downstream.

**Chosen approach: lazy realign-on-read, by id, with an `Arc::ptr_eq` fast-path.**
The `VariableRead` leaf, at eval time, fetches the `StoredVariable` from
`eval_ctx.state()` and produces a `ScopedValue` aligned to the reader's current
batch:

- **Fast path (common case -- nothing reshaped the rows between `let` and read):**
  `Arc::ptr_eq(stored_ids, current_id_col)`. An untouched pass-through keeps the
  same id `Arc`, so this is an O(1) identity check (no column scan). On a match,
  hand back the stored `ScopedValue` unchanged.
  - Soundness: `ptr_eq` is *sufficient* but not *necessary* for "aligned". A false
    "not equal" (different `Arc`, value-identical) just triggers a gather that
    reproduces the identical result -- never a wrong answer, only an occasionally
    unnecessary gather.
- **Slow path (same-scope, rows reshaped):** id-keyed gather, reusing the existing
  join primitives in `join.rs`:
  1. `U16IdJoinLookup::try_new_from_array(&stored_ids)` (u16 for root/signal, u32
     for child records) -- maps `stored_id -> stored_row_index`.
  2. `try_build_simple_join_ids(current_id_col, &stored_lookup)` -> `Int32Array`
     take-indices (null where a current id was absent at definition time, e.g. a
     row that did not exist when the `let` ran -- correctly yields null).
  3. `take(&stored_values, &indices, None)` -> a new array of current-batch
     length/order; wrap as `ScopedValue` with the current scope + current ids.
  These primitives already handle primitive/dict id encodings and null ids. Expose
  them -- or better, a thin `pub(crate) fn realign_by_id(stored_values, stored_ids,
  target_ids)` wrapper in `join.rs` that also hosts the `ptr_eq` fast-path -- since
  they are currently private.
- **Cross-scope read (e.g. a `Record` variable read inside `apply attributes` --
  the 1:many log -> attributes case):** NOTHING new needed. The read is `Record`
  vs `Attribute`, so `compute_join_alignment` takes the existing
  `RecordToAttributesJoin` path (join.rs:323), not the positional fast-path, and
  realigns by id -- provided the stored variable carries its root ids (it does).
  This is the same id-join the engine already does for
  `severity_number + attributes["x"]`.

The realigned value is genuinely positionally aligned to the reader's batch, so
when it reaches `compute_join_alignment` and hits `EqualScope` (join.rs:285), that
path is **correct** -- we restored its precondition at the leaf instead of poking a
hole in the join. The leaf's `execute_as_id_mask` can reuse the existing
"evaluate as value then convert" path the `bitmap` module already uses.

### C.4 Runtime scoping hooks (push/pop around child-stage loops)

The scope stack (C.1) governs visibility; `push_scope`/`pop_scope` wrap each
**child-stage loop** in the nesting stages. Confirmed loops (all drive children
with the same `&mut ExecutionState`):

- `ApplyPipelineStage` (`apply.rs`): attributes loop (87-97) and data-point loop
  (115-127).
- `ConditionalPipelineStage` (`conditional.rs`): per-branch + default loops in
  `execute` (315-325, 357-368), `execute_on_attributes` (476-486, 497-509), and
  `execute_on_metric_data_points` (640-650, 694-706).
- `fork.rs`: its branch loops (symmetric; confirm during implementation).

Subtleties the code makes concrete:

- Conditional branches are **siblings, not nesting**: they run sequentially against
  the same `&mut ExecutionState`. Each branch gets its **own**
  `push_scope`/`pop_scope` (NOT one push around the whole conditional), so branch B
  does not see a `let` declared in branch A -- matching the parser side where each
  branch is its own `InnerPipelineBuilder`.
- Do **NOT** reuse `init_state_for_conditional_branch` /
  `clear_state_for_conditional_branch` (conditional.rs:232-241, 386-395) for
  variable push/pop. Those fire **once around the whole conditional, before
  filtering**, specifically to capture *cross-branch* state from the *unfiltered*
  batch -- the opposite of per-branch, filtered variable scoping.

## Future: variable mutation (design sketch, not this pass)

When mutation (`set <var> = ...`) is implemented, it is a distinct operation from
`let` re-declaration:

- **Assign to the declaring frame, not the top frame.** Add a
  `get_variable_frame_mut(name)` that walks the scope chain to the frame where the
  name was declared and writes there, so the change persists past a nested scope
  (opposite of a shadow).
- **Per-branch id-keyed scatter + merge at branch join.** A `set <var>` inside a
  conditional branch runs on a filtered subset; the post-branch value is the
  id-wise merge of (unchanged rows) + (each branch's new slice). This reuses the
  same id-alignment machinery as reads, now on the write path.
- **Type reconciliation across branches.** First cut: reject conflicting types
  (e.g. `string` in one branch, `int` in another). Widening to a union / AnyValue
  type is a later enhancement. This also answers "should uninitialized variables
  have an explicit type" -- revisit alongside widening.

Nothing in this pass may assume a variable value is *immutable after declaration*
in a way that blocks this. In practice this is already satisfied: storing
id-carrying `ScopedValue`s supports scatter, and we deliberately do **not**
constant-fold a `let` RHS (unlike KQL), so a real runtime value always exists.

## Testing

Each test gets `/// Scenario:` and `/// Guarantees:` doc comments; tests must pass
independently and in any order (no retries).

OPL parser unit tests:

- `let x = <expr>` produces a `Set` transform with a `Variable` destination.
- A later bare reference to `x` parses as a `Variable` read; an undeclared bare
  name still parses as a record-field `Source`.
- Self-reference: in `let x = x`, the RHS `x` parses as a `Source` (field), while
  `let tmp = "5" | let tmp = tmp` parses the second RHS `tmp` as a `Variable` read
  (re-declaration sees the existing variable). (Both are required by the
  "self-reference" rule.)
- Inner-sees-outer: inside a nested pipeline (e.g. `apply`), a reference to an
  enclosing variable parses as a `Variable` read.
- Inner-invisible-outside: a variable declared only inside a nested pipeline is NOT
  in scope after it closes (a later bare reference parses as a `Source`).
- Shadowing: a `let` inside a nested pipeline reusing an outer name resolves to a
  `Variable` read inside, and the outer binding is still in scope after.
- Name validation: `let and = 1` / `let where = 1` rejected; `let` reusing a known
  function name rejected.
- Mutation: `logs | let x = 1 | set x = 2` is rejected with
  `ParserError::SyntaxNotSupported`, pointing at `let` (A.5).

Planner validation tests:

- Field-shadowing: `logs | let severity_text = 1` is rejected (strict), and the
  same name is still rejected inside a narrowed/nested scope against that scope's
  record type.
- 1:many assignment: `logs | let v = severity_text | set resource.attributes["x"] = v`
  is rejected, naming the variable's per-record provenance against the
  per-resource destination.

Planner/execution integration tests (pattern: `exec_logs_pipeline` in
`pipeline.rs`):

- `let tmp = <col>` then `where tmp == ...` and `set <field> = tmp`; assert the
  variable column is NOT present in the output batch; assert per-record correctness
  and reuse across stages.
- **Alignment after a top-level `where`** (the headline hazard):
  `logs | let v = severity_text | where event_name == "foo" | set body = v` --
  assert each surviving record's `body` equals ITS OWN captured `v`, not a
  misaligned neighbour. Fails loudly if the lazy id-gather is missing.
- **Alignment inside a conditional branch**: a variable declared before an `if`,
  read inside a branch whose condition filters/reorders rows -- assert per-record
  correctness within the branch, and that sibling branches do not see each other's
  `let`s.
- **Cross-scope 1:many read**: a `Record`-scoped variable declared on logs, read
  inside `apply attributes { ... }`, broadcasts correctly to each attribute row of
  its owning record.
- **`ptr_eq` fast-path**: a `let` immediately followed by a read with no reshaping
  in between produces correct values (optionally assert array reuse to confirm the
  fast-path was taken).

Error cases: reading an undeclared variable; unsupported RHS for `let`.

## Validation commands (per AGENTS.md)

- After Rust edits: `cd rust/otap-dataflow && cargo check -p <crate>` for the
  affected crates (`otel-arrow-dfe-query-engine-languages`,
  `otel-arrow-dfe-query-engine`).
- Iterative: `cd rust/otap-dataflow && cargo xtask quick-check`.
- Final: `cd rust/otap-dataflow && cargo xtask check`.
- ASCII-only Rust source; run `python3 tools/sanitycheck.py` after any
  Markdown/YAML/Python changes.
- Add a `.chloggen` entry (`enhancement` or `new_component`) since this is
  user-facing (new OPL `let` capability).
- Update the OPL spec and user guide from Part 1 (the rules there are written to be
  lifted into user-facing docs).

## KQL comparison (reference; contrib `kql-parser`)

KQL already supports `let` and is a useful reference, but differs:

- **Syntax**: KQL `let` is a top-level `;`-separated prologue statement
  (`let x = ...; T | where c == x`), not a piped stage. OPL's `let` is a pipeline
  stage. (grammar: `kql-parser/src/kql.pest`, `variable_definition_expression`.)
- **Write AST**: KQL's `parse_variable_definition_expression`
  (`kql-parser/src/shared_expressions.rs`) builds exactly the AST we reuse -- `Set`
  with `MutableValueExpression::Variable`. Validates our write shape.
- **Folding**: at the top level KQL rewrites that `Set` into a pipeline `constant`
  (if statically foldable) or a `SetGlobalVariable` initialization
  (`kql-parser/src/query_expression.rs`). The raw `Set`/`Variable` only survives
  **inside UDF bodies**. We do NOT fold -- our `let` stays a runtime `Set` with a
  `Variable` destination so it can be a per-record column.
- **Scoping**: KQL top-level `let`s share a flat namespace and forbid
  redefinition/shadowing (`KS201`). Nested scopes exist only for UDF bodies /
  `summarize`, and local lookups do not walk to parent locals. We allow
  shadowing/redefinition and need inner-sees-outer-locals (A.0).
- **Closest precedent for us**: the KQL UDF-body path
  (`query_expression.rs` ~204-224) emits a surviving `Set` with a `Variable`
  destination plus `ScalarExpression::Variable` reads -- the path most like our
  OPL plan. Its UDF test (`let f = (){ let a = 'hello world'; a }`) asserts both a
  `Variable` write and read.

## Out of scope (first pass)

- KQL `let` wiring (the AST already supports it; can follow later).
- Program-level globals (`PipelineInitialization::SetGlobalVariable`) beyond what
  already exists.
- Variable mutation (`set <var> = ...`) -- deferred; see "Future: variable
  mutation".
