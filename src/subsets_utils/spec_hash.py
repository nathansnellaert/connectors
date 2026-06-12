"""Per-spec code hashing via AST walk.

Pure stdlib utility. `compute_spec_hash()` reads a node module's source file
without importing it, walks the AST of a target function plus transitively
reachable connector-local code, applies cosmetic-resilience canonicalization
(strip docstrings, normalize string literals, sort module-level constants,
alpha-rename locals + parameters), and returns
a sha256 hex digest. None on parse failure — caller treats as force re-run.

The hash is a function of:
  - the target FunctionDef's body (Tier-1 + Tier-2 canonicalized)
  - every same-module top-level def/const transitively reachable from it
  - every connector-local imported def reached via fence-dir resolution
  - external imports (subsets_utils, requests, stdlib) → leaf tokens by module name

Cosmetic edits (whitespace, comments, docstrings, local-var renames) do not
change the hash. Behavioral edits (signature change, helper edit, constant
value change) do.

Used by:
  - the harness ImplementStep pre-spawn to write expected_hashes.json
  - the orchestrator post-success to stamp _metadata.code_hash
Both layers call this same pure function so the values agree by construction.
"""
from __future__ import annotations

import ast
import copy
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


# =============================================================================
# Tier-1: strip docstrings
# =============================================================================

class _StripDocstrings(ast.NodeTransformer):
    """Drop the leading docstring Expr from each function / class / module body.

    A docstring is a bare string literal at the start of the body — its only
    runtime effect is to populate __doc__, which has no behavioral impact on
    fetch logic.
    """

    def _strip(self, node):
        body = getattr(node, "body", None)
        if (body and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)):
            node.body = body[1:] or [ast.Pass()]
        return node

    def visit_FunctionDef(self, node):
        self.generic_visit(node)
        return self._strip(node)

    def visit_AsyncFunctionDef(self, node):
        self.generic_visit(node)
        return self._strip(node)

    def visit_ClassDef(self, node):
        self.generic_visit(node)
        return self._strip(node)

    def visit_Module(self, node):
        self.generic_visit(node)
        return self._strip(node)


# =============================================================================
# Tier-2: alpha-rename locals + parameters within each FunctionDef
# =============================================================================

class _AlphaRenameLocals(ast.NodeTransformer):
    """Rename a function's parameters and locally-bound names to canonical
    _v0, _v1, ... in deterministic order. Preserves references to globals,
    imports, and builtins (any name not bound inside the function).

    This makes `def f(row): return row.x` and `def f(record): return record.x`
    hash identically while still distinguishing semantic edits.
    """

    def _rename_function(self, node):
        bound: list[str] = []
        seen: set[str] = set()

        def add(name):
            if name and name not in seen:
                seen.add(name)
                bound.append(name)

        # Parameters in deterministic order
        for a in (list(getattr(node.args, "posonlyargs", []))
                  + list(node.args.args)
                  + list(getattr(node.args, "kwonlyargs", []))):
            add(a.arg)
        if node.args.vararg:
            add(node.args.vararg.arg)
        if node.args.kwarg:
            add(node.args.kwarg.arg)

        # Locally-assigned names in the body (Store context)
        for stmt in node.body:
            for sub in ast.walk(stmt):
                if isinstance(sub, ast.Name) and isinstance(sub.ctx, (ast.Store, ast.Del)):
                    add(sub.id)
                elif isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    # Inner def's name is bound in the enclosing scope
                    add(sub.name)
                elif isinstance(sub, ast.arg):
                    # Nested fn parameters — those get handled when we recurse
                    # into the nested fn via generic_visit below
                    pass

        rename = {orig: f"_v{i}" for i, orig in enumerate(bound)}

        # Rewrite parameter names
        for a in (list(getattr(node.args, "posonlyargs", []))
                  + list(node.args.args)
                  + list(getattr(node.args, "kwonlyargs", []))):
            if a.arg in rename:
                a.arg = rename[a.arg]
        if node.args.vararg and node.args.vararg.arg in rename:
            node.args.vararg.arg = rename[node.args.vararg.arg]
        if node.args.kwarg and node.args.kwarg.arg in rename:
            node.args.kwarg.arg = rename[node.args.kwarg.arg]

        # Rewrite Name nodes in body and inner-def names
        class _RewriteNames(ast.NodeTransformer):
            def visit_Name(self, n):
                if n.id in rename:
                    return ast.copy_location(
                        ast.Name(id=rename[n.id], ctx=n.ctx), n
                    )
                return n

            def visit_FunctionDef(self, n):
                if n.name in rename:
                    n.name = rename[n.name]
                # Recurse — but inner fns get their own _AlphaRenameLocals pass
                # via the outer transformer's generic_visit
                return n

            visit_AsyncFunctionDef = visit_FunctionDef
            visit_ClassDef = visit_FunctionDef

        node.body = [_RewriteNames().visit(s) for s in node.body]
        return node

    def visit_FunctionDef(self, node):
        # Process inner fns first (so their locals get renamed independently)
        self.generic_visit(node)
        return self._rename_function(node)

    visit_AsyncFunctionDef = visit_FunctionDef


# =============================================================================
# Canonicalization pipeline
# =============================================================================

def _canonicalize(node: ast.AST) -> ast.AST:
    """Apply Tier-1 + Tier-2 canonicalization to a deep copy of node."""
    n = copy.deepcopy(node)
    n = _StripDocstrings().visit(n)
    n = _AlphaRenameLocals().visit(n)
    ast.fix_missing_locations(n)
    return n


# =============================================================================
# Module index — top-level symbols of one source file
# =============================================================================

@dataclass
class _ModuleIndex:
    file: Path
    funcs: dict[str, ast.AST] = field(default_factory=dict)
    consts: dict[str, ast.AST] = field(default_factory=dict)
    imports: dict[str, tuple[str, str, int]] = field(default_factory=dict)
    # imports: local_name -> (module, original_name, level)
    # level: 0 = absolute import; >0 = relative (number of dots)


def _build_index(file: Path) -> _ModuleIndex | None:
    try:
        src = file.read_text(encoding="utf-8")
        tree = ast.parse(src)
    except (OSError, SyntaxError, UnicodeDecodeError, ValueError):
        return None
    idx = _ModuleIndex(file=file)
    for stmt in tree.body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            idx.funcs[stmt.name] = stmt
        elif isinstance(stmt, ast.Assign):
            for t in stmt.targets:
                if isinstance(t, ast.Name):
                    idx.consts[t.id] = stmt
        elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
            idx.consts[stmt.target.id] = stmt
        elif isinstance(stmt, ast.Import):
            for alias in stmt.names:
                local = alias.asname or alias.name.split(".")[0]
                idx.imports[local] = (alias.name, "", 0)
        elif isinstance(stmt, ast.ImportFrom):
            mod = stmt.module or ""
            level = stmt.level or 0
            for alias in stmt.names:
                local = alias.asname or alias.name
                idx.imports[local] = (mod, alias.name, level)
    return idx


# =============================================================================
# Import resolution
# =============================================================================

def _resolve_local_module(mod: str, level: int, from_file: Path,
                          fence_dirs: list[Path]) -> Path | None:
    """Resolve an import target to a source file inside fence_dirs.
    Returns None if the import points outside the fence (external)."""
    if level > 0:
        # Relative import — resolve from the importing file's directory
        base = from_file.parent
        for _ in range(level - 1):
            base = base.parent
        parts = mod.split(".") if mod else []
        target = base
        for p in parts:
            target = target / p
        # Try .py file, then package __init__.py
        for cand in (target.with_suffix(".py"), target / "__init__.py"):
            if cand.is_file():
                return cand
        return None

    if not mod:
        return None

    parts = mod.split(".")
    for fence in fence_dirs:
        target = fence
        for p in parts:
            target = target / p
        for cand in (target.with_suffix(".py"), target / "__init__.py"):
            if cand.is_file():
                return cand
    return None


# =============================================================================
# Reference collection
# =============================================================================

def _free_references(node: ast.AST) -> Iterable[str]:
    """Yield every Name-in-Load and Attribute-root reference inside node.

    We yield the root name of any Attribute chain (e.g. `subsets_utils.get`
    yields `subsets_utils`). Names that turn out to be parameters, locals,
    or builtins are filtered downstream by failing to resolve in the module
    index.
    """
    for n in ast.walk(node):
        if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load):
            yield n.id
        elif isinstance(n, ast.Attribute):
            base = n
            while isinstance(base, ast.Attribute):
                base = base.value
            if isinstance(base, ast.Name) and isinstance(base.ctx, ast.Load):
                yield base.id


# =============================================================================
# Walk + collect parts
# =============================================================================

# Names we never try to resolve — common builtins / keywords-as-Names.
_BUILTIN_SKIP = {
    "True", "False", "None", "print", "len", "range", "list", "dict",
    "set", "tuple", "str", "int", "float", "bool", "bytes", "type",
    "isinstance", "issubclass", "hasattr", "getattr", "setattr", "delattr",
    "iter", "next", "enumerate", "zip", "map", "filter", "sorted",
    "min", "max", "sum", "abs", "any", "all", "open", "Exception",
    "ValueError", "TypeError", "KeyError", "RuntimeError", "OSError",
    "IndexError", "AttributeError", "ImportError", "NotImplementedError",
    "__name__", "__doc__", "__file__", "self", "cls",
}


class _ParseFailed(Exception):
    """Raised internally when an in-fence source file fails to parse. Bubbles
    out to compute_spec_hash which surfaces it as None — the caller treats
    None as 'force re-run.'"""


def _walk_and_collect(
    file: Path,
    qualname: str,
    fence_dirs: list[Path],
    visited: set[tuple[str, str]],
    indices: dict[Path, _ModuleIndex],
    parts: list[str],
) -> None:
    key = (str(file), qualname)
    if key in visited:
        return
    visited.add(key)

    idx = indices.get(file)
    if idx is None:
        idx = _build_index(file)
        if idx is None:
            raise _ParseFailed(f"{file.name}:{qualname}")
        indices[file] = idx

    if qualname == "__module__":
        # Whole-module reference (rare — fired only when an imported package
        # __init__ is itself the target). Hash every top-level symbol.
        for name in sorted(idx.funcs):
            _walk_and_collect(file, name, fence_dirs, visited, indices, parts)
        for name in sorted(idx.consts):
            _walk_and_collect(file, name, fence_dirs, visited, indices, parts)
        return

    node = idx.funcs.get(qualname) or idx.consts.get(qualname)
    if node is None:
        parts.append(f"MISSING:{file.name}:{qualname}")
        return

    parts.append(f"{file.name}:{qualname}:" + ast.dump(_canonicalize(node)))

    for ref in _free_references(node):
        if ref in _BUILTIN_SKIP:
            continue
        # Same-module symbol?
        if ref in idx.funcs or ref in idx.consts:
            _walk_and_collect(file, ref, fence_dirs, visited, indices, parts)
            continue
        # Imported?
        if ref in idx.imports:
            mod, orig, level = idx.imports[ref]
            resolved = _resolve_local_module(mod, level, idx.file, fence_dirs)
            if resolved is not None:
                # Local import — recurse into the imported file
                target_qualname = orig if orig else "__module__"
                _walk_and_collect(resolved, target_qualname, fence_dirs,
                                  visited, indices, parts)
            else:
                # External (subsets_utils, requests, stdlib) — leaf token
                rel = "." * level + mod
                parts.append(f"ext:{rel}:{orig}")
            continue
        # Unresolved: parameter, local, or builtin we don't know about. Skip.


# =============================================================================
# Public API
# =============================================================================

def compute_spec_hash(
    fn_source_file: str | Path | None,
    fn_qualname: str,
    *,
    fence_dirs: list[str | Path] | None = None,
) -> str | None:
    """Hash a NodeSpec's transitively-reachable code.

    fn_source_file: Path to the .py file containing the spec's fn (from
        inspect.getsourcefile, captured at spec-dump time).
    fn_qualname: The fn's __name__ (NodeSpec.fn is required to be top-level
        importable, so dotted qualnames are not expected).
    fence_dirs: Directories inside which to resolve imports recursively
        (typically [connector/src]). Imports outside the fence become leaf
        version tokens.

    Returns: sha256 hex digest, or None on failure (caller treats as
    force re-run).
    """
    try:
        if not fn_source_file:
            return None
        file = Path(fn_source_file)
        if not file.is_file():
            return None
        fence = [Path(d) for d in (fence_dirs or [])]
        visited: set[tuple[str, str]] = set()
        indices: dict[Path, _ModuleIndex] = {}
        parts: list[str] = []
        try:
            _walk_and_collect(file, fn_qualname, fence, visited, indices, parts)
        except _ParseFailed:
            return None
        parts.sort()
        h = hashlib.sha256()
        for p in parts:
            h.update(p.encode("utf-8"))
            h.update(b"\n")
        return h.hexdigest()
    except Exception:
        return None


def compute_sql_spec_hash(sql: str) -> str | None:
    """Hash a SqlNodeSpec's query text.

    The SQL string IS the spec's whole body, so no AST walk — just
    whitespace-normalize (cosmetic reflowing of the query must not force a
    re-run) and sha256. Returns None on a non-string so callers get the same
    force-re-run semantics as compute_spec_hash.
    """
    if not isinstance(sql, str) or not sql.strip():
        return None
    normalized = " ".join(sql.split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
