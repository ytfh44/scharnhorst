# Agent Guidelines

## Path Handling
- **ALWAYS use OS-neutral relative paths** (e.g., `src/main.rs`, `./module/file.ts`).
- **NEVER use absolute paths** (e.g., `C:\Users\...`, `/home/...`) in code, configs, or documentation.
- Use `path.join()` or equivalent for dynamic path construction to ensure cross-platform compatibility.

## Code Quality
- Follow existing code style and conventions in the repository.
- Write small, testable functions with clear responsibilities.
- Prefer explicit error handling over panics or undefined behavior.

### Error Handling - CRITICAL
- **STRICTLY FORBIDDEN**: `unwrap()`, `expect()`, `panic!()`, or any code that can cause runtime crashes
- **ALWAYS** propagate errors using `Result`/`Option` return types
- **ALWAYS** use `?` operator for error propagation in functions returning `Result`
- **ALWAYS** use `.unwrap_or()`, `.unwrap_or_else()`, `.unwrap_or_default()`, or pattern matching for safe defaults
- For truly unrecoverable states, return an error to the caller - let the application decide policy
- **NEVER** introduce new panic points in library code

### "Small" Definition - Core Principle
"Small" means **loose coupling between branches** - code structure should enable independent reasoning and modification:
- **Use iterators instead of nested loops/matches**: Chain operations with `.iter()`, `.map()`, `.filter()`, `.flat_map()` to avoid deep nesting
- **Assemble iterators for dynamic traversal**: Highly dynamic data should be traversed via composable iterator chains, not recursive nested loops
- **Prefer flat over nested**: `a?.b()?.c()` over `if a { if b { c } }`
- **Decouple branches**: Each code path should be modifiable without touching siblings
- **Single responsibility per scope**: Each function/block handles one concern; split early

## Commits
- Do NOT commit changes unless explicitly requested by the user.
- When committing, write clear, concise messages that explain the "why" not just the "what".
