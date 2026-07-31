```markdown
# flight-monitor Development Patterns

> Auto-generated skill from repository analysis

## Overview
This skill teaches you the core development patterns and conventions used in the `flight-monitor` Python codebase. You'll learn how to structure code, follow naming and import/export styles, and implement or enhance features—especially in the central `monitor.py` file. The guide also covers testing patterns and provides ready-to-use commands for common workflows.

## Coding Conventions

### File Naming
- Use **camelCase** for file names.
  - Example: `flightMonitor.py`, `dataFetcher.py`

### Import Style
- Use **relative imports** within the project.
  - Example:
    ```python
    from .utils import parseFlightData
    ```

### Export Style
- Use **named exports** (i.e., define specific functions or classes to be imported elsewhere).
  - Example:
    ```python
    def monitorFlights():
        # logic here
        pass

    __all__ = ['monitorFlights']
    ```

## Workflows

### Single-file Feature Enhancement: monitor.py
**Trigger:** When you want to add or improve a core monitoring or bot feature.  
**Command:** `/feature-monitor-py`

1. **Edit `monitor.py`**  
   Implement new logic or endpoints, or enhance existing features within `monitor.py`.
   - Example:
     ```python
     def addNewFlightAlert(flight_id):
         # New logic for flight alerts
         pass
     ```
2. **Test the new or improved feature**  
   Ensure your changes work as intended. If test files exist (e.g., `monitor.test.py`), update or add tests.
3. **Commit changes**  
   Write a detailed commit message describing the enhancement.
   - Example:  
     ```
     Add real-time flight status endpoint to monitor.py
     ```

## Testing Patterns

- **Framework:** Unknown (no specific framework detected).
- **File Pattern:** Test files are named with `*.test.*` (e.g., `monitor.test.py`).
- **Practice:** Place tests alongside the files they test, using the `.test.` infix.
- **Example:**
  ```python
  # monitor.test.py
  from .monitor import monitorFlights

  def test_monitorFlights_handles_empty():
      assert monitorFlights([]) == []
  ```

## Commands

| Command              | Purpose                                               |
|----------------------|-------------------------------------------------------|
| /feature-monitor-py  | Start a monitor.py feature enhancement workflow       |
```
