# Maps format names (used in your schema YAML) to DuckDB-compatible regex patterns
 
FORMAT_PATTERNS = {
    "email": r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$",
    "phone": r"^\+?[0-9\s\-\(\)]{7,20}$",
    # "date":  r"^\d{4}-\d{2}-\d{2}$",                         # YYYY-MM-DD
    # "datetime": r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?$",  # YYYY-MM-DD HH:MM[:SS]
}

#  if the column is already a valid DATE type, it passed type validation,
#  so its format is already guaranteed correct by DuckDB itself. 
#  
#  You don't need regex on date/timestamp columns at all.