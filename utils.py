def datasource_quote_date(functional_ordered_column_value_previous):
    unquoted_value = functional_ordered_column_value_previous.replace("'", "")
    return f"'#{unquoted_value}#'"
