from tableauhyperapi import Date

def datasource_quote_date(functional_ordered_column_value_previous: Date):
    if functional_ordered_column_value_previous is None or functional_ordered_column_value_previous == "":
        return None
    unquoted_value = functional_ordered_column_value_previous.to_date().isoformat()
    return f"#{unquoted_value}#"
