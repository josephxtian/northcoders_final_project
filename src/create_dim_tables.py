import pg8000.native
from s3_read_function.s3_read_function import read_file_from_s3

# This function will set up the dimensions tables
def set_up_dims_table(database_connection,table_names):
    # delete all existing dim tables
    for dim_table in dimensions_tables_creation:
        database_connection.run(f"DROP TABLE IF EXISTS {dim_table};")
    # create list of tables to be created from 
    dim_tables_to_be_created = []
    for dim_table in dimensions_tables_creation:
      if all(dependent in table_names for dependent in list(dimensions_tables_creation[dim_table][1:])):
        dim_tables_to_be_created.append(dim_table)
    for table in dim_tables_to_be_created:
        database_connection.run(f"CREATE TEMPORARY TABLE {dimensions_tables_creation[table][0]};")
    return dim_tables_to_be_created


def put_info_into_dims_schema(database_connection, dim_tables_created):

    dimension_value_rows = {}
    # use select statement to choose information required for star schema
    for table in dim_tables_created:
        print(f'{table}<<<<table')
        print(f'{dimensions_tables_creation[table]}<<<<dim_table_creations')

        if table == "dim_date":
          #here
            continue
        elif table == "dim_currency":
            continue
        print(f'{dimension_value_rows[table]}<<<<dim_val_rows')
        dimension_value_rows[table] =  database_connection.run(f'''
        INSERT INTO {table} {str(tuple(dim_tables_column_headers[table])).replace("'","")}
        {dimensions_insertion_queries[table]}
        ''')
        
    # return as variable
    return dimension_value_rows