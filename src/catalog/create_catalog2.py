# encoding: utf-8
# Date: 2022/7/12 10:20

__author__ = 'yudan.chen'

from pyflink.table import *
from pyflink.table.catalog import HiveCatalog, CatalogDatabase, ObjectPath, CatalogBaseTable

settings = EnvironmentSettings.in_batch_mode()
t_env = TableEnvironment.create(settings)

# Create a HiveCatalog
catalog = HiveCatalog("myhive", None, "<path_of_hive_conf>")

# Register the catalog
t_env.register_catalog("myhive", catalog)

# Create a catalog database
database = CatalogDatabase.create_instance({"k1": "v1"}, None)
catalog.create_database("mydb", database)

# Create a catalog table
schema = Schema.new_builder() \
    .column("name", DataTypes.STRING()) \
    .column("age", DataTypes.INT()) \
    .build()

catalog_table = t_env.create_table("myhive.mydb.mytable", TableDescriptor.for_connector("kafka")
                                   .schema(schema)
                                   # …
                                   .build())

# tables should contain "mytable"
tables = catalog.list_tables("mydb")