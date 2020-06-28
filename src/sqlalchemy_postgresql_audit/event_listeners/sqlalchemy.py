"""Defines event listeners for:

    - creating table objects
    - generating trigger/procedure DDL for audit tables.

"""
from sqlalchemy import Column, DateTime, String, Table, Integer

from sqlalchemy_postgresql_audit.ddl import (
    get_audit_spec,
    get_create_transaction_trigger_ddl,
    get_create_data_trigger_ddl,
    get_drop_trigger_ddl,
)
from sqlalchemy_postgresql_audit.dialect import (
    DEFAULT_AUDIT_TABLE_FUNCTION_NAMING_CONVENTION,
    DEFAULT_AUDIT_TABLE_NAMING_CONVENTION,
    DEFAULT_AUDIT_TABLE_TRIGGER_CONVENTION,
)


def create_audit_table(target, parent):
    """Create an audit table and generate procedure/trigger DDL.

    Naming conventions can be defined for a few of the named elements:

        - audit.table: Controls the name of the table
        - audit.function: Controls the name of the function
        - audit.trigger: Controls the name of the trigger on the table

    This function creates a new companion table to store row versions.

    Any :class:`sqlalchemy.sql.schema.Column`s specified in `table.info['session_settings']` will
    be copied and included in the audit table.

    This function will leave a key in the audited table:

        .. code-block:: python

            table.info['audit.is_audited']

    And a key in the audit table:

        .. code-block:: python

            table.info['audit.is_audit_table']

    Additionally you can find the relevant create/drop ddl at the followng keys:

        .. code-block:: python

            table.info['audit.create_ddl']
            table.info['audit.drop_ddl']

    :param target: The :class:`sqlalchemy.sql.schema.Table` to make an audit table for
    :param parent: The :class:`sqlalchemy.sql.schema.MetaData` to associate the audit table with.
    :return: None
    """
    audit_spec = get_audit_spec(target)

    if not audit_spec.get("enabled"):
        return

    audit_table_naming_convention = parent.naming_convention.get(
        "audit.table", DEFAULT_AUDIT_TABLE_NAMING_CONVENTION
    )
    audit_function_naming_convention = parent.naming_convention.get(
        "audit.function", DEFAULT_AUDIT_TABLE_FUNCTION_NAMING_CONVENTION
    )
    audit_trigger_naming_convention = parent.naming_convention.get(
        "audit.trigger", DEFAULT_AUDIT_TABLE_TRIGGER_CONVENTION
    )

    audit_table_name = audit_table_naming_convention % {
        "table_name": target.name,
        "schema": audit_spec["schema"] or "public",
    }
    audit_function_name = audit_function_naming_convention % {
        "table_name": target.name,
        "schema": audit_spec["schema"] or "public",
    }
    audit_trigger_name = audit_trigger_naming_convention % {
        "table_name": target.name,
        "schema": audit_spec["schema"] or "public",
    }

    columns = [
        Column(col.name, col.type, nullable=True) for col in target.columns.values()
    ]
    session_setting_columns = [col.copy() for col in audit_spec["session_settings"]]
    for col in session_setting_columns:
        col.name = "audit_{}".format(col.name)

    column_elements = session_setting_columns + columns

    audit_transaction_table = Table(
        "audit_transaction_table",
        target.metadata,
        Column('id', Integer, primary_key=True),
        Column("audit_operation", String(1), nullable=False),
        Column("audit_operation_timestamp", DateTime, nullable=False),
        Column("audit_current_user", String(64), nullable=False),
        *session_setting_columns,
        Column('table_name', String(64), nullable=False),
        schema=audit_spec["schema"],
        extend_existing=True
    )

    audit_data_table = Table(
        "audit_data_table",
        target.metadata,
        Column('id', Integer, primary_key=True),
        Column('data', String, nullable=False),
        extend_existing=True
    )

    target.info["audit.create_ddl"] = get_create_transaction_trigger_ddl(
        target.columns,
        audit_transaction_table.columns,
        "audit_transaction_function",
        "audit_transaction_trigger",
        target.fullname,
        audit_transaction_table.fullname,
        session_setting_columns,
    ), get_create_data_trigger_ddl(
        target.columns,
        audit_data_table.columns,
        "audit_data_function",
        "audit_data_trigger",
        target.fullname,
        audit_data_table.fullname,
    )

    target.info["audit.drop_ddl"] = get_drop_trigger_ddl(
        "audit_transaction_function", "audit_transaction_trigger", target.fullname
    ),get_drop_trigger_ddl(
        "audit_data_function", "audit_data_trigger", target.fullname
    )

    audit_transaction_table.info["audit.target_table"] = target

    audit_transaction_table.info["audit.is_audit_table"] = True

    audit_data_table.info["audit.target_table"] = target

    audit_data_table.info["audit.is_audit_table"] = True

    target.info["audit.is_audited"] = True
